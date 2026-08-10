import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

import grpc
from h2pcontrol.mccdaq.v1.mccdaq_pb2 import (
    AnalogReadRequest,
    AnalogReadResponse,
    AnalogSample,
    AnalogStreamRequest,
    AnalogStreamResponse,
    AnalogWriteRequest,
    AnalogWriteResponse,
)
from h2pcontrol.mccdaq.v1.mccdaq_pb2_grpc import MccDaqServiceServicer
from h2pcontrol.sdk.server import Server
from mcculw import ul
from mcculw.device_info import DaqDeviceInfo
from mcculw.enums import AnalogInputMode, BoardInfo, InfoType, ULRange
from mcculw.ul import ULError

logger = logging.getLogger(__name__)

board_num = 0


preferred_ai_range = ULRange.BIP10VOLTS
preferred_ao_range = ULRange.BIP10VOLTS


@dataclass(frozen=True)
class _BoardCaps:
    """What the board actually supports, read once at startup."""

    product_name: str
    ai_num_chans: int
    ai_resolution: int
    ai_range: ULRange
    ao_num_chans: int
    ao_resolution: int
    ao_range: ULRange


def _pick_range(preferred: ULRange, supported: list[ULRange], kind: str) -> ULRange:
    if not supported:
        raise RuntimeError(f"board {board_num} reports no supported {kind} ranges")
    if preferred in supported:
        return preferred
    logger.warning(
        "%s range %s not supported by board, falling back to %s (supported: %s)",
        kind,
        preferred.name,
        supported[0].name,
        ", ".join(r.name for r in supported),
    )
    return supported[0]


def _probe_board() -> _BoardCaps:
    """
    Query the board's channel counts, resolutions and supported ranges.

    Note that probing the AO ranges writes 0 counts to AO channel 0, so this must only
    be called at startup, before anything depends on the output state.
    """
    info = DaqDeviceInfo(board_num)
    ai_info = info.get_ai_info()
    ao_info = info.get_ao_info()
    if not ai_info.is_supported:
        raise RuntimeError(f"board {board_num} ({info.product_name}) has no analog input")
    if not ao_info.is_supported:
        raise RuntimeError(f"board {board_num} ({info.product_name}) has no analog output")

    caps = _BoardCaps(
        product_name=info.product_name,
        ai_num_chans=ai_info.num_chans,
        ai_resolution=ai_info.resolution,
        ai_range=_pick_range(preferred_ai_range, ai_info.supported_ranges, "AI"),
        ao_num_chans=ao_info.num_chans,
        ao_resolution=ao_info.resolution,
        ao_range=_pick_range(preferred_ao_range, ao_info.supported_ranges, "AO"),
    )
    logger.info(
        "%s on board %d: %d AI channels (%d-bit, %s, %s), %d AO channels (%d-bit, %s)",
        caps.product_name,
        board_num,
        caps.ai_num_chans,
        caps.ai_resolution,
        caps.ai_range.name,
        _ai_input_mode(),
        caps.ao_num_chans,
        caps.ao_resolution,
        caps.ao_range.name,
    )
    return caps


def _ai_input_mode() -> str:
    """
    The board's analog input mode, for the startup log.
    """
    try:
        mode = ul.get_config(InfoType.BOARDINFO, board_num, 0, BoardInfo.ADAIMODE)
        return AnalogInputMode(mode).name
    except (ULError, ValueError):
        return "input mode unknown"


def _read_channel(caps: _BoardCaps, channel: int) -> AnalogSample:
    # to_eng_units is only defined up to 16 bits; higher-resolution boards need the
    # 32-bit variants or the counts get scaled against the wrong full-scale value.
    if caps.ai_resolution > 16:
        raw = ul.a_in_32(board_num, channel, caps.ai_range)
        volts = ul.to_eng_units_32(board_num, caps.ai_range, raw)
    else:
        raw = ul.a_in(board_num, channel, caps.ai_range)
        volts = ul.to_eng_units(board_num, caps.ai_range, raw)
    # Wall-clock, so samples are comparable with timestamps from other services.
    timestamp_us = time.time_ns() // 1000
    logger.debug("analog read", extra={"channel": channel, "volts": volts, "raw": raw})
    return AnalogSample(channel=channel, volts=volts, raw=raw, timestamp_us=timestamp_us)


class MccDaqService(Server, MccDaqServiceServicer):
    def __init__(self, config):
        super().__init__(config)
        self._caps: _BoardCaps | None = None
        # The UL is a blocking, non-reentrant C library, so every call runs in a worker
        # thread behind this lock rather than on the event loop.
        self._ul_lock = asyncio.Lock()
        try:
            self._caps = _probe_board()
        except (ULError, RuntimeError) as e:
            logger.error("DAQ board %d unavailable at startup: %s", board_num, e)

    def _healthy(self) -> bool:
        return self._caps is not None

    async def _ul_call[T](self, fn: Callable[..., T], *args: object) -> T:
        async with self._ul_lock:
            return await asyncio.to_thread(fn, *args)

    async def _board(self, context: grpc.aio.ServicerContext) -> _BoardCaps:
        """Return the board capabilities, re-probing if startup detection failed."""
        if self._caps is not None:
            return self._caps
        try:
            self._caps = await self._ul_call(_probe_board)
        except (ULError, RuntimeError) as e:
            await context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f"DAQ board {board_num} unavailable: {e}",
            )
        return self._caps

    async def AnalogRead(
        self, request: AnalogReadRequest, context: grpc.aio.ServicerContext
    ) -> AnalogReadResponse:
        logger.info("AnalogRead: channel=%d", request.channel)
        caps = await self._board(context)
        if not 0 <= request.channel < caps.ai_num_chans:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"AI channel {request.channel} out of range "
                f"(board has {caps.ai_num_chans} channels)",
            )
        try:
            sample = await self._ul_call(_read_channel, caps, request.channel)
        except ULError as e:
            await context.abort(grpc.StatusCode.INTERNAL, f"analog read failed: {e.message}")
        return AnalogReadResponse(sample=sample)

    async def AnalogStream(self, request: AnalogStreamRequest, context: grpc.aio.ServicerContext):
        channels = list(request.channels)
        rate_hz = request.rate_hz if request.rate_hz > 0 else 100.0
        interval_s = 1.0 / rate_hz
        logger.info("analog stream started", extra={"channels": channels, "rate_hz": rate_hz})

        caps = await self._board(context)
        for channel in channels:
            if not 0 <= channel < caps.ai_num_chans:
                await context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    f"AI channel {channel} out of range (board has {caps.ai_num_chans} channels)",
                )

        while not context.cancelled():
            for channel in channels:
                try:
                    sample = await self._ul_call(_read_channel, caps, channel)
                except ULError as e:
                    await context.abort(
                        grpc.StatusCode.INTERNAL, f"analog read failed: {e.message}"
                    )
                yield AnalogStreamResponse(sample=sample)
            await asyncio.sleep(interval_s)

    async def AnalogWrite(
        self, request: AnalogWriteRequest, context: grpc.aio.ServicerContext
    ) -> AnalogWriteResponse:
        logger.info("AnalogWrite: channel=%d volts=%f", request.channel, request.volts)
        caps = await self._board(context)
        if not 0 <= request.channel < caps.ao_num_chans:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"AO channel {request.channel} out of range "
                f"(board has {caps.ao_num_chans} channels)",
            )
        if not caps.ao_range.range_min <= request.volts <= caps.ao_range.range_max:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"{request.volts}V is outside the AO range {caps.ao_range.name} "
                f"({caps.ao_range.range_min}V to {caps.ao_range.range_max}V)",
            )
        try:
            await self._ul_call(ul.v_out, board_num, request.channel, caps.ao_range, request.volts)
        except ULError as e:
            await context.abort(grpc.StatusCode.INTERNAL, f"analog write failed: {e.message}")
        return AnalogWriteResponse()
