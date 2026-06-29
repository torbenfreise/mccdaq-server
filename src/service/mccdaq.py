import asyncio
import logging
import time

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
from h2pcontrol.sdk.server import GoNogoMixin, Server
from mcculw import ul
from mcculw.enums import ULRange
from mcculw.ul import ULError

logger = logging.getLogger(__name__)

board_num = 0
ai_range = ULRange.BIP2VOLTS
ao_range = ULRange.UNI10VOLTS


def _read_channel(channel: int) -> AnalogSample:
    raw = ul.a_in(board_num, channel, ai_range)
    volts = ul.to_eng_units(board_num, ai_range, raw)
    timestamp_us = int(time.monotonic() * 1_000_000)
    logger.debug("analog read", extra={"channel": channel, "volts": volts, "raw": raw})
    return AnalogSample(channel=channel, volts=volts, raw=raw, timestamp_us=timestamp_us)


class MccDaqService(Server, GoNogoMixin, MccDaqServiceServicer):
    def __init__(self, config):
        super().__init__(config)
        self._requested: dict[int, float] = {}

    def _healthy(self) -> bool:
        return True

    async def _go_nogo(self) -> tuple[bool, str]:
        if not self._requested:
            return True, ""
        for channel, target_volts in self._requested.items():
            try:
                raw = ul.a_in(board_num, channel, ao_range)
                actual_volts = ul.to_eng_units(board_num, ao_range, raw)
            except ULError as e:
                return False, f"readback failed on channel {channel}: {e.message}"
            if abs(actual_volts - target_volts) > 0.05:
                return False, (
                    f"channel {channel} output mismatch: "
                    f"requested {target_volts:.3f}V, actual {actual_volts:.3f}V"
                )
        return True, ""

    async def AnalogRead(
            self, request: AnalogReadRequest, context: grpc.aio.ServicerContext
    ) -> AnalogReadResponse:
        logger.info("AnalogRead: channel=%d", request.channel)
        return AnalogReadResponse(sample=_read_channel(request.channel))

    async def AnalogStream(self, request: AnalogStreamRequest, context: grpc.aio.ServicerContext):
        channels = list(request.channels)
        rate_hz = request.rate_hz if request.rate_hz > 0 else 100.0
        interval_s = 1.0 / rate_hz
        logger.info("analog stream started", extra={"channels": channels, "rate_hz": rate_hz})

        while not context.cancelled():
            for channel in channels:
                yield AnalogStreamResponse(sample=_read_channel(channel))
            await asyncio.sleep(interval_s)

    async def AnalogWrite(
            self, request: AnalogWriteRequest, context: grpc.aio.ServicerContext
    ) -> AnalogWriteResponse:
        logger.info("AnalogWrite: channel=%d volts=%f", request.channel, request.volts)
        ul.v_out(board_num, request.channel, ao_range, request.volts)
        return AnalogWriteResponse()
