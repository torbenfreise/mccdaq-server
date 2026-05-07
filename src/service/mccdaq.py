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
from h2pcontrol.sdk import H2PServer
from mcculw import ul
from mcculw.enums import ULRange

logger = logging.getLogger(__name__)

board_num = 0
ai_range = ULRange.BIP2VOLTS
ao_range = ULRange.UNI5VOLTS


def _read_channel(channel: int) -> AnalogSample:
    raw = ul.a_in(board_num, channel, ai_range)
    volts = ul.to_eng_units(board_num, ai_range, raw)
    timestamp_us = int(time.monotonic() * 1_000_000)
    logger.debug("AI%d: %f V (raw=%d)", channel, volts, raw)
    return AnalogSample(channel=channel, volts=volts, raw=raw, timestamp_us=timestamp_us)


class MccDaqService(H2PServer, MccDaqServiceServicer):
    def _healthy(self) -> bool:
        return True

    async def AnalogRead(
        self, request: AnalogReadRequest, context: grpc.aio.ServicerContext
    ) -> AnalogReadResponse:
        logger.info("AnalogRead: channel=%d", request.channel)
        return AnalogReadResponse(sample=_read_channel(request.channel))

    async def AnalogStream(self, request: AnalogStreamRequest, context: grpc.aio.ServicerContext):
        channels = list(request.channels)
        rate_hz = request.rate_hz if request.rate_hz > 0 else 100.0
        interval_s = 1.0 / rate_hz
        logger.info("AnalogStream: channels=%s rate_hz=%f", channels, rate_hz)

        while not context.cancelled():
            for channel in channels:
                yield AnalogStreamResponse(sample=_read_channel(channel))
            await asyncio.sleep(interval_s)

    async def AnalogWrite(
        self, request: AnalogWriteRequest, context: grpc.aio.ServicerContext
    ) -> AnalogWriteResponse:
        logger.info("AnalogWrite: channel=%d volts=%f", request.channel, request.volts)
        raw_out = ul.from_eng_units(board_num, ao_range, request.volts)
        ul.a_out(board_num, request.channel, ao_range, raw_out)
        return AnalogWriteResponse()
