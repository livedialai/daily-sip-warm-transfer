#!/usr/bin/env python3
"""Bring-your-own-carrier SIP -> Daily room -> Pipecat voice agent, with warm transfer.

Call flow
---------
    caller -> carrier DID -> OpenSIPS :5080 -> Daily room (SIP dial-in URI)
                                              |
                                              +-> this agent (STT -> LLM -> TTS)

    "transfer me to a human" -> hold prompt -> Daily dialOut (REST) -> OpenSIPS
                             -> carrier -> human phone joins the same room

Everything is driven by environment variables. There is no scenario, no phone
number and no room property baked into this file.
"""
import asyncio
import os
import sys

from loguru import logger

from pipecat.adapters.schemas.function_schema import FunctionSchema
from pipecat.adapters.schemas.tools_schema import ToolsSchema
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import (
    BotStoppedSpeakingFrame,
    Frame,
    InputAudioRawFrame,
    OutputAudioRawFrame,
    TTSSpeakFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineTask
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
)
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.services.deepgram.stt import DeepgramSTTService
from pipecat.services.inworld.tts import InworldTTSService
from pipecat.services.llm_service import FunctionCallParams
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.transports.daily.transport import DailyParams, DailyTransport

ROOM_URL = os.environ["DAILY_ROOM_URL"]
ROOM_NAME = ROOM_URL.rstrip("/").split("/")[-1]

# Owner meeting token. Daily only lets an owner/admin participant start a
# dial-out; a plain room join fails with "Dialout(Properties(UserMustBeAdmin))".
DAILY_TOKEN = os.environ.get("DAILY_TOKEN") or None
DAILY_API_KEY = os.environ["DAILY_API_KEY"]

# SIP URI of the human the agent transfers to, e.g. sip:+15551234567@carrier.example
TRANSFER_TARGET = os.environ["TRANSFER_TARGET"]

DG_KEY = os.environ["DEEPGRAM_API_KEY"]
INW_KEY = os.environ["INWORLD_API_KEY"]
INW_VOICE = os.environ["INWORLD_VOICE"]
LLM_BASE = os.environ.get("LLM_BASE", "https://api.openai.com/v1")
LLM_KEY = os.environ["LLM_API_KEY"]
LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-4o-mini")
STT_LANGUAGE = os.environ.get("STT_LANGUAGE", "en")

OPENER = os.environ.get("OPENER_TEXT", "Hello, how can I help you?")
HOLD = os.environ.get("HOLD_TEXT", "One moment please, I am connecting you.")
FAIL = os.environ.get(
    "FAIL_TEXT", "I could not reach anyone right now. Is there anything else I can do?"
)

TRANSFER_TIMEOUT_S = int(os.environ.get("TRANSFER_TIMEOUT_S", "45"))

PROMPT = os.environ.get(
    "SYSTEM_PROMPT",
    "You are the phone assistant of a small business. Answer in at most two short "
    "sentences, no filler phrases, no lists, never repeat the question. Whenever the "
    "caller asks for a human, the owner or a colleague, call the tool "
    "transfer_to_human immediately. Otherwise answer briefly; if you cannot answer, "
    "offer to connect the caller.",
)

STATE = {"armed": False, "dialing": False, "human": False, "greeted": False}


async def daily_dialout(sip_uri: str):
    """Start the second leg through the Daily REST API.

    The REST endpoint honours the `codecs` field; the Python/JS client SDKs
    silently drop it, so the offer keeps Daily's default codec set.
    """
    import aiohttp

    url = f"https://api.daily.co/v1/rooms/{ROOM_NAME}/dialOut/start"
    payload = {
        "sipUri": sip_uri,
        "provider": "daily",
        "codecs": {"audio": ["PCMA", "PCMU"]},
    }
    async with aiohttp.ClientSession() as sess:
        async with sess.post(
            url, json=payload, headers={"Authorization": f"Bearer {DAILY_API_KEY}"}
        ) as r:
            try:
                data = await r.json(content_type=None)
            except Exception:
                data = {"raw": await r.text()}
            return r.status, data


class Gate(FrameProcessor):
    """Mutes the agent in both directions once the bridge to the human is up."""

    def __init__(self, on_bot_stopped=None, **kwargs):
        super().__init__(**kwargs)
        self.open = True
        self.on_bot_stopped = on_bot_stopped

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, BotStoppedSpeakingFrame) and self.on_bot_stopped:
            await self.on_bot_stopped()
        if not self.open and isinstance(frame, (OutputAudioRawFrame, InputAudioRawFrame)):
            return
        await self.push_frame(frame, direction)


async def main():
    transport = DailyTransport(
        ROOM_URL,
        DAILY_TOKEN,
        os.environ.get("BOT_NAME", "Phone assistant"),
        DailyParams(audio_in_enabled=True, audio_out_enabled=True, transcription_enabled=False),
    )

    stt = DeepgramSTTService(
        api_key=DG_KEY,
        settings=DeepgramSTTService.Settings(model=os.environ.get("STT_MODEL", "nova-3"), language=STT_LANGUAGE),
    )
    tts = InworldTTSService(
        api_key=INW_KEY, voice_id=INW_VOICE, model=os.environ.get("TTS_MODEL", "inworld-tts-2")
    )
    llm = OpenAILLMService(base_url=LLM_BASE, api_key=LLM_KEY, model=LLM_MODEL)

    async def transfer(params: FunctionCallParams):
        logger.info("transfer requested: {}", params.arguments)
        STATE["armed"] = True
        await params.result_callback(HOLD)
        await task.queue_frames([TTSSpeakFrame(HOLD)])

    tools = ToolsSchema(
        standard_tools=[
            FunctionSchema(
                name="transfer_to_human",
                description=(
                    "Connects the caller to a human. Call it as soon as the caller asks "
                    "for a person, the owner or a colleague."
                ),
                properties={},
                required=[],
                handler=transfer,
            )
        ]
    )
    context = LLMContext(messages=[{"role": "system", "content": PROMPT}], tools=tools)
    pair = LLMContextAggregatorPair(
        context, user_params=LLMUserAggregatorParams(vad_analyzer=SileroVADAnalyzer())
    )

    async def after_hold_spoken():
        if not STATE["armed"] or STATE["dialing"]:
            return
        STATE["dialing"] = True
        logger.info("hold prompt done -> dialOut {}", TRANSFER_TARGET)
        status, data = await daily_dialout(TRANSFER_TARGET)
        if status >= 400 or (isinstance(data, dict) and data.get("error")):
            logger.error("dialout rejected: {} {}", status, data)
            STATE["armed"] = STATE["dialing"] = False
            await task.queue_frames([TTSSpeakFrame(FAIL)])
            return
        logger.info("dialout started: {}", (data or {}).get("sessionId", "?"))
        in_gate.open = out_gate.open = False

        async def watchdog():
            await asyncio.sleep(TRANSFER_TIMEOUT_S)
            if STATE["dialing"]:
                logger.warning("nobody answered -> back to the caller")
                STATE["armed"] = STATE["dialing"] = False
                in_gate.open = out_gate.open = True
                await task.queue_frames([TTSSpeakFrame(FAIL)])

        asyncio.create_task(watchdog())

    in_gate = Gate()
    out_gate = Gate(on_bot_stopped=after_hold_spoken)

    pipeline = Pipeline(
        [
            transport.input(),
            in_gate,
            stt,
            pair.user(),
            llm,
            tts,
            out_gate,
            transport.output(),
            pair.assistant(),
        ]
    )
    task = PipelineTask(pipeline)

    @transport.event_handler("on_client_connected")
    async def on_client_connected(transport, client):
        logger.info("participant joined: {}", getattr(client, "id", "?"))
        if STATE["dialing"]:
            STATE["human"] = True
            STATE["armed"] = STATE["dialing"] = False
            in_gate.open = out_gate.open = False
            logger.info("human on the line, bridge is up")
            return
        if not STATE["greeted"]:
            STATE["greeted"] = True
            await task.queue_frames([TTSSpeakFrame(OPENER)])

    @transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(transport, client):
        logger.info("participant left: {}", getattr(client, "id", "?"))
        if not STATE["armed"]:
            await task.cancel()

    @transport.event_handler("on_dialout_connected")
    async def on_dialout_connected(transport, data):
        logger.info("dialing ... {}", data)

    @transport.event_handler("on_dialout_answered")
    async def on_dialout_answered(transport, data):
        logger.info("human answered: {}", data)
        in_gate.open = out_gate.open = False

    @transport.event_handler("on_dialout_error")
    async def on_dialout_error(transport, data):
        logger.error("dialout error: {}", data)
        STATE["armed"] = STATE["dialing"] = False
        in_gate.open = out_gate.open = True
        await task.queue_frames([TTSSpeakFrame(FAIL)])

    @transport.event_handler("on_dialout_stopped")
    async def on_dialout_stopped(transport, data):
        logger.info("dialout stopped: {}", data)
        STATE["armed"] = STATE["dialing"] = False
        in_gate.open = out_gate.open = True

    await PipelineRunner().run(task)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
