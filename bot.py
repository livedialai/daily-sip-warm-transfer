#!/usr/bin/env python3
"""Pipecat-Agent am vsip-Trunk: Begruessung + Warmtransfer mit Briefing und Rueckfrage.

DID -> vsip -> OpenSIPS:5080 -> Daily-Room (SIP-URI)
Bot im Raum: Deepgram (STT) / DeepSeek via melious (LLM) / Inworld (TTS)

Ablauf Transfer (nach Pipecat-Referenz daily-pstn-warm-transfer):
  1. Anrufer will zum Chef -> Tool mit_chef_verbinden(briefing)
  2. Ansage an den Anrufer, danach Hold-Musik (SoundfileMixer) an
  3. dialOut per REST (Codec/AVPF-Fix braucht den REST-Weg)
  4. Chef nimmt ab -> Musik aus, Bot liest dem Chef das Briefing vor und fragt:
     "Soll ich den Anrufer jetzt verbinden?"
  5. Chef sagt ja  -> Ansage "Ich verbinde", Bot verstummt, beide hoeren sich
     Chef sagt nein -> Chef-Bein wird getrennt (eject), zurueck zum Anrufer
  6. Keine Abnahme nach 45 s oder keine Entscheidung -> zurueck zum Anrufer
"""
import asyncio
import os
import re
import sys
import time
import wave

import daily
from loguru import logger

from pipecat.adapters.schemas.function_schema import FunctionSchema
from pipecat.adapters.schemas.tools_schema import ToolsSchema
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import (
    BotStoppedSpeakingFrame,
    Frame,
    InputAudioRawFrame,
    MixerEnableFrame,
    OutputAudioRawFrame,
    TranscriptionFrame,
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
from pipecat.audio.mixers.soundfile_mixer import SoundfileMixer

ROOM_URL = os.environ["DAILY_ROOM_URL"]
CHEF_SIP = os.environ["CHEF_SIP"]
DAILY_TOKEN = os.environ.get("DAILY_TOKEN") or None
TEST_SIP = os.environ.get("TEST_SIP")
DAILY_API_KEY = os.environ["DAILY_API_KEY"]
ROOM_NAME = ROOM_URL.rstrip("/").split("/")[-1]
DG_KEY = os.environ["DEEPGRAM_API_KEY"]
INW_KEY = os.environ["INWORLD_API_KEY"]
INW_VOICE = os.environ["INWORLD_VOICE"]
LLM_BASE = os.environ.get("LLM_BASE", "https://api.melious.ai/v1")
LLM_KEY = os.environ["LLM_API_KEY"]
LLM_MODEL = os.environ.get("LLM_MODEL", "deepseek-v4.1-flash")
HOLD_WAV = os.environ.get("HOLD_MUSIC_WAV", "/opt/pipecat-bot/hold_music.wav")
HOLD_WAV16 = os.environ.get("HOLD_MUSIC_WAV16", "/opt/pipecat-bot/hold16.wav")

OPENER = "Wie kann ich Ihnen helfen?"
HOLD = "Einen Moment bitte, ich verbinde Sie mit dem Chef."
FAIL = "Ich konnte den Chef gerade nicht erreichen. Kann ich sonst etwas für Sie tun?"
BUSY = "Der Chef hat gerade keine Zeit. Kann ich Ihnen sonst weiterhelfen?"
BRIEF_INTRO = "Chef, hier ist die Telefonassistenz."
BRIEF_QUESTION = "Soll ich den Anrufer jetzt verbinden?"
NO_ANSWER_REASK = "Soll ich den Anrufer verbinden? Bitte ja oder nein."
CONNECTING = "In Ordnung, ich verbinde Sie jetzt."
BACK_TO_CALLER = "Alles klar, ich kümmere mich weiter."

PROMPT = (
    "Du bist die Telefonassistenz eines kleinen Betriebs. Du sprichst Deutsch, ruhig und "
    "freundlich, hoechstens zwei kurze Saetze pro Antwort. Keine Floskeln, keine Listen, "
    "keine Wiederholung der Frage. Wenn der Anrufer den Chef, den Inhaber oder einen "
    "Mitarbeiter sprechen will, rufst du sofort das Tool mit_chef_verbinden auf und gibst "
    "dort ein Briefing mit: ein bis zwei Saetze, was der Anrufer will, mit den Fakten, die "
    "er genannt hat (Name, Grund, Uhrzeit, Anzahl). Erfinde nichts dazu. "
    "Sonst beantwortest du die Frage knapp; kannst du sie nicht beantworten, bietest du an, "
    "mit dem Chef zu verbinden."
)

YES_RE = re.compile(
    r"\b(ja|jawohl|jep|klar|gerne|genau|richtig|natuerlich|naturlich|sicher|ok|okay|"
    r"durchstellen|verbinden|stellen sie durch|mach(en)? sie|passt)\b",
    re.IGNORECASE,
)
NO_RE = re.compile(
    r"\b(nein|nee|ne|nicht|kein(e|en)?|spaeter|spater|negativ|abweisen|"
    r"keine zeit|auf keinen fall|passt nicht|gerade schlecht)\b",
    re.IGNORECASE,
)

STATE = {
    "armed": False,      # Transfer angefordert, Ansage laeuft
    "dialing": False,    # dialOut laeuft, Chef noch nicht dran
    "chef": False,       # Chef ist im Raum
    "greeted": False,    # Opener gesprochen
    "awaiting": False,   # warten auf Ja/Nein des Chefs
    "closing": False,    # Verbindungs-Ansage laeuft, danach Bot stumm
    "connected": False,  # Bruecke steht, Bot haelt sich raus
    "caller_id": None,
    "caller_perms": {},
    "caller_send": [],
    "chef_perms": {},
    "chef_id": None,
    "briefing": "",
    "asks": 0,
}


def pid_of(client) -> str | None:
    """Teilnehmer-Event liefert ein dict (daily) oder ein Objekt (andere Transports)."""
    if isinstance(client, dict):
        return client.get("id") or client.get("sessionId") or client.get("session_id")
    return getattr(client, "id", None) or getattr(client, "session_id", None)


def uname_of(client) -> str:
    if isinstance(client, dict):
        return str(client.get("user_name") or client.get("userName") or "")
    return str(getattr(client, "user_name", "") or "")



async def daily_dialout(sip_uri: str):
    """Dial-out ueber die Daily-REST-API (nur so lassen sich die Codecs erzwingen)."""
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
    """Blockt Bot-Audio (out) bzw. Anrufer-Audio (in), wenn geschlossen."""

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


class ChefWatch(FrameProcessor):
    """Liest die Antwort des Chefs deterministisch aus und haelt sie vom LLM fern."""

    def __init__(self, on_answer, **kwargs):
        super().__init__(**kwargs)
        self.on_answer = on_answer

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if STATE["awaiting"] and isinstance(frame, TranscriptionFrame):
            text = (frame.text or "").strip()
            if text:
                logger.info("Chef sagt: {}", text)
                if NO_RE.search(text):
                    await self.on_answer(False, text)
                elif YES_RE.search(text):
                    await self.on_answer(True, text)
            return  # nie ans LLM weiterreichen
        await self.push_frame(frame, direction)


class MohTrack:
    """Eigener Daily-Audio-Track 'moh'.

    Der Anrufer darf per Permissions nur diesen Track empfangen, der Chef nur
    den normalen Mikrofon-Track (Briefing). Damit laufen Wartemusik beim Anrufer
    und Briefing beim Chef gleichzeitig, ohne sich zu vermischen.
    """

    def __init__(self, path: str, sample_rate: int = 16000, chunk_ms: int = 20):
        self.path = path
        self.sample_rate = sample_rate
        self.chunk = int(sample_rate * 2 * chunk_ms / 1000)
        self.on = False
        self._src = None
        self._task = None
        self._pcm = b""

    def _load(self):
        with wave.open(self.path, "rb") as w:
            if w.getframerate() != self.sample_rate or w.getnchannels() != 1:
                logger.warning("MOH: {} Hz/{} ch != {} Hz/1 ch", w.getframerate(), w.getnchannels(), self.sample_rate)
            self._pcm = w.readframes(w.getnframes())
            import array as _arr
            a = _arr.array("h"); a.frombytes(self._pcm)
            logger.info("MOH-Datei: {} s, peak={}", round(len(a) / self.sample_rate, 2), max((abs(x) for x in a), default=0))

    async def attach(self, client):
        if self._src is not None:
            return
        if not self._pcm:
            self._load()
        from pipecat.transports.daily.transport import DailyCustomAudioTrackParams

        track = await client.add_custom_audio_track(
            "moh", DailyCustomAudioTrackParams(sample_rate=self.sample_rate, channels=1)
        )
        self._src = track.source
        self._task = asyncio.create_task(self._loop())
        logger.info("MOH-Track 'moh' angelegt ({} Hz, {} Bytes)", self.sample_rate, len(self._pcm))

    async def _loop(self):
        data = self._pcm
        if not data:
            return
        pos = 0
        silence = b"\x00" * self.chunk
        step = self.chunk / (self.sample_rate * 2)
        while True:
            if self.on:
                buf = data[pos:pos + self.chunk]
                pos += self.chunk
                if len(buf) < self.chunk:
                    buf = (buf + data)[: self.chunk]
                    pos = self.chunk - len(buf) if False else pos
                if pos >= len(data):
                    pos = 0
            else:
                buf = silence
            try:
                self._src.write_frames(buf)
            except Exception as e:  # noqa: BLE001
                logger.error("MOH write_frames: {}", e)
                return
            await asyncio.sleep(step)


async def main():
    moh = MohTrack(HOLD_WAV16)

    def moh_on(on: bool):
        moh.on = on
        logger.info("Wartemusik {}", "an" if on else "aus")

    transport = DailyTransport(
        ROOM_URL,
        DAILY_TOKEN,
        "Telefonassistenz",
        DailyParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            transcription_enabled=False,
        ),
    )

    stt = DeepgramSTTService(
        api_key=DG_KEY,
        settings=DeepgramSTTService.Settings(model="nova-3", language="de"),
    )
    tts = InworldTTSService(api_key=INW_KEY, voice_id=INW_VOICE, model="inworld-tts-2")
    llm = OpenAILLMService(base_url=LLM_BASE, api_key=LLM_KEY, model=LLM_MODEL)

    task = None  # wird unten gesetzt

    async def transfer(params: FunctionCallParams):
        brief = str(params.arguments.get("briefing") or "").strip()
        STATE["briefing"] = brief or "Der Anrufer möchte mit Ihnen sprechen."
        logger.info("Transfer angefordert, Briefing: {}", STATE["briefing"])
        STATE["armed"] = True
        await params.result_callback(None)   # keine Extra-Ansage: HOLD kommt als eigener Satz
        await task.queue_frames([TTSSpeakFrame(HOLD)])

    tools = ToolsSchema(
        standard_tools=[
            FunctionSchema(
                name="mit_chef_verbinden",
                description=(
                    "Verbindet den Anrufer mit dem Chef. Aufrufen, sobald der Anrufer den Chef, "
                    "den Inhaber oder einen Mitarbeiter sprechen will. Im Feld briefing steht "
                    "kurz, worum es geht, damit du es dem Chef vorlesen kannst."
                ),
                properties={
                    "briefing": {
                        "type": "string",
                        "description": (
                            "Ein bis zwei Saetze: was der Anrufer will, mit den genannten Fakten. "
                            "Nur Fakten aus dem Gespraech, nichts erfinden."
                        ),
                    }
                },
                required=["briefing"],
                handler=transfer,
            )
        ]
    )
    context = LLMContext(messages=[{"role": "system", "content": PROMPT}], tools=tools)
    pair = LLMContextAggregatorPair(
        context, user_params=LLMUserAggregatorParams(vad_analyzer=SileroVADAnalyzer())
    )

    async def reset_to_caller(text: str, mixer_off=True):
        STATE.update(armed=False, dialing=False, chef=False, awaiting=False, closing=False)
        STATE["connected"] = False
        STATE["chef_id"] = None
        in_gate.open = out_gate.open = True
        await set_caller_hear("full")
        if mixer_off:
            moh_on(False)
        await task.queue_frames([TTSSpeakFrame(text)])

    def mute_base(base):
        """Aus den echten Track-Namen des Teilnehmers eine Stumm-Map bauen.
        Map-Keys (customAudio/customVideo) brauchen den '*'-Key, sonst lehnt Daily ab."""
        out = {}
        for k, v in (base or {}).items():
            out[k] = {"*": False} if isinstance(v, dict) else False
        if not out:
            out = {"microphone": False}
        return out

    def bot_id():
        try:
            p = transport._client.participants() or {}
            loc = p.get("local") or {}
            return loc.get("id") or loc.get("session_id")
        except Exception as e:  # noqa: BLE001
            logger.error("participants() fehlgeschlagen: {}", e)
            return None

    def with_moh(base, allowed: bool):
        out = dict(base or {})
        ca = out.get("customAudio")
        ca = dict(ca) if isinstance(ca, dict) else {"*": True}
        ca["moh"] = allowed
        out["customAudio"] = ca
        return out

    async def push_perms(pid: str, perms: dict, label: str):
        try:
            err = await transport.update_remote_participants({pid: {"permissions": perms}})
            logger.info("Permissions {} pid={} {} -> {}", label, pid[:8], perms, err or "ok")
            if err:
                logger.error("Permissions abgelehnt ({}): {}", label, err)
            return not err
        except Exception as e:  # noqa: BLE001
            logger.error("update_remote_participants fehlgeschlagen: {}", e)
            return False

    async def set_caller_hear(mode: str):
        """full = Anrufer hoert Chef+Bot (keine Musik),
        moh  = Anrufer hoert ausschliesslich die Wartemusik (kein Briefing)."""
        pid = STATE.get("caller_id")
        if not pid:
            logger.warning("Permissions: kein caller_id bekannt")
            return
        base = (STATE.get("caller_perms") or {}).get("base") or {}
        if mode == "moh":
            # alles aus, nur den Track 'moh' erlauben - ohne User-IDs (robuster)
            base_mute = mute_base(base)
            ca = dict(base_mute.get("customAudio") or {})
            ca["moh"] = True
            base_mute["customAudio"] = ca
            perms = {"canReceive": {"base": base_mute, "byUserId": {}}, "canSend": []}
        else:
            perms = {
                "canReceive": {"base": with_moh(base, False), "byUserId": {}},
                "canSend": STATE.get("caller_send") or [],
            }
        await push_perms(pid, perms, f"caller/{mode}")

    async def set_chef_hear():
        pid = STATE.get("chef_id")
        if not pid:
            return
        base = (STATE.get("chef_perms") or {}).get("base") or {}
        perms = {"canReceive": {"base": with_moh(base, False), "byUserId": {}}}
        await push_perms(pid, perms, "chef/kein_moh")

    async def after_hold_spoken():
        if not STATE["armed"] or STATE["dialing"]:
            return
        STATE["armed"] = False        # genau ein Dialout pro Transfer
        STATE["dialing"] = True
        logger.info("Ansage fertig -> Hold-Musik an, dialOut {}", CHEF_SIP)
        moh_on(True)
        in_gate.open = False          # Anrufer hoert Musik, sein Reden interessiert nicht
        out_gate.open = False         # Bot schweigt waehrend Hold
        status, data = await daily_dialout(CHEF_SIP)
        if status >= 400 or (isinstance(data, dict) and data.get("error")):
            logger.error("dialout abgelehnt: {} {}", status, data)
            await reset_to_caller(FAIL)
            return
        logger.info("dialout gestartet: {} status={}", (data or {}).get("sessionId", "?"), status)

        async def watchdog():
            for _ in range(9):  # 45 s
                await asyncio.sleep(5)
                if not STATE["dialing"]:
                    return
            logger.warning("Chef hat nicht abgenommen -> zurueck zum Anrufer")
            await reset_to_caller(FAIL)

        asyncio.create_task(watchdog())

    async def chef_answered_yes(accept: bool, text: str):
        STATE["awaiting"] = False
        if accept:
            STATE["connected"] = True
            STATE["closing"] = True
            logger.info("Chef stimmt zu -> Ansage, danach Bruecke")
            # Anrufer bleibt auf Wartemusik, bis der Satz komplett gesprochen ist
            await task.queue_frames([TTSSpeakFrame(CONNECTING)])
            return
        logger.info("Chef lehnt ab -> Chef-Bein trennen")
        STATE["chef"] = False
        logger.info("stop_dialout {}", STATE["chef_id"])
        try:
            await transport.stop_dialout(STATE["chef_id"])
        except Exception as e:  # noqa: BLE001
            logger.error("stop_dialout fehlgeschlagen: {}", e)
            try:
                transport._client.eject_remote_participants([STATE["chef_id"]])
            except Exception as e2:  # noqa: BLE001
                logger.error("eject fehlgeschlagen: {}", e2)
        await asyncio.sleep(0.5)
        await reset_to_caller(BUSY)

    async def chef_joined(pid: str):
        STATE.update(chef=True, dialing=False, awaiting=True, asks=1)
        STATE["chef_id"] = pid
        in_gate.open = True
        out_gate.open = True
        await set_caller_hear("moh")    # Anrufer hoert nur Wartemusik
        moh_on(True)
        await set_chef_hear()           # Chef hoert Briefing, keine Musik
        logger.info("Chef im Raum ({}) -> Briefing", pid)
        await task.queue_frames(
            [TTSSpeakFrame(f"{BRIEF_INTRO} {STATE['briefing']} {BRIEF_QUESTION}")]
        )

        async def decision_watchdog():
            await asyncio.sleep(12)
            if not STATE["awaiting"]:
                return
            if STATE["asks"] < 2:
                STATE["asks"] = 2
                logger.info("keine Entscheidung -> nachfragen")
                await task.queue_frames([TTSSpeakFrame(NO_ANSWER_REASK)])
                await asyncio.sleep(12)
                if not STATE["awaiting"]:
                    return
            logger.warning("Chef hat nicht entschieden -> Abbruch")
            await chef_answered_yes(False, "timeout")

        asyncio.create_task(decision_watchdog())

    in_gate = Gate()
    out_gate = Gate(on_bot_stopped=after_hold_spoken)

    async def on_out_bot_stopped():
        await after_hold_spoken()
        if STATE["closing"]:
            STATE["closing"] = False
            logger.info("Ansage fertig -> Anrufer freigeben, Musik aus")
            await set_caller_hear("full")
            moh_on(False)
            in_gate.open = out_gate.open = False
            logger.info("Bruecke steht, Bot haelt sich raus")

    out_gate.on_bot_stopped = on_out_bot_stopped

    stages = [
        transport.input(),
        in_gate,
        stt,
        ChefWatch(on_answer=chef_answered_yes),
        pair.user(),
        llm,
        tts,
        out_gate,
        transport.output(),
        pair.assistant(),
    ]
    pipeline = Pipeline(stages)
    task = PipelineTask(pipeline)

    @transport.event_handler("on_joined")
    async def on_joined(transport, data):
        logger.info("Bot im Raum -> MOH-Track anlegen")
        try:
            await moh.attach(transport._client)
        except Exception as e:  # noqa: BLE001
            logger.error("MOH attach fehlgeschlagen: {}", e)

    @transport.event_handler("on_client_connected")
    async def on_client_connected(transport, client):
        pid = pid_of(client)
        logger.info("Teilnehmer im Raum: {} user={} raw={}", pid, uname_of(client), str(client)[:220])
        if not pid:
            return
        info = client.get("info", {}) if isinstance(client, dict) else {}
        if info.get("isOwner"):
            # Geister-Teilnehmer eines vorherigen Bot-Starts: nicht begruessen
            logger.info("Owner-Teilnehmer ignoriert: {}", pid)
            return
        joined = info.get("joinedAt") or 0
        age = time.time() - joined if joined else 999
        if age > 120:
            # Leiche aus einem frueheren Anruf (SIP-Bein blieb im Raum)
            logger.info("Stale Teilnehmer ignoriert: {} ({}s alt)", pid[:8], int(age))
            return
        if STATE["dialing"] and not STATE["chef"]:
            STATE["chef_perms"] = (info.get("permissions") or {}).get("canReceive") or {}
            logger.info("Chef-Permissions: {}", info.get("permissions"))
            await chef_joined(pid)
            return
        if not STATE["greeted"]:
            STATE["greeted"] = True
            STATE["caller_id"] = pid
            perms = (info.get("permissions") or {})
            STATE["caller_perms"] = perms.get("canReceive") or {}
            STATE["caller_send"] = perms.get("canSend") or []
            logger.info("Anrufer-Permissions: {}", perms)
            await task.queue_frames([TTSSpeakFrame(OPENER)])

    @transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(transport, client):
        pid = pid_of(client)
        logger.info("Teilnehmer weg: {}", pid)
        if STATE["chef"] and pid == STATE["chef_id"]:
            STATE["chef"] = False
            if STATE["connected"]:
                await task.cancel()
            return
        if pid == STATE["caller_id"] and not STATE["connected"]:
            await task.cancel()

    @transport.event_handler("on_dialout_connected")
    async def on_dialout_connected(transport, data):
        logger.info("waehlt ... {}", data)

    @transport.event_handler("on_dialout_answered")
    async def on_dialout_answered(transport, data):
        logger.info("Chef-Bein angenommen: {}", data)

    @transport.event_handler("on_dialout_error")
    async def on_dialout_error(transport, data):
        logger.error("Dialout-Fehler: {}", data)
        await reset_to_caller(FAIL)

    @transport.event_handler("on_dialout_stopped")
    async def on_dialout_stopped(transport, data):
        logger.info("Dialout beendet: {}", data)
        if STATE["connected"]:
            STATE["connected"] = False
            STATE["chef"] = False
            await reset_to_caller("Der Chef ist aufgelegt. Kann ich sonst etwas für Sie tun?")

    runner = PipelineRunner()

    async def selftest():
        await asyncio.sleep(6)
        logger.info("SELFTEST dialout -> {}", TEST_SIP)
        status, data = await daily_dialout(TEST_SIP)
        logger.info("SELFTEST Ergebnis: status={} data={}", status, data)
        await asyncio.sleep(12)
        logger.info("SELFTEST Ende")

    if TEST_SIP:
        asyncio.create_task(selftest())

    if os.environ.get("SELFTEST_TTS"):
        async def tts_selftest():
            await asyncio.sleep(6)
            await task.queue_frames([TTSSpeakFrame(OPENER)])
            logger.info("SELFTEST_TTS: Opener in die Pipeline gelegt")

        asyncio.create_task(tts_selftest())

    await runner.run(task)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
