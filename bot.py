#!/usr/bin/env python3
"""Pipecat agent on a BYOC SIP trunk: greeting + warm transfer with briefing and approval.

DID -> SIP carrier -> SBC (OpenSIPS:5080) -> Daily room (SIP URI)
Bot in the room: Speechmatics linden-1 / Deepgram (STT), OpenAI-compatible LLM, Inworld (TTS)

Transfer flow (following the pipecat example daily-pstn-warm-transfer):
  1. Caller asks for the boss -> tool mit_chef_verbinden(briefing)
  2. Announcement to the caller, then hold music on (published by moh_agent.py)
  3. dialOut through the REST API (codec/AVPF handling needs that path)
  4. The human picks up -> music off, the bot reads the briefing to them and asks
     "Shall I connect the caller now?"
  5. yes  -> "connecting you", the bot goes silent, both parties hear each other
     no   -> the human's leg is ejected, back to the caller
  6. No answer after 45 s or no decision -> back to the caller
"""
import asyncio
import collections
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
    VADUserStartedSpeakingFrame,
    VADUserStoppedSpeakingFrame,
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
from pipecat.services.speechmatics.stt import SpeechmaticsSTTService
from pipecat.services.speechmatics.stt import TurnDetectionMode as SMTurnMode
from speechmatics.agent_stt import AdditionalVocabEntry
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
KEYTERMS = ["Chef", "verbinden", "Rückruf", "Bestellung", "Lieferung", "Termin"]
MOH_STATE_FILE = os.environ.get("MOH_STATE_FILE", "/tmp/moh.state")
MOH_USER = "moh-hold"
HOLD_WAV16 = os.environ.get("HOLD_MUSIC_WAV16", "/opt/pipecat-bot/hold16.wav")

BOT_NAME = os.getenv("BOT_NAME") or "Telefonassistenz"
OPENER = os.getenv("OPENER_TEXT") or "Wie kann ich Ihnen helfen?"
HOLD = os.getenv("HOLD_TEXT") or "Einen Moment bitte, ich verbinde Sie mit dem Chef."
FAIL = os.getenv("FAIL_TEXT") or "Ich konnte den Chef gerade nicht erreichen. Kann ich sonst etwas für Sie tun?"
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
    async def _post_once():
        async with aiohttp.ClientSession() as sess:
            async with sess.post(
                url, json=payload, headers={"Authorization": f"Bearer {DAILY_API_KEY}"}
            ) as r:
                try:
                    data = await r.json(content_type=None)
                except Exception:
                    data = {"raw": await r.text()}
                return r.status, data

    status, data = await _post_once()
    if status == 400 and "active" in str(data).lower():
        # Ein Dialout eines abgebrochenen Anrufs haengt noch: kurz warten, dann erneut.
        logger.warning("dialout bereits aktiv -> 2 s warten, zweiter Versuch")
        await asyncio.sleep(2.0)
        status, data = await _post_once()
    return status, data


class EnergyVAD(FrameProcessor):
    """Energie-VAD: schliesst Sprech-Turns (fuer Speechmatics EXTERNAL).

    Silero laeuft in dieser Pipecat-Version nicht im Transport, deshalb
    entscheidet hier die Frame-Energie: Sprache ab VAD_RMS, Turn-Ende nach
    VAD_HANGOVER_MS ohne Pegel.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.thresh_min = int(os.getenv("VAD_RMS", "600"))
        self.hang = float(os.getenv("VAD_HANGOVER_MS", "900")) / 1000.0
        self.speaking = False
        self.last_voice = 0.0
        self.turns = 0
        self.hist = collections.deque(maxlen=250)   # ~5 s Frame-Pegel
        self.noise = 120.0
        self.cnt = 0

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, InputAudioRawFrame) and frame.audio:
            import array as _arr
            a = _arr.array("h")
            data = frame.audio
            a.frombytes(data[: len(data) // 2 * 2])
            if a:
                peak = max(abs(min(a)), abs(max(a)))
                now = time.monotonic()
                # Rauschboden: 10-Perzentil der letzten ~5 s, Schwelle = 2,5x Rauschen
                self.hist.append(peak)
                self.cnt += 1
                if self.cnt % 5 == 0 and self.hist:
                    h = sorted(self.hist)
                    self.noise = float(h[len(h) // 10])
                thr = max(self.thresh_min, self.noise * 2.5)
                if peak >= thr:
                    self.last_voice = now
                    if not self.speaking:
                        self.speaking = True
                        logger.debug("VAD start peak={} thr={:.0f} noise={:.0f}", peak, thr, self.noise)
                        await self.push_frame(VADUserStartedSpeakingFrame())
                elif self.speaking and (now - self.last_voice) >= self.hang:
                    self.speaking = False
                    self.turns += 1
                    logger.info(
                        "VAD stop: Stille {:.0f} ms, Turn {}".format(
                            (now - self.last_voice) * 1000, self.turns
                        )
                    )
                    await self.push_frame(VADUserStoppedSpeakingFrame(stop_secs=self.hang))
        await self.push_frame(frame, direction)


class Probe(FrameProcessor):
    """Diagnose: zaehlt ankommende Audio-Frames und Transkripte."""

    def __init__(self, label: str, **kwargs):
        super().__init__(**kwargs)
        self.label = label
        self.audio = 0
        self.texts = 0
        self.peak = 0
        self.sum_sq = 0
        self.n = 0

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, InputAudioRawFrame):
            self.audio += 1
            if getattr(self, "srate", None) != frame.sample_rate:
                self.srate = frame.sample_rate
                logger.info("AUDIO-RATE: {} Hz, {} Kanaele", frame.sample_rate, frame.num_channels)
            data = frame.audio or b""
            if data:
                import array as _arr
                a = _arr.array("h")
                a.frombytes(data[: len(data) - (len(data) % 2)])
                if a:
                    self.peak = max(self.peak, max(abs(x) for x in a))
                    self.sum_sq += sum(x * x for x in a)
                    self.n += len(a)
        elif isinstance(frame, TranscriptionFrame):
            self.texts += 1
            logger.info("{} Transkript: {}", self.label, (frame.text or "")[:120])
        await self.push_frame(frame, direction)


class TransLog(FrameProcessor):
    """Loggt jedes finale Transkript, damit sichtbar ist, was verstanden wurde."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.last = ""

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, TranscriptionFrame):
            t = (frame.text or "").strip()
            if t and t != self.last:
                self.last = t
                logger.info("VERSTANDEN: {}", t)
        await self.push_frame(frame, direction)


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


class MohControl:
    """Wartemusik laeuft in einem eigenen Teilnehmer (moh_agent.py).

    Der Bot gibt nur einen Leash aus: solange er alle Sekunde "on"
    schreibt, spielt der Agent. Bleibt der Heartbeat aus (Crash,
    Neustart, haengender Call), stellt der Agent nach 3 s selbst ab.
    """

    def __init__(self, path: str):
        self.path = path
        self.on = False
        self._task = None

    def _write(self, on: bool):
        try:
            with open(self.path, "w") as f:
                f.write("on" if on else "off")
        except OSError as e:
            logger.error("MOH-Status nicht schreibbar: {}", e)

    async def _beat(self):
        while True:
            self._write(True)
            await asyncio.sleep(1.0)

    def set(self, on: bool):
        self.on = on
        if on:
            self._write(True)
            if not self._task or self._task.done():
                self._task = asyncio.create_task(self._beat())
        else:
            if self._task:
                self._task.cancel()
                self._task = None
            self._write(False)


async def main():
    moh = MohControl(MOH_STATE_FILE)

    async def mute_moh_for_bot(mute: bool):
        """Bot selbst darf die Musik nicht hoeren (sonst landet sie in der STT)."""
        mp = moh_pid()
        if not mp:
            return
        try:
            err = await transport.update_subscriptions(
                participant_settings={mp: {"microphone": not mute}}
            )
            logger.info("Bot-Abo Wartemusik {} -> {}", "aus" if mute else "an", err or "ok")
        except Exception as e:  # noqa: BLE001
            logger.error("update_subscriptions: {}", e)

    def moh_on(on: bool):
        moh.set(on)
        logger.info("Wartemusik {}", "an" if on else "aus")
        asyncio.create_task(mute_moh_for_bot(on))

    transport = DailyTransport(
        ROOM_URL,
        DAILY_TOKEN,
        BOT_NAME,
        DailyParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            transcription_enabled=False,
        ),
    )

    # Wichtig: sample_rate/encoding muessen explizit gesetzt sein. Ohne Angabe
    # schickt Pipecat "None" an Deepgram, Deepgram nimmt 16 kHz an, wir senden
    # aber die Pipeline-Rate (24 kHz) -> keine Transkripte.
    IN_RATE = 16000
    try:
        IN_RATE = int(transport.audio_in_sample_rate or 16000)
    except Exception:
        pass
    logger.info("STT-Rate (Pipeline-Eingang): {} Hz", IN_RATE)
    STT_PROVIDER = (os.getenv("STT_PROVIDER") or "speechmatics").strip().lower()
    STT_LANG = os.getenv("STT_LANGUAGE") or "de"
    if STT_PROVIDER == "speechmatics":
        # linden-1 = Speechmatics Agent-STT-Modell, auf Telefon-Audio (16 kHz) deutlich
        # praeziser als nova-3. Turn-Ende kommt von unserem EnergyVAD (EXTERNAL).
        vocab = [
            AdditionalVocabEntry(content=w) for w in KEYTERMS
        ]
        stt = SpeechmaticsSTTService(
            api_key=os.getenv("SPEECHMATICS_API_KEY"),
            sample_rate=IN_RATE if IN_RATE in (8000, 16000) else 16000,
            settings=SpeechmaticsSTTService.Settings(
                language=STT_LANG,
                model=os.getenv("SPEECHMATICS_MODEL") or "linden-1",
                enable_partials=True,
                emit_sentences=False,
                turn_detection_mode=SMTurnMode.EXTERNAL,
                additional_vocab=vocab,
            ),
        )
        logger.info("STT: Speechmatics {} ({}), {} Hz, Turn=EXTERNAL", os.getenv("SPEECHMATICS_MODEL") or "linden-1", STT_LANG, IN_RATE)
    else:
        stt = DeepgramSTTService(
            api_key=DG_KEY,
            sample_rate=IN_RATE,
            settings=DeepgramSTTService.Settings(
                model=os.getenv("DEEPGRAM_MODEL") or "nova-3",
                language=STT_LANG,
                interim_results=True,
                punctuate=True,
                smart_format=True,
                numerals=True,
                endpointing=300,
                keyterm=KEYTERMS,
            ),
        )
        logger.info("STT: Deepgram {} ({}), {} Hz", os.getenv("DEEPGRAM_MODEL") or "nova-3", STT_LANG, IN_RATE)

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

    def moh_pid():
        """Session-ID des Wartemusik-Teilnehmers (moh_agent.py)."""
        try:
            for p in (transport._client.participants() or {}).values():
                info = p.get("info") or {}
                if (info.get("userName") or "") == MOH_USER:
                    return p.get("id") or info.get("id")
        except Exception as e:  # noqa: BLE001
            logger.error("moh_pid: {}", e)
        return None

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
            can = {"base": mute_base(base)}
            mp = moh_pid()
            if mp:
                can["byParticipantId"] = {mp: {"microphone": True}}
            perms = {"canReceive": can, "canSend": []}
        else:
            perms = {
                "canReceive": {"base": dict(base)},
                "canSend": STATE.get("caller_send") or [],
            }
        await push_perms(pid, perms, f"caller/{mode}")

    async def set_chef_hear():
        pid = STATE.get("chef_id")
        if not pid:
            return
        base = (STATE.get("chef_perms") or {}).get("base") or {}
        can = {"base": dict(base)}
        mp = moh_pid()
        if mp:
            can["byParticipantId"] = {mp: {"microphone": False}}
        perms = {"canReceive": can}
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
            logger.info("Chef stimmt zu -> Musik aus, Ansage, danach Bruecke")
            # Anrufer muss die Ansage hoeren koennen: Rechte frei, Musik aus.
            # Die Bruecke selbst oeffnet erst nach BotStoppedSpeaking.
            await set_caller_hear("full")
            moh_on(False)
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

    probe = Probe("IN")
    vad = EnergyVAD()
    translog = TransLog()

    stages = [
        transport.input(),
        in_gate,
        probe,
        vad,
        stt,
        translog,
        ChefWatch(on_answer=chef_answered_yes),
        pair.user(),
        llm,
        tts,
        out_gate,
        transport.output(),
        pair.assistant(),
    ]
    pipeline = Pipeline(stages)

    async def probe_report():
        while True:
            await asyncio.sleep(15)
            if probe.audio or probe.texts:
                rms = int((probe.sum_sq / probe.n) ** 0.5) if probe.n else 0
                logger.info("PROBE audio_frames={} peak={} rms={} transkripte={}",
                            probe.audio, probe.peak, rms, probe.texts)

    asyncio.create_task(probe_report())
    task = PipelineTask(pipeline)

    @transport.event_handler("on_client_connected")
    async def on_client_connected(transport, client):
        pid = pid_of(client)
        logger.info("Teilnehmer im Raum: {} user={} raw={}", pid, uname_of(client), str(client)[:220])
        if not pid:
            return
        info = client.get("info", {}) if isinstance(client, dict) else {}
        if (info.get("userName") or "") == MOH_USER:
            logger.info("Wartemusik-Teilnehmer ignoriert: {}", pid[:8])
            return
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

    moh.set(False)      # keine Altlast nach Neustart

    await runner.run(task)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
