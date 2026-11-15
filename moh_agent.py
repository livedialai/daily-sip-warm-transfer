import asyncio, json, os, sys, time, urllib.request

from loguru import logger

logger.remove()
logger.add(sys.stderr, level="INFO")

from pipecat.audio.mixers.soundfile_mixer import SoundfileMixer
from pipecat.frames.frames import MixerEnableFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.transports.daily.transport import DailyParams, DailyTransport

ROOM = os.environ["DAILY_ROOM_URL"]
KEY = os.environ["DAILY_API_KEY"]
WAV = os.environ.get("HOLD_MUSIC_WAV16", "/opt/pipecat-bot/hold16.wav")
STATE = os.environ.get("MOH_STATE_FILE", "/tmp/moh.state")


def log(msg, *args):
    print("[moh] " + (msg.format(*args) if args else msg), flush=True)


def make_token() -> str:
    name = ROOM.rstrip("/").rsplit("/", 1)[-1]
    body = json.dumps({"properties": {
        "room_name": name, "user_name": "moh-hold", "is_owner": False,
        "exp": int(time.time()) + 86400, "start_audio_off": True,
    }}).encode()
    req = urllib.request.Request(
        "https://api.daily.co/v1/meeting-tokens", data=body,
        headers={"Authorization": "Bearer " + KEY, "Content-Type": "application/json"})
    return json.load(urllib.request.urlopen(req))["token"]


LEASE_SECS = 3.0


def wanted() -> bool:
    """Musik nur, solange der Bot einen frischen Heartbeat schreibt.

    Der Bot schreibt jede Sekunde "on" in die Statusdatei. Ist die Datei
    aelter als 3 s (Crash, Neustart, haengender Call) oder steht "off"
    drin, bleibt die Musik aus. Damit kann sich kein alter "on"-Stand
    ueber Anrufe hinweg festsetzen.
    """
    try:
        with open(STATE) as f:
            content = f.read().strip()
        age = time.time() - os.path.getmtime(STATE)
        return content == "on" and age < LEASE_SECS
    except OSError:
        return False


async def main():
    mixer = SoundfileMixer(sound_files={"hold": WAV}, default_sound="hold",
                           volume=1.0, mixing=False, loop=True)
    transport = DailyTransport(
        ROOM, make_token(), "MOH",
        DailyParams(audio_in_enabled=True, audio_out_enabled=True,
                    audio_out_mixer=mixer, vad_enabled=False,
                    transcription_enabled=False))
    pipeline = Pipeline([transport.input(), transport.output()])
    task = PipelineTask(pipeline, params=PipelineParams(
        audio_in_sample_rate=16000, audio_out_sample_rate=16000,
        allow_interruptions=False))

    @transport.event_handler("on_joined")
    async def on_joined(transport, data):
        log("joined, mixing={}", wanted())

    async def control():
        cur = None
        while True:
            w = wanted()
            if w != cur:
                cur = w
                await task.queue_frames([MixerEnableFrame(enable=w)])
                log("mixing -> {}", w)
            await asyncio.sleep(0.15)

    asyncio.create_task(control())
    await PipelineRunner().run(task)


if __name__ == "__main__":
    asyncio.run(main())
