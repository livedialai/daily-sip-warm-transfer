# Daily SIP warm transfer (Pipecat)

Voice agent that answers a phone call, talks to the caller, and transfers them to a
human — but only after that human has heard a short briefing and agreed to take the
call. Built with [Pipecat](https://github.com/pipecat-ai/pipecat) on a Daily room with
a BYOC SIP trunk in front of it.

```
caller ──SIP──▶ carrier ──▶ SBC / OpenSIPS ──SIP──▶ Daily room ──▶ Pipecat agent
                                                          │            (STT→LLM→TTS)
                                                          └──▶ SIP leg: the human
```

## What the agent does

1. Greets the caller with a pre-generated opener (spoken from an audio file, so no
   TTS latency on the first turn).
2. Answers questions using the LLM. If the caller asks for the boss, the LLM calls
   `transfer_to_boss(briefing)` and writes a one-or-two-sentence briefing.
3. Tells the caller to wait and starts **hold music for the caller only**.
4. Dials the human through the Daily REST API (`dialOut`) and waits for an answer.
5. When the human picks up, the hold music stops for them, the briefing is read
   **only to them**, followed by "Shall I connect the caller now?".
6. `yes` → caller's audio is released, the agent says "connecting you" and then goes
   silent; the two parties talk directly.
   `no` → the human's leg is ejected and the caller is taken back.
   No answer within `TRANSFER_TIMEOUT_S` (default 45 s) → back to the caller.

Two rules shaped the whole design:

* the caller must never hear the briefing or the human's decision, only hold music;
* the human decides verbally — the agent waits for an answer, it does not bridge on
  its own.

## Hold music without a second track

Custom audio tracks are not forwarded to SIP legs: a SIP participant only ever
receives the **microphone** track of the other participants. Publishing hold music on
a custom track therefore works in the browser and stays silent on the phone.

So the music is published by a **second Daily participant** (`moh_agent.py`) that
publishes the loop as its microphone. The agent controls who may receive it with
per-participant permissions:

```
caller : microphone=false, byParticipantId {moh-hold: microphone=true}
human  : microphone=true,  byParticipantId {moh-hold: microphone=false}
```

A receiver map that only contains `{"*": ...}` is not enough — the per-participant
entries are what isolate the music.

### The lease file

`moh_agent.py` never trusts its own state. The agent writes `on`/`off` plus a
heartbeat into `$MOH_STATE_FILE` once per second; the music player only plays when the
content is `on` **and** the file is younger than 3 s. A crashed agent, a restart or a
call that dies mid-transfer can never leave the music running into the next call.

## Turn detection

Pipecat's user aggregator closes a turn on the STT's proposals. Speechmatics in
`EXTERNAL` mode closes a turn when it receives `VADUserStoppedSpeakingFrame`; the
provider's own sentence segmentation is disabled (`emit_sentences=False`), otherwise
every sentence becomes its own LLM call:

```
without VAD   "Hallo. Wie geht es Ihnen heute?"  → turn
              "Ich."                             → turn
              "Wollte nur kurz fragen."          → turn
with VAD      "Hallo. Wie geht es Ihnen heute? Ich wollte nur kurz fragen." → one turn
```

`EnergyVAD` (in `bot.py`) sits directly in front of the STT and emits the VAD frames:
adaptive threshold from the 10th percentile of the last 5 s (floor `max(600,
2.5 × noise)`), 900 ms hangover. That is deliberately provider-independent — it also
works when the pipeline runs with Deepgram.

## Speech recognition

`STT_PROVIDER=speechmatics` (default) or `deepgram`. Measured on 16 kHz phone audio
(8 kHz G.711, upsampled), German, four sentences:

```
speechmatics linden-1 : "Hallo. Wie geht es Ihnen heute? Ich wollte nur kurz fragen."
deepgram     nova-3   : "Hallo, wie geht es Ihnen heute? Ich wollte nur kurz"
```

Timing inside the room (caller side, one utterance):

```
speech end            → VAD stop           900 ms hangover
VAD stop              → transcript         286 ms
transcript            → first bot audio    1.4 s   (LLM + TTS)
```

## Files

| file | role |
| --- | --- |
| `bot.py` | the agent: pipeline, tool, transfer state machine, hold-music control |
| `moh_agent.py` | second Daily participant that publishes the hold loop |
| `scripts/make_moh.py` | turns any music file into a seamless 16 kHz mono loop |
| `scripts/sip_probe.py` | sends OPTIONS/INVITE to check the SIP side |
| `opensips/opensips.cfg` | SBC config, incl. the SDP `RTP/AVPF` → `RTP/AVP` rewrite |
| `systemd/pipecat-agent.service`, `systemd/moh-agent.service` | units |

## Setup

```bash
python3 -m venv venv && . venv/bin/activate
pip install "pipecat-ai[daily,speechmatics,deepgram,inworld,openai,silero]"
cp .env.example .env      # fill in the keys
```

Requirements: a Daily room, a Daily API key, a meeting token with
`{"properties": {"is_owner": true}}` (the REST `dialOut` call needs an owner token),
a SIP trunk that terminates on your SBC, and the usual provider keys.

```bash
set -a && . ./.env && set +a
python bot.py             # terminal 1
python moh_agent.py       # terminal 2
```

## Configuration

See `.env.example`. The important ones:

| variable | meaning |
| --- | --- |
| `DAILY_ROOM_URL`, `DAILY_API_KEY`, `DAILY_TOKEN` | room and owner token |
| `CHEF_SIP` | SIP URI of the human, in the carrier's expected format |
| `TRANSFER_TIMEOUT_S` | how long to ring before falling back to the caller |
| `STT_PROVIDER` | `speechmatics` or `deepgram` |
| `STT_LANGUAGE`, `SPEECHMATICS_MODEL`, `DEEPGRAM_MODEL` | recognition model |
| `VAD_RMS`, `VAD_HANGOVER_MS` | energy VAD threshold (`0` = adaptive) and hangover |
| `LLM_BASE`, `LLM_API_KEY`, `LLM_MODEL` | any OpenAI-compatible endpoint |
| `HOLD_MUSIC_WAV16` | the loop, 16 kHz mono s16 |

Wording lives in the environment, not in the code: `OPENER_TEXT`, `HOLD_TEXT`,
`FAIL_TEXT`, `BOT_NAME`.

## Building a hold loop

```bash
python scripts/make_moh.py input.mp3 hold16.wav
```

The script finds the musical period by autocorrelation, crossfades the seam, applies
8 ms edge fades (a loop seam that jumps by more than ~0.1 in sample value is audible
as a click) and normalises to −18 LUFS, −2 dBFS peak.

## Verification

What the logs should show for one transfer:

```
VAD stop: Stille 902 ms, Turn 1
VERSTANDEN: <the full utterance, one transcript>
Wartemusik an                    (caller only)
dialOut -> 200
DialoutAnsweredFrame             -> Briefing an den Chef, Musik bleibt beim Anrufer
Chef: ja                          -> Wartemusik aus, Rechte frei, "Ich verbinde Sie"
BotStoppedSpeakingFrame           -> Brücke steht, Bot verstummt
```

## Limitations

* One caller per room; the transfer state machine is global (`STATE`).
* The briefing quality depends on the LLM's tool-call arguments — the prompt asks for
  facts only ("invent nothing"), but it is not validated.
* `dialOut` needs an owner meeting token; a normal token is rejected.
* Codec negotiation is left to the carrier: the SBC rewrites `RTP/AVPF` to `RTP/AVP`
  because several trunks reject AVPF with `488 Not Acceptable Here`.

MIT licensed.
