# daily-sip-warm-transfer

A bring-your-own-carrier voice agent: inbound SIP call -> OpenSIPS -> Daily room -> Pipecat agent, with a **warm transfer** that dials a human back out through your own trunk and puts both parties into the same room.

No telephony account, phone number or scenario is baked in. Everything is environment driven.

```
caller ──PSTN──► carrier ──SIP──► OpenSIPS :5080 ──► Daily room (SIP dial-in URI)
                                                        │
                                                        ├─► agent (Deepgram STT / LLM / Inworld TTS)
                                                        │
"connect me"  ──► hold prompt ──► Daily dialOut (REST) ──► OpenSIPS ──► carrier ──► human
                                                                                   │
                                        human joins the same room, agent mutes ◄───┘
```

## Why this repo exists

Most "SIP + voice agent" tutorials stop at inbound audio. Making the transfer leg work against a real carrier is where the time goes. This repo documents the four traps that cost the most:

| # | Trap | Symptom | Fix |
|---|------|---------|-----|
| 1 | Daily dial-out requires an **owner** participant | `Dialout(Properties(UserMustBeAdmin))`, no call ever leaves | create a meeting token with `is_owner: true` and pass it as the second `DailyTransport` argument |
| 2 | The Python/JS SDKs **silently drop `codecs`** | carrier answers `488 Not Acceptable Here` | start the leg through the REST endpoint `POST /v1/rooms/{room}/dialOut/start`, which honours `codecs` |
| 3 | Daily offers **`RTP/AVPF`** | `488` for every codec (G.722, PCMU, PCMA all tested) | rewrite the SDP profile to `RTP/AVP` in the proxy: `subst_body('/RTP\/AVPF/RTP\/AVP/g')` |
| 4 | Trunk registration lives in the **registrant table** | inbound works after a restart, then dies | `uac_registrant` + `timer_interval`, verify `REGISTER 401 -> 200 OK` |

Trap 3 is the interesting one: the codec is a red herring. `488 Not Acceptable Here` looks like a codec mismatch, but a direct INVITE with the same codec in a plain `RTP/AVP` offer goes straight through `100 -> 183 -> 200`.

## Files

```
bot.py                     Pipecat agent: greeting, tool call, hold prompt, dial-out, mute gate, watchdog
opensips/opensips.cfg      Minimal proxy: trunk registration, inbound leg, transfer leg (+ AVPF rewrite)
scripts/sip_probe.py       Send a raw INVITE with a chosen codec and print the full reply chain
systemd/pipecat-agent.service
.env.example
```

## Quick start

```bash
# 1. agent
python3.11 -m venv venv && ./venv/bin/pip install pipecat-ai[daily,deepgram,openai,silero] aiohttp
cp .env.example .env && $EDITOR .env          # room URL, owner token, transfer target, keys
set -a; . ./.env; set +a
./venv/bin/python bot.py

# 2. proxy
sudo cp opensips/opensips.cfg /etc/opensips/opensips.cfg   # edit the defines first
sudo opensips -C -f /etc/opensips/opensips.cfg
sudo systemctl restart opensips

# 3. prove the transfer leg without ringing anybody
python scripts/sip_probe.py <carrier-ip> 5060 4930000000 pcma
#   488 -> SDP profile still AVPF     183/200 -> leg is fine
```

## Environment

| Variable | Required | Meaning |
|---|---|---|
| `DAILY_ROOM_URL` | yes | `https://<domain>.daily.co/<room>` |
| `DAILY_API_KEY` | yes | REST key, used for the dial-out call |
| `DAILY_TOKEN` | yes | meeting token with `is_owner: true` (trap 1) |
| `TRANSFER_TARGET` | yes | SIP URI of the human, e.g. `sip:+15551234567@carrier.example` |
| `DEEPGRAM_API_KEY` / `INWORLD_API_KEY` / `INWORLD_VOICE` / `LLM_API_KEY` | yes | STT / TTS / LLM |
| `LLM_BASE`, `LLM_MODEL`, `STT_MODEL`, `TTS_MODEL`, `STT_LANGUAGE` | no | provider overrides |
| `SYSTEM_PROMPT`, `OPENER_TEXT`, `HOLD_TEXT`, `FAIL_TEXT` | no | wording; keep your own voice here, never hardcode a scenario in the code |
| `TRANSFER_TIMEOUT_S` | no | seconds before the agent comes back to the caller (default 45) |

## Behaviour

* Greeting only for the first participant; the human joining later does not get greeted.
* After the tool call the agent speaks the hold prompt and **only then** starts the dial-out, so the caller is never left in silence.
* Both audio gates close as soon as the human leg is started: the agent stays silent during the conversation and does not transcribe it.
* If nobody answers within `TRANSFER_TIMEOUT_S`, the gates reopen and the agent asks the caller again.
* `+` prefix matters: E.164 with `+` is accepted by most carriers, bare digits are often rejected with `404`.

## Hold music on a separate track

While the human gets the briefing, the caller should keep hearing hold music - and only the caller, not the human. Both can sit in the same Daily room, because Daily lets you publish more than one audio track and hand out permissions per track.

```text
track "microphone"  (Pipecat output)  -> TTS: greeting, hold prompt, briefing
track "moh"         (own publisher)   -> hold music loop

caller during briefing   canReceive: microphone=false, customAudio={"*": false, "moh": true}
human during briefing    canReceive: microphone=true,  customAudio={"*": true,  "moh": false}
caller after "yes"       canReceive: microphone=true,  customAudio={"*": true,  "moh": false}
```

Notes that cost time to find out:

* `canReceive` takes a **map**, `false` is rejected (`invalid type: boolean 'false', expected a map`).
* Inside `customAudio`/`customVideo` the `'*'` key is mandatory (`missing field '*' key`). An empty `{}` is rejected, `{"byUserId": {}}` is accepted but filters nothing.
* Publish the extra track **when the agent joins the room**, not when the first caller connects: participants subscribe to the tracks that exist at their join, a later track never reaches them.
* The loop is 16 kHz mono s16 (`hold16.wav`), written in 20 ms chunks; frames keep flowing as silence while the track is idle, so the publisher is not torn down between calls.
* The file must be normalised. A loop peaking at 622/32768 (`-34 dBFS`) is inaudible on a phone line.

The agent keeps the caller on hold until the closing sentence has left the pipeline (`BotStoppedSpeakingFrame`), then flips the permissions and drops the music - otherwise the caller still hears the last words.

## Diagnostics

```bash
# what the carrier answered for a single dial-out
journalctl -u opensips -f | grep -E "INVITE sip|SIP/2.0 [0-9]{3}"

# registration state
mysql -N -B opensips -e "SELECT id,state,expiry FROM registrant;"
```

| Reply | Meaning |
|---|---|
| `488 Not Acceptable Here` | SDP profile rejected (AVPF) - see trap 3 |
| `404 Invalid phone number` | number format or trunk authorization rejected |
| `403 Forbidden` | From/CLI not allowed for this trunk |
| `Dialout(Properties(UserMustBeAdmin))` | missing owner token - see trap 1 |

## License

MIT
