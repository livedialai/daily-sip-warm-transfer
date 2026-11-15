"""MOH-Loop bauen: musikalisch passende Laenge (Autokorrelation), Crossfade-Seam, Loudness."""
import subprocess, sys, wave
import numpy as np

SRC = sys.argv[1] if len(sys.argv) > 1 else "input.wav"
OUT = sys.argv[2] if len(sys.argv) > 2 else "hold16.wav"
SR = 16000


def dec(path):
    p = subprocess.run(["ffmpeg", "-v", "error", "-i", path, "-ar", str(SR), "-ac", "1",
                        "-f", "s16le", "-"], capture_output=True)
    return np.frombuffer(p.stdout, dtype=np.int16).astype(np.float32) / 32768.0


a = dec(SRC)
hop = SR // 100  # 10 ms
env = np.array([np.sqrt((a[i:i + hop] ** 2).mean()) for i in range(0, len(a) - hop, hop)])
env = env - env.mean()

best, best_lag = -1e9, None
for lag in range(800, 2600):  # 8 .. 26 s
    c = float(np.dot(env[:-lag], env[lag:]) / (len(env) - lag))
    if c > best:
        best, best_lag = c, lag
L = best_lag / 100.0
print(f"Loop-Laenge aus Autokorrelation: {L:.2f}s (score {best:.5f})")

start = 2.0
seg = a[int(start * SR): int((start + L) * SR)]
xf = int(0.30 * SR)
w = np.linspace(0, 1, xf, dtype=np.float32)
loop = np.concatenate([seg[:-xf], seg[-xf:] * (1 - w) + seg[:xf] * w])

# 8-ms-Flanken an beiden Enden -> Seam geht durch Null (kein Klick)
n_f = int(0.008 * SR)
ramp = np.linspace(0, 1, n_f, dtype=np.float32) ** 0.5
loop = loop.copy()
loop[:n_f] *= ramp
loop[-n_f:] *= ramp[::-1]

# Loudness: -18 LUFS, TP -2 dBFS
tmp = "/tmp/_loop_raw.wav"
with wave.open(tmp, "wb") as wf:
    wf.setnchannels(1); wf.setsampwidth(2); wf.setframerate(SR)
    wf.writeframes((np.clip(loop, -1, 1) * 32767).astype(np.int16).tobytes())

p = subprocess.run(["ffmpeg", "-v", "error", "-y", "-i", tmp, "-af",
                    "loudnorm=I=-18:TP=-2:LRA=7", "-ar", str(SR), "-ac", "1",
                    "-c:a", "pcm_s16le", OUT], capture_output=True, text=True)
print("ffmpeg rc", p.returncode, p.stderr[:200])

with wave.open(OUT) as wf:
    data = np.frombuffer(wf.readframes(wf.getnframes()), dtype=np.int16).astype(np.float32) / 32768.0
    print("OUT", OUT, "secs", round(len(data) / SR, 2), "peak", round(float(np.abs(data).max()), 3),
          "rms", round(float(np.sqrt((data ** 2).mean())), 4),
          "seam-jump", round(float(abs(data[0] - data[-1])), 4),
          "bytes", wf.getnframes() * 2)
