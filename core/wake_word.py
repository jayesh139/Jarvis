"""
Local wake-word detection for JARVIS ("Jarvis").

Design goals:
  • ZERO cost when the feature is off — openwakeword is imported ONLY inside
    start()/install helpers, never at module load. If the user never enables
    wake word, none of this touches the app.
  • ZERO latency on the audio path — the microphone callback only ever does a
    cheap, non-blocking queue push (feed()); the actual model inference runs in
    this module's own background thread, so the real-time audio thread and the
    Gemini stream are never slowed.
  • Fully local & offline — audio fed here never leaves the machine; there is no
    network call except the one-time model download the user triggers from the UI.

openwakeword ships small ONNX models (a few MB each) and runs comfortably on a
CPU. The pretrained model id is still ``hey_jarvis`` (no shorter public model
exists); the score threshold is tuned so a clear "Jarvis" is enough — you do
not need to say "Hey" first.
"""
from __future__ import annotations

import queue
import subprocess
import sys
import threading
from pathlib import Path
from typing import Callable

# openwakeword's pretrained Jarvis model (trained on "Hey Jarvis", also fires
# on a clear "Jarvis" at the lowered threshold below).
WAKE_MODEL = "hey_jarvis"
WAKE_PHRASE = "Jarvis"
# Score in [0,1]; above this counts as a detection. 0.35 catches "Jarvis"
# alone more reliably than the old 0.5 (which wanted the full "Hey Jarvis").
DEFAULT_THRESHOLD = 0.35
# Mic frames arrive at 16 kHz int16; this is just the detector's input rate.
SAMPLE_RATE = 16000


def is_installed() -> bool:
    """True if the openwakeword package is importable (no model check)."""
    try:
        import importlib.util
        return importlib.util.find_spec("openwakeword") is not None
    except Exception:
        return False


def _models_dir() -> Path | None:
    try:
        import openwakeword
        d = Path(openwakeword.__file__).resolve().parent / "resources" / "models"
        return d if d.is_dir() else None
    except Exception:
        return None


def is_ready() -> bool:
    """True if openwakeword is installed AND its model files are present on disk.

    This is a cheap, DETERMINISTIC file-existence check. It deliberately does NOT
    construct a Model to probe readiness — doing that is slow and, worse, can clash
    with the detector's own Model when it's already running, which intermittently
    returned False and made the UI flicker to 'not downloaded'. Never raises.

    We require the ONNX set: the detector loads with inference_framework=\"onnx\",
    and a lone melspectrogram.tflite is not enough (that mismatch used to spam
    NO_SUCHFILE on every mic frame).
    """
    if not is_installed():
        return False
    try:
        models_dir = _models_dir()
        if models_dir is None:
            return False
        has_wake = any(models_dir.glob(f"{WAKE_MODEL}*.onnx"))
        has_mel = (models_dir / "melspectrogram.onnx").exists()
        has_emb = (models_dir / "embedding_model.onnx").exists()
        return bool(has_wake and has_mel and has_emb)
    except Exception:
        return False


def _ensure_melspectrogram_onnx(logger: Callable[[str], None] = print) -> bool:
    """openwakeword's download_models() sometimes leaves mel as tflite-only."""
    models_dir = _models_dir()
    if models_dir is None:
        return False
    target = models_dir / "melspectrogram.onnx"
    if target.exists() and target.stat().st_size > 100_000:
        return True
    urls = (
        "https://github.com/dscripka/openWakeWord/releases/download/v0.5.1/melspectrogram.onnx",
        "https://github.com/dscripka/openWakeWord/releases/download/v0.5.0/melspectrogram.onnx",
    )
    import urllib.request
    for url in urls:
        try:
            logger(f"Wake word: fetching melspectrogram.onnx…")
            urllib.request.urlretrieve(url, target)
            if target.exists() and target.stat().st_size > 100_000:
                return True
        except Exception as e:
            logger(f"Wake word: mel download failed ({e})")
    return target.exists()


def install_and_download(logger: Callable[[str], None] = print,
                         notify: Callable[[str], None] | None = None) -> tuple[bool, str]:
    """
    One-click setup for the UI button: pip-install openwakeword if missing, then
    download the wake model. Returns (ok, message). Never raises — every failure
    is reported through the returned message and the logger.
    """
    _tell = notify or (lambda _msg: None)
    try:
        if not is_installed():
            logger("Wake word: installing openwakeword (one-time)…")
            _tell("Wake word: installing openwakeword (one-time)…")
            r = subprocess.run(
                [sys.executable, "-m", "pip", "install", "openwakeword"],
                capture_output=True, text=True,
            )
            if r.returncode != 0:
                tail = (r.stderr or r.stdout or "").strip().splitlines()[-1:] or [""]
                return False, f"pip install failed: {tail[0][:160]}"
        # Download the pretrained melspectrogram/embedding + wake models.
        logger("Wake word: downloading models…")
        _tell("Wake word: downloading models…")
        try:
            import openwakeword.utils as _u
            try:
                _u.download_models([WAKE_MODEL])
            except TypeError:
                _u.download_models()   # older signature downloads the default set
        except Exception as e:
            return False, f"model download failed: {e}"

        # download_models() has been observed to leave only melspectrogram.tflite
        # while the ONNX loader still needs melspectrogram.onnx — fetch it.
        if not _ensure_melspectrogram_onnx(logger):
            return False, "installed, but melspectrogram.onnx could not be downloaded."

        if not is_ready():
            return False, "installed, but the wake model could not be loaded."
        logger("Wake word: ready.")
        return True, "Wake word installed and ready."
    except Exception as e:
        return False, f"setup error: {e}"


class WakeWordDetector:
    """
    Runs the wake model in a dedicated thread. The mic thread calls feed() with
    raw int16 frames; detections invoke on_detect() (called from this thread —
    the callback must marshal to whatever loop/UI it needs).
    """

    def __init__(self, on_detect: Callable[[], None],
                 threshold: float = DEFAULT_THRESHOLD,
                 logger: Callable[[str], None] = print,
                 notify: Callable[[str], None] | None = None):
        self._on_detect = on_detect
        self._threshold = threshold
        self._logger    = logger
        # See PluginRegistry: `logger` is the console and gets everything,
        # `notify` is the activity log and gets only what the user must act on.
        self._notify    = notify or (lambda _msg: None)
        self._queue: queue.Queue = queue.Queue(maxsize=50)
        self._thread: threading.Thread | None = None
        self._running = False
        self._model = None
        self._ready = False
        # After one failed load, do not retry on every mic frame (that spammed
        # the console thousands of times when the .onnx file was missing).
        # Call reset_load_failure() after a successful model download.
        self._load_failed = False
        self._load_error = ""

    def reset_load_failure(self) -> None:
        """Allow start() to try again (e.g. after install_and_download)."""
        self._load_failed = False
        self._load_error = ""

    def start(self) -> bool:
        """Load the model and spawn the inference thread. Returns True on success.
        Safe to call again — a no-op if already running. Never raises."""
        if self._running:
            return True
        if self._load_failed:
            return False
        # Don't even ask ONNXRuntime if the files are not on disk yet.
        if not is_ready():
            self._load_failed = True
            self._load_error = "model files not downloaded"
            self._logger("Wake word: model not downloaded yet — use ⚙ → WAKE WORD → DOWNLOAD.")
            self._notify("Wake word model missing — download it from ⚙ → WAKE WORD.")
            return False
        try:
            from openwakeword.model import Model
            # Prefer ONNX; fall back to tflite if the runtime is available.
            last_err: Exception | None = None
            self._model = None
            for fw in ("onnx", "tflite"):
                try:
                    self._model = Model(wakeword_models=[WAKE_MODEL],
                                        inference_framework=fw)
                    break
                except Exception as e:
                    last_err = e
                    self._model = None
            if self._model is None:
                raise last_err or RuntimeError("no inference framework worked")
        except Exception as e:
            self._load_failed = True
            self._load_error = str(e)
            self._logger(f"Wake word: could not load model — {e}")
            self._notify("Wake word unavailable — download the model from ⚙ → WAKE WORD, or use F4.")
            self._model = None
            return False
        self._running = True
        self._ready = True
        self._load_failed = False
        self._thread = threading.Thread(target=self._loop, daemon=True, name="WakeWordThread")
        self._thread.start()
        self._logger(f"Wake word: listening for '{WAKE_PHRASE}'.")
        return True

    def stop(self) -> None:
        self._running = False
        # unblock the thread if it's waiting on the queue
        try:
            self._queue.put_nowait(None)
        except Exception:
            pass
        self._model = None
        self._ready = False

    @property
    def ready(self) -> bool:
        return self._ready

    def feed(self, frame_int16) -> None:
        """Called from the mic callback (real-time thread). Must stay cheap and
        never block — the frame is copied and dropped if the queue is backed up."""
        if not self._running:
            return
        try:
            # frame_int16 is a numpy int16 array (possibly 2-D mono) — flatten to 1-D
            data = frame_int16[:, 0].copy() if getattr(frame_int16, "ndim", 1) > 1 else frame_int16.copy()
            self._queue.put_nowait(data)
        except queue.Full:
            pass
        except Exception:
            pass

    def _loop(self) -> None:
        import numpy as np
        while self._running:
            try:
                frame = self._queue.get()
                if frame is None or not self._running:
                    break
                scores = self._model.predict(np.asarray(frame, dtype=np.int16))
                score = 0.0
                if isinstance(scores, dict):
                    # match the jarvis model regardless of exact key suffix
                    for k, v in scores.items():
                        if "jarvis" in k.lower():
                            score = max(score, float(v))
                    if score == 0.0 and scores:
                        score = max(float(v) for v in scores.values())
                if score >= self._threshold:
                    # drain any backlog so we don't double-fire on the same utterance
                    self._drain()
                    try:
                        self._on_detect()
                    except Exception as e:
                        self._logger(f"Wake word: on_detect error — {e}")
            except Exception as e:
                self._logger(f"Wake word: inference error — {e}")

    def _drain(self) -> None:
        try:
            while True:
                self._queue.get_nowait()
        except Exception:
            pass
