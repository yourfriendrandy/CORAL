# config/logger.py

import os
import time
import psutil
import logging
import subprocess
from logging.handlers import RotatingFileHandler
from config.config import COMPRESSED_LOG as COMPRESSED_MODE

# -----------------------------
# PATH & LOG FILE SETUP
# -----------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LOG_PATH = os.path.join(BASE_DIR, "runtime_logs")
SESSION_TIMESTAMP = time.strftime("%Y%m%d_%H%M%S")
LOG_FILE = os.path.join(LOG_PATH, f"session_{SESSION_TIMESTAMP}.log")
LOG_RETENTION_DAYS = 7

# Ensure log directory exists
os.makedirs(LOG_PATH, exist_ok=True)


# -----------------------------
# PURGE OLD LOG FILES
# -----------------------------
def purge_old_logs():
    now = time.time()
    for fname in os.listdir(LOG_PATH):
        fpath = os.path.join(LOG_PATH, fname)
        if fname.endswith(".log") and os.path.isfile(fpath):
            age = (now - os.path.getmtime(fpath)) / (60 * 60 * 24)
            if age > LOG_RETENTION_DAYS:
                os.remove(fpath)


purge_old_logs()

# -----------------------------
# LOGGER SETUP
# -----------------------------
CORALLogger = logging.getLogger("CORALLogger")
CORALLogger.setLevel(logging.INFO)
CORALLogger.propagate = True  # Now allows bubbling if needed

formatter = logging.Formatter("[%(asctime)s] %(message)s", "%Y-%m-%d %H:%M:%S")

# File handler (rotating)
file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=3)
file_handler.setFormatter(formatter)
CORALLogger.addHandler(file_handler)


# Compression-aware filter for console
class CompressionAwareFilter(logging.Filter):
    def __init__(self, compressed: bool):
        super().__init__()
        self.compressed = compressed

    def filter(self, record):
        return not self.compressed or getattr(record, "show", False)


# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
console_handler.addFilter(CompressionAwareFilter(COMPRESSED_MODE))
CORALLogger.addHandler(console_handler)

logger = CORALLogger  # Global handle


# -----------------------------
# LOGGING HELPERS
# -----------------------------
def log_status(msg: str, show: bool = True):
    CORALLogger.info(msg, extra={"show": show})


def log_runtime_start(script_name: str = "Unnamed Script"):
    log_status(f"🚀 Starting `{script_name}` | Session {SESSION_TIMESTAMP}")


def log_stage(name: str):
    log_status(f"➡️  {name}...", show=True)


def log_success(name: str):
    log_status(f"✔️  {name}", show=True)


def log_error(msg: str, exc: Exception = None):
    detail = f"{msg} | Exception: {str(exc)}" if exc else msg
    log_status(f"❌ {detail}", show=True)


def log_resource_usage(tag="📊 Resource Check"):
    process = psutil.Process(os.getpid())
    mem = process.memory_info().rss / (1024 * 1024)
    cpu = process.cpu_percent(interval=0.1)
    log_status(f"{tag} | Memory: {mem:.2f} MB | CPU: {cpu:.1f}%", show=True)


def log_step_progress(stage: str, done: int, total: int):
    pct = (done / total) * 100 if total else 0
    log_status(f"🔄 {stage}: {done}/{total} ({pct:.1f}%)", show=True)


# -----------------------------
# SUBPROCESS UTILITY (Live Console + Captured Logs)
# -----------------------------
def run_script_streamed(script_path, env=None):
    """
    Runs a script using subprocess.Popen and streams stdout to the console in real-time.
    Captures both stdout and stderr for logging/reporting.

    Returns:
        (returncode, stdout_str, stderr_str)
    """
    env = env or os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"  # 🔥 Key fix: Force unbuffered output

    process = subprocess.Popen(
        ["python3", "-u", script_path],  # 🔥 '-u' forces unbuffered mode
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # Combine stderr into stdout to avoid async messiness
        text=True,
        env=env
    )

    stdout_lines = []

    for line in process.stdout:
        print(line, end="")  # Realtime output
        stdout_lines.append(line)

    process.wait()
    return process.returncode, "".join(stdout_lines), ""  # stderr is now in stdout