import os

# -----------------------------
# LOCAL OVERRIDE SUPPORT
# -----------------------------
# You can set LOCAL_TWIN_ONLY = True/False in any script before importing config.py to override this
try:
    LOCAL_TWIN_ONLY
except NameError:
    LOCAL_TWIN_ONLY = None

TWIN_ONLY = LOCAL_TWIN_ONLY if LOCAL_TWIN_ONLY is not None else (
    os.getenv("TWIN_ONLY", "false").lower() in ("true", "1", "yes")
)

# -----------------------------
# BASE PATHS
# -----------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(BASE_DIR, "databases")
DEFAULT_OUTPUT = os.path.join(BASE_DIR, "text_output")
TWIN_OUTPUT = os.path.join(BASE_DIR, "twin_text_output")
TEXT_OUTPUT_PATH = TWIN_OUTPUT if TWIN_ONLY else DEFAULT_OUTPUT

# Ensure required directories exist
os.makedirs(DB_PATH, exist_ok=True)
os.makedirs(TEXT_OUTPUT_PATH, exist_ok=True)

# -----------------------------
# CONSTANTS
# -----------------------------
INTEGER_LIMIT = int(os.getenv("INTEGER_LIMIT", "2000000"))

# -----------------------------
# BATCH / CHUNK SETTINGS
# -----------------------------
BATCH_SIZE = 10000   # Default number of records per DB fetch/write
CHUNK_SIZE = 10000   # Default number of integers per processing chunk

# -----------------------------
# CACHING CONFIGURATION
# -----------------------------
# You can adjust these to tune performance and memory usage
CACHE_CONFIG = {
    "LFU_GENERAL": 5000,
    "LFU_COLLATZ": 1000,
    "LFU_MODE_STEPS": 500,
}
