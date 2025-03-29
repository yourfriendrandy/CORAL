import os

# -----------------------------
# LOCAL OVERRIDE SUPPORT
# -----------------------------
try:
    LOCAL_TWIN_ONLY
except NameError:
    LOCAL_TWIN_ONLY = None

try:
    LOCAL_PARTITION_MODE
except NameError:
    LOCAL_PARTITION_MODE = None

# -----------------------------
# ENVIRONMENT-BASED FLAGS
# -----------------------------
ENV_TWIN_ONLY = os.getenv("TWIN_ONLY", "false").lower() in ("true", "1", "yes")
ENV_PARTITION_MODE = os.getenv("PARTITION_MODE", "false").lower() in ("true", "1", "yes")

TWIN_ONLY = False
PARTITION_MODE = False

# -----------------------------
# SANITY CHECKER FLAG
# -----------------------------
SANITY_CHECK = False

# -----------------------------
# INTEGER LIMIT
# -----------------------------
INTEGER_LIMIT = 1000000

# -----------------------------
# BASE PATHING
# -----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)

# -----------------------------
# TEXT OUTPUT ROUTING
# -----------------------------
if PARTITION_MODE:
    TEXT_OUTPUT_PATH = os.path.join(ROOT_DIR, "text_output_partition_mode")
elif TWIN_ONLY:
    TEXT_OUTPUT_PATH = os.path.join(ROOT_DIR, "text_output_twin_only")
else:
    TEXT_OUTPUT_PATH = os.path.join(ROOT_DIR, "text_output_all_motifs")

os.makedirs(TEXT_OUTPUT_PATH, exist_ok=True)

# -----------------------------
# BATCH / CHUNK SETTINGS
# -----------------------------
BATCH_SIZE = 10000   # Default number of records per DB fetch/write
CHUNK_SIZE = 10000   # Default number of integers per processing chunk

# -----------------------------
# CACHING CONFIGURATION
# -----------------------------
CACHE_CONFIG = {
    "LFU_GENERAL": 5000,
    "LFU_COLLATZ": 1000,
    "LFU_MODE_STEPS": 500,
}

# -----------------------------
# MODE CHECK HELPER
# -----------------------------
def print_active_mode():
    """Prints which mode is currently active."""
    print("⚙️  Output Mode Active:", end=" ")
    if PARTITION_MODE:
        print("PARTITION_MODE (non-twin motifs)")
    elif TWIN_ONLY:
        print("TWIN_ONLY (B = Y + 1 motifs only)")
    else:
        print("ALL motifs (no filtering)")
