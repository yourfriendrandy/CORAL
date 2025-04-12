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
# LOG VERBOSITY MODE
# -----------------------------
try:
    LOCAL_COMPRESSED_LOG
except NameError:
    LOCAL_COMPRESSED_LOG = None

ENV_COMPRESSED_LOG = os.getenv("CORAL_COMPRESSED", "false").lower() in ("true", "1", "yes")


def resolve_compression_mode():
    return LOCAL_COMPRESSED_LOG if LOCAL_COMPRESSED_LOG is not None else ENV_COMPRESSED_LOG


COMPRESSED_LOG = False


# -----------------------------
# SANITY CHECKER FLAG
# -----------------------------
SANITY_CHECK = False

# ------------------------------
# SYSTEM CONSTRAINTS
# ------------------------------
M = 3
Z = 2
ASSUME_ONE_STEP_REDUCTION = True
# ------------------------------
# INTEGER RANGE
# ------------------------------
INTEGER_MIN = 1
INTEGER_MAX = 1000
INTEGER_RANGE = (INTEGER_MIN, INTEGER_MAX)
# Note: 0 is excluded from processing logic.
#       It's always mapped to 0 in all CORAL systems, and does not participate in expansion/contraction.
# ------------------------------
# OPERATOR POLARITY (GUI-set)
# ------------------------------
B_POLARITY = "Bplus"

# Map B_POLARITY to internal keyword
B_EXPANSION_POLARITY = {
    "Bplus": "positive",
    "Bminus": "negative",
    "Bboth": "both"
}[B_POLARITY]

# ------------------------------
# DYNAMICALLY DERIVED N_POLARITY
# ------------------------------


def get_n_polarity(min_val: int, max_val: int) -> str:
    if min_val >= 0:
        return "Nplus"
    elif max_val <= 0:
        return "Nminus"
    else:
        return "Nboth"


N_POLARITY = "Nplus"

# ------------------------------
# GET GOING AFTER F?
# ------------------------------
F = 7  # Confirmed factor
AUTO_RESUME = True


# ------------------------------
# CORAL SYSTEM TAG
# ------------------------------
def get_coral_tag():
    return f"CORAL_{M}_{Z}_{B_POLARITY}_{N_POLARITY}"


CORAL_TAG = "CORAL_3_2_Bplus_Nplus"
# -----------------------------
# BASE PATHING
# -----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)


# -----------------------------
# DATABASE & TEXT OUTPUT ROUTING
# -----------------------------
def resolve_active_modes():
    twin = LOCAL_TWIN_ONLY if LOCAL_TWIN_ONLY is not None else ENV_TWIN_ONLY
    part = LOCAL_PARTITION_MODE if LOCAL_PARTITION_MODE is not None else ENV_PARTITION_MODE
    return twin, part


TWIN_ONLY = False
DATABASE_PATH = os.path.join(ROOT_DIR, "databases", get_coral_tag())
TEXT_OUTPUT_ROOT = os.path.join(ROOT_DIR, "text_output", get_coral_tag())

TEXT_OUTPUT_ANALYSIS = os.path.join(TEXT_OUTPUT_ROOT, "text_output_analysis")

TEXT_OUTPUT_DIRS = {
    "default": os.path.join(TEXT_OUTPUT_ROOT, "text_output_all_motifs"),
    "twin": os.path.join(TEXT_OUTPUT_ROOT, "text_output_twin_only"),
    "partition": os.path.join(TEXT_OUTPUT_ROOT, "text_output_partition_mode"),
}

TEXT_OUTPUT_ACTIVE = (
    TEXT_OUTPUT_DIRS["partition"] if PARTITION_MODE
    else TEXT_OUTPUT_DIRS["twin"] if TWIN_ONLY
    else TEXT_OUTPUT_DIRS["default"]
)

ACTIVE_MODES = (
    "partition" if PARTITION_MODE
    else "twin" if TWIN_ONLY
    else "all"
)

# Create all necessary directories
for path in list(TEXT_OUTPUT_DIRS.values()) + [TEXT_OUTPUT_ANALYSIS, DATABASE_PATH]:
    os.makedirs(path, exist_ok=True)


def print_active_mode():
    """Prints which mode is currently active."""
    print("⚙️  Output Mode Active:", end=" ")
    print(f"{ACTIVE_MODES.upper()} mode")


# -----------------------------
# BATCH / CHUNK / PORT SETTINGS
# -----------------------------
BATCH_SIZE = 1000   # Default number of records per DB fetch/write
CHUNK_SIZE = 50  # Default number of integers per processing chunk
PORT_BATCH_SIZE = 1  # Number of compute batches before flushing from SQLite3 to DuckDB
