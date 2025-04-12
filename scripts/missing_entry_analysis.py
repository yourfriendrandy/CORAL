#!/usr/bin/env python3
import json
import ray

from config.config import (
    INTEGER_MAX,
    F,
    TWIN_ONLY,
    PARTITION_MODE,
    print_active_mode,
)
from config.logger import logger, log_success
from config.ray_utils import run_ray_futures
from config.class_utils import PipelineStep
from config.db_utils import read_duckdb, get_db_paths
from config.path_utils import get_path

# -----------------------------
# Constants
# -----------------------------
MAX_RANGE = F * INTEGER_MAX
FULL_LOOKUP_TABLE = (
    "lookup_twin"
    if TWIN_ONLY else "lookup_partitioned"
    if PARTITION_MODE else "lookup"
)
MOTIF_INTEGERS_TABLE = "integers"
SEQUENCE_TABLE = "system_cache"

# -----------------------------
# Global variables for Ray task
# -----------------------------
known_set = set()
motif_dict = {}

# -----------------------------
# Helper Functions
# -----------------------------
def get_known_integers():
    rows = read_duckdb(FULL_LOOKUP_TABLE, as_dict=False)
    return set(row[0] for row in rows)

def get_motif_entries():
    rows = read_duckdb(MOTIF_INTEGERS_TABLE, as_dict=False)
    return {row[1]: (row[2], row[3], row[4]) for row in rows}  # An → (L, H, T)

def load_sequence(n: int):
    _, duck_path = get_db_paths(SEQUENCE_TABLE)
    import duckdb
    with duckdb.connect(duck_path, read_only=True) as conn:
        result = conn.execute("SELECT sequence FROM system_cache WHERE n = ?", (n,)).fetchone()
        return json.loads(result[0]) if result else []

def classify_convergence(n):
    seq = load_sequence(n)
    for i, x in enumerate(seq):
        if x in known_set:
            if x in motif_dict:
                L, H, T = motif_dict[x]
                if i < L:
                    return ("before_L", x, i)
                elif i == L:
                    return ("at_L", x, i)
                else:
                    return ("after_L", x, i)
            return ("non_motif", x, i)
        if x in (1, 2, 4):
            return ("classic_loop", x, i)
    return ("unresolved", None, len(seq))

@ray.remote
def analyze_missing_batch(batch):
    return [
        {
            "n": n,
            "converges_to": target,
            "convergence_type": conv_type,
            "steps": steps,
        }
        for n in batch
        for conv_type, target, steps in [classify_convergence(n)]
    ]

# -----------------------------
# Main Driver
# -----------------------------
@PipelineStep("Analyze missing entry convergence", logger.info)
def process_missing_entry_analysis():
    print_active_mode()
    ray.init(ignore_reinit_error=True)

    global known_set, motif_dict
    known_set = get_known_integers()
    motif_dict = get_motif_entries()
    full_range = set(range(1, MAX_RANGE + 1))
    missing = sorted(full_range - known_set)

    logger.info(f"🔍 {len(missing)} missing integers out of {MAX_RANGE}")

    # Chunk data
    chunk_size = 1000
    chunks = [missing[i:i + chunk_size] for i in range(0, len(missing), chunk_size)]

    results = []

    run_ray_futures(
        chunked_data=chunks,
        remote_function=analyze_missing_batch,
        store_callback=lambda chunk_result: results.extend(chunk_result),
        log_prefix="Missing Entry"
    )

    # Output
    mode_tag = (
        "twin_only" if TWIN_ONLY else
        "partition_mode" if PARTITION_MODE else
        "default_mode"
    )
    output_path = get_path("text_output", "tagged", f"missing_entry_analysis_{mode_tag}.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    logger.info(f"✅ Missing entry analysis complete. Results written to: {output_path}")
    log_success(f"Coverage: {MAX_RANGE - len(missing)} / {MAX_RANGE} integers mapped.")

# Entrypoint
if __name__ == "__main__":
    process_missing_entry_analysis()
    ray.shutdown()