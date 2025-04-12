#!/usr/bin/env python3
import json
import ray
from itertools import chain

from config.config import (
    INTEGER_MAX,
    TWIN_ONLY,
    PARTITION_MODE,
    print_active_mode,
)
from config.logger import logger
from config.class_utils import SchemaAwareWriter, PipelineStep
from config.ray_utils import run_ray_futures
from config.db_utils import read_duckdb
from config.path_utils import get_path

# -----------------------------
# Ray Task
# -----------------------------
@ray.remote
def extract_cycle_integers(chunk: list[tuple[int, str]]) -> list[tuple[int]]:
    """Ray task to extract unique integers from (n, sequence_json) chunks."""
    unique = set()
    for n, json_blob in chunk:
        try:
            seq = json.loads(json_blob)
            unique.update(i for i in seq if i <= 3 * INTEGER_MAX)
        except Exception:
            continue
    return [(val,) for val in unique]

# -----------------------------
# Main Driver
# -----------------------------
@PipelineStep("Generate full_lookup from motif + product cycles", logger.info)
def process_full_lookup():
    print_active_mode()
    ray.init(ignore_reinit_error=True)

    # 🧠 Read cycle data
    motif_cycles = read_duckdb("integer_cycles", as_dict=False)
    product_cycles = read_duckdb("product_cycles", as_dict=False)

    all_rows = list(chain(motif_cycles, product_cycles))

    def chunkify(lst, size):
        return [lst[i:i + size] for i in range(0, len(lst), size)]

    chunks = chunkify(all_rows, size=500)

    logger.info(f"📦 Extracting integers from {len(all_rows)} total cycles across motif + product...")

    # 🚀 Run in parallel
    results = run_ray_futures(
        chunked_data=chunks,
        remote_function=extract_cycle_integers,
        store_callback=None,
        log_prefix="Full Lookup",
    )

    # 🧮 Deduplicate + sort
    all_integers = sorted({val[0] for chunk in results for val in chunk})

    # 📝 Write into all relevant DBs
    rows = [(val,) for val in all_integers]
    writers = {
        "full_lookup": SchemaAwareWriter("lookup"),
        "full_lookup_twin": SchemaAwareWriter("lookup_twin"),
        "full_lookup_partitioned": SchemaAwareWriter("lookup_partitioned"),
    }

    # Always write full set to main lookup table
    writers["full_lookup"].write_rows(rows)

    # Mode-specific filtered output
    if TWIN_ONLY:
        writers["full_lookup_twin"].write_rows(rows)
    elif PARTITION_MODE:
        writers["full_lookup_partitioned"].write_rows(rows)

    for w in writers.values():
        w.close()

    # 📄 Write output file
    mode_suffix = (
        "twin_output.txt" if TWIN_ONLY
        else "partitioned_output.txt" if PARTITION_MODE
        else "output.txt"
    )
    output_path = get_path("text_output", "tagged", f"full_lookup_{mode_suffix}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        f.write("### Full Lookup Output (Sorted Unique Integers) ###\n\n")
        f.write("\n".join(map(str, all_integers)))

    logger.info(f"📄 Full lookup output written to {output_path}")
    logger.info("✅ Full lookup generation complete.")

# Entrypoint
if __name__ == "__main__":
    process_full_lookup()
    ray.shutdown()