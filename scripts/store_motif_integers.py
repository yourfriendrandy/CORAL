#!/usr/bin/env python3

import ray
from config.config import (
    INTEGER_MIN, INTEGER_MAX, M, Z,
    print_active_mode
)
from config.logger import logger
from config.db_utils import (
    setup_table,
    read_duckdb,
    read_resume_checkpoint,
    write_resume_checkpoint
)
from config.class_utils import SchemaAwareWriter, PipelineStep
from config.ray_utils import init_ray, run_ray_futures


# -----------------------------
# Step 1: Setup + Resume Logic
# -----------------------------
@PipelineStep("Setup motif_integers table", logger.info)
def initialize():
    setup_table("integers")


def determine_resume():
    try:
        return read_resume_checkpoint("integers") + 1
    except FileNotFoundError:
        return INTEGER_MIN


# -----------------------------
# Step 2: Load A₀ Motif Roots
# -----------------------------
@PipelineStep("Load A₀ motif roots", logger.info)
def load_motif_roots():
    return read_duckdb("parameters", as_dict=True, filter_by_mode=True)


# -----------------------------
# Step 3: Generate An/Ln Values
# -----------------------------
@ray.remote
def generate_subsequences(motif_batch):
    results = []
    for motif in motif_batch:
        A0, L0, H, T = motif["A"], motif["L"], motif["H"], motif["T"]
        for B in range(1, Z):
            step_A = Z ** (T + B)
            step_L = Z * (M ** H)
            X = 0
            while True:
                An = A0 + X * step_A
                Ln = L0 + X * step_L
                if An > INTEGER_MAX:
                    break
                results.append((A0, An, Ln, H, T))
                X += 1
    return results


# -----------------------------
# Step 4: Main Runner
# -----------------------------
def main():
    print_active_mode()
    init_ray()
    initialize()

    motif_roots = load_motif_roots()
    chunks = [motif_roots[i:i + 25] for i in range(0, len(motif_roots), 25)]

    writer = SchemaAwareWriter("integers")
    max_an = 0

    def store(batch):
        nonlocal max_an
        writer.write_rows(batch)
        if batch:
            local_max = max(row[1] for row in batch)  # index 1 is An
            max_an = max(max_an, local_max)

    run_ray_futures(
        chunked_data=chunks,
        remote_function=generate_subsequences,
        store_callback=store,
        log_prefix="Motif Subsequence"
    )

    writer.close()
    write_resume_checkpoint("integers", max_an)
    logger.info(f"✅ motif_integers complete. Last checkpointed An = {max_an}")


# -----------------------------
# Entrypoint
# -----------------------------
if __name__ == "__main__":
    main()
    ray.shutdown()