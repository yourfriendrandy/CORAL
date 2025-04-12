#!/usr/bin/env python3
import ray
import argparse
from config.config import (
    INTEGER_MIN, INTEGER_MAX, CHUNK_SIZE, PORT_BATCH_SIZE,
    M, Z, CORAL_TAG, print_active_mode
)
from config.logger import logger
from config.db_utils import (
    setup_table,
    port_and_prune_sqlite_to_duckdb,
    read_resume_checkpoint, write_resume_checkpoint
)
from config.class_utils import SchemaAwareWriter, PipelineStep, DiskLoopTracker
from config.ray_utils import stream_ray_batches, init_ray
from config.path_utils import get_path

# -----------------------------
# Argument Parsing
# -----------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--resume_from_max", action="store_true", help="Resume from INTEGER_MAX checkpoint")
parser.add_argument("--phase2_resume", action="store_true", help="Resume from F-expanded phase")
args = parser.parse_args()

# -----------------------------
# CORAL Sequence Generator
# -----------------------------
def generate_CORAL_sequence(n, m, z, entry_points=None):
    seq, visited = [], set()
    while True:
        if n in visited:
            loop_start = seq.index(n)
            return seq[:loop_start], seq[loop_start:]
        if entry_points and n in entry_points:
            return seq, []
        visited.add(n)
        seq.append(n)
        if n % z == 0:
            n //= z
        else:
            for b in range(1, z):
                if (m * n + b) % z == 0:
                    n = (m * n + b) // z
                    break
            else:
                raise ValueError(f"Expansion failed at n={n} for m={m}, z={z}")

# -----------------------------
# Ray Worker
# -----------------------------
@ray.remote
def compute_batch_worker(chunk, m, z):
    writer = SchemaAwareWriter("system_cache", duck=False)
    loop_tracker = DiskLoopTracker(use_duck=False)
    entry_points = loop_tracker.get_all_entry_points()

    for n in chunk:
        try:
            main_seq, loop = generate_CORAL_sequence(n, m, z, entry_points)
            writer.write_json_rows([(n, main_seq + loop)])
            if loop:
                loop_tracker.add_loop_with_entry(loop, main_seq + loop, source_n=n)
        except Exception as e:
            logger.warning(f"⚠️ Failed for n={n}: {e}")

    loop_tracker.close()
    return True

# -----------------------------
# Export Infinite Loops
# -----------------------------
@PipelineStep("Export infinite loops", logger.info)
def export_infinite_loops():
    loop_tracker = DiskLoopTracker()
    output_path = get_path("text_output", "infinite_loops.txt")
    loop_tracker.export_all(output_path, system_tag=f"{CORAL_TAG} (M={M}, Z={Z})")
    loop_tracker.close()

# -----------------------------
# Get Range of Integers
# -----------------------------
@PipelineStep("Determine resume point", logger.info)
def get_range():
    from config.config import F  # Deferred import after arg parsing

    if args.resume_from_max:
        start = read_resume_checkpoint("system_cache") + 1
        end = INTEGER_MAX
        logger.info("📍 Phase 1 resume (resume_from_max)")

    elif args.phase2_resume:
        if INTEGER_MIN <= -1:
            start = F * INTEGER_MIN
            end = INTEGER_MIN - 1
            logger.info(f"📍 Phase 2 resume (lower): {start} → {end}")
        elif INTEGER_MAX >= 1:
            start = INTEGER_MAX + 1
            end = F * INTEGER_MAX
            logger.info(f"📍 Phase 2 resume (upper): {start} → {end}")
        else:
            logger.warning("⚠️ Phase 2 conditions not met — defaulting to main range.")
            start, end = INTEGER_MIN, INTEGER_MAX
    else:
        start, end = INTEGER_MIN, INTEGER_MAX
        logger.info("📍 Fresh run — full range")

    full_range = (
        list(range(start, end + 1)) if start <= end
        else list(range(start, end - 1, -1))
    )
    chunks = [full_range[i:i + CHUNK_SIZE] for i in range(0, len(full_range), CHUNK_SIZE)]
    logger.info(f"📈 Range: {start} to {end}, total chunks: {len(chunks)}")
    return chunks, end

# -----------------------------
# Port Callback
# -----------------------------
def port_callback():
    retained = port_and_prune_sqlite_to_duckdb("system_cache")
    write_resume_checkpoint("system_cache", retained)
    logger.info(f"📥 Ported up to n = {retained}")

# -----------------------------
# Pipeline Entry Point
# -----------------------------
@PipelineStep("Run CORAL Pipeline", logger.info)
def populate_coral_db_disk_based():
    print_active_mode()
    init_ray()
    setup_table("system_cache")

    chunks, end = get_range()

    @PipelineStep("Sequence generation with Ray", logger.info)
    def run_ray():
        stream_ray_batches(
            chunked_data=chunks,
            remote_function=compute_batch_worker,
            remote_args=(M, Z),
            store_callback=lambda _: logger.info("✅ Chunk complete"),
            max_in_flight=4,
            port_every=PORT_BATCH_SIZE,
            port_callback=port_callback,
            log_prefix="CORAL"
        )

    run_ray()
    port_and_prune_sqlite_to_duckdb("system_cache")
    export_infinite_loops()

    logger.info(f"✅ CORAL complete for {CORAL_TAG} — M={M}, Z={Z}, up to n={end}")

# -----------------------------
# Entrypoint
# -----------------------------
if __name__ == "__main__":
    populate_coral_db_disk_based()
    ray.shutdown()