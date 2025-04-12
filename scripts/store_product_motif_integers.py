#!/usr/bin/env python3
import ray
from config.config import (
    INTEGER_MIN,  # noqa: F401
    INTEGER_MAX,
    CHUNK_SIZE,
    F,
    print_active_mode
)
from config.logger import logger
from config.class_utils import PipelineStep, SchemaAwareWriter
from config.db_utils import (
    read_resume_checkpoint,
    write_resume_checkpoint,
    port_and_prune_sqlite_to_duckdb,
    read_duckdb
)
from config.ray_utils import run_ray_futures

# -----------------------------
# Config
# -----------------------------
PRODUCT_LIMIT = F * INTEGER_MAX


# -----------------------------
# Ray Remote
# -----------------------------
@ray.remote
def compute_product_batch(batch, sequence_map):
    results = []
    for row in batch:
        motif_A, An, Ln, H, T = row
        product_An = An * F
        if product_An <= PRODUCT_LIMIT:
            results.append((motif_A, product_An, Ln, H, T))
    return results


# -----------------------------
# Main Logic
# -----------------------------
@PipelineStep("Generate product motif integers", logger.info)
def generate_product_motif_integers():
    print_active_mode()
    ray.init(ignore_reinit_error=True)

    writer = SchemaAwareWriter("products")

    # Load motif integers with mode filtering
    motif_rows = read_duckdb(
        table_name="integers",
        as_dict=True,
        filter_by_mode=True
    )

    if not motif_rows:
        logger.warning("⚠️ No motif integers found for current mode.")
        return

    full_rows = [
        (row["motif_A"], row["integer_An"], row["integer_L"], row["H"], row["T"])
        for row in motif_rows
        if row["integer_An"] <= INTEGER_MAX
    ]

    # Resumption logic
    try:
        last_seen = read_resume_checkpoint("products")
    except FileNotFoundError:
        last_seen = 0

    remaining_rows = [row for row in full_rows if row[1] * F > last_seen]

    if not remaining_rows:
        logger.info("✅ Product motif integers are already complete for this range.")
        return

    logger.info(f"🔁 Resuming from product_An > {last_seen}")
    logger.info(f"📈 {len(remaining_rows)} subsequences to process...")

    # Ray Batch Execution
    chunks = [remaining_rows[i:i + CHUNK_SIZE] for i in range(0, len(remaining_rows), CHUNK_SIZE)]
    run_ray_futures(
        chunked_data=chunks,
        remote_function=compute_product_batch,
        store_callback=writer.write_rows_mode_filtered,
        log_prefix="ProductMotif"
    )

    writer.close()

    max_written = port_and_prune_sqlite_to_duckdb(
        table="products",
        scope="local"
    )
    write_resume_checkpoint("products", max_written)

    logger.info("🎉 Product motif generation complete.")


# -----------------------------
# Entrypoint
# -----------------------------
if __name__ == "__main__":
    generate_product_motif_integers()
    ray.shutdown()