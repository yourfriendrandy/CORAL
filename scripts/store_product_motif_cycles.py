#!/usr/bin/env python3
import os
import json
import ray
from itertools import chain

from config.config import DB_PATH, TEXT_OUTPUT_PATH, CACHE_CONFIG, BATCH_SIZE, TWIN_ONLY
from config.db_utils import connect_db, setup_product_motif_cycles_db

# -----------------------------
# Ray Initialization
# -----------------------------
ray.init(ignore_reinit_error=True, num_cpus=os.cpu_count() - 1)

# -----------------------------
# Helper Functions
# -----------------------------
def trim_cycle_at_L(cycle, L):
    if L in cycle:
        return cycle[: cycle.index(L) + 1]
    return cycle

def fetch_product_motif_integers():
    """Yields batches of product motif integers, filtered by TWIN_ONLY condition."""
    conn = connect_db("product_motif_integers.db")
    cursor = conn.cursor()

    # Attach motif_parameters.db
    cursor.execute(f"ATTACH DATABASE '{os.path.join(DB_PATH, 'motif_parameters.db')}' AS mp")

    query = """
        SELECT p.motif_A, p.product_N, p.integer_L
        FROM product_motif_integers p
        JOIN mp.motif_parameters m ON p.motif_A = m.A
    """
    if TWIN_ONLY:
        query += " WHERE m.B = m.Y + 1"

    cursor.execute(query)
    while True:
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break
        yield rows

    conn.close()

@ray.remote
def fetch_trimmed_cycles(batch):
    conn = connect_db("collatz_sequences.db")
    cursor = conn.cursor()

    # Validate rows and unpack
    valid_batch = [row for row in batch if len(row) == 3]
    if not valid_batch:
        return {}, []

    product_N_batch = [row[1] for row in valid_batch]
    placeholders = ','.join(['?'] * len(product_N_batch))
    query = f"SELECT n, sequence FROM collatz_cache WHERE n IN ({placeholders})"

    cursor.execute(query, product_N_batch)
    fetched = {row[0]: json.loads(row[1]) for row in cursor.fetchall()}
    conn.close()

    trimmed_cycles = {}
    missing_products = []

    for _, product_N, integer_L in valid_batch:
        if product_N in fetched:
            full_cycle = fetched[product_N]
            if integer_L in full_cycle:
                trimmed = full_cycle[: full_cycle.index(integer_L) + 1]
                trimmed_cycles[product_N] = trimmed
            else:
                trimmed_cycles[product_N] = full_cycle
        else:
            missing_products.append(product_N)

    return trimmed_cycles, missing_products

def store_cycles(product_cycles):
    conn = connect_db("product_motif_cycles.db")
    cursor = conn.cursor()
    cursor.executemany(
        "INSERT OR IGNORE INTO product_motif_cycles (product_N, cycle) VALUES (?, ?)",
        [(k, json.dumps(v)) for k, v in product_cycles.items()]
    )
    conn.commit()
    conn.close()

# -----------------------------
# Main Execution
# -----------------------------
def process_product_motif_cycles_ray():
    print("🚀 Using Ray to process product motif cycles...")
    setup_product_motif_cycles_db()

    # Flatten generator output
    data = list(chain.from_iterable(fetch_product_motif_integers()))
    chunks = [data[i:i + BATCH_SIZE] for i in range(0, len(data), BATCH_SIZE)]

    futures = [fetch_trimmed_cycles.remote(chunk) for chunk in chunks]
    results = ray.get(futures)

    output_file = os.path.join(TEXT_OUTPUT_PATH, "product_motif_cycles_output.txt")
    all_missing = []

    with open(output_file, "w") as f:
        f.write("### Product Motif Cycles Output (Trimmed at L) ###\n\n")
        for product_cycles, missing in results:
            store_cycles(product_cycles)
            for product_N, cycle in product_cycles.items():
                f.write(f"{product_N}: {cycle}\n")
            all_missing.extend(missing)

        if all_missing:
            f.write("\n### Missing Collatz Cycles (Not Found in collatz_sequences.db) ###\n")
            f.write(", ".join(map(str, all_missing)) + "\n")

    print("✅ Step 5 complete. Results saved to product_motif_cycles.db and output file.")

if __name__ == "__main__":
    process_product_motif_cycles_ray()
