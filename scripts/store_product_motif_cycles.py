#!/usr/bin/env python3
import os
import json
import ray
from itertools import chain

from config.config import TEXT_OUTPUT_PATH, CACHE_CONFIG, BATCH_SIZE, TWIN_ONLY, PARTITION_MODE, INTEGER_LIMIT
from config.db_utils import connect_db, setup_product_motif_cycles_db, get_project_root

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

def fetch_existing_product_ns():
    conn = connect_db("product_motif_cycles.db")
    cursor = conn.cursor()
    cursor.execute("SELECT product_N FROM product_motif_cycles")
    existing = {row[0] for row in cursor.fetchall()}
    conn.close()
    return existing

def fetch_product_motif_integers():
    conn = connect_db("product_motif_integers.db")
    cursor = conn.cursor()

    cursor.execute("ATTACH DATABASE ? AS mp", (os.path.join(get_project_root(), "databases", "motif_parameters.db"),))

    query = """
        SELECT p.motif_A, p.product_N, p.integer_L
        FROM product_motif_integers p
        JOIN mp.motif_parameters m ON p.motif_A = m.A
    """

    if TWIN_ONLY and not PARTITION_MODE:
        query += " WHERE m.B = m.Y + 1"
    elif PARTITION_MODE and not TWIN_ONLY:
        query += " WHERE m.B != m.Y + 1"

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
            trimmed_cycles[product_N] = trim_cycle_at_L(full_cycle, integer_L)
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

    existing_product_ns = fetch_existing_product_ns()
    print(f"🔎 Skipping {len(existing_product_ns)} already stored product_N values.")

    data = [
        row for row in chain.from_iterable(fetch_product_motif_integers())
        if row[1] not in existing_product_ns
    ]

    chunks = [data[i:i + BATCH_SIZE] for i in range(0, len(data), BATCH_SIZE)]
    futures = [fetch_trimmed_cycles.remote(chunk) for chunk in chunks]
    results = ray.get(futures)

    for product_cycles, _ in results:
        store_cycles(product_cycles)

    # Output
    os.makedirs(TEXT_OUTPUT_PATH, exist_ok=True)
    output_file = os.path.join(TEXT_OUTPUT_PATH, "product_motif_cycles_output.txt")

    conn = connect_db("product_motif_cycles.db")
    cursor = conn.cursor()

    cursor.execute("ATTACH DATABASE ? AS pmi", (os.path.join(get_project_root(), "databases", "product_motif_integers.db"),))
    cursor.execute("ATTACH DATABASE ? AS params", (os.path.join(get_project_root(), "databases", "motif_parameters.db"),))

    if TWIN_ONLY:
        filter_clause = "AND m.B = m.Y + 1"
    elif PARTITION_MODE:
        filter_clause = "AND m.B != m.Y + 1"
    else:
        filter_clause = ""

    cursor.execute(f"""
        SELECT c.product_N, c.cycle
        FROM product_motif_cycles c
        JOIN pmi.product_motif_integers pi ON c.product_N = pi.product_N
        JOIN params.motif_parameters m ON pi.motif_A = m.A
        WHERE c.product_N <= ?
        {filter_clause}
    """, (3 * INTEGER_LIMIT,))
    rows = cursor.fetchall()
    conn.close()

    with open(output_file, "w") as f:
        f.write("### Product Motif Cycles Output (Trimmed at L) ###\n\n")
        for product_N, cycle_json in rows:
            f.write(f"{product_N}: {json.loads(cycle_json)}\n")

        all_missing = list(chain.from_iterable([missing for _, missing in results]))
        if all_missing:
            f.write("\n### Missing Collatz Cycles (Not Found in collatz_sequences.db) ###\n")
            f.write(", ".join(map(str, all_missing)) + "\n")

    print(f"✅ Step 5 complete. {len(data)} new cycles processed. Output written to:\n→ {output_file}")

if __name__ == "__main__":
    process_product_motif_cycles_ray()
