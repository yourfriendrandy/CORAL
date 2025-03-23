#!/usr/bin/env python3
import os
import json
import ray

from config.config import TEXT_OUTPUT_PATH, BATCH_SIZE, INTEGER_LIMIT, TWIN_ONLY, DB_PATH
from config.db_utils import connect_db, setup_full_lookup_db

# -----------------------------
# Initialize Ray
# -----------------------------
ray.shutdown()
ray.init(ignore_reinit_error=True, num_cpus=os.cpu_count() - 1)

@ray.remote
def extract_unique_integers_from_cycles(db_name, cycle_table, join_condition, cycle_column):
    """
    Reads and extracts unique integers from filtered Collatz cycles.
    Applies JOIN with motif_parameters for TWIN_ONLY filtering.
    """
    conn = connect_db(db_name)
    cursor = conn.cursor()
    cursor.execute(f"ATTACH DATABASE '{os.path.join(DB_PATH, 'motif_parameters.db')}' AS mp")

    query = f"""
        SELECT c.{cycle_column}
        FROM {cycle_table} c
        JOIN mp.motif_parameters m ON {join_condition}
    """
    if TWIN_ONLY:
        query += " WHERE m.B = m.Y + 1"

    cursor.execute(query)
    unique_ints = set()

    while True:
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break
        for (cycle_json,) in rows:
            cycle = json.loads(cycle_json)
            unique_ints.update(cycle)

    conn.close()
    return unique_ints

def store_unique_integers(unique_integers):
    """Stores unique integers into full_lookup.db."""
    conn, cursor = setup_full_lookup_db()
    cursor.executemany(
        "INSERT OR IGNORE INTO full_lookup (unique_int) VALUES (?)",
        [(num,) for num in unique_integers]
    )
    conn.commit()
    conn.close()
    print(f"✅ Stored {len(unique_integers)} unique integers in full_lookup.db.")

def write_output_file(unique_integers):
    """Writes sorted unique integers to a text file."""
    output_file = os.path.join(TEXT_OUTPUT_PATH, "full_lookup_output.txt")
    if unique_integers:
        with open(output_file, "w") as file:
            file.write("### Full Lookup Output (Sorted Unique Integers) ###\n\n")
            file.write("\n".join(map(str, sorted(unique_integers))))
        print(f"📄 Output saved to '{output_file}'.")
    else:
        print("⚠️ No unique integers found. Skipping text output.")

# -----------------------------
# Main Execution
# -----------------------------
if __name__ == "__main__":
    print("🔁 Starting full lookup with Ray...")

    # Filter motif_cycles by motif_integer == A
    motif_future = extract_unique_integers_from_cycles.remote(
        "motif_cycles.db",
        "motif_cycles",
        "c.motif_integer = m.A",
        "cycle"
    )

    # Filter product_motif_cycles by product_N / 3 == A
    product_future = extract_unique_integers_from_cycles.remote(
        "product_motif_cycles.db",
        "product_motif_cycles",
        "c.product_N / 3 = m.A",
        "cycle"
    )

    motif_unique, product_unique = ray.get([motif_future, product_future])
    all_unique = motif_unique.union(product_unique)

    store_unique_integers(all_unique)
    write_output_file(all_unique)

    print("✅ Step 6 complete. Filtered unique integers stored in `full_lookup.db` and written to text output.")
