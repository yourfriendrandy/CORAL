#!/usr/bin/env python3
import os
from config.config import (
    INTEGER_LIMIT,
    TWIN_ONLY,
    PARTITION_MODE,
    TEXT_OUTPUT_PATH,
    print_active_mode,
)
from config.db_utils import (
    connect_db,
    setup_product_motif_integers_db,
    get_project_root
)

PRODUCT_LIMIT = 3 * INTEGER_LIMIT  # 3An upper bound

# -----------------------------
# Helper Functions
# -----------------------------
def fetch_motif_integers():
    conn = connect_db("motif_integers.db")
    cursor = conn.cursor()

    motif_param_path = os.path.join(get_project_root(), "databases", "motif_parameters.db")
    cursor.execute("ATTACH DATABASE ? AS params", (motif_param_path,))

    if TWIN_ONLY:
        query = """
            SELECT m.motif_A, m.integer_N, m.integer_L
            FROM motif_integers m
            JOIN params.motif_parameters p ON m.motif_A = p.A
            WHERE p.B = p.Y + 1
        """
    elif PARTITION_MODE:
        query = """
            SELECT m.motif_A, m.integer_N, m.integer_L
            FROM motif_integers m
            JOIN params.motif_parameters p ON m.motif_A = p.A
            WHERE p.B != p.Y + 1
        """
    else:
        query = "SELECT motif_A, integer_N, integer_L FROM motif_integers"

    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    return rows

def get_last_product_An():
    conn = connect_db("product_motif_integers.db")
    cursor = conn.cursor()
    cursor.execute("SELECT motif_A, MAX(product_N) FROM product_motif_integers GROUP BY motif_A")
    rows = cursor.fetchall()
    conn.close()

    last_seen = {motif_A: max_product_N // 3 for motif_A, max_product_N in rows if max_product_N is not None}
    if last_seen:
        print(f"🔁 Resumption Check:")
        print(f"   → {len(last_seen)} motifs found in product_motif_integers")
        print(f"   → Min last An: {min(last_seen.values())}")
        print(f"   → Max last An: {max(last_seen.values())}")
    else:
        print("ℹ️ No existing entries found in product_motif_integers. Full pass will be generated.")
    return last_seen

def generate_product_motif_integers(motif_integers, last_seen_An):
    product_motif_integers = {}
    for motif_A, An, Ln in motif_integers:
        if motif_A in last_seen_An and An <= last_seen_An[motif_A]:
            continue
        product_N = An * 3
        if product_N <= PRODUCT_LIMIT:
            product_motif_integers.setdefault(motif_A, []).append((product_N, Ln))
    return product_motif_integers

def query_output_product_motifs():
    conn = connect_db("product_motif_integers.db")
    cursor = conn.cursor()
    motif_param_path = os.path.join(get_project_root(), "databases", "motif_parameters.db")
    cursor.execute("ATTACH DATABASE ? AS params", (motif_param_path,))

    query = """
        SELECT p.motif_A, p.product_N, p.integer_L, m.B, m.Y
        FROM product_motif_integers p
        JOIN params.motif_parameters m ON p.motif_A = m.A
        WHERE p.product_N <= ?
    """
    if TWIN_ONLY:
        query += " AND m.B = m.Y + 1"
    elif PARTITION_MODE:
        query += " AND m.B != m.Y + 1"

    cursor.execute(query, (PRODUCT_LIMIT,))
    rows = cursor.fetchall()
    conn.close()
    return rows

# -----------------------------
# Main Execution
# -----------------------------
def process_product_motif_integers():
    print_active_mode()
    print(f"Processing product motif integers (3An) up to {PRODUCT_LIMIT}...")

    setup_product_motif_integers_db()
    motif_integers = fetch_motif_integers()
    last_seen = get_last_product_An()
    product_data = generate_product_motif_integers(motif_integers, last_seen)

    # ✅ Insert into DB
    all_rows = [
        (motif_A, product_N, integer_L)
        for motif_A, values in product_data.items()
        for product_N, integer_L in values
    ]
    if all_rows:
        conn = connect_db("product_motif_integers.db")
        cursor = conn.cursor()
        cursor.executemany('''
            INSERT OR IGNORE INTO product_motif_integers (motif_A, product_N, integer_L)
            VALUES (?, ?, ?)
        ''', all_rows)
        conn.commit()
        conn.close()

    # ✅ Output entire contents based on toggle and limit
    os.makedirs(TEXT_OUTPUT_PATH, exist_ok=True)
    output_file = os.path.join(TEXT_OUTPUT_PATH, "product_motif_int_output.txt")
    motif_output = query_output_product_motifs()

    with open(output_file, "w") as file:
        file.write(f"Product Motif Integers (3An) up to {PRODUCT_LIMIT}:\n\n")
        grouped = {}

        for A, product_N, L, B, Y in motif_output:
            grouped.setdefault((A, B, Y), []).append((product_N, L))

        for (A, B, Y), values in sorted(grouped.items()):
            L0 = values[0][1]
            file.write(f"A0={A}, L0={L0}, B={B}, Y={Y}:\n")
            file.write("    " + ", ".join(str(prod_N) for prod_N, _ in values) + "\n\n")

    print(f"✅ Step 3 complete. {len(all_rows)} new product motif integers stored and output saved to:\n→ {output_file}")

if __name__ == "__main__":
    process_product_motif_integers()
