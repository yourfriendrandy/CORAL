#!/usr/bin/env python3
import os

from config.config import INTEGER_LIMIT, TEXT_OUTPUT_PATH, TWIN_ONLY, DB_PATH
from config.db_utils import (
    connect_db,
    setup_product_motif_integers_db
)

PRODUCT_LIMIT = 3 * INTEGER_LIMIT  # The valid range for 3An values

# -----------------------------
# Helper Functions
# -----------------------------
def fetch_motif_integers():
    """Retrieves all integers from motif_integers.db with optional TWIN_ONLY filter."""
    conn = connect_db("motif_integers.db")
    cursor = conn.cursor()

    cursor.execute("ATTACH DATABASE ? AS params", (os.path.join(DB_PATH, "motif_parameters.db"),))

    if TWIN_ONLY:
        cursor.execute("""
            SELECT m.motif_A, m.integer_N, m.integer_L
            FROM motif_integers m
            JOIN params.motif_parameters p ON m.motif_A = p.A
            WHERE p.B = p.Y + 1
        """)
    else:
        cursor.execute("""
            SELECT m.motif_A, m.integer_N, m.integer_L
            FROM motif_integers m
        """)

    motif_integers = cursor.fetchall()
    conn.close()
    return motif_integers

def generate_product_motif_integers(motif_integers):
    """Generates 3An values and retains L."""
    product_motif_integers = {}
    for motif_A, integer_N, integer_L in motif_integers:
        product_N = integer_N * 3
        if product_N <= PRODUCT_LIMIT:
            if motif_A not in product_motif_integers:
                product_motif_integers[motif_A] = []
            product_motif_integers[motif_A].append((product_N, integer_L))
    return product_motif_integers

# -----------------------------
# Main Processing
# -----------------------------
def process_product_motif_integers():
    print(f"Processing product motif integers (3An) up to {PRODUCT_LIMIT}...")

    setup_product_motif_integers_db()

    motif_integers = fetch_motif_integers()
    product_motif_data = generate_product_motif_integers(motif_integers)

    output_file = os.path.join(TEXT_OUTPUT_PATH, "product_motif_int_output.txt")

    with open(output_file, "w") as file:
        file.write(f"Product Motif Integers (3An) up to {PRODUCT_LIMIT}:\n\n")
        for motif_A, values in product_motif_data.items():
            file.write(f"A0={motif_A}:\n")
            file.write("    " + ", ".join(f"{prod_N}" for prod_N, _ in values) + "\n\n")

    # Store in database
    conn = connect_db("product_motif_integers.db")
    cursor = conn.cursor()
    all_product_integers = [
        (motif_A, product_N, integer_L)
        for motif_A, values in product_motif_data.items()
        for product_N, integer_L in values
    ]
    cursor.executemany('''
        INSERT OR IGNORE INTO product_motif_integers (motif_A, product_N, integer_L)
        VALUES (?, ?, ?)
    ''', all_product_integers)
    conn.commit()
    conn.close()

    print(f"Step 3 complete. Product motif integers stored in product_motif_integers.db and saved to '{output_file}'.")

if __name__ == "__main__":
    process_product_motif_integers()
