#!/usr/bin/env python3
import os

from config.config import (
    INTEGER_LIMIT,
    TWIN_ONLY,
    TEXT_OUTPUT_PATH
)
from config.db_utils import (
    connect_db,
    setup_motif_integers_db
)

# -----------------------------
# Helper Functions
# -----------------------------
def fetch_motif_parameters():
    """Fetches motif parameters from motif_parameters.db with optional TWIN_ONLY filter."""
    conn = connect_db("motif_parameters.db")
    cursor = conn.cursor()

    if TWIN_ONLY:
        cursor.execute("SELECT A, L, B, Y FROM motif_parameters WHERE B = Y + 1")
    else:
        cursor.execute("SELECT A, L, B, Y FROM motif_parameters")

    motifs = cursor.fetchall()
    conn.close()
    return motifs

def generate_motif_integers(A0, L0, B, Y):
    """Generates An and Ln values from motif equations."""
    motif_integers = []
    X = 0
    while True:
        An = A0 + (X * (2 ** (B + 1)))
        Ln = L0 + (X * (2 * (3 ** Y)))
        if An > INTEGER_LIMIT:
            break
        motif_integers.append((A0, An, Ln))
        X += 1
    return motif_integers

# -----------------------------
# Main Processing
# -----------------------------
def process_motif_integers():
    print(f"Processing motif integers up to {INTEGER_LIMIT}...")

    setup_motif_integers_db()
    motifs = fetch_motif_parameters()
    all_motif_integers = []

    output_file = os.path.join(TEXT_OUTPUT_PATH, "motif_integers_output.txt")
    
    with open(output_file, "w") as file:
        file.write(f"Motif Integers up to {INTEGER_LIMIT}:\n\n")

        for A0, L0, B, Y in motifs:
            motif_integers = generate_motif_integers(A0, L0, B, Y)
            all_motif_integers.extend(motif_integers)
            file.write(f"A0={A0}, L0={L0}, B={B}, Y={Y}:\n")
            for _, An, Ln in motif_integers:
                file.write(f"  An={An}, Ln={Ln}\n")
            file.write("\n")

    conn = connect_db("motif_integers.db")
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT OR IGNORE INTO motif_integers (motif_A, integer_N, integer_L)
        VALUES (?, ?, ?)
    ''', all_motif_integers)
    conn.commit()
    conn.close()

    print(f"✅ Step 2 complete. Motif integers stored in motif_integers.db and saved to '{output_file}'.")

if __name__ == "__main__":
    process_motif_integers()
