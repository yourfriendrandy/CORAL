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
    setup_motif_integers_db,
    get_project_root
)

# -----------------------------
# Helper Functions
# -----------------------------
def fetch_motif_parameters():
    """Fetches motif parameters using TWIN_ONLY or PARTITION_MODE filtering."""
    conn = connect_db("motif_parameters.db")
    cursor = conn.cursor()

    if TWIN_ONLY:
        cursor.execute("SELECT A, L, B, Y FROM motif_parameters WHERE B = Y + 1")
    elif PARTITION_MODE:
        cursor.execute("SELECT A, L, B, Y FROM motif_parameters WHERE B != Y + 1")
    else:
        cursor.execute("SELECT A, L, B, Y FROM motif_parameters")

    motifs = cursor.fetchall()
    conn.close()
    return motifs

def get_last_processed_An():
    """Returns a dictionary mapping motif_A to max(An) seen so far."""
    conn = connect_db("motif_integers.db")
    cursor = conn.cursor()
    cursor.execute("SELECT motif_A, MAX(integer_N) FROM motif_integers GROUP BY motif_A")
    results = cursor.fetchall()
    conn.close()
    return {motif_A: max_An for motif_A, max_An in results if max_An is not None}

def generate_motif_integers(A0, L0, B, Y, last_An=None):
    """Generates An and Ln values using recurrence relations, resuming from last_An if provided."""
    motif_integers = []
    step_A = 2 ** (B + 1)
    step_L = 2 * (3 ** Y)

    if last_An is not None and last_An >= A0:
        X_start = (last_An - A0) // step_A + 1
    else:
        X_start = 0

    X = X_start
    while True:
        An = A0 + (X * step_A)
        Ln = L0 + (X * step_L)
        if An > INTEGER_LIMIT:
            break
        motif_integers.append((A0, An, Ln))
        X += 1

    return motif_integers

def query_output_motif_integers():
    """Requeries all relevant motif integers from DB for output display, within (1, INTEGER_LIMIT)."""
    conn = connect_db("motif_integers.db")
    cursor = conn.cursor()
    cursor.execute("ATTACH DATABASE ? AS params", (os.path.join(get_project_root(), "databases", "motif_parameters.db"),))

    query = """
        SELECT m.motif_A, m.integer_N, m.integer_L, p.B, p.Y
        FROM motif_integers m
        JOIN params.motif_parameters p ON m.motif_A = p.A
        WHERE m.integer_N <= ?
    """
    if TWIN_ONLY:
        query += " AND p.B = p.Y + 1"
    elif PARTITION_MODE:
        query += " AND p.B != p.Y + 1"

    cursor.execute(query, (INTEGER_LIMIT,))
    rows = cursor.fetchall()
    conn.close()
    return rows

# -----------------------------
# Main Processing
# -----------------------------
def process_motif_integers():
    print_active_mode()
    print(f"Processing motif integers up to {INTEGER_LIMIT}...")

    setup_motif_integers_db()
    motifs = fetch_motif_parameters()
    last_seen = get_last_processed_An()
    all_motif_integers = []

    print(f"🔎 Checking resumption status for {len(motifs)} motifs...")

    for A0, L0, B, Y in motifs:
        max_existing_An = last_seen.get(A0)

        if max_existing_An:
            print(f"🔁 Resuming motif A0={A0}: last An in DB = {max_existing_An}")
        else:
            print(f"🆕 New motif A0={A0}: no prior An found")

        motif_integers = generate_motif_integers(A0, L0, B, Y, last_An=max_existing_An)
        all_motif_integers.extend(motif_integers)

    if all_motif_integers:
        print(f"📝 Inserting {len(all_motif_integers)} new (An, Ln) entries into motif_integers.db...")
        conn = connect_db("motif_integers.db")
        cursor = conn.cursor()
        cursor.executemany('''
            INSERT OR IGNORE INTO motif_integers (motif_A, integer_N, integer_L)
            VALUES (?, ?, ?)
        ''', all_motif_integers)
        conn.commit()
        conn.close()
    else:
        print("✅ No new motif integers to insert — all sequences up to INTEGER_LIMIT are complete.")

    # ✅ Re-query everything from DB for (1, INTEGER_LIMIT) and write output
    os.makedirs(TEXT_OUTPUT_PATH, exist_ok=True)
    output_file = os.path.join(TEXT_OUTPUT_PATH, "motif_integers_output.txt")
    motif_output = query_output_motif_integers()

    with open(output_file, "w") as file:
        file.write(f"Motif Integers up to {INTEGER_LIMIT}:\n\n")
        grouped = {}

        for A, An, Ln, B, Y in motif_output:
            grouped.setdefault((A, B, Y), []).append((An, Ln))

        for (A, B, Y), values in sorted(grouped.items()):
            L0 = values[0][1]  # First Ln for header
            file.write(f"A0={A}, L0={L0}, B={B}, Y={Y}:\n")
            for An, Ln in values:
                file.write(f"  An={An}, Ln={Ln}\n")
            file.write("\n")

    print(f"📄 Output file saved to '{output_file}'")
    print(f"✅ Step 2 complete. Motif integers stored and printed.")

if __name__ == "__main__":
    process_motif_integers()
