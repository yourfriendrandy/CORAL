#!/usr/bin/env python3
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import ray
import json
from collections import defaultdict

from config.config import INTEGER_LIMIT, print_active_mode
from config.db_utils import connect_db, get_project_root
from config.logger import logger, log_resource_usage


# -----------------------------
# Ray Initialization
# -----------------------------
ray.shutdown()
logger.info(f"🚀 Initializing Ray with {os.cpu_count() - 1} CPUs...")
ray.init(ignore_reinit_error=True, num_cpus=os.cpu_count() - 1)

# -----------------------------
# Helpers
# -----------------------------
def get_oe_pattern(sequence):
    return ''.join(['O' if n % 2 == 1 else 'E' for n in sequence])

def get_k_segments(oe_pattern):
    segments = []
    current = 1
    for i in range(1, len(oe_pattern)):
        if oe_pattern[i] == oe_pattern[i - 1]:
            current += 1
        else:
            segments.append(current)
            current = 1
    segments.append(current)
    return segments

# -----------------------------
# [S1] Motif Overlap Detection
# -----------------------------
def check_motif_overlap_across_sets():
    conn = connect_db("motif_integers.db")
    cursor = conn.cursor()
    cursor.execute("SELECT motif_A, integer_N FROM motif_integers")
    motif_map = defaultdict(set)
    for motif_A, n in cursor.fetchall():
        motif_map[n].add(motif_A)
    conn.close()
    return [(n, list(owners)) for n, owners in motif_map.items() if len(owners) > 1]

# -----------------------------
# [S2] 3An Encapsulation (Parallelized)
# -----------------------------
@ray.remote
def check_3an_encapsulation_batch(batch, product_ns):
    violations = []
    for n, seq_json in batch:
        for three_an in product_ns:
            if three_an != n and f"{three_an}" in seq_json:
                violations.append((three_an, n))
    return violations

def check_3an_encapsulation():
    conn = connect_db("product_motif_integers.db")
    cursor = conn.cursor()
    cursor.execute("SELECT product_N FROM product_motif_integers")
    product_ns = set(row[0] for row in cursor.fetchall())
    conn.close()

    conn = connect_db("collatz_sequences.db")
    cursor = conn.cursor()
    cursor.execute("SELECT n, sequence FROM collatz_cache WHERE n <= ?", (INTEGER_LIMIT,))
    all_rows = cursor.fetchall()
    conn.close()

    chunk_size = 10000
    chunks = [all_rows[i:i+chunk_size] for i in range(0, len(all_rows), chunk_size)]
    futures = [check_3an_encapsulation_batch.remote(chunk, product_ns) for chunk in chunks]
    results = ray.get(futures)
    return [item for sublist in results for item in sublist]

# -----------------------------
# [S3] Post-L Entropy Detection
# -----------------------------
@ray.remote
def check_post_L_entropy_batch(batch, L_map):
    flagged = []
    for n, sequence_json in batch:
        if n not in L_map:
            continue
        L = L_map[n]
        sequence = json.loads(sequence_json)
        if L not in sequence:
            continue
        L_index = sequence.index(L)
        post_L = sequence[L_index + 1:]
        if not post_L:
            continue
        oe_pattern = get_oe_pattern(post_L)
        k_segs = get_k_segments(oe_pattern)
        if 1 in k_segs:
            flagged.append((n, oe_pattern, k_segs))
    return flagged

def check_post_L_entropy():
    conn = connect_db("motif_integers.db")
    cursor = conn.cursor()
    cursor.execute("SELECT integer_N, integer_L FROM motif_integers")
    L_map = {row[0]: row[1] for row in cursor.fetchall()}
    conn.close()

    conn = connect_db("collatz_sequences.db")
    cursor = conn.cursor()
    cursor.execute("SELECT n, sequence FROM collatz_cache WHERE n <= ?", (INTEGER_LIMIT,))
    all_rows = cursor.fetchall()
    conn.close()

    chunk_size = 10000
    chunks = [all_rows[i:i+chunk_size] for i in range(0, len(all_rows), chunk_size)]
    futures = [check_post_L_entropy_batch.remote(chunk, L_map) for chunk in chunks]
    results = ray.get(futures)
    return [item for sublist in results for item in sublist]

# -----------------------------
# Output Printer
# -----------------------------
def print_results(overlaps, encapsulations, entropy_flags):
    print("\n===== CORAL: Internal Sanity Checker =====\n")

    print("🔍 [S1] Motif Overlaps:")
    if overlaps:
        for n, motifs in overlaps:
            print(f"  - {n} appears in multiple motifs: {motifs}")
    else:
        print("  ✅ No motif overlaps detected.")

    print("\n🔍 [S2] 3An Encapsulation:")
    if encapsulations:
        for three_an, container in encapsulations:
            print(f"  - 3An = {three_an} appears in sequence of {container}")
    else:
        print("  ✅ No 3An encapsulations found.")

    print("\n🔍 [S3] Post-L Entropy (k=1 segments):")
    if entropy_flags:
        for n, pattern, ksegs in entropy_flags:
            print(f"  - {n} has post-L OE: {pattern}, k-segments: {ksegs}")
    else:
        print("  ✅ No unexpected entropy detected after L.")

    print("\n✅ Sanity check complete.\n")

# -----------------------------
# Entry Point
# -----------------------------
if __name__ == "__main__":
    print_active_mode()
    logger.info("🧠 Running internal sanity checks...")
    log_resource_usage("Before sanity checks")

    overlaps = check_motif_overlap_across_sets()
    encapsulations = check_3an_encapsulation()
    entropy_flags = check_post_L_entropy()

    print_results(overlaps, encapsulations, entropy_flags)

    logger.info("✅ Sanity checker complete.")
    log_resource_usage("After sanity checks")
    ray.shutdown()
