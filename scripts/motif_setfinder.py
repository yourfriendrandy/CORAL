#!/usr/bin/env python3
import os
import json
from collections import defaultdict, OrderedDict

import ray
from config.config import (
    INTEGER_LIMIT, TEXT_OUTPUT_PATH, CACHE_CONFIG
)
from config.db_utils import connect_db, setup_motif_parameters_db

ray.init(ignore_reinit_error=True, num_cpus=os.cpu_count() - 1)

# -----------------------------
# LFU Cache
# -----------------------------
class LFUCache:
    def __init__(self, capacity=None):
        self.capacity = capacity or CACHE_CONFIG["LFU_GENERAL"]
        self.cache = {}
        self.frequency = defaultdict(int)
        self.order = OrderedDict()

    def get(self, n):
        if n in self.cache:
            self.frequency[n] += 1
            self.order.move_to_end(n)
            return self.cache[n]
        return None

    def put(self, n, sequence):
        if len(self.cache) >= self.capacity:
            self._evict()
        self.cache[n] = sequence
        self.frequency[n] = 1
        self.order[n] = sequence

    def _evict(self):
        least_freq = min(self.frequency.values())
        evict = next(k for k, v in self.frequency.items() if v == least_freq)
        self.cache.pop(evict)
        self.frequency.pop(evict)
        self.order.pop(evict)

# -----------------------------
# Collatz and Helpers
# -----------------------------
def collatz_steps(n, lfu_cache):
    cached_sequence = lfu_cache.get(n)
    if cached_sequence:
        return cached_sequence

    sequence = []
    original_n = n
    while n > 1:
        sequence.append(n)
        n = n // 2 if n % 2 == 0 else 3 * n + 1
    sequence.append(1)

    lfu_cache.put(original_n, sequence)
    return sequence

def convert_to_OE(cycle):
    return ''.join(['O' if num % 2 else 'E' for num in cycle])

def longest_common_suffix(seq1, seq2):
    min_len = min(len(seq1), len(seq2))
    i = 1
    while i <= min_len and seq1[-i] == seq2[-i]:
        i += 1
    return seq1[-(i - 1):] if i > 1 else []

@ray.remote
def find_mode_step_changes(chunk, existing_As):
    lfu_cache = LFUCache(capacity=CACHE_CONFIG["LFU_MODE_STEPS"])
    return [
        n for n in chunk
        if n not in existing_As and
           len(collatz_steps(n, lfu_cache)) == len(collatz_steps(n * 3, lfu_cache)) + 1
    ]

def process_motif(A, lfu_cache, seen_motifs):
    A_cycle = collatz_steps(A, lfu_cache)
    product_cycle = collatz_steps(A * 3, lfu_cache)

    motif_OE = convert_to_OE(A_cycle)
    product_OE = convert_to_OE(product_cycle)

    common_suffix = longest_common_suffix(A_cycle, product_cycle)
    L = common_suffix[0] if common_suffix else None

    suffix_index = len(A_cycle) - len(common_suffix) if common_suffix else len(A_cycle)
    motif_OE_prefix = motif_OE[:suffix_index]

    product_suffix_index = len(product_OE) - len(common_suffix) if common_suffix else len(product_OE)
    product_OE_prefix = product_OE[:product_suffix_index]

    B = motif_OE_prefix.count('E')
    Y = motif_OE_prefix.count('O')

    motif_key = (B, Y, motif_OE_prefix)
    if motif_key in seen_motifs:
        return None
    seen_motifs[motif_key] = A

    return (A, B, L, Y, motif_OE_prefix, product_OE_prefix)

# -----------------------------
# Output Helper
# -----------------------------
def write_output_file(filename, content):
    output_file = os.path.join(TEXT_OUTPUT_PATH, filename)
    with open(output_file, "w") as file:
        file.write(content)
    print(f"✅ Output saved to {output_file}")

# -----------------------------
# Main Execution
# -----------------------------
def main():
    setup_motif_parameters_db()
    conn = connect_db("motif_parameters.db")
    cursor = conn.cursor()

    cursor.execute("SELECT A FROM motif_parameters")
    existing_As = {row[0] for row in cursor.fetchall()}

    odd_integers = [i for i in range(1, INTEGER_LIMIT + 1, 2)]
    chunks = [odd_integers[i:i + 10000] for i in range(0, len(odd_integers), 10000)]

    futures = [find_mode_step_changes.remote(chunk, existing_As) for chunk in chunks]
    result_lists = ray.get(futures)

    mode_integers = [n for sublist in result_lists for n in sublist]
    print(f"🔍 Found {len(mode_integers)} new mode step integers.")

    lfu_cache = LFUCache(capacity=CACHE_CONFIG["LFU_COLLATZ"])
    seen_motifs = {}
    new_rows = []

    for A in mode_integers:
        result = process_motif(A, lfu_cache, seen_motifs)
        if result:
            new_rows.append(result)

    cursor.executemany('''
        INSERT OR IGNORE INTO motif_parameters (A, B, L, Y, motif_OE, product_OE)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', new_rows)

    conn.commit()
    conn.close()

    write_output_file("mode_step_integers.txt", "### Mode Step Integers ###\n\n" + "\n".join(map(str, mode_integers)))
    print("✅ Step 1 complete. Database and text output updated.")

if __name__ == "__main__":
    main()
