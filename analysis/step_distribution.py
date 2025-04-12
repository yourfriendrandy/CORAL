import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import ray
import sqlite3
import json
from collections import Counter
import matplotlib.pyplot as plt
import numpy as np
from config.db_utils import connect_db

# Configurable
n_limit = 60000
multiplicative_factors = [3, 9, 27, 81, 243]
colors = ['b', 'g', 'r', 'c', 'm']

# Load step counts from your existing collatz_sequences.db
def load_step_counts(limit):
    conn = connect_db("collatz_sequences.db")
    cursor = conn.cursor()
    cursor.execute("SELECT n, sequence FROM collatz_cache WHERE n <= ?", (limit,))
    data = {n: len(json.loads(seq)) - 1 for n, seq in cursor.fetchall()}
    conn.close()
    return data

# Remote Ray function for parallel step delta computation
@ray.remote
def compute_step_deltas(chunk, factor, step_map):
    deltas = []
    for n in chunk:
        steps_n = step_map.get(n)
        steps_mult = step_map.get(n * factor)
        if steps_n is not None and steps_mult is not None:
            deltas.append(steps_mult - steps_n)
    return deltas

def main():
    ray.init(ignore_reinit_error=True)

    # Step 1: Load steps from DB
    print("🔍 Loading step counts from DB...")
    step_map = load_step_counts(3 * n_limit)  # Covers all up to 243n
    odd_numbers = [n for n in range(1, n_limit + 1, 2)]

    mode_results = {}
    plt.figure(figsize=(10, 6))

    # Step 2: Compute deltas for each factor in parallel
    for idx, factor in enumerate(multiplicative_factors):
        print(f"⚙️ Processing factor {factor}...")
        chunks = [odd_numbers[i:i + 1000] for i in range(0, len(odd_numbers), 1000)]
        futures = [compute_step_deltas.remote(chunk, factor, step_map) for chunk in chunks]

        results = ray.get(futures)
        step_changes = [delta for sublist in results for delta in sublist]

        count = len(step_changes)
        counter = Counter(step_changes)
        mode, mode_count = counter.most_common(1)[0]
        pos, neg, zero = sum(d > 0 for d in step_changes), sum(d < 0 for d in step_changes), sum(d == 0 for d in step_changes)

        mode_results[factor] = {
            'mode_step_change': mode,
            'mode_count': mode_count,
            'mode_change_type': "positive" if mode > 0 else "negative" if mode < 0 else "zero",
            'proportion_mode': mode_count / count,
            'positive_count': pos,
            'negative_count': neg,
            'zero_count': zero,
            'proportion_positive': pos / count,
            'proportion_negative': neg / count,
            'proportion_zero': zero / count,
            'mean_step_change': np.mean(step_changes),
            'std_dev_step_change': np.std(step_changes)
        }

        plt.hist(step_changes, bins=50, alpha=0.5, color=colors[idx], label=f"{factor}x")

    plt.xlabel("Step Change")
    plt.ylabel("Frequency")
    plt.title("Step Change Distribution (Odd n ≤ 60,000)")
    plt.legend()
    plt.tight_layout()
    plt.show()

    for factor, result in mode_results.items():
        print(f"\n🔢 Results for multiplication by {factor}:")
        print(f"  Mode Step Change: {result['mode_step_change']} ({result['mode_change_type']})")
        print(f"  Proportion Mode: {result['proportion_mode']:.4f} ({result['mode_count']}/{len(odd_numbers)})")
        print(f"  Positive: {result['positive_count']} ({result['proportion_positive']:.4f})")
        print(f"  Negative: {result['negative_count']} ({result['proportion_negative']:.4f})")
        print(f"  Zero:     {result['zero_count']} ({result['proportion_zero']:.4f})")
        print(f"  Mean: {result['mean_step_change']:.2f}")
        print(f"  Std Dev: {result['std_dev_step_change']:.2f}")

    ray.shutdown()

if __name__ == "__main__":
    main()
