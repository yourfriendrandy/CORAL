#!/usr/bin/env python3

import ray
import json
from pathlib import Path
from statistics import mean, stdev
from collections import Counter
from config.config import CORAL_TAG, INTEGER_MIN, INTEGER_MAX, CHUNK_SIZE
from config.logger import logger
from config.db_utils import read_duckdb
from config.ray_utils import init_ray

# -----------------------------
# Constants
# -----------------------------
TABLE = "system_cache"
OUTPUT_PATH = Path("text_output") / CORAL_TAG / "step_mode_results.json"
F_PATH = Path("text_output") / CORAL_TAG / "best_factors_consolidated.json"

# -----------------------------
# Utilities
# -----------------------------
def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]

@ray.remote
def compute_step_deltas(chunk, F):
    keys = {n for n in chunk} | {n * F for n in chunk}
    where = f"n IN ({','.join(map(str, keys))})"
    row_map = {
        row["n"]: json.loads(row["sequence"])
        for row in read_duckdb(TABLE, where_clause=where, as_dict=True)
    }

    deltas = []
    for n in chunk:
        seq_n = row_map.get(n)
        seq_f = row_map.get(n * F)
        if seq_n and seq_f:
            deltas.append(len(seq_f) - len(seq_n))
    return deltas

def analyze_deltas(deltas, F):
    total = len(deltas)
    counter = Counter(deltas)
    mode, mode_count = counter.most_common(1)[0]
    pos = sum(d > 0 for d in deltas)
    neg = sum(d < 0 for d in deltas)
    zero = sum(d == 0 for d in deltas)

    return {
        "F": F,
        "total_pairs": total,
        "mode_step_change": mode,
        "mode_count": mode_count,
        "proportion_mode": mode_count / total,
        "positive_count": pos,
        "negative_count": neg,
        "zero_count": zero,
        "proportion_positive": pos / total,
        "proportion_negative": neg / total,
        "proportion_zero": zero / total,
        "mean_step_change": mean(deltas),
        "std_dev_step_change": stdev(deltas) if total > 1 else 0.0
    }

def run_step_mode_analysis():
    logger.info("🔍 Step Mode Analyzer Started")
    init_ray()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Load top F values
    if not F_PATH.exists():
        raise FileNotFoundError(f"❌ Could not find F summary file at: {F_PATH}")
    with open(F_PATH) as f:
        top_factors = json.load(f).get("top_factors", [])
    F_values = [f["F"] for f in top_factors]
    if not F_values:
        raise ValueError("❌ No F values found in top_factors.")

    logger.info(f"📥 Loaded top F values: {F_values}")

    all_ns = list(range(INTEGER_MIN, INTEGER_MAX + 1))
    chunks = list(chunked(all_ns, CHUNK_SIZE))

    results = []
    for F in F_values:
        logger.info(f"📊 Analyzing Δ steps for F = {F}")
        futures = [compute_step_deltas.remote(chunk, F) for chunk in chunks]
        delta_lists = ray.get(futures)
        all_deltas = [d for sublist in delta_lists for d in sublist]

        if not all_deltas:
            logger.warning(f"⚠️ No deltas found for F = {F}")
            continue

        stats = analyze_deltas(all_deltas, F)
        results.append(stats)
        logger.info(f"✅ Done F = {F}: Mode Δ = {stats['mode_step_change']}, Mean = {stats['mean_step_change']:.3f}")

    OUTPUT_PATH.write_text(json.dumps(results, indent=2))
    logger.info(f"📄 Saved full results to {OUTPUT_PATH}")


if __name__ == "__main__":
    run_step_mode_analysis()
    ray.shutdown()