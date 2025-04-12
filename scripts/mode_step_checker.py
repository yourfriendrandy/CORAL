#!/usr/bin/env python3
import ray
import json
from pathlib import Path
from statistics import mean, stdev
from collections import Counter
from config.config import CORAL_TAG, INTEGER_MIN, INTEGER_MAX, F, CHUNK_SIZE
from config.logger import logger
from config.db_utils import read_duckdb
from config.ray_utils import init_ray

# -----------------------------
# Constants
# -----------------------------
TABLE = "system_cache"
OUTPUT_PATH = Path("text_output") / CORAL_TAG / "step_mode_results.json"

# -----------------------------
# Utility
# -----------------------------
def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


@ray.remote
def compute_step_deltas(chunk, F):
    keys = {n for n in chunk} | {n * F for n in chunk}
    row_map = {
        row["n"]: json.loads(row["sequence"])
        for row in read_duckdb(TABLE, where_clause=f"n IN ({','.join(map(str, keys))})", as_dict=True)
    }

    deltas = []
    for n in chunk:
        seq_n = row_map.get(n)
        seq_f = row_map.get(n * F)
        if seq_n and seq_f:
            deltas.append(len(seq_f) - len(seq_n))
    return deltas


def run_step_mode_analysis():
    logger.info("🔍 Step Mode Analyzer Started")
    init_ray()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    all_ns = list(range(INTEGER_MIN, INTEGER_MAX + 1))
    chunks = list(chunked(all_ns, CHUNK_SIZE))
    futures = [compute_step_deltas.remote(chunk, F) for chunk in chunks]

    results = ray.get(futures)
    all_deltas = [d for sublist in results for d in sublist]
    logger.info(f"📊 Aggregated {len(all_deltas)} total deltas")

    if not all_deltas:
        logger.warning("⚠️ No valid deltas computed.")
        return

    counter = Counter(all_deltas)
    mode, mode_count = counter.most_common(1)[0]
    total = len(all_deltas)
    pos, neg, zero = sum(d > 0 for d in all_deltas), sum(d < 0 for d in all_deltas), sum(d == 0 for d in all_deltas)

    result = {
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
        "mean_step_change": mean(all_deltas),
        "std_dev_step_change": stdev(all_deltas) if total > 1 else 0.0
    }

    OUTPUT_PATH.write_text(json.dumps(result, indent=2))
    logger.info(f"✅ Step mode results written to: {OUTPUT_PATH}")


if __name__ == "__main__":
    run_step_mode_analysis()
    ray.shutdown()