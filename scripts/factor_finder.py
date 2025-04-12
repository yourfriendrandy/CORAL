#!/usr/bin/env python3

import os
import ray
import json
import subprocess
from math import gcd
from pathlib import Path
from config.config import (
    INTEGER_MIN, INTEGER_MAX, M, Z, CORAL_TAG, CHUNK_SIZE
)
from config.logger import logger
from config.ray_utils import init_ray
from config.db_utils import read_duckdb  # If needed

# -----------------------------
# Constants
# -----------------------------
F_CANDIDATES = list(range(M, M * M + 1))
TOP_K = 3
DUCKDB_TABLE = "system_cache"
CONFIG_PATH = Path("config") / "config.py"
OUTPUT_DIR = Path("text_output") / CORAL_TAG
OUTPUT_PATH = OUTPUT_DIR / "best_factors_consolidated.json"
MARKER_DIR = Path(".metadata")
MARKER_FILE = MARKER_DIR / f"{CORAL_TAG}_init_done.flag"

LOW_RANGE = range(INTEGER_MIN, INTEGER_MAX // 10 + 1)
FULL_RANGE = range(INTEGER_MIN, INTEGER_MAX + 1)

AUTO_RESUME = False
FORCE_INIT = os.getenv("FORCE_INIT", "false").lower() == "true"


# -----------------------------
# Utilities
# -----------------------------
def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


@ray.remote
def fetch_and_compare(ns, F, Z):
    targets = {n for n in ns if n % Z != 0}
    targets |= {n * F for n in targets}

    sequence_map = {row["n"]: json.loads(row["sequence"])
                    for row in read_duckdb(DUCKDB_TABLE, where_clause=f"n IN ({','.join(map(str, targets))})", as_dict=True)}

    total, short_deltas = 0, 0
    for n in ns:
        if n % Z == 0 or (n * F) % Z == 0:
            continue
        seq_n = sequence_map.get(n)
        seq_f = sequence_map.get(n * F)
        if not seq_n or not seq_f:
            continue
        delta = abs(len(seq_n) - len(seq_f))
        if delta > 0:
            total += 1
            if delta <= 3:
                short_deltas += 1

    return short_deltas, total


def stream_f_deltas(F, Z, ns, label=""):
    short, total = 0, 0
    for i, chunk in enumerate(chunked(ns, CHUNK_SIZE), 1):
        s, t = ray.get(fetch_and_compare.remote(chunk, F, Z))
        short += s
        total += t
        logger.info(f"[{label}] Chunk {i} → Δ≤3: {s}, Total: {t}")
    return short, total


def update_config_file(selected_F: int):
    lines = Path(CONFIG_PATH).read_text().splitlines()
    updated = [f"F = {selected_F}  # Auto-selected best factor" if line.strip().startswith("F =") else line for line in lines]
    if not any(line.startswith("F =") for line in lines):
        updated.append(f"F = {selected_F}  # Auto-selected best factor")
    Path(CONFIG_PATH).write_text("\n".join(updated) + "\n")
    logger.info(f"🧪 Updated F = {selected_F} in config.py")


def run_auto_resume(best_F: int):
    update_config_file(best_F)
    logger.info(f"🚀 Auto-resuming with F = {best_F}")
    subprocess.run(["python3", "scripts/populate_CORAL_system.py", "--resume_from_max"])


# -----------------------------
# Main Entry
# -----------------------------
def factor_finder_streamed():
    if MARKER_FILE.exists() and not FORCE_INIT:
        print(f"[💤] Initialization already completed for {CORAL_TAG}. Skipping factor_finder.")
        return

    logger.info("🔎 Streaming-safe factor analysis started...")
    init_ray()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    MARKER_DIR.mkdir(parents=True, exist_ok=True)

    scores = []
    for F in F_CANDIDATES:
        if gcd(F, Z) != 1:
            continue

        logger.info(f"🔧 Testing F = {F}...")
        low_short, low_total = stream_f_deltas(F, Z, LOW_RANGE, label="LOW")
        full_short, full_total = stream_f_deltas(F, Z, FULL_RANGE, label="FULL")

        if low_total == 0 or full_total == 0:
            logger.warning(f"⚠️ Insufficient data for F = {F}")
            continue

        low_ratio = low_short / low_total
        full_ratio = full_short / full_total
        score = full_ratio - low_ratio

        logger.info(f"✅ F = {F} Δscore: {score:.5f}, LOW: {low_ratio:.4f}, FULL: {full_ratio:.4f}")
        scores.append({
            "F": F, "score": score,
            "low_ratio": low_ratio, "full_ratio": full_ratio
        })

    scores.sort(key=lambda x: x["score"], reverse=True)
    top_k = scores[:TOP_K]
    OUTPUT_PATH.write_text(json.dumps({"top_factors": top_k}, indent=2))
    logger.info(f"📊 Top F scores saved to {OUTPUT_PATH}")

    MARKER_FILE.touch()

    if AUTO_RESUME and top_k:
        run_auto_resume(top_k[0]["F"])


if __name__ == "__main__":
    factor_finder_streamed()
    ray.shutdown()