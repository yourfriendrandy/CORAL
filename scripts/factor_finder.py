#!/usr/bin/env python3
import json
import ray
import subprocess
from pathlib import Path
from math import gcd
from config.config import (
    INTEGER_MIN, INTEGER_MAX, M, Z, CORAL_TAG
)
from config.logger import logger
from config.ray_utils import init_ray
from config.db_utils import read_duckdb_table_as_dict

# -----------------------------
# One-time Marker System
# -----------------------------
import os
MARKER_DIR = Path(".metadata")
MARKER_DIR.mkdir(exist_ok=True)
MARKER_FILE = MARKER_DIR / f"{CORAL_TAG}_init_done.flag"
FORCE_INIT = os.getenv("FORCE_INIT", "false").lower() == "true"

if MARKER_FILE.exists() and not FORCE_INIT:
    print(f"[💤] Initialization already completed for {CORAL_TAG}. Skipping factor_finder.")
    exit(0)

# -----------------------------
# Settings
# -----------------------------
F_CANDIDATES = list(range(M, M * M + 1))
TOP_K = 3
DUCKDB_TABLE = "system_cache"
FACTOR_JSON = Path(f"text_output_{CORAL_TAG}") / "best_factors_consolidated.json"
CONFIG_PATH = Path("config") / "config.py"
LOW_RANGE = range(INTEGER_MIN, INTEGER_MAX // 10 + 1)
FULL_RANGE = range(INTEGER_MIN, INTEGER_MAX + 1)
CHUNK_SIZE = 10000

AUTO_RESUME = False  # Optional toggle

# -----------------------------
# Utilities
# -----------------------------
def chunked(seq, size):
    return [seq[i:i + size] for i in range(0, len(seq), size)]

@ray.remote
def fetch_and_compare(ns, F, Z):
    from config.db_utils import read_duckdb_table_as_dict
    import json

    n_set = set()
    for n in ns:
        if n % Z == 0:
            continue
        n_set.add(n)
        n_set.add(n * F)

    sequence_map = read_duckdb_table_as_dict(DUCKDB_TABLE, column="n", keys=list(n_set))
    short_deltas = 0
    total = 0

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

def update_config_file(selected_F: int):
    with open(CONFIG_PATH, "r") as f:
        lines = f.readlines()

    with open(CONFIG_PATH, "w") as f:
        found = False
        for line in lines:
            if line.strip().startswith("F ="):
                f.write(f"F = {selected_F}  # Selected best factor for this CORAL run\n")
                found = True
            else:
                f.write(line)
        if not found:
            f.write(f"\nF = {selected_F}  # Selected best factor for this CORAL run\n")
    logger.info(f"🧪 Updated F = {selected_F} in config.py")

def stream_f_deltas(F, Z, ns, label=""):
    short, total = 0, 0
    chunks = chunked(ns, CHUNK_SIZE)

    for i, chunk in enumerate(chunks, 1):
        future = fetch_and_compare.remote(chunk, F, Z)
        s, t = ray.get(future)
        short += s
        total += t
        logger.info(f"[{label}] Chunk {i}/{len(chunks)} → Δ≤3: {s}, Total: {t}")

    return short, total

# -----------------------------
# Main Execution
# -----------------------------
def factor_finder_streamed():
    logger.info("🔎 Streaming-safe factor analysis started...")
    init_ray()

    factor_scores = []

    for F in F_CANDIDATES:
        if gcd(F, Z) != 1:
            continue
        logger.info(f"🔧 Processing F = {F}...")

        low_short, low_total = stream_f_deltas(F, Z, LOW_RANGE, label="LOW")
        full_short, full_total = stream_f_deltas(F, Z, FULL_RANGE, label="FULL")

        if full_total == 0 or low_total == 0:
            logger.warning(f"⚠️ Skipping F = {F}: insufficient data.")
            continue

        low_ratio = low_short / low_total
        full_ratio = full_short / full_total
        score = full_ratio - low_ratio

        logger.info(f"✅ F = {F} → Δscore: {score:.5f}, low: {low_ratio:.4f}, full: {full_ratio:.4f}")

        factor_scores.append({
            "F": F,
            "score": score,
            "low_ratio": low_ratio,
            "full_ratio": full_ratio
        })

    factor_scores.sort(key=lambda x: x["score"], reverse=True)
    top_factors = factor_scores[:TOP_K]

    out_dir = Path(f"text_output_{CORAL_TAG}")
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / "best_factors_consolidated.json"

    with open(out_path, "w") as f:
        json.dump({"top_factors": top_factors}, f, indent=2)

    logger.info(f"📊 Top F scores saved to {out_path}")

    if not MARKER_FILE.exists():
        MARKER_FILE.touch()

    if AUTO_RESUME and top_factors:
        best_F = top_factors[0]["F"]
        update_config_file(best_F)
        logger.info(f"🚀 Auto-resuming with F = {best_F}")
        subprocess.run(["python3", "scripts/populate_CORAL_system.py", "--resume_from_max"])


# -----------------------------
# Entrypoint
# -----------------------------
if __name__ == "__main__":
    factor_finder_streamed()