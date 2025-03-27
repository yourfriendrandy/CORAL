#!/usr/bin/env python3
import subprocess
import os
import time

from config.config import INTEGER_LIMIT, TWIN_ONLY, PARTITION_MODE
from config.logger import logger, log_resource_usage

# -----------------------------
# Path & Environment Setup
# -----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")

# Pass config to subprocesses via environment
os.environ["INTEGER_LIMIT"] = str(INTEGER_LIMIT)
os.environ["TWIN_ONLY"] = "true" if TWIN_ONLY else "false"
os.environ["PARTITION_MODE"] = "true" if PARTITION_MODE else "false"

scripts = [
    "populate_collatz_database.py",
    "motif_setfinder.py",
    "store_motif_integers.py",
    "store_product_motif_integers.py",
    "store_motif_cycles.py",
    "store_product_motif_cycles.py",
    "full_lookup.py",
    "missing_entry_analysis.py",
]

# -----------------------------
# Runtime Execution
# -----------------------------
failed_scripts = []
overall_start_time = time.time()

for script in scripts:
    script_path = os.path.join(SCRIPTS_DIR, script)
    logger.info(f"🟡 Starting {script} with INTEGER_LIMIT={INTEGER_LIMIT}, TWIN_ONLY={TWIN_ONLY}, PARTITION_MODE={PARTITION_MODE}...")

    start_time = time.time()

    # Set PYTHONPATH so config modules resolve properly in subprocesses
    os.environ["PYTHONPATH"] = BASE_DIR

    result = subprocess.run(["python3", script_path], capture_output=True, text=True, env=os.environ)
    elapsed_time = time.time() - start_time

    logger.info(f"⏱️ {script} completed in {elapsed_time:.2f} seconds.")
    log_resource_usage(f"After {script}")

    if result.stdout.strip():
        logger.info(f"{script} stdout:\n{result.stdout.strip()}")

    if result.stderr.strip():
        logger.warning(f"{script} stderr:\n{result.stderr.strip()}")

    if result.returncode != 0:
        logger.error(f"❌ {script} failed (exit code {result.returncode})")
        failed_scripts.append(script)
        break  # Optional: remove to continue on failure

# -----------------------------
# Summary
# -----------------------------
total_time = time.time() - overall_start_time
logger.info(f"⏱️ Total pipeline runtime: {total_time:.2f} seconds.")

if failed_scripts:
    logger.warning("⚠️ The following script(s) failed:")
    for script in failed_scripts:
        logger.warning(f" - {script}")
else:
    logger.info("✅ All scripts completed successfully.")
