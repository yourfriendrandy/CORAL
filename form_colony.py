#!/usr/bin/env python3

import os
import sys
import time
from pathlib import Path
from config.config import (
    INTEGER_RANGE, TWIN_ONLY, PARTITION_MODE, get_coral_tag,
    M, Z, B_POLARITY
)
from config.logger import logger, log_resource_usage, run_script_streamed
from config.db_utils import setup_all_tables
from config.path_utils import initialize_paths, get_path, register_output_path

# -----------------------------
# Paths & Constants
# -----------------------------
BASE_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = BASE_DIR / "scripts"
FLAG_PATH = BASE_DIR / "pipeline_running.flag"
SANITY_FLAG_PATH = BASE_DIR / "config" / "sanity_ready.flag"
CONFIG_PATH = BASE_DIR / "config" / "config.py"
DATABASES_DIR = BASE_DIR / "databases"

CORAL_TAG = get_coral_tag()
CORAL_PATH = DATABASES_DIR / CORAL_TAG

# Mark pipeline as running
FLAG_PATH.write_text("running")

# Set environment variables for subprocesses
os.environ.update({
    "INTEGER_RANGE": str(INTEGER_RANGE),
    "TWIN_ONLY": str(TWIN_ONLY).lower(),
    "PARTITION_MODE": str(PARTITION_MODE).lower(),
    "PYTHONPATH": str(BASE_DIR)
})

# -----------------------------
# Initialization
# -----------------------------
logger.info(f"🧮 Generating system rules for CORAL_TAG: {CORAL_TAG}")
run_script_streamed(SCRIPTS_DIR / "see_system_rules.py", env=os.environ)

logger.info(f"🗄️ Setting up DB tables for CORAL_TAG: {CORAL_TAG}")
setup_all_tables()

logger.info("🧭 Initializing PATH_MAP with known files...")
initialize_paths()

# -----------------------------
# Reset F if system is new
# -----------------------------
if not CORAL_PATH.exists():
    try:
        with CONFIG_PATH.open() as f:
            lines = f.readlines()

        with CONFIG_PATH.open("w") as f:
            found = False
            for line in lines:
                if line.strip().startswith("F ="):
                    f.write("F = None  # Reset by form_colony.py on new system init\n")
                    found = True
                else:
                    f.write(line)
            if not found:
                f.write("\nF = None  # Added by form_colony.py\n")

        logger.info(f"🔁 New system detected. Reset F = None in config.py for {CORAL_TAG}")
    except Exception as e:
        logger.error(f"⚠️ Failed to reset F: {e}")
else:
    logger.info(f"🔁 Existing system detected. Keeping F for {CORAL_TAG}")

# -----------------------------
# Pipeline Scripts
# -----------------------------
scripts = [
    "populate_CORAL_system.py",
    "factor_finder.py",
    "mode_step_checker.py",
    "confirm_factor_and_resume.py",
    "motif_setfinder.py",
    "store_motif_integers.py",
    "store_product_motif_integers.py",
    "store_motif_cycles.py",
    "store_product_motif_cycles.py",
    "full_lookup.py",
    "missing_entry_analysis.py",
]

# -----------------------------
# Run Pipeline
# -----------------------------
failed_scripts = []
start_total = time.time()

logger.info("🚀 Launching CORAL pipeline")
logger.info(f" - CORAL_TAG: {CORAL_TAG}")
logger.info(f" - INTEGER_RANGE: {INTEGER_RANGE}")
logger.info(f" - M = {M}, Z = {Z}, B_POLARITY = {B_POLARITY}")
logger.info(f" - TWIN_ONLY = {TWIN_ONLY}, PARTITION_MODE = {PARTITION_MODE}")

for script in scripts:
    script_path = SCRIPTS_DIR / script
    logger.info(f"🟡 Starting {script}...")
    start = time.time()

    returncode, stdout, stderr = run_script_streamed(script_path, env=os.environ)
    duration = time.time() - start

    logger.info(f"⏱️ {script} completed in {duration:.2f} seconds.")
    log_resource_usage(f"After {script}")

    if stdout.strip():
        logger.info(f"{script} stdout:\n{stdout.strip()}")
    if stderr.strip():
        logger.warning(f"{script} stderr:\n{stderr.strip()}")

    if returncode != 0:
        logger.error(f"❌ {script} failed (exit code {returncode})")
        failed_scripts.append(script)
        break  # Optional: continue with next script

# -----------------------------
# Summary
# -----------------------------
total_duration = time.time() - start_total
logger.info(f"⏱️ Pipeline finished in {total_duration:.2f} seconds.")

if failed_scripts:
    logger.warning("⚠️ The following script(s) failed:")
    for script in failed_scripts:
        logger.warning(f" - {script}")
else:
    logger.info("✅ All scripts completed successfully.")
    if not TWIN_ONLY and not PARTITION_MODE:
        SANITY_FLAG_PATH.write_text("ready")

# -----------------------------
# Cleanup
# -----------------------------
try:
    FLAG_PATH.unlink()
except FileNotFoundError:
    logger.warning("⚠️ pipeline_running.flag already removed or missing.")