#!/usr/bin/env python3

import tkinter as tk
from tkinter import messagebox, scrolledtext
import json
import subprocess
import sys
import shutil
from config.dicts import DB_MAP, DB_TAG_MAP
from config.logger import logger
from config.config import CORAL_TAG
from config.path_utils import (
    get_path, register_output_path, initialize_paths
)

# --------------------------
# Initialize dynamic paths
# --------------------------
initialize_paths()

# --------------------------
# Paths + Constants
# --------------------------
FACTOR_PATH = get_path("text_output", CORAL_TAG, "best_factors_consolidated.json")
STEP_MODE_PATH = get_path("text_output", CORAL_TAG, "step_mode_results.json")
CHECKPOINT_PATH = get_path("checkpoints", "f_confirmed.json")
OUTPUT_DIR = get_path("text_output", CORAL_TAG)
DB_DIR = get_path("databases", CORAL_TAG)

register_output_path("confirmed_factor_checkpoint", CHECKPOINT_PATH)


# --------------------------
# Helper Functions
# --------------------------
def load_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load {path}: {e}")
        return {}


def get_current_config_F():
    config_path = get_path("config", "config.py")
    lines = config_path.read_text().splitlines()
    for line in lines:
        if line.strip().startswith("F ="):
            try:
                return int(line.split("=")[1].split("#")[0].strip())
            except:
                return None
    return None


def update_config_F(F):
    config_path = get_path("config", "config.py")
    lines = config_path.read_text().splitlines()
    updated = [f"F = {F}  # Confirmed factor" if line.strip().startswith("F =") else line for line in lines]
    if not any(line.strip().startswith("F =") for line in lines):
        updated.append(f"F = {F}  # Confirmed factor")
    config_path.write_text("\n".join(updated) + "\n")


def write_checkpoint_F(F):
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CHECKPOINT_PATH, "w") as f:
        json.dump({"F": F}, f, indent=2)


def nuke_old_motif_data():
    preserve_prefixes = {f"{CORAL_TAG}_sequences", f"{CORAL_TAG}_loops"}
    motif_files = set()

    # From DB_MAP
    for table, info in DB_MAP.items():
        if info.get("is_motif"):
            motif_files.add(f"{info['db_name']}.db")
            motif_files.add(f"{info['db_name']}.duckdb")

    # From DB_TAG_MAP (prefix applies only to .duckdb)
    for table, info in DB_TAG_MAP.items():
        if info.get("is_motif"):
            motif_files.add(f"{info['db_name']}.db")
            motif_files.add(f"{CORAL_TAG}_{info['db_name']}.duckdb")

    # Nuke motif DBs
    if DB_DIR.exists():
        for db_file in DB_DIR.glob("*.db"):
            if db_file.name in motif_files and db_file.stem not in preserve_prefixes:
                db_file.unlink()
                logger.info(f"💣 Deleted DB: {db_file}")
        for duckdb_file in DB_DIR.glob("*.duckdb"):
            if duckdb_file.name in motif_files and duckdb_file.stem not in preserve_prefixes:
                duckdb_file.unlink()
                logger.info(f"💣 Deleted DuckDB: {duckdb_file}")

    # Nuke output dir + checkpoint
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
        logger.info(f"🧹 Deleted output dir: {OUTPUT_DIR}")

    if CHECKPOINT_PATH.exists():
        CHECKPOINT_PATH.unlink()
        logger.info(f"🧹 Deleted checkpoint: {CHECKPOINT_PATH}")


# --------------------------
# GUI Handlers
# --------------------------
def confirm_and_continue(F):
    current = get_current_config_F()
    if current is not None and current != F:
        answer = messagebox.askyesno("Confirm", f"Current F = {current}. Overwrite with F = {F}?\n\n"
                                                "This will delete all motif-related DBs and outputs.\n"
                                                "Proceed?")
        if not answer:
            return
        nuke_old_motif_data()

    update_config_F(F)
    write_checkpoint_F(F)
    logger.info(f"✅ Factor {F} confirmed and saved.")
    messagebox.showinfo("Confirmed", f"Factor {F} confirmed.\nContinuing pipeline...")
    root.destroy()
    subprocess.run(["python3", "scripts/populate_CORAL_system.py", "--phase2_resume"])


def cancel_pipeline():
    logger.warning("🛑 Factor not confirmed. Pipeline paused.")
    messagebox.showinfo("Aborted", "Pipeline paused. Edit F or rerun factor_finder.py.")
    root.destroy()
    sys.exit(0)

# --------------------------
# Load Data
# --------------------------
factor_data = load_json(FACTOR_PATH)
step_data = load_json(STEP_MODE_PATH)
if not factor_data or "top_factors" not in factor_data:
    print("❌ No top factor found. Run factor_finder.py first.")
    sys.exit(1)

top_factor = factor_data["top_factors"][0]
F = top_factor["F"]

# --------------------------
# Build GUI
# --------------------------
root = tk.Tk()
root.title("Confirm Best Factor (F)")

frame = tk.Frame(root, padx=10, pady=10)
frame.pack(fill="both", expand=True)

tk.Label(frame, text=f"Suggested F = {F}", font=("Helvetica", 14, "bold")).pack(pady=(0, 10))

text_box = scrolledtext.ScrolledText(frame, width=100, height=30, font=("Courier", 10))
text_box.pack()

text_box.insert(tk.END, "===== Factor Finder Results =====\n")
text_box.insert(tk.END, json.dumps(factor_data, indent=2))
text_box.insert(tk.END, "\n\n===== Step Mode Analyzer Results =====\n")
text_box.insert(tk.END, json.dumps(step_data, indent=2))
text_box.config(state=tk.DISABLED)

btn_frame = tk.Frame(frame)
btn_frame.pack(pady=10)

tk.Button(btn_frame, text="✅ Confirm and Continue", command=lambda: confirm_and_continue(F),
          width=30, bg="green", fg="white").pack(side=tk.LEFT, padx=10)
tk.Button(btn_frame, text="❌ Cancel", command=cancel_pipeline,
          width=15, bg="red", fg="white").pack(side=tk.RIGHT, padx=10)

root.mainloop()