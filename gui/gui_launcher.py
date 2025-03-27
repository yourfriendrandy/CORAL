#!/usr/bin/env python3
import tkinter as tk
from tkinter import messagebox
import subprocess
import os
import sys

# -----------------------------
# Paths and Environment Setup
# -----------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
CONFIG_PATH = os.path.join(PROJECT_ROOT, "config", "config.py")
FORM_COLONY_SCRIPT = os.path.join(PROJECT_ROOT, "form_colony.py")  # <-- NEW NAME
VENV_PYTHON = os.path.join(PROJECT_ROOT, "venv", "bin", "python3") if sys.platform != "win32" else os.path.join(PROJECT_ROOT, "venv", "Scripts", "python.exe")
SANITY_READY = os.path.exists(os.path.join(PROJECT_ROOT, "config", "sanity_ready.flag"))

# -----------------------------
# GUI Actions
# -----------------------------
def update_config_and_run():
    try:
        twin = twin_var.get()
        partition = partition_var.get()
        integer_limit = int(limit_entry.get())
        sanity = run_sanity_var.get()

        if twin and partition:
            messagebox.showerror("Invalid Configuration", "Cannot enable both TWIN_ONLY and PARTITION_MODE.")
            return

        # Rewrite config.py
        with open(CONFIG_PATH, "r") as f:
            lines = f.readlines()

        with open(CONFIG_PATH, "w") as f:
            for line in lines:
                if line.startswith("TWIN_ONLY"):
                    f.write(f"TWIN_ONLY = {twin}\n")
                elif line.startswith("PARTITION_MODE"):
                    f.write(f"PARTITION_MODE = {partition}\n")
                elif line.startswith("INTEGER_LIMIT"):
                    f.write(f"INTEGER_LIMIT = {integer_limit}\n")
                elif line.startswith("SANITY_CHECK"):
                    f.write(f"SANITY_CHECK = {sanity}\n")
                else:
                    f.write(line)

        print("\n🌱 Config Applied:")
        print(f"  TWIN_ONLY      = {twin}")
        print(f"  PARTITION_MODE = {partition}")
        print(f"  INTEGER_LIMIT  = {integer_limit}")
        print(f"  SANITY_CHECK   = {sanity}\n")

        # Launch CORAL main pipeline
        subprocess.Popen([VENV_PYTHON, FORM_COLONY_SCRIPT])
        root.destroy()

    except Exception as e:
        messagebox.showerror("Error", f"Something went wrong:\n{str(e)}")

def run_sanity_checker_alone():
    try:
        subprocess.Popen([
            VENV_PYTHON,
            os.path.join(PROJECT_ROOT, "analysis", "sanity_checker.py")
        ])
        root.destroy()
    except Exception as e:
        messagebox.showerror("Error", f"Could not launch sanity checker:\n{str(e)}")

# -----------------------------
# GUI Layout
# -----------------------------
root = tk.Tk()
root.title("CORAL Configuration Launcher")

# Integer Limit Entry
tk.Label(root, text="INTEGER_LIMIT:").grid(row=0, column=0, padx=10, pady=10, sticky="e")
limit_entry = tk.Entry(root)
limit_entry.insert(0, "1000000")
limit_entry.grid(row=0, column=1, padx=10)

# Mode Toggles
twin_var = tk.BooleanVar()
partition_var = tk.BooleanVar()
run_sanity_var = tk.BooleanVar()

tk.Checkbutton(root, text="Enable TWIN_ONLY Mode", variable=twin_var).grid(row=1, column=0, columnspan=2, pady=5)
tk.Checkbutton(root, text="Enable PARTITION_MODE", variable=partition_var).grid(row=2, column=0, columnspan=2, pady=5)
tk.Checkbutton(root, text="Enable Sanity Check (Default Mode only)", variable=run_sanity_var).grid(row=3, column=0, columnspan=2, pady=5)

# Run Pipeline Button
tk.Button(root, text="Run Pipeline", command=update_config_and_run).grid(row=4, column=0, columnspan=2, pady=20)

# Sanity Checker Button
sanity_button = tk.Button(
    root,
    text="Run Sanity Checker Only",
    command=run_sanity_checker_alone,
    state=tk.NORMAL if SANITY_READY else tk.DISABLED
)
sanity_button.grid(row=5, column=0, columnspan=2, pady=(0, 10))

if not SANITY_READY:
    tk.Label(root, text="(Run Default Mode once to unlock)", fg="gray").grid(row=6, column=0, columnspan=2)

# -----------------------------
# Run GUI
# -----------------------------
root.mainloop()
