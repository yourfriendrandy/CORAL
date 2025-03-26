#!/usr/bin/env python3
import tkinter as tk
from tkinter import messagebox
import subprocess
import os
import sys

# Adjust paths based on script location
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
CONFIG_PATH = os.path.join(PROJECT_ROOT, "config", "config.py")
MASTER_SCRIPT = os.path.join(PROJECT_ROOT, "completeness_algorithm_master.py")
VENV_PYTHON = os.path.join(PROJECT_ROOT, "venv", "bin", "python3") if sys.platform != "win32" else os.path.join(PROJECT_ROOT, "venv", "Scripts", "python.exe")

def update_config_and_run():
    try:
        twin = twin_var.get()
        partition = partition_var.get()
        integer_limit = int(limit_entry.get())

        # Guard against invalid settings
        if twin and partition:
            messagebox.showerror("Invalid Configuration", "Cannot enable both TWIN_ONLY and PARTITION_MODE.")
            return

        # Rewrite config.py toggles
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
                else:
                    f.write(line)

        # 👇 Print summary to terminal
        print("\n🌱 Config Applied:")
        print(f"  TWIN_ONLY = {twin}")
        print(f"  PARTITION_MODE = {partition}")
        print(f"  INTEGER_LIMIT = {integer_limit}\n")

        # Launch main pipeline from venv
        subprocess.Popen([
            os.path.join("..", "venv", "bin", "python3"),
            MASTER_SCRIPT
        ])

        root.destroy()

    except Exception as e:
        messagebox.showerror("Error", f"Something went wrong:\n{str(e)}")

# -----------------------------
# GUI Layout
# -----------------------------
root = tk.Tk()
root.title("Collatz Config Launcher")

tk.Label(root, text="INTEGER_LIMIT:").grid(row=0, column=0, padx=10, pady=10)
limit_entry = tk.Entry(root)
limit_entry.insert(0, "1000000")  # Default
limit_entry.grid(row=0, column=1)

twin_var = tk.BooleanVar()
partition_var = tk.BooleanVar()

tk.Checkbutton(root, text="Enable TWIN_ONLY Mode", variable=twin_var).grid(row=1, column=0, columnspan=2, pady=5)
tk.Checkbutton(root, text="Enable PARTITION_MODE", variable=partition_var).grid(row=2, column=0, columnspan=2, pady=5)

tk.Button(root, text="Run Pipeline", command=update_config_and_run).grid(row=3, column=0, columnspan=2, pady=20)

root.mainloop()
