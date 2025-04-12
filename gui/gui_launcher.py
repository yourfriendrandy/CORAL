import tkinter as tk
from tkinter import messagebox, StringVar, OptionMenu

import subprocess
import os
import sys
import math
import threading
import platform
import time
from pathlib import Path
from PIL import Image, ImageTk


# -----------------------------
# Paths and Setup
# -----------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
sys.path.insert(0, PROJECT_ROOT)

CONFIG_PATH = os.path.join(PROJECT_ROOT, "config", "config.py")
FORM_COLONY_SCRIPT = os.path.join(PROJECT_ROOT, "form_colony.py")
SANITY_FLAG_PATH = os.path.join(PROJECT_ROOT, "config", "sanity_ready.flag")
SANITY_READY = os.path.exists(SANITY_FLAG_PATH)

from config.safety import start_emergency_watcher, EMERGENCY_FLAG # noqa: E402
from config.heuristic_utils import estimate_system_behavior  # noqa: E402

try:
    from utils.export_zip import export_system_zip
except Exception:
    def export_system_zip():
        messagebox.showinfo("Export", "Export logic not available.")

VENV_PYTHON = (
    os.path.join(PROJECT_ROOT, "venv", "bin", "python3")
    if sys.platform != "win32"
    else os.path.join(PROJECT_ROOT, "venv", "Scripts", "python.exe")
)

if EMERGENCY_FLAG.exists():
    EMERGENCY_FLAG.unlink()


# -----------------------------
# Spinner Setup
# -----------------------------
spinner_frames = []
spinner_label = None
spinner_running = False

def load_spinner():
    try:
        spinner_path = os.path.join(PROJECT_ROOT, "assets", "spinner.gif")
        spinner_img = Image.open(spinner_path)
        while True:
            spinner_frames.append(ImageTk.PhotoImage(spinner_img.copy()))
            spinner_img.seek(len(spinner_frames))
    except Exception as e:
        print("⚠️ Spinner not loaded:", e)


def animate_spinner(index=0):
    if spinner_running and spinner_frames:
        spinner_label.config(image=spinner_frames[index])
        root.after(62, animate_spinner, (index + 1) % len(spinner_frames))


# -----------------------------
# Logic
# -----------------------------
def valid_m_values(z):
    if abs(z) <= 1:
        return []

    raw_values = [m for m in range(z + 1, z * z) if math.gcd(z, m) == 1 and m > 0]
    if not collatz_like_only.get():
        return sorted(raw_values + [-m for m in raw_values])

    filtered = []
    for m in raw_values:
        h = estimate_system_behavior(m, z)
        if 1 < h["heuristic"] <= 1.125:
            filtered.append(m)

    return sorted(filtered + [-m for m in filtered])


def update_m_dropdown(*args):
    try:
        z_val = int(z_entry.get())
    except ValueError:
        selected_m.set("Invalid Z")
        warning_label.grid_remove()
        menu = m_dropdown["menu"]
        menu.delete(0, "end")
        menu.add_command(label="Invalid Z", command=tk._setit(selected_m, "Invalid Z"))
        return

    values = valid_m_values(z_val)
    menu = m_dropdown["menu"]
    menu.delete(0, "end")

    if not values:
        selected_m.set("No valid M")
        menu.add_command(label="No valid M", command=tk._setit(selected_m, "No valid M"))
        warning_label.grid_remove()
        return

    selected_m.set(str(values[0]))
    for val in values:
        menu.add_command(label=str(val), command=tk._setit(selected_m, str(val)))

    if z_val < 0 or any(m < 0 for m in values):
        warning_label.grid()
    else:
        warning_label.grid_remove()


def run_heuristic_analysis():
    try:
        m_raw = selected_m.get()
        if m_raw in ("Invalid Z", "No valid M"):
            raise ValueError("Cannot estimate: invalid M or Z")
        m = int(m_raw)
        z = int(z_entry.get())

        result = estimate_system_behavior(m, z)

        msg = (
            f"🧠 Structural Heuristic for M = {m}, Z = {z}\n\n"
            f"• Minimal k-segment length needed: {result['k_required']}\n"
            f"• Test fragment length: {result['fragment_length']}\n"
            f"• Expansion steps (H): {result['H']}\n"
            f"• Compression steps (T): {result['T']}\n"
            f"• Heuristic (M^H / Z^T): {result['heuristic']:.5f}\n"
            f"• Classification: {result['category']}\n\n"
            f"Test Fragment:\n{result['sequence'][:120]}{'...' if len(result['sequence']) > 120 else ''}"
        )

        messagebox.showinfo("Heuristic Estimate", msg)

    except Exception as e:
        messagebox.showerror("Heuristic Error", str(e))


def launch_pipeline_in_terminal():
    system = platform.system()
    command = f'{VENV_PYTHON} {FORM_COLONY_SCRIPT}'
    cwd = PROJECT_ROOT

    if system == "Darwin":
        subprocess.Popen([
            "osascript",
            "-e", f'tell application "Terminal"',  # noqa: F541
            "-e", f'do script "cd \\"{cwd}\\"; {command}"',
            "-e", "activate",
            "-e", "end tell"
        ])
    elif system == "Windows":
        subprocess.Popen([
            "cmd.exe", "/c",
            f'start cmd.exe /k "cd /d {cwd} && {command}"'
        ])
    else:  # Linux
        try:
            subprocess.Popen([
                "x-terminal-emulator", "-e",
                f"bash -c 'cd \"{cwd}\" && {command}; exec bash'"
            ])
        except FileNotFoundError:
            try:
                subprocess.Popen([
                    "gnome-terminal", "--", "bash", "-c",
                    f"cd \"{cwd}\" && {command}; exec bash"
                ])
            except Exception as e:
                messagebox.showerror("Linux Terminal Error", f"Failed to open terminal:\n{e}")


def run_pipeline():
    global spinner_running
    spinner_running = True
    spinner_label.grid(row=16, column=0, columnspan=2, pady=5)
    hold_label.grid(row=17, column=0, columnspan=2)
    animate_spinner()
    launch_pipeline_in_terminal()

    start_emergency_watcher()

    def monitor_flag():
        flag_path = os.path.join(PROJECT_ROOT, "pipeline_running.flag")
        while os.path.exists(flag_path):
            time.sleep(1)
        root.after(0, on_pipeline_complete)

    threading.Thread(target=monitor_flag, daemon=True).start()


def stop_spinner():
    global spinner_running
    spinner_running = False
    spinner_label.grid_remove()
    hold_label.grid_remove()


def on_pipeline_complete():
    stop_spinner()
    sanity_button.config(state=tk.NORMAL if os.path.exists(SANITY_FLAG_PATH) else tk.DISABLED)
    export_button.config(state=tk.NORMAL)


def update_config_and_run():
    try:
        twin = twin_var.get()
        part = partition_var.get()
        z = int(z_entry.get())
        min_val = int(min_entry.get())
        max_val = int(max_entry.get())
        compressed = compressed_var.get()
        b_polarity = selected_b_polarity.get()
        m_raw = selected_m.get()

        if m_raw in ("Invalid Z", "No valid M"):
            raise ValueError("Please select a valid Z and M")
        m = int(m_raw)
        if abs(z) <= 1:
            raise ValueError("Z cannot be 0, 1, or -1")
        if twin and part:
            raise ValueError("Cannot enable both TWIN_ONLY and PARTITION_MODE")

        if min_val < 0 and max_val > 0:
            n_polarity = "Nboth"
        elif max_val < 0:
            n_polarity = "Nminus"
        else:
            n_polarity = "Nplus"

        tag = f"CORAL_{m}_{z}_{b_polarity}_{n_polarity}"

        with open(CONFIG_PATH, "r") as f:
            lines = f.readlines()

        with open(CONFIG_PATH, "w") as f:
            for line in lines:
                if line.startswith("TWIN_ONLY"):
                    f.write(f"TWIN_ONLY = {twin}\n")
                elif line.startswith("PARTITION_MODE"):
                    f.write(f"PARTITION_MODE = {part}\n")
                elif line.startswith("COMPRESSED_LOG"):
                    f.write(f"COMPRESSED_LOG = {compressed}\n")
                elif line.startswith("INTEGER_MIN"):
                    f.write(f"INTEGER_MIN = {min_val}\n")
                elif line.startswith("INTEGER_MAX"):
                    f.write(f"INTEGER_MAX = {max_val}\n")
                elif line.startswith("Z ="):
                    f.write(f"Z = {z}\n")
                elif line.startswith("M ="):
                    f.write(f"M = {m}\n")
                elif line.startswith("B_POLARITY"):
                    f.write(f"B_POLARITY = \"{b_polarity}\"\n")
                elif line.startswith("N_POLARITY"):
                    f.write(f"N_POLARITY = \"{n_polarity}\"\n")
                elif line.startswith("CORAL_TAG"):
                    f.write(f"CORAL_TAG = \"{tag}\"\n")
                else:
                    f.write(line)

        run_pipeline()

    except Exception as e:
        messagebox.showerror("Config Error", str(e))


def run_sanity_checker():
    try:
        subprocess.Popen([
            VENV_PYTHON,
            os.path.join(PROJECT_ROOT, "analysis", "sanity_checker.py")
        ])
        root.destroy()
    except Exception as e:
        messagebox.showerror("Sanity Checker", f"Failed to launch sanity checker:\n{e}")

def manual_emergency_flag():
    Path("pipeline_halt.flag").touch()
    messagebox.showinfo("Emergency Set", "pipeline_halt.flag has been created.\nThe pipeline will shut down shortly.")
    root.destroy()

# -----------------------------
# GUI Setup
# -----------------------------
root = tk.Tk()
root.title("CORAL Launcher 🧬")

tk.Label(root, text="INTEGER_MIN:").grid(row=0, column=0, padx=10, pady=5, sticky="e")
min_entry = tk.Entry(root)
min_entry.insert(0, "1")
min_entry.grid(row=0, column=1, padx=10)

tk.Label(root, text="INTEGER_MAX:").grid(row=1, column=0, padx=10, pady=5, sticky="e")
max_entry = tk.Entry(root)
max_entry.insert(0, "1000000")
max_entry.grid(row=1, column=1, padx=10)

tk.Label(root, text="Z (≠ 0, ±1):").grid(row=2, column=0, padx=10, pady=5, sticky="e")
z_entry = tk.Entry(root)
z_entry.insert(0, "2")
z_entry.grid(row=2, column=1, padx=10)
z_entry.bind("<KeyRelease>", update_m_dropdown)

tk.Label(root, text="Select M:").grid(row=3, column=0, padx=10, pady=5, sticky="e")
selected_m = StringVar()
m_dropdown = OptionMenu(root, selected_m, "Loading...")
m_dropdown.grid(row=3, column=1, padx=10)

tk.Label(root, text="B Polarity:").grid(row=4, column=0, padx=10, pady=5, sticky="e")
selected_b_polarity = StringVar(value="Bplus")
b_dropdown = OptionMenu(root, selected_b_polarity, "Bplus", "Bminus", "Bboth")
b_dropdown.grid(row=4, column=1, padx=10)

warning_label = tk.Label(root, text="⚠️ Warning: negative fields increase complexity.", fg="orange")
warning_label.grid(row=5, column=0, columnspan=2)
warning_label.grid_remove()

collatz_like_only = tk.BooleanVar(value=True)
tk.Checkbutton(root, text="Show only Collatz-like M values", variable=collatz_like_only, command=update_m_dropdown).grid(row=6, column=0, columnspan=2)

tk.Button(root, text="Estimate System Behavior", command=run_heuristic_analysis).grid(row=7, column=0, columnspan=2, pady=(5, 15))

compressed_var = tk.BooleanVar()
tk.Checkbutton(root, text="Show additional log information?", variable=compressed_var).grid(row=8, column=0, columnspan=2)

twin_var = tk.BooleanVar()
partition_var = tk.BooleanVar()

tk.Label(root, text="--- Modes ---").grid(row=9, column=0, columnspan=2, pady=(15, 2))
tk.Checkbutton(root, text="Enable TWIN_ONLY Mode", variable=twin_var).grid(row=10, column=0, columnspan=2)
tk.Checkbutton(root, text="Enable PARTITION_MODE", variable=partition_var).grid(row=11, column=0, columnspan=2)

tk.Button(root, text="Run Pipeline", command=update_config_and_run).grid(row=12, column=0, columnspan=2, pady=15)

sanity_button = tk.Button(root, text="Run Sanity Checker Only", command=run_sanity_checker, state=tk.NORMAL if SANITY_READY else tk.DISABLED)
sanity_button.grid(row=13, column=0, columnspan=2, pady=(0, 5))

tk.Label(root, text="(Default mode must complete once to unlock)", fg="gray").grid(row=14, column=0, columnspan=2)

export_button = tk.Button(root, text="📦 Export System Data", command=export_system_zip, state=tk.DISABLED)
export_button.grid(row=15, column=0, columnspan=2, pady=(10, 5))

tk.Button(root, text="🛑 Emergency Exit", command=manual_emergency_flag,
          bg="red", fg="white").grid(row=16, column=0, columnspan=2, pady=(10, 5))

spinner_label = tk.Label(root)
spinner_label.grid_remove()
hold_label = tk.Label(root, text="Hold, please...", font=("Helvetica", 10), fg="gray")
hold_label.grid_remove()

load_spinner()
update_m_dropdown()
root.mainloop()
