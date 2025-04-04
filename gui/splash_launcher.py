import tkinter as tk
from PIL import Image, ImageTk
import subprocess
import os
import sys
import threading

# -----------------------------
# Paths
# -----------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
GUI_LAUNCH = os.path.join(PROJECT_ROOT, "gui", "gui_launcher.py")
VENV_PYTHON = os.path.join(PROJECT_ROOT, "venv", "bin", "python3") if sys.platform != "win32" else os.path.join(PROJECT_ROOT, "venv", "Scripts", "python.exe")
SPLASH_PATH = os.path.join(PROJECT_ROOT, "assets", "CORAL Splash.gif")

# -----------------------------
# Splash Window
# -----------------------------
def show_splash():
    splash = tk.Tk()
    splash.overrideredirect(True)
    splash.configure(bg="#ff9999")

    img = Image.open(SPLASH_PATH)
    frames = []

    try:
        while True:
            frames.append(ImageTk.PhotoImage(img.copy()))
            img.seek(len(frames))  # move to next frame
    except EOFError:
        pass

    label = tk.Label(splash, bg="#ff9999")
    label.pack()
    splash.geometry("320x140+600+300")

    def animate(index=0):
        label.configure(image=frames[index])
        splash.after(62, animate, (index + 1) % len(frames))  # 16 fps = ~62 ms/frame

    animate()
    return splash

# -----------------------------
# GUI Launcher Thread
# -----------------------------
def launch_gui():
    subprocess.Popen(
        [VENV_PYTHON, GUI_LAUNCH],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    splash = show_splash()
    threading.Thread(target=launch_gui).start()
    splash.after(3500, splash.destroy)  # ~3.5 seconds
    splash.mainloop()
