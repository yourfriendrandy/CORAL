# config/logger.py
import logging
import os
import time
import psutil

# -----------------------------
# SETUP: Create log directory
# -----------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LOG_PATH = os.path.join(BASE_DIR, "runtime_logs")
os.makedirs(LOG_PATH, exist_ok=True)

# Unique log file for each session
timestamp = time.strftime("%Y%m%d_%H%M%S")
log_file = os.path.join(LOG_PATH, f"session_{timestamp}.log")

# -----------------------------
# LOGGER CONFIGURATION
# -----------------------------
logger = logging.getLogger("CollatzLogger")
logger.setLevel(logging.INFO)
logger.propagate = False  # Prevents double logging if imported multiple times

# Formatter
formatter = logging.Formatter("[%(asctime)s] %(message)s", "%Y-%m-%d %H:%M:%S")

# File Handler
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Console Handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# -----------------------------
# SYSTEM RESOURCE LOGGER
# -----------------------------
def log_resource_usage(tag="Resource Check"):
    process = psutil.Process(os.getpid())
    memory = process.memory_info().rss / (1024 * 1024)  # MB
    cpu = process.cpu_percent(interval=0.1)             # %
    logger.info(f"{tag} 📊 Memory: {memory:.2f} MB | CPU: {cpu:.1f}%")
