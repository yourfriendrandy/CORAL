# config/safety.py
import os
import sys
import time
import threading
from pathlib import Path
from config.logger import logger

EMERGENCY_FLAG = Path("pipeline_halt.flag")

def check_emergency_shutdown():
    if EMERGENCY_FLAG.exists():
        logger.warning("🛑 Emergency shutdown triggered via pipeline_halt.flag")
        sys.exit(99)

def start_emergency_watcher(interval=1.5):
    def watch():
        while True:
            check_emergency_shutdown()
            time.sleep(interval)

    t = threading.Thread(target=watch, daemon=True)
    t.start()