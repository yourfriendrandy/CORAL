#!/bin/bash

# Path to your project root
PROJECT_DIR="$HOME/Documents/completeness_algorithm"

# Open new Terminal and launch GUI
osascript <<EOF
tell application "Terminal"
    activate
    do script "cd \"$PROJECT_DIR/gui\" && source ../venv/bin/activate && python3 gui_launcher.py"
end tell
EOF