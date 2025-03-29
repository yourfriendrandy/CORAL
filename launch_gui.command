#!/bin/bash

# Path to your project root
PROJECT_DIR="$HOME/Documents/CORAL"

# Open new Terminal and launch GUI
osascript <<EOF
tell application "Terminal"
    activate
    do script "cd \"$PROJECT_DIR\" && source venv/bin/activate && python3 gui/gui_launcher.py"
end tell
EOF