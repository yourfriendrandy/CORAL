# config/text_utils.py

import os
from config.config import TEXT_OUTPUT_ACTIVE


def write_text_output(filename: str, content: str, subdir: str = ""):
    """
    Writes a text file to the active output directory (optionally into a subfolder).
    
    - filename: Name of the file (e.g. "motif_summary.txt")
    - content: String content to write
    - subdir: Optional subfolder inside TEXT_OUTPUT_ACTIVE
    """
    output_dir = os.path.join(TEXT_OUTPUT_ACTIVE, subdir) if subdir else TEXT_OUTPUT_ACTIVE
    os.makedirs(output_dir, exist_ok=True)
    
    path = os.path.join(output_dir, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    return path  # Optional: return for logging/debugging


def append_text_output(filename: str, content: str, subdir: str = ""):
    """
    Appends text to an existing file in the active output directory.
    """
    output_dir = os.path.join(TEXT_OUTPUT_ACTIVE, subdir) if subdir else TEXT_OUTPUT_ACTIVE
    os.makedirs(output_dir, exist_ok=True)

    path = os.path.join(output_dir, filename)
    with open(path, "a", encoding="utf-8") as f:
        f.write(content)
    return path
# config/text_utils.py

TEXT_OUTPUT_FILES = {
    "system_rules": "system_rules.txt",
    "motif_cycles": "motif_cycles_output.txt",
    "product_cycles": "product_motif_cycles_output.txt",
    "three_n_2x": "three_n_times_2x_exceptions.txt",
    # add more as needed...
}

def get_output_path(key: str, subdir: str = ""):
    filename = TEXT_OUTPUT_FILES.get(key)
    if not filename:
        raise ValueError(f"No text output file registered for key: {key}")
    path = os.path.join(TEXT_OUTPUT_ACTIVE, subdir, filename) if subdir else os.path.join(TEXT_OUTPUT_ACTIVE, filename)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path