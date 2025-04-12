import os
import zipfile
from tkinter import filedialog, messagebox

from config.config import CORAL_TAG, ROOT_DIR

def get_text_output_flags(base_dir):
    flags = []
    check_map = {
        "text_output_all_motifs": "D",
        "text_output_twin_only": "T",
        "text_output_partition_mode": "P",
        "text_output_analysis": "A"
    }

    for folder, flag in check_map.items():
        folder_path = os.path.join(base_dir, folder)
        if os.path.exists(folder_path) and any(
            fname.endswith(('.txt', '.csv', '.png', '.db'))
            for fname in os.listdir(folder_path)
        ):
            flags.append(flag)

    return sorted(flags)

def export_system_zip():
    base_db_dir = os.path.join(ROOT_DIR, "databases", CORAL_TAG)
    text_output_dir = os.path.join(ROOT_DIR, "text_output", CORAL_TAG)

    # Check if pipeline has been run
    pipeline_flag = os.path.join(base_db_dir, "pipeline_complete.flag")
    if not os.path.exists(pipeline_flag):
        messagebox.showerror("Export Blocked", "Pipeline must be run at least once before exporting system data.")
        return

    # Check which folders are populated
    flags = get_text_output_flags(text_output_dir)
    suffix = "".join(flags)
    zip_name = f"{CORAL_TAG}_{suffix or 'EMPTY'}.zip"

    # Ask user for save location
    save_path = filedialog.asksaveasfilename(
        defaultextension=".zip",
        filetypes=[("ZIP files", "*.zip")],
        initialfile=zip_name,
        title="Save System Archive"
    )

    if not save_path:
        return  # User canceled

    try:
        with zipfile.ZipFile(save_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Add database folder
            for foldername, _, filenames in os.walk(base_db_dir):
                for filename in filenames:
                    filepath = os.path.join(foldername, filename)
                    arcname = os.path.relpath(filepath, ROOT_DIR)
                    zipf.write(filepath, arcname)

            # Add relevant text_output folders
            for folder_flag in flags:
                folder_name = {
                    "D": "text_output_all_motifs",
                    "T": "text_output_twin_only",
                    "P": "text_output_partition_mode",
                    "A": "text_output_analysis"
                }[folder_flag]

                full_path = os.path.join(text_output_dir, folder_name)
                for foldername, _, filenames in os.walk(full_path):
                    for filename in filenames:
                        filepath = os.path.join(foldername, filename)
                        arcname = os.path.relpath(filepath, ROOT_DIR)
                        zipf.write(filepath, arcname)

        messagebox.showinfo("✅ Export Successful", f"System data saved to:\n{save_path}")

    except Exception as e:
        messagebox.showerror("Error", f"Failed to create zip:\n{str(e)}")
