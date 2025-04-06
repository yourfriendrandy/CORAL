import argparse
from blob_utils import load_blob_data
from blob_renderer import launch_blob_window

def main():
    parser = argparse.ArgumentParser(description="Launch Blob visualization.")
    parser.add_argument("--coral_tag", required=True, help="Name of the CORAL system (e.g., CORAL_3_2)")
    parser.add_argument("--input_path", required=True, help="Path to cold storage folder or .zip")
    parser.add_argument("--range_limit", type=int, default=None, help="Optional integer limit for rendering")
    args = parser.parse_args()

    print(f"🧬 Loading system: {args.coral_tag}")
    system_data = load_blob_data(args.input_path, args.coral_tag, args.range_limit)

    print("🫧 Launching Blob...")
    launch_blob_window(system_data)

if __name__ == "__main__":
    main()