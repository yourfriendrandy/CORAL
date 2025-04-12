from pathlib import Path
from config.config import CORAL_TAG
from config.dicts import DB_MAP, DB_TAG_MAP, PATH_MAP, FOLDERS, SUBFOLDERS

# -----------------------------
# Project Root
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]

def get_project_root() -> Path:
    return PROJECT_ROOT

# -----------------------------
# Main Path Resolver
# -----------------------------
def get_path(key: str, *subpaths, ensure_exists: bool = False) -> Path:
    """
    Resolves CORAL project paths using declared folder structure, subpaths, and special DB keys.
    
    Special handling:
    • get_path("duckdb", "products") → CORAL_TAG duckdb path
    • get_path("sqlite", "parameters") → CORAL_TAG sqlite temp path
    • get_path("text_output", "some_output.json") → general file path

    Args:
        key (str): Named folder or file key (from PATH_MAP, FOLDERS, SUBFOLDERS, or DB table name).
        *subpaths (str): Any additional subdirectories or filenames.
        ensure_exists (bool): Raise error if resulting path doesn't exist.

    Returns:
        Path: Fully resolved path.
    """
    if key == "duckdb":
        table = subpaths[0]
        db_name = DB_TAG_MAP.get(table, DB_MAP.get(table))["db_name"]
        prefix = f"{CORAL_TAG}_" if table in DB_TAG_MAP else ""
        return SUBFOLDERS["database_tag"]() / f"{prefix}{db_name}.duckdb"

    elif key == "sqlite":
        table = subpaths[0]
        db_name = DB_TAG_MAP.get(table, DB_MAP.get(table))["db_name"]
        return SUBFOLDERS["database_tag"]() / f"{db_name}_temp.db"

    elif key in PATH_MAP:
        base = PATH_MAP[key]
    elif key in FOLDERS:
        base = FOLDERS[key]
    elif key in SUBFOLDERS:
        base = SUBFOLDERS[key]()
    else:
        raise KeyError(f"Unknown path key: {key}")

    resolved = base.joinpath(*subpaths) if subpaths else base
    if ensure_exists and not resolved.exists():
        raise FileNotFoundError(f"Path does not exist: {resolved}")
    return resolved


# -----------------------------
# Dynamic Path Discovery
# -----------------------------
def initialize_paths():
    """
    Walks FOLDER_NAMES and SUBFOLDER_NAMES recursively to find all files
    with supported extensions. Updates PATH_MAP with dynamic keys.
    """
    extensions = {".py", ".json", ".gif", ".txt"}
    base_map = {}

    def resolve_subfolder(path_so_far, subfolder_dict):
        for name, sub in subfolder_dict.items():
            if callable(sub):
                try:
                    sub = sub()
                except Exception:
                    continue  # Skip if CORAL_TAG not initialized
            next_path = path_so_far / sub
            if isinstance(SUBFOLDER_NAMES.get(name), dict):
                resolve_subfolder(next_path, SUBFOLDER_NAMES[name])
            else:
                register_files(next_path, path_key=[name], base_map=base_map)

    def register_files(base_path: Path, path_key: list[str], base_map: dict):
        if not base_path.exists():
            return
        for file in base_path.glob("**/*"):
            if file.is_file() and file.suffix in extensions:
                rel = file.relative_to(base_path)
                key = "_".join(path_key + list(rel.parts)).replace(".", "_")
                base_map[key] = file

    for folder, name in FOLDER_NAMES.items():
        base_path = PROJECT_ROOT / name
        register_files(base_path, path_key=[folder], base_map=base_map)

        # If folder has SUBFOLDER_NAMES, recurse
        if folder in SUBFOLDER_NAMES:
            resolve_subfolder(base_path, SUBFOLDER_NAMES[folder])

    PATH_MAP.update(base_map)