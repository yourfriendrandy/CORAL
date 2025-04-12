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
def resolve_to_path(base):
    """Ensure any string or lambda from FOLDERS/SUBFOLDERS becomes a Path object."""
    if isinstance(base, str):
        return PROJECT_ROOT / base
    if callable(base):
        return resolve_to_path(base())
    return base  # Already a Path


def get_path(key: str, *subpaths, ensure_exists: bool = False) -> Path:
    """
    Resolves CORAL project paths using declared folder structure, subpaths, and special DB keys.
    """
    if key == "duckdb":
        table = subpaths[0]
        db_name = DB_TAG_MAP.get(table, DB_MAP.get(table))["db_name"]
        prefix = f"{CORAL_TAG}_" if table in DB_TAG_MAP else ""
        return resolve_to_path(SUBFOLDERS["database_tag"]) / f"{prefix}{db_name}.duckdb"

    elif key == "sqlite":
        table = subpaths[0]
        db_name = DB_TAG_MAP.get(table, DB_MAP.get(table))["db_name"]
        return resolve_to_path(SUBFOLDERS["database_tag"]) / f"{db_name}_temp.db"

    elif key in PATH_MAP:
        base = PATH_MAP[key]
    elif key in FOLDERS:
        base = resolve_to_path(FOLDERS[key])
    elif key in SUBFOLDERS:
        base = resolve_to_path(SUBFOLDERS[key])
    else:
        raise KeyError(f"Unknown path key: {key}")

    resolved = base.joinpath(*subpaths) if subpaths else base
    if ensure_exists and not resolved.exists():
        raise FileNotFoundError(f"Path does not exist: {resolved}")
    return resolved


# -----------------------------
# Register dynamic file paths
# -----------------------------
def register_output_path(name: str, path: Path):
    """
    Manually registers a new output path (e.g., a JSON or TXT file).
    Example:
        register_output_path("custom_output", Path("text_output/example.json"))
    """
    PATH_MAP[name] = path


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
            if isinstance(SUBFOLDERS.get(name), dict):
                resolve_subfolder(next_path, SUBFOLDERS[name])
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

    for folder_key, folder_str in FOLDERS.items():
        folder_path = PROJECT_ROOT / folder_str
        register_files(folder_path, path_key=[folder_key], base_map=base_map)
        if folder_key in SUBFOLDERS:
            resolve_subfolder(folder_path, SUBFOLDERS[folder_key])

    PATH_MAP.update(base_map)