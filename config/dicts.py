# config/dicts.py
def get_CORAL_TAG():
    from config.config import get_coral_tag
    return get_coral_tag()


# use get_CORAL_TAG() anywhere you need it, or define:
CORAL_TAG = get_CORAL_TAG()

"""MODE_PARAMS = { 
    "default_mode": {False, False}, 
    "twin_only": {True, False},
    "partition_mode":{True, True}
    }"""
# -----------------------------------
# Centralized paths from root
# -----------------------------------
PATH_MAP = {}

FOLDERS = {
    "analysis": "analysis",
    "assets": "assets",
    "blob": "blob",
    "checkpoints": "checkpoints",
    "config": "config",
    "databases": "databases",
    "gui": "gui",
    "scripts": "scripts",
    "text_output": "text_output",
    "utils": "utils",
}

SUBFOLDERS = {
    "databases": {
        "metadata": "all_systems_metadata",
        "tagged": lambda: CORAL_TAG,
    },
    "all_systems_metadata": {
        "local": "local",
        "chunked": "chunked",
    },
    "text_output": {
        "tagged": lambda: CORAL_TAG,
    },
    f"{CORAL_TAG}": {  # under text_output/{CORAL_TAG}/
        "all_motifs": "text_output_all_motifs",
        "twin_only": "text_output_twin_only",
        "partition_mode": "text_output_partition_mode",
        "analysis": "text_output_analysis",
    }
}


# -----------------------------------
# 🧠 DB table classification maps
# -----------------------------------

# Tables that map directly to system-level constructs
DB_TAG_MAP = {
    "system_cache": {
        "db_name": "sequences",
        "is_metadata": False,
        "is_motif": False,
    },
    "infinite_loops": {
        "db_name": "loops",
        "is_metadata": False,
        "is_motif": False,
    },
    "loop_entry_points": {
        "db_name": "loops",
        "is_metadata": False,
        "is_motif": False,
    },
}

# Tables used in motif/pipeline structure
DB_MAP = {
    "parameters": {
        "db_name": "motif_parameters",
        "is_metadata": False,
        "is_motif": True,
    },
    "motif_registry": {
        "db_name": "motif_registry",
        "is_metadata": False,
        "is_motif": True,
    },
    "integers": {
        "db_name": "motif_integers",
        "is_metadata": False,
        "is_motif": True,
    },
    "products": {
        "db_name": "product_motif_integers",
        "is_metadata": False,
        "is_motif": True,
    },
    "integer_cycles": {
        "db_name": "motif_cycles",
        "is_metadata": False,
        "is_motif": True,
    },
    "product_cycles": {
        "db_name": "product_motif_cycles",
        "is_metadata": False,
        "is_motif": True,
    },
    "lookup": {
        "db_name": "full_lookup",
        "is_metadata": False,
        "is_motif": True,
    },
    "lookup_twin": {
        "db_name": "full_lookup_twin",
        "is_metadata": False,
        "is_motif": True,
    },
    "lookup_partitioned": {
        "db_name": "full_lookup_partitioned",
        "is_metadata": False,
        "is_motif": True,
    },
    "entry_points": {
        "db_name": "motif_entry_points",
        "is_metadata": False,
        "is_motif": True,
    },
    "entry_points_twin": {
        "db_name": "motif_entry_points_twin",
        "is_metadata": False,
        "is_motif": True,
    },
    "entry_points_partitioned": {
        "db_name": "motif_entry_points_partitioned",
        "is_metadata": False,
        "is_motif": True,
    },
}

# Used for validation / schema enforcement
SCHEMAS = {
    "system_cache": "n INTEGER PRIMARY KEY, sequence TEXT NOT NULL",
    "infinite_loops": "normalized_hash TEXT PRIMARY KEY, loop TEXT, sequence TEXT NOT NULL",
    "loop_entry_points": "integer INTEGER PRIMARY KEY, loop TEXT",

    "parameters": (
        "A INTEGER PRIMARY KEY, L INTEGER, H INTEGER, T INTEGER, "
        "motif_EC TEXT, product_EC TEXT"  # , "motif_EC_meta TEXT, product_EC_meta TEXT"
    ),

    "motif_registry": "T INTEGER, H INTEGER, motif_EC TEXT, PRIMARY KEY (T, H, motif_EC)",

    "integers": (
        "motif_A INTEGER, integer_An INTEGER, integer_L INTEGER, H INTEGER, "
        "T INTEGER, PRIMARY KEY (motif_A, integer_An)"
    ),
    "products": (
        "motif_A INTEGER, product_An INTEGER, integer_L INTEGER, "
        "H INTEGER, T INTEGER, PRIMARY KEY (motif_A, product_An)"
    ),

    "integer_cycles": "motif_integer INTEGER PRIMARY KEY, H INTEGER, T INTEGER, cycle TEXT",
    "product_cycles": "product_integer INTEGER PRIMARY KEY, H INTEGER, T INTEGER, cycle TEXT",

    "lookup": "unique_int INTEGER PRIMARY KEY",
    "lookup_twin": "unique_int INTEGER PRIMARY KEY",
    "lookup_partitioned": "unique_int INTEGER PRIMARY KEY",

    "entry_points": "integer INTEGER PRIMARY KEY, entry_step INTEGER, entered_motif INTEGER",
    "entry_points_twin": "integer INTEGER PRIMARY KEY, entry_step INTEGER, entered_motif INTEGER",
    "entry_points_partitioned": "integer INTEGER PRIMARY KEY, entry_step INTEGER, entered_motif INTEGER",
}
#Add in compressed log dicts and standardized text outputs as dicts here, to be referenced by classes.