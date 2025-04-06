import os
import duckdb
import zipfile

def load_blob_data(path, coral_tag, range_limit=None):
    if path.endswith(".zip"):
        # Future: extract + load zip contents
        raise NotImplementedError("ZIP import not yet supported.")
    
    db_path = os.path.join(path, f"{coral_tag}_sequences.duckdb")
    con = duckdb.connect(db_path)

    query = f"SELECT * FROM system_cache"
    if range_limit:
        query += f" WHERE n <= {range_limit}"

    df = con.execute(query).fetchdf()
    con.close()
    return df