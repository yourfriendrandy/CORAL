import os
import sqlite3

# Define absolute path to your database
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
db_path = os.path.join(project_root, "databases", "motif_parameters.db")

# Step 1: Delete the database file if it exists
if os.path.exists(db_path):
    os.remove(db_path)
    print("🗑️ Deleted existing motif_parameters.db")
else:
    print("ℹ️ No existing motif_parameters.db found — starting fresh")

# Step 2: Recreate database file and motif_parameters table
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE motif_parameters (
        A INTEGER PRIMARY KEY,
        B INTEGER,
        L INTEGER,
        Y INTEGER,
        motif_OE TEXT,
        product_OE TEXT
    )
""")

conn.commit()
conn.close()

print("✅ motif_parameters.db has been reset and initialized.")
