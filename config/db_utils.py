import sqlite3
import os
import json
from config.config import DB_PATH, TWIN_ONLY

def connect_db(db_name):
    """Connects to a SQLite database in the shared DB_PATH."""
    return sqlite3.connect(os.path.join(DB_PATH, db_name))

def setup_collatz_db():
    """Creates collatz_sequences.db and ensures the collatz_cache table exists."""
    conn = connect_db("collatz_sequences.db")
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS collatz_cache (
            n INTEGER PRIMARY KEY,
            sequence TEXT
        )
    ''')
    conn.commit()
    conn.close()

def fetch_sequence_from_db(n):
    """Fetches a Collatz sequence from the database by integer."""
    conn = connect_db("collatz_sequences.db")
    cursor = conn.cursor()
    cursor.execute('SELECT sequence FROM collatz_cache WHERE n = ?', (n,))
    result = cursor.fetchone()
    conn.close()
    return json.loads(result[0]) if result else None

def store_sequences_in_db(sequences):
    """Stores a list of (n, sequence) pairs into collatz_cache with batch insert."""
    conn = connect_db("collatz_sequences.db")
    cursor = conn.cursor()
    cursor.executemany(
        'INSERT OR REPLACE INTO collatz_cache (n, sequence) VALUES (?, ?)',
        [(n, json.dumps(seq)) for n, seq in sequences]
    )
    conn.commit()
    conn.close()

def setup_motif_parameters_db():
    """Creates and initializes motif_parameters.db with the required table."""
    conn = connect_db("motif_parameters.db")
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS motif_parameters (
            A INTEGER PRIMARY KEY,
            B INTEGER,
            L INTEGER,
            Y INTEGER,
            motif_OE TEXT,
            product_OE TEXT
        )
    ''')
    conn.commit()
    conn.close()

def setup_motif_integers_db():
    """Creates motif_integers.db and ensures its table exists."""
    conn = connect_db("motif_integers.db")
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS motif_integers (
            motif_A INTEGER,
            integer_N INTEGER,
            integer_L INTEGER,
            PRIMARY KEY (motif_A, integer_N),
            FOREIGN KEY (motif_A) REFERENCES motif_parameters(A)
        )
    ''')
    conn.commit()
    conn.close()

def setup_product_motif_integers_db():
    """Creates product_motif_integers.db and ensures its table exists."""
    conn = connect_db("product_motif_integers.db")
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS product_motif_integers (
            motif_A INTEGER,
            product_N INTEGER,
            integer_L INTEGER,
            PRIMARY KEY (motif_A, product_N),
            FOREIGN KEY (motif_A) REFERENCES motif_parameters(A)
        )
    ''')
    conn.commit()
    conn.close()

def setup_motif_cycles_db():
    """Creates motif_cycles.db and ensures the table exists."""
    conn = connect_db("motif_cycles.db")
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS motif_cycles (
            motif_integer INTEGER PRIMARY KEY,
            cycle TEXT
        )
    ''')
    conn.commit()
    conn.close()

def setup_product_motif_cycles_db():
    """Creates product_motif_cycles.db and ensures the table exists."""
    conn = connect_db("product_motif_cycles.db")
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS product_motif_cycles (
            product_N INTEGER PRIMARY KEY,
            cycle TEXT
        )
    ''')
    conn.commit()
    conn.close()

def setup_full_lookup_db():
    """Creates full_lookup.db and ensures the table exists."""
    conn = connect_db("full_lookup.db")
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS full_lookup (
            unique_int INTEGER PRIMARY KEY
        )
    ''')
    conn.commit()
    return conn, cursor

def setup_motif_entry_db():
    """Creates motif_entry_points.db and ensures the table exists."""
    conn = connect_db("motif_entry_points.db")
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS motif_entry_points (
            integer_N INTEGER PRIMARY KEY,
            entry_step INTEGER,
            entered_motif INTEGER
        )
    ''')
    conn.commit()
    conn.close()


def get_twin_filter_condition():
    """
    Returns the appropriate SQL WHERE condition to apply based on TWIN_ONLY flag.
    This ensures consistent filtering logic across all scripts.
    """
    if TWIN_ONLY:
        return "WHERE B = Y + 1"
    return ""  # No condition if TWIN_ONLY is off
