def query_integers_by_mode(table: str, column: str, max_value: int) -> list[int]:
    """
    Queries integers from a table column, filtered by mode and max_value.

    Args:
        table (str): Table name to query from (e.g. "motif_integers").
        column (str): Column containing the integers to return (e.g. "An" or "3An").
        max_value (int): Inclusive upper bound for integer values.

    Returns:
        List[int]: Mode-filtered and sorted list of integers.
    """
    conn = connect_db(table)
    cursor = conn.cursor()

    base_query = f"SELECT {column} FROM {table} WHERE {column} <= ?"
    if TWIN_ONLY:
        base_query += " AND T = H + 1"
    elif PARTITION_MODE:
        base_query += " AND T != H + 1"

    cursor.execute(base_query, (max_value,))
    result = sorted(row[0] for row in cursor.fetchall())
    conn.close()
    return result
