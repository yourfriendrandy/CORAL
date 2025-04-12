#!/usr/bin/env python3
import os
import ray
import json
from config.config import (
    INTEGER_MIN, INTEGER_MAX, TEXT_OUTPUT_ACTIVE, M, Z, ASSUME_ONE_STEP_REDUCTION, F,
    TWIN_ONLY, PARTITION_MODE, CHUNK_SIZE
)
from config.db_utils import (
    read_duckdb, write_resume_checkpoint, read_resume_checkpoint,
    port_and_prune_sqlite_to_duckdb, insert_batch, setup_table, connect_db
)
from config.class_utils import PipelineStep, SchemaAwareWriter
from config.logger import logger

# -----------------------------
# Helpers
# -----------------------------

def convert_to_EC(cycle):
    return ''.join(['E' if num % 2 else 'C' for num in cycle])

def longest_common_suffix(seq1, seq2):
    min_len = min(len(seq1), len(seq2))
    for i in range(1, min_len + 1):
        if seq1[-i:] == seq2[-i:]:
            return seq1[-i:]
    return []

def check_mode_step_change(n, F, seq_n, seq_f):
    if ASSUME_ONE_STEP_REDUCTION and len(seq_n) != len(seq_f) + 1:
        logger.warning(f"❌ Mode step failure: len({n})={len(seq_n)} ≠ len({n*F})+1={len(seq_f)+1}")

def check_convergence_at_L(n, F, seq_n, seq_f):
    common = longest_common_suffix(seq_n, seq_f)
    if not common:
        logger.warning(f"❌ No convergence: {n} and {n*F} do not share a suffix")

# -----------------------------
# Ray Worker
# -----------------------------

@ray.remote
def process_and_insert_motif(n, F, sequence_n, sequence_f):
    check_mode_step_change(n, F, sequence_n, sequence_f)
    check_convergence_at_L(n, F, sequence_n, sequence_f)

    if ASSUME_ONE_STEP_REDUCTION and len(sequence_n) != len(sequence_f) + 1:
        return None

    motif_EC = convert_to_EC(sequence_n)
    product_EC = convert_to_EC(sequence_f)
    common = longest_common_suffix(sequence_n, sequence_f)
    if not common:
        return None

    L = common[0]
    prefix_len = len(sequence_n) - len(common)
    motif_prefix = motif_EC[:prefix_len]
    product_prefix = product_EC[:len(product_EC) - len(common)]
    T = motif_prefix.count('C')
    H = motif_prefix.count('E')

    conn = connect_db("motif_registry")
    try:
        insert_batch(conn, "motif_registry", [(T, H, motif_prefix)], "(T, H, motif_EC)", conflict="IGNORE")
    finally:
        conn.close()

    return (n, L, H, T, motif_prefix, product_prefix)

# -----------------------------
# Output
# -----------------------------

def write_output_file(filename, content):
    path = os.path.join(TEXT_OUTPUT_ACTIVE, filename)
    with open(path, "w") as f:
        f.write(content)
    logger.info(f"✅ Output saved to {path}")

# -----------------------------
# Main Pipeline
# -----------------------------

def main():
    ray.init(ignore_reinit_error=True)

    with PipelineStep("Setup DB + Resume", logger.info):
        setup_table("motif_registry")
        writer = SchemaAwareWriter("parameters")
        existing = {row[0] for row in read_duckdb("parameters", where_clause=f"A <= {INTEGER_MAX}")}
        resume_point = read_resume_checkpoint("parameters")

    candidates = [
        n for n in range(INTEGER_MIN, INTEGER_MAX + 1)
        if n % Z != 0 and n not in existing and n >= resume_point
    ]
    logger.info(f"🧮 Remaining integers: {len(candidates)}")

    with PipelineStep("Streaming motif detection", logger.info):
        for i in range(0, len(candidates), CHUNK_SIZE):
            batch = candidates[i:i + CHUNK_SIZE]
            keys = batch + [n * F for n in batch]

            # 🔁 Load all required sequences in one DuckDB read
            rows = read_duckdb("system_cache", where_clause=f"n IN ({','.join(map(str, keys))})", as_dict=True)
            sequence_map = {row["n"]: json.loads(row["sequence"]) for row in rows}

            valid = [
                n for n in batch
                if n in sequence_map and n * F in sequence_map and
                (not ASSUME_ONE_STEP_REDUCTION or len(sequence_map[n]) == len(sequence_map[n * F]) + 1)
            ]

            futures = [
                process_and_insert_motif.remote(n, F, sequence_map[n], sequence_map[n * F])
                for n in valid
            ]

            results = ray.get(futures)
            new_rows = [res for res in results if res is not None]
            writer.write_rows(new_rows)
            write_resume_checkpoint("parameters", batch[-1])

    writer.close()

    with PipelineStep("Sync to DuckDB", logger.info):
        retained = port_and_prune_sqlite_to_duckdb("parameters")
        write_resume_checkpoint("parameters", retained)

    with PipelineStep("Write output file", logger.info):
        where_clause = f"A <= {INTEGER_MAX}"
        if TWIN_ONLY:
            where_clause += " AND (T = H + 1)"
        elif PARTITION_MODE:
            where_clause += " AND (T != H + 1)"
        filtered = read_duckdb("parameters", where_clause=where_clause)
        content = "### Mode Step Integers ###\n\n" + "\n".join(str(row[0]) for row in filtered)
        write_output_file("mode_step_integers.txt", content)

    logger.info("🎉 Motif discovery complete.")


if __name__ == "__main__":
    main()