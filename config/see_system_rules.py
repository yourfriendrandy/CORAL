from config.config import M, Z, CORAL_TAG, B_EXPANSION_POLARITY
from pathlib import Path


# -----------------------
# POLARITY SUPPORT FOR B
# -----------------------

def get_expansion_classes(m: int, z: int, sign_mode: str = "positive") -> dict[int, int]:
    """
    Returns {b: r} where ((m * n) + b) ≡ 0 mod z ⇨ n ≡ -b * m⁻¹ mod z
    """
    try:
        m_inv = pow(m, -1, z)
    except ValueError as e:
        raise ValueError(f"Modular inverse does not exist for m = {m}, z = {z}") from e

    if sign_mode == "positive":
        b_range = range(1, z)
    elif sign_mode == "negative":
        b_range = range(-z + 1, 0)
    elif sign_mode == "both":
        b_range = list(range(-z + 1, 0)) + list(range(1, z))
    else:
        raise ValueError(f"Invalid sign_mode '{sign_mode}'. Must be 'positive', 'negative', or 'both'.")

    return {b: (-b * m_inv) % z for b in b_range}


# -----------------------
# SINGLE SYSTEM GENERATOR
# -----------------------

def write_system_rules_for(
    m: int,
    z: int,
    sign_mode: str = "positive",
    tag: str = None,
    out_path: Path = None
):
    """
    Writes the expansion classes (b-values and residues) for a given system (m, z).
    Output written to `text_output/{tag}/system_rules.txt` unless `out_path` is specified.
    """
    tag = tag or f"CORAL_{m}_{z}"
    rules = get_expansion_classes(m, z, sign_mode=sign_mode)

    output_dir = out_path or (Path("text_output") / tag)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "system_rules.txt"

    with open(output_file, "w") as f:
        f.write(f"System Rules for CORAL_TAG = '{tag}' (m = {m}, z = {z})\n")
        f.write(f"Expansion Sign Mode = '{sign_mode}'\n")
        f.write(f"Expansion classes (B = {len(rules)}):\n\n")
        for b, residue in sorted(rules.items()):
            f.write(f"b = {b:+} → triggers when n ≡ {residue} mod {z}\n")
        f.write("Note: n ≡ 0 mod z is handled by the contraction rule (n / z), not by expansion.\n")
        f.write(f"\nValid residue classes: {sorted(set(rules.values()))}\n")

    print(f"✅ Saved system rules for m={m}, z={z} in {output_file}")


# -----------------------
# BATCH RUNNER (OPTIONAL)
# -----------------------

def write_all_known_system_rules(systems: list[tuple[int, int]], polarity: str = "positive"):
    """
    Writes system rules for a list of (m, z) systems.
    """
    for m, z in systems:
        write_system_rules_for(m, z, sign_mode=polarity)


# -----------------------
# CLI Entry Point
# -----------------------

if __name__ == "__main__":
    # Default to current CORAL_TAG config if run directly
    write_system_rules_for(M, Z, sign_mode=B_EXPANSION_POLARITY, tag=CORAL_TAG)
