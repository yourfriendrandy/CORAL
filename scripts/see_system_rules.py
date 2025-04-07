# see_system_rules.py

from config.config import M, Z, CORAL_TAG, B_EXPANSION_POLARITY
from pathlib import Path


b_rules = get_expansion_classes(M, Z, sign_mode=EXPANSION_B_SIGN)


def get_expansion_classes(m: int, z: int, sign_mode = str: "positive") -> dict[int, int]:
    """
    Returns a mapping {b: r} such that for a given expansion constant b,
    the corresponding residue class r satisfies ((m * n) + b) ≡ 0 mod z
    ⇨ n ≡ -b * m⁻¹ mod z
    
    This describes the modular trigger condition for each b ∈ [1, z-1].
    """
    try:
        m_inv = pow(m, -1, z)
            if sign_mode == "positive"
                b_range = range(1,z)
            elif sign_mode == "negative"
                b_range = range(-z,-1)
            else sign_mode == "both"
                b_range = list(range(-z,-1)) + list(range(1,z))
    except ValueError as e:
        raise ValueError(f"Modular inverse does not exist for m = {m}, z = {z}") from e

    return {b: (-b * m_inv) % z for b in range(1, z)}


def write_system_rules():
    rules = get_expansion_classes(M, Z)
    output_dir = Path("text_output") / CORAL_TAG
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "system_rules.txt"

    with open(output_file, "w") as f:
        f.write(f"System Rules for CORAL_TAG='{CORAL_TAG}' (m={M}, z={Z})\n\n")
        f.write(f"Expansion classes (B = {len(rules)}):\n\n")
        for b, residue in rules.items():
            f.write(f"b = {b} : triggers when n ≡ {residue} mod {Z}\n")
        f.write(f"\nValid residue classes = {set(rules.values())}\n")

    print(f"✅ Saved system rules for m={M}, z={Z} in text_output/{CORAL_TAG}/system_rules.txt")


if __name__ == "__main__":
    write_system_rules()