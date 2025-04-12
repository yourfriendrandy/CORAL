
def estimate_system_behavior(M, Z):
    M = abs(M)
    Z = abs(Z) #May not actually be a good descriptor of behavior when M and/or Z are negative
    k = 1
    while M / (Z ** k) >= 1:
        k += 1
    length = Z ** (k - 1)

    H = 0  # expansions
    T = 0  # compressions
    sequence = []

    for i in range(1, length + 1):
        power = 0
        n = i
        while n % Z == 0:
            n //= Z
            power += 1
        compression_len = power + 1
        sequence.append("E")
        sequence.extend(["C"] * compression_len)
        H += 1
        T += compression_len

    heuristic = (M ** H) / (Z ** T)
    if heuristic > 1.125:
        category = "⚠️ Dangerous"
    elif 1 < heuristic <= 1.125:
        category = "🌀 Collatz-like"
    else:
        category = "✅ Trivial"

    return {
        "M": M,
        "Z": Z,
        "k_required": k,
        "fragment_length": length,
        "H": H,
        "T": T,
        "heuristic": heuristic,
        "sequence": ''.join(sequence),
        "category": category
    }
