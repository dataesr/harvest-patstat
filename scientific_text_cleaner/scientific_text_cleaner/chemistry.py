import re

SUB_MAP = str.maketrans("0123456789", "₀₁₂₃₄₅₆₇₈₉")


def reconstruct_chemical(text):
    return re.sub(r"\b([A-Z]{1,3})\s+(\d+)", r"\1\2", text)


def fix_chemical_spacing(text):
    text = re.sub(r"\b([A-Z])\s+(\d+)", r"\1\2", text)
    text = re.sub(r"\b([A-Z][a-z]?)\s+([A-Z][a-z]?)", r"\1\2", text)
    return text


def to_chemical_unicode(text):
    return re.sub(
        r"([A-Z][a-z]*)(\d+)",
        lambda m: m.group(1) + m.group(2).translate(SUB_MAP),
        text
    )


def normalize_cement_chemistry(text):
    # indices
    text = re.sub(r"([A-Z])(\d+)",
                  lambda m: m.group(1) + m.group(2).translate(SUB_MAP),
                  text)

    # variables indices
    text = re.sub(r"Ax", "Aₓ", text)
    text = re.sub(r"Fy", "Fᵧ", text)

    # primes
    text = text.replace("x'", "x′")
    text = text.replace("y'", "y′")

    return text


def normalize_chemical_formula(text):
    # indices
    text = re.sub(r"([A-Z])(\d+)",
                  lambda m: m.group(1) + m.group(2).translate(SUB_MAP),
                  text)

    # charges ioniques
    text = re.sub(r"SO4\s*2-", "SO₄²⁻", text)
    text = re.sub(r"Cl-", "Cl⁻", text)

    return text
