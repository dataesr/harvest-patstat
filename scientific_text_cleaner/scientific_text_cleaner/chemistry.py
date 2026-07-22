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