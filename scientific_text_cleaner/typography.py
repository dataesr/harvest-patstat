import re

def normalize_typo(text):

    text = re.sub(r"[‐–—]", "-", text)

    text = re.sub(r"([A-Za-z])([ΣΔαβγμ])", r"\1 \2", text)
    text = re.sub(r"([ΣΔαβγμ])([A-Za-z])", r"\1 \2", text)

    return text