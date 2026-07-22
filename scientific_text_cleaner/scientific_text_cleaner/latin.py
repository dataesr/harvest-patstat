import re

def fix_latin_names(text):
    return re.sub(r"\b([A-Z]{3,})\s+([A-Z]{3,})\b", r"\1 \2", text)
