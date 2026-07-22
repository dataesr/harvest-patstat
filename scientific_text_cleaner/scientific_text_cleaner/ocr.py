import re

def clean_ocr_text(text):
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\?+", " ", text)
    return re.sub(r"\s+", " ", text)
