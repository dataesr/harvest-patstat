import html
import re
import unicodedata

from application.server.main.logger import get_logger

from scientific_text_cleaner import protect_angles, restore_angles
from scientific_text_cleaner import parse_html
from scientific_text_cleaner import reconstruct_chemical, fix_chemical_spacing, to_chemical_unicode
from scientific_text_cleaner import clean_ocr_text
from scientific_text_cleaner import fix_latin_names
from scientific_text_cleaner import normalize_typo

logger = get_logger(__name__)

def clean_text(value):
    logger.debug(f"start cleaning text: {value}")
    if not isinstance(value, str):
        return value

    original = value

    value = html.unescape(value)

    value = protect_angles(value)

    value = clean_ocr_text(value)

    value = fix_latin_names(value)

    value = reconstruct_chemical(value)
    value = fix_chemical_spacing(value)
    value = to_chemical_unicode(value)

    value = parse_html(value)

    value = normalize_typo(value)

    value = unicodedata.normalize("NFKC", value)

    value = re.sub(r"\s+", " ", value)

    value = restore_angles(value)

    # ✅ garde-fou anti perte
    if len(value) < 0.6 * len(original):
        return original

    logger.debug(f"end cleaning text: {value}")

    return value.strip()