import html
import re
import unicodedata
from urllib.parse import unquote

from application.server.main.logger import get_logger

from .html_processing import parse_html
from .latex_processing import parse_latex
from .typography import normalize_typo

logger = get_logger(__name__)


def clean_text(value):
    logger.debug(f"start cleaning text: {value}")
    if not isinstance(value, str):
        logger.debug("end cleaning not str")
        return value

    value = value.replace("{Sn[Zn<sub>4</sub>Sn<sub>4</sub>S<sub>17</sub>]}<sup>6−</sup>",
                          "Sn[Zn<sub>4</sub>Sn<sub>4</sub>S<sub>17</sub>]<sup>6−</sup>")

    # HTML decode
    # logger.debug("decode HTML")
    prev = None
    while prev != value:
        prev = value
        value = html.unescape(value)

    # URL decode
    # logger.debug("decode URL")
    value = unquote(value)

    # HTML + TEX parsing
    # logger.debug("clean TeX in HTML entities")
    value = parse_html(value)

    # LaTeX fallback
    # logger.debug("parse LaTeX")
    value = parse_latex(value)

    # Unicode normalize
    # logger.debug("normalize unicode")
    value = unicodedata.normalize("NFKC", value)

    # clean invisible chars
    # logger.debug("clean invisble chars")
    value = re.sub(r"[\x00-\x1F\x7F]", "", value)

    # typography
    # logger.debug("normalize typography")
    value = normalize_typo(value)

    # spaces
    # logger.debug("clean spaces")
    value = re.sub(r"\s+", " ", value)

    logger.debug(f"end cleaning text: {value}")

    return value.strip()
