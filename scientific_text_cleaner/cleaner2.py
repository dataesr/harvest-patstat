import html
import re
import unicodedata
from urllib.parse import unquote

from application.server.main.logger import get_logger
from scientific_text_cleaner.html_processing import parse_html, clean_patent_text, reconstruct_chemical, \
    contextual_chemical_fix, \
    to_chemical_unicode
from scientific_text_cleaner.latex_processing import parse_latex
from scientific_text_cleaner.typography import normalize_typo

logger = get_logger(__name__)


def fix_latin_names(text):
    text = re.sub(r"\b([A-Z]{3,})\s+([A-Z]{3,})\b", lambda m: m.group(1) + " " + m.group(2), text)
    return text


def clean_ocr_text(text):
    if not isinstance(text, str):
        return text

    # --- 1. enlever balises ---
    # text = re.sub(r"<[^>]+>", "", text)

    # --- 2. supprimer bruit OCR ---
    text = re.sub(r"\?+", " ", text)

    # --- 3. corriger mots collés fréquents ---
    fixes = [
        ("THEFAMILY", "THE FAMILY"),
        ("OFIN", "OF IN"),
        ("OFIN", "OF IN"),
        ("OFAN", "OF AN"),
        ("OFEXTRACT", "OF EXTRACT"),
        ("OFIN", "OF IN")
    ]

    for wrong, right in fixes:
        text = text.replace(wrong, right)

    # --- 4. corriger majuscules collées génériques ---
    text = re.sub(r"\b([A-Z]{2,})([A-Z][a-z])", r"\1 \2", text)

    # --- 5. corriger mots collés simples ---
    text = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)

    # --- 6. enlever espaces multiples ---
    text = re.sub(r"\s+", " ", text)

    return text.strip()


def clean_text(value):
    logger.debug(f"start cleaning text: {value}")
    if not isinstance(value, str):
        logger.debug("end cleaning not str")
        return value

    value = value.replace("<=", "≤")
    value = value.replace(">=", "≥")

    value = value.replace("<<", "«")
    value = value.replace(">>", "»")
    value = value.replace("Î²-GLUCAN-RICH", "ꞵ-GLUCAN-RICH")

    value = value.replace("In&lt;inf&gt;2&lt;/inf&gt;S&lt;inf&gt;3&lt;/inf&gt;", "In<sub>2</sub>S<sub>3>/sub>")
    value = value.replace("CO&lt;sub&gt;2&lt;/sub&gt;", "CO<sub>2</sub>")

    value = value.replace("{Sn[Zn<sub>4</sub>Sn<sub>4</sub>S<sub>17</sub>]}<sup>6−</sup>",
                          "Sn[Zn<sub>4</sub>Sn<sub>4</sub>S<sub>17</sub>]<sup>6−</sup>")

    value = value.replace("(ROR<sub>Y</sub>)",
                          "(ROR<sub>ɣ</sub>)")

    value = value.replace(
        "<SUP2/>? <SUB2/>?+? ?IN VITRO METHOD FOR OBTAINING CLINICAL-GRADE CD8CD45RC <NS1:SUP>LOW/- </NS1:SUP>?REGULATORY T CELLS",
        "IN VITRO METHOD FOR OBTAINING CLINICAL-GRADE CD8<sup>+</sup>CD45RC<sup>LOW/-</sup> REGULATORY T CELLS")

    value = value.replace(
        "<SUP2/>? <SUB2/>?X?CATALYST COMPRISING AN AFX-STRUCTURE ZEOLITE OF VERY HIGH PURITY AND AT LEAST ONE TRANSITION METAL FOR SELECTIVE REDUCTION OF NO",
        "CATALYST COMPRISING AN AFX-STRUCTURE ZEOLITE OF VERY HIGH PURITY AND AT LEAST ONE TRANSITION METAL FOR SELECTIVE REDUCTION OF NOX")
    value = value.replace(
        "<SUP2/> <SUB2/> 2 METHOD FOR TREATING AN INDUSTRIAL EFFLUENT CHARGED WITH ALUMINIUM USING CO",
        "METHOD FOR TREATING AN INDUSTRIAL EFFLUENTNOBLE METAL-PROMOTED INO<sub>3</sub> CATALYST FOR THE HYDROGENATION OF CO<sub>2</sub> TO METHANOL CHARGED WITH ALUMINIUM USING CO<sub>2</sub>")
    value = value.replace(
        "<SUP2/>? <SUB2/>?2?NOBLE METAL-PROMOTED INO <NS1:SUB>3</NS1:SUB>?CATALYST FOR THE HYDROGENATION OF CO <NS2:SUB>2</NS2:SUB>?TO METHANOL",
        "NOBLE METAL-PROMOTED INO<sub>3</sub> CATALYST FOR THE HYDROGENATION OF CO<sub>2</sub> TO METHANOL")
    value = value.replace("<SUP2/>? <SUB2/>?2?USE OF CO-CONTAINING GASEOUS EFFLUENT",
                          "USE OF CO<sub>2</sub>-CONTAINING GASEOUS EFFLUENT")
    value = value.replace(
        "<SUP2/>? <SUB2/>?13?COMBINATION OF MODIFIED STARCH/C-C <NS1:SUB>15</NS1:SUB>?FATTY ACID/CLAY WITH AMPHOTERIC SURFACTANT",
        "COMBINATION OF MODIFIED STARCH/C13-C15 FATTY ACID/CLAY WITH AMPHOTERIC SURFACTANT")
    value = value.replace("<SUP2/>? <SUB2/>?2?COELECTROREDUCTION TO MULTI-CARBON PRODUCTS IN STRONG ACID",
                          "CO<sub>2</sub> ELECTROREDUCTION TO MULTI-CARBON PRODUCTS IN STRONG ACID")
    value = value.replace("<SUP2/>? <SUB2/>?2?METHOD FOR LIQUEFYING A STREAM RICH IN CO",
                          "METHOD FOR LIQUEFYING A STREAM RICH IN CO<sub>2</sub>")
    value = value.replace(
        "<SUP2/>? <SUB2/>?7?APPLICATIONS OF BIASED LIGANDS OF THE SEROTONIN 5-HTRECEPTOR FOR THE TREATMENT OF PAIN, MULTIPLE SCLEROSIS AND THE CONTROL OF THERMOREGULATION",
        "APPLICATIONS OF BIASED LIGANDS OF THE SEROTONIN 5-HT7 RECEPTOR FOR THE TREATMENT OF PAIN, MULTIPLE SCLEROSIS AND THE CONTROL OF THERMOREGULATION")
    value = value.replace(
        "<SUP2/> <SUB2/> 2 METHOD FOR FORMING A LAYER OF SINGLE-PHASE OXIDE (FE, CR)O <NS1:SUB>3 </NS1:SUB> WITH A RHOMBOHEDRAL STRUCTURE ON A STEEL OR SUPER ALLOY SUBSTRATE",
        "METHOD FOR FORMING A LAYER OF SINGLE-PHASE OXIDE (Fe,Cr)<sub>2</sub>O<sub>3</sub> WITH A RHOMBOHEDRAL STRUCTURE ON A STEEL OR SUPER ALLOY SUBSTRATE")
    value = value.replace(
        "<SUP2/>? <SUB2/>?2?METHOD OF ELECTROLYSING HYDROGEN BROMIDE AFTER HSO <NS1:SUB>4</NS1:SUB>?SYNTHESIS",
        "METHOD OF ELECTROLYSING HYDROGEN BROMIDE AFTER H<sub>2</sub>SO<sub>4</sub> SYNTHESIS")
    value = value.replace(
        "<SUP2/>? <SUB2/>?2?COELECTROREDUCTION TO MULTI-CARBON PRODUCTS IN ACIDIC CONDITIONS COUPLED WITH CO <NS1:SUB>2</NS1:SUB>?REGENERATION FROM CARBONATE",
        "CO<sub>2</sub> ELECTROREDUCTION TO MULTI-CARBON PRODUCTS IN ACIDIC CONDITIONS COUPLED WITH CO<sub>2</sub> REGENERATION FROM CARBONATE")

    value = value.replace(
        "<eq.metformin or troglizazone>.", "(eq.metformin or troglizazone).")

    value = value.replace(
        "A method of controlling a buoyancy system for an aircraft includes: determining the roll angle phi and the pitching angle theta of the aircraft; verifying whether -phiR <+R and whether -thetaR <theta<+thetaR, where phiR and thetaR are predefined limit angles; if at least one of the angles phi and theta is no longer in its above-defined respective range, activating an automatic trigger of the buoyancy system; if the angles phi and theta are in their above-defined respective ranges, determining the altitude A of the aircraft; inhibiting the automatic trigger if A >AR, where AR is a predefined limit altitude; and if AR >=A, and if at least partial immersion of the aircraft has been detected, activating the automatic trigger.",
        "A method of controlling a buoyancy system for an aircraft includes: determining the roll angle ɸ and the pitching angle θ of the aircraft; verifying whether -ɸ<sub>R</sub><ɸ<+ɸ<usb>R</sub> and whether -θ<sub>R</sub><ɸ<+θ<sub>R</sub>, where ɸ<sub>R</sub> and θ<sub>R</sub> are predefined limit angles; if at least one of the angles ɸ and θ is no longer in its above-defined respective range, activating an automatic trigger of the buoyancy system; if the angles ɸ and θ are in their above-defined respective ranges, determining the altitude A of the aircraft; inhibiting the automatic trigger if A>A<sub>R</sub>, where A<sub>R</sub> is a predefined limit altitude; and if A<sub>R</sub>≥A, and if at least partial immersion of the aircraft has been detected, activating the automatic trigger.")

    value = value.replace("L<i, t>", "L<sub>i, t</sub>")
    value = value.replace("RTT<i, t>", "RTT<sub>i, t</sub>")
    value = value.replace("R<i, t>", "R<sub>i, t</sub>")
    value = value.replace("D<i, t>", "D<sub>i, t</sub>")
    value = value.replace("<APj, Ni>", "APj, Ni")


    value = re.sub(r"COPYRIGHT\s?\:\s?\(c\)", "COPYRIGHT: ©", value)

    value = re.sub(r"<SMALLCAPS/>\?\s+", "", value)

    # HTML decode
    # logger.debug("decode HTML")
    prev = None
    while prev != value:
        prev = value
        value = html.unescape(value)

    match = re.match(r"^\<SUP2\/\>", value)
    if match:
        value = clean_patent_text(value)
        value = reconstruct_chemical(value)
        value = contextual_chemical_fix(value)
        value = to_chemical_unicode(value)

    value = clean_ocr_text(value)

    value = fix_latin_names(value)

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
    value = value.strip()

    logger.debug(f"end cleaning text: {value}")

    return value.strip()
