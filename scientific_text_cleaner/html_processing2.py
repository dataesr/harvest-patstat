import re

from bs4 import BeautifulSoup

from scientific_text_cleaner import parse_latex


# SUB_MAP = str.maketrans("0123456789+-=()", "₀₁₂₃₄₅₆₇₈₉₊₋₌₍₎")
# SUP_MAP = str.maketrans("0123456789+-=()", "⁰¹²³⁴⁵⁶⁷⁸⁹⁺⁻⁼⁽⁾")
#

# def to_italic(text: str) -> str:
#     res = []
#     for c in text:
#         if 'A' <= c <= 'Z':
#             res.append(chr(0x1D434 + ord(c) - ord('A')))
#         elif 'a' <= c <= 'z':
#             res.append(chr(0x1D44E + ord(c) - ord('a')))
#         else:
#             res.append(c)
#     return "".join(res)


def clean_patent_text(text):
    if not isinstance(text, str):
        return text

    # --- 2. supprimer balises auto-closing ---
    text = re.sub(r"<SUP\d*/>", "", text)
    text = re.sub(r"<SUB\d*/>", "", text)

    # --- 3. remplacer balises SUB ---
    text = re.sub(r"<NS\d+:SUB>\s*(\d+)\s*</NS\d+:SUB>", r"<sub>\1</sub>", text)

    # --- 4. supprimer autres balises ---
    # text = re.sub(r"<[^>]+>", "", text)

    # --- 5. supprimer bruit "?"" ---
    text = text.replace("?", " ")

    # --- 6. fixer motifs cassés type "C-C 15" ---
    text = re.sub(r"\b([A-Z])-\s*([A-Z])\s*(\d+)",
                  lambda m: f"{m.group(1)}{m.group(3)}-{m.group(2)}",
                  text)

    # --- 7. fixer cas type "C 13-C 15" ---
    text = re.sub(r"\b([A-Z])\s*(\d+)\s*-\s*([A-Z])\s*(\d+)",
                  r"\1\2-\3\4",
                  text)

    # --- 8. fixer cas CO 2 ---
    text = re.sub(r"\b([A-Z]+)\s+(\d+)", r"\1<sub>\2</sub>", text)

    # --- 9. espaces propres ---
    text = re.sub(r"\s+", " ", text)

    return text.strip()


def reconstruct_chemical(text):
    # --- 1. corriger cas type "CO 2" → "CO2"
    text = re.sub(r"\b([A-Z]{1,3})\s+(\d+)\b", r"\1<sub>\2</sub>", text)

    # --- 2. corriger cas type "H S O 4" → "HSO4"
    text = re.sub(r"\b([A-Z])\s+([A-Z])\s+([A-Z])\s*(\d+)",
                  r"\1\2\3<sub>\4</sub>", text)

    # --- 3. corriger cas type "C 13 - C 15"
    text = re.sub(r"\bC\s*[-]?\s*(\d+)\s*-\s*C\s*[-]?\s*(\d+)",
                  r"C\1-C\2", text)

    # --- 4. corriger "C-C 15"
    text = re.sub(r"\bC-C\s*(\d+)",
                  r"C\1-C", text)

    # --- 5. corriger chaine incomplète → compléter plage
    text = re.sub(r"C(\d+)-C\b", r"C\1-C?", text)

    # --- 6. corriger "CO 2 - regeneration"
    text = re.sub(r"(CO2)\s*-\s*(\w)",
                  r"\1 \2", text)

    return text


def contextual_chemical_fix(text):
    # C-C 15 → C13-C15 si on trouve 13 plus loin
    match = re.search(r"C-C\s*(\d+).*?(\d+)", text)
    if match:
        c1 = match.group(1)
        c2 = match.group(2)
        text = re.sub(r"C-C\s*\d+", f"C{c1}-C{c2}", text)

    return text


def to_chemical_unicode(text):
    # transformer CO2 → CO₂
    return re.sub(r"([A-Z][A-Za-z]*)(\d+)",
                  lambda m: m.group(1) + "<sub>" + m.group(2) + "</sub>",
                  text)


def parse_html(text: str) -> str:
    soup = BeautifulSoup(text, "html.parser")

    if "<italic>" in soup.get_text():
        value = soup.get_text()
        value = value.replace("italic", "i")
        soup = BeautifulSoup(value, "html.parser")

    if "<em>" in soup.get_text():
        value = soup.get_text()
        value = value.replace("em", "i")
        soup = BeautifulSoup(value, "html.parser")

    if "<inf>" in soup.get_text():
        value = soup.get_text()
        value = value.replace("inf", "sub")
        if "<formula>" in value:
            value = value.replace("<formula>", "")
            value = value.replace("</formula>", "")
        if "<roman>" in value:
            value = value.replace("<roman>", "")
            value = value.replace("</roman>", "")
        soup = BeautifulSoup(value, "html.parser")

    for tag in soup.find_all("i"):
        italic = tag.get_text()
        tag.replace_with("<i>" + italic + "</i>")

    for tag in soup.find_all("sp"):
        sp = tag.get_text()
        tag.replace_with("<sup>" + sp + "</sup>")

    for tag in soup.find_all("sup"):
        sup = tag.get_text()
        tag.replace_with("<sup>" + sup + "</sup>")

    for tag in soup.find_all("sb"):
        sb = tag.get_text()
        tag.replace_with("<sub>" + sb + "</sub>")

    for tag in soup.find_all("sub"):
        sub = tag.get_text()
        tag.replace_with("<sub>" + sub + "</sub>")

    for tag in soup.find_all("u"):
        tu = tag.get_text()
        tag.replace_with("<u>" + tu + "</u>")

    for tag in soup.find_all("b"):
        tb = tag.get_text()
        tag.replace_with("<b>" + tb + "</b>")

    # for tag in soup.find_all("i"):
    #     tag.replace_with(to_italic(tag.get_text()))
    #
    # for tag in soup.find_all("sub"):
    #     tag.replace_with(tag.get_text().translate(SUB_MAP))
    #
    # for tag in soup.find_all("sup"):
    #     tag.replace_with(tag.get_text().translate(SUP_MAP))

    for tag in soup.find_all("tex"):
        latex = tag.get_text()
        tag.replace_with(" " + parse_latex(latex) + " ")

    return soup.get_text()
