from bs4 import BeautifulSoup

from .latex_processing import parse_latex

SUB_MAP = str.maketrans("0123456789+-=()", "₀₁₂₃₄₅₆₇₈₉₊₋₌₍₎")
SUP_MAP = str.maketrans("0123456789+-=()", "⁰¹²³⁴⁵⁶⁷⁸⁹⁺⁻⁼⁽⁾")


def to_italic(text: str) -> str:
    res = []
    for c in text:
        if 'A' <= c <= 'Z':
            res.append(chr(0x1D434 + ord(c) - ord('A')))
        elif 'a' <= c <= 'z':
            res.append(chr(0x1D44E + ord(c) - ord('a')))
        else:
            res.append(c)
    return "".join(res)


def parse_html(text: str) -> str:
    soup = BeautifulSoup(text, "html.parser")

    for tag in soup.find_all("i"):
        tag.replace_with(to_italic(tag.get_text()))

    for tag in soup.find_all("sub"):
        tag.replace_with(tag.get_text().translate(SUB_MAP))

    for tag in soup.find_all("sup"):
        tag.replace_with(tag.get_text().translate(SUP_MAP))

    for tag in soup.find_all("tex"):
        latex = tag.get_text()
        tag.replace_with(" " + parse_latex(latex) + " ")

    return soup.get_text()
