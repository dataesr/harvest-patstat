import re

from pylatexenc.latex2text import LatexNodes2Text

latex_converter = LatexNodes2Text()


def parse_latex(expr):
    if not expr:
        return ""

    expr = re.sub(r"\$(.*?)\$", r"\1", expr)

    # ΣΔ
    expr = expr.replace("SigmaDelta", r"\Sigma\Delta")

    try:
        return latex_converter.latex_to_text(expr)
    except Exception:
        return expr
