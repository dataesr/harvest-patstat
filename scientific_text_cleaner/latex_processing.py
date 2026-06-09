import re

from pylatexenc.latex2text import LatexNodes2Text

latex_converter = LatexNodes2Text()


def parse_latex(expr):
    if not expr:
        return ""

    expr = re.sub(r"\$(.*?)\$", r"\1", expr)

    # ΣΔ
    expr = expr.replace("SigmaDelta", r"\Sigma\Delta")
    expr = expr.replace("Delta Sigma", r"\Delta\Sigma")
    expr = expr.replace(",times,", r"\times")
    expr = expr.replace("rm HT_rm c", r"HT<sub>c</sub>")
    expr = expr.replace("f_tau", r"f<sub>\tau</sub>")
    expr = expr.replace("f_max≫268", r"f<sub>max></sub>\gg268")

    try:
        return latex_converter.latex_to_text(expr)
    except Exception:
        return expr
