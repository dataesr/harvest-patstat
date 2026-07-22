import re
from pylatexenc.latex2text import LatexNodes2Text

latex_converter = LatexNodes2Text()


def parse_latex_fragment(expr):
    if not expr:
        return ""

    try:
        expr = expr.replace("\\\\", "\\")
        expr = expr.replace("SigmaDelta", r"\Sigma\Delta")
        expr = expr.replace("Delta Sigma", r"\Delta\Sigma")
        expr = expr.replace(",times,", r"\times")
        expr = expr.replace("rm HT_rm c", r"HT<sub>c</sub>")
        expr = expr.replace("f_tau", r"f<sub>\tau</sub>")
        expr = expr.replace("f_max≫268", r"f<sub>max></sub>\gg268")
        expr = expr.replace("^{\circ}", r"<sup>\circ</sup>")
        expr = expr.replace("{\\rm Fe}/{\\rm Ni}_{\rm x}{\\rm Zn}_{1-{\\rm x}}{\\rm Fe}_{2}{\\rm O}_{4}",
                            r"Fe/Ni<sub>x</sub>Zn<sub>1-x</sub>Fe<sub>2</sub>O<sub>4</sub>")
        expr = expr.replace("{Fe}/{Ni}_{x}{Zn}_{1-{x}}{Fe}_{2}{O}_{4}",
                            r"Fe/Ni<sub>x</sub>Zn<sub>1-x</sub>Fe<sub>2</sub>O<sub>4</sub>")
        expr = expr.replace("\[F(x) =\\begin{cases}\\frac{x^2}{w^2} + \left(\\text{tempo} - \\frac{2}{w}\\right)x + 1 &"
                            " \\text{if } \\text{diff} > 0 \\\[6pt]-\\frac{x^2}{w^2} + \left(\\text{tempo} +"
                            " \\frac{2}{w}\\right)x - 1 & \\text{if } \\text{diff} < 0\end{cases}\]",
                            r"F(x) =x<sup>2</sup>/w<sup>2</sup> + (tempo - 2/w)x + 1    if diff > 0\n"
                            r"-x<sup>2</sup>/w<sup>2</sup> + (tempo + 2/w)x - 1    if diff < 0")

        expr = re.sub(r"\\(rm|hbox|text)\b", "", expr)

        return latex_converter.latex_to_text(expr)

    except Exception:
        return expr


def clean_latex_titles(text):
    if not isinstance(text, str):
        return text

    # corriger tex cassé
    text = text.replace("&amp;lt;tex&amp;gt;", "")
    text = text.replace("&amp;lt;/tex&amp;gt;", "")
    text = text.replace("$&amp;lt;/tex&amp;gt;", "")
    text = text.replace("&gt;tex&lt;", "")
    text = text.replace("&gt;/tex&lt;", "")
    text = text.replace("&lt;", "<")
    text = text.replace("&gt;", ">")
    text = re.sub(r"\s*\rm\s*", "", text)

    if "<formula" in text:
        if "<tex" in text:
            text = re.sub(
                r"<formula[^>]*>(.*?)</formula>",
                lambda m: " " + m.group(1) + " ",
                text,
                flags=re.IGNORECASE
            )
    if "<inline-formula" in text:
        if "<tex" in text:
            text = re.sub(
                r"<inline\-formula[^>]*>(.*?)</inline\-formula>",
                lambda m: " " + m.group(1) + " ",
                text,
                flags=re.IGNORECASE
            )

    if "<tex-math" in text:
        if "<tex" in text:
            text = re.sub(
                r"<tex\-math[^>]*>(.*?)</tex\-math>",
                lambda m: " " + m.group(1) + " ",
                text,
                flags=re.IGNORECASE
            )

    if "$^{{" in text:
        text = re.sub(
            r"\$\s?\^\{*(.*?)\}*\$",
            lambda m: "<sup>" + parse_latex_fragment(m.group(1)) + "</sup>",
            text,
            flags=re.IGNORECASE
        )

    if "$_{{" in text:
        text = re.sub(
            r"\$\s?\_\{*(.*?)\}*\$",
            lambda m: "<sub>" + parse_latex_fragment(m.group(1)) + "</sub>",
            text,
            flags=re.IGNORECASE
        )

    # <tex>
    text = re.sub(
        r"<tex[^>]*>(.*?)</tex>",
        lambda m: " " + parse_latex_fragment(m.group(1)) + " ",
        text,
        flags=re.IGNORECASE
    )

    # <formula>
    text = re.sub(
        r"<formula[^>]*>(.*?)</formula>",
        lambda m: " " + clean_latex_titles(m.group(1)) + " ",
        text,
        flags=re.IGNORECASE
    )

    # $...$
    text = re.sub(
        r"\$(.*?)\$",
        lambda m: " " + parse_latex_fragment(m.group(1)) + " ",
        text
    )

    # ^
    text = re.sub(
        r"\s?\^\{+(.*?)\}+",
        lambda m: "<sup>" + parse_latex_fragment(m.group(1)) + "</sup>",
        text
    )

    text = re.sub(
        r"\s?_\{+(.*?)\}+",
        lambda m: "<sub>" + parse_latex_fragment(m.group(1)) + "</sub>",
        text
    )

    # ^
    text = re.sub(
        r"\s?\^([\+\-−]*[\w]+[\+\-−]*)",
        lambda m: "<sup>" + m.group(1) + "</sup>",
        text
    )

    text = re.sub(
        r"\s?_([\+\-−]*[\w]+[\+\-−]*)",
        lambda m: "<sub>" + m.group(1) + "</sub>",
        text
    )



    return text
