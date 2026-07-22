import re


def detect_issues(original, cleaned):

    issues = []

    if not isinstance(original, str) or not isinstance(cleaned, str):
        issues.append("non_string")
        return issues

    # --- 1 truncation
    if len(cleaned) < 0.6 * len(original):
        issues.append("truncation")

    # --- 2 perte de chiffres
    if len(re.findall(r"\d", cleaned)) < 0.5 * len(re.findall(r"\d", original)):
        issues.append("digit_loss")

    # --- 3 perte %
    if "%" in original and "%" not in cleaned:
        issues.append("percent_loss")

    # --- 4 perte <>
    if ("<" in original or ">" in original) and ("<" not in cleaned and ">" not in cleaned):
        issues.append("angle_loss")

    # --- 5 html restant
    if re.search(r"<[^>]+>", cleaned):
        issues.append("html_not_cleaned")

    # --- 6 formules perdues
    if re.search(r"\d+\^", original) and not re.search(r"\^", cleaned):
        issues.append("formula_loss")

    return issues


# ✅ SCORE QUALITÉ
def compute_quality_score(original, cleaned):

    issues = detect_issues(original, cleaned)

    score = 100

    penalties = {
        "truncation": 50,
        "digit_loss": 20,
        "percent_loss": 10,
        "angle_loss": 10,
        "formula_loss": 15,
        "html_not_cleaned": 10,
        "non_string": 100
    }

    for issue in issues:
        score -= penalties.get(issue, 5)

    return max(score, 0), issues
