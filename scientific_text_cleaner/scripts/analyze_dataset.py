import pandas as pd
from scientific_text_cleaner.scientific_text_cleaner.quality import compute_quality_score


def analyze_file(input_file, output_file):

    df = pd.read_excel(input_file, engine="openpyxl")

    scores = []
    issues_list = []

    colonnes = ["fr", "en", "default", "display_name_title"]

    dico_res = {"fr": [], "en": [], "default": [], "french": [], "english": [], "default2": [],
                "display_name_title": [],
                "display_name_title2": [], "fr2": [], "en2": [], "quality_score_fr": [], "rule_issues_fr": [],
                "quality_score_en": [],
                "rule_issues_en": [], "quality_score_default": [], "rule_issues_default": [], "quality_score_title": [],
                "rule_issues_title": []}

    for _, row in df.iterrows():
        dico_res["fr"].append(row.fr)
        dico_res["fr2"].append(row.fr2)
        quality_score_fr, rule_issues_fr = compute_quality_score(
            row["fr"],
            row["fr2"]
        )
        dico_res["quality_score_fr"].append(quality_score_fr)
        dico_res["rule_issues_fr"].append(rule_issues_fr)

        dico_res["en"].append(row.en)
        dico_res["en2"].append(row.en2)
        quality_score_en, rule_issues_en = compute_quality_score(
            row["en"],
            row["en2"]
        )
        dico_res["quality_score_en"].append(quality_score_en)
        dico_res["rule_issues_en"].append(rule_issues_en)

        dico_res["default"].append(row.default)
        dico_res["default2"].append(row.default2)
        quality_score_def, rule_issues_def = compute_quality_score(
            row["default"],
            row["default2"]
        )
        dico_res["quality_score_default"].append(quality_score_def)
        dico_res["rule_issues_default"].append(rule_issues_def)

        dico_res["display_name_title"].append(row.display_name_title)
        dico_res["display_name_title2"].append(row.display_name_title2)
        score, issues = compute_quality_score(
            row["display_name_title"],
            row["display_name_title2"]
        )
        dico_res["quality_score_title"].append(score)
        dico_res["rule_issues_title"].append(issues)

    df = pd.DataFrame(dico_res)

    # ✅ classement (pire → meilleur)
    df = df.sort_values(by="quality_score")

    df.to_excel(output_file, index=False)

    print("✅ Analyse terminée :", output_file)


if __name__ == "__main__":
    analyze_file(
        "cleaned_text.xlsx",
        "analysis_results.xlsx"
    )
