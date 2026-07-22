import pandas as pd
from scientific_text_cleaner.quality import compute_quality_score


def analyze_file(input_file, output_file):

    df = pd.read_excel(input_file, engine="openpyxl")

    scores = []
    issues_list = []

    for idx, row in df.iterrows():

        original = row["display_name_title"]
        cleaned = row["display_name_title2"]

        score, issues = compute_quality_score(original, cleaned)

        scores.append(score)
        issues_list.append(",".join(issues))

    df["quality_score"] = scores
    df["issues"] = issues_list

    # ✅ classement (pire → meilleur)
    df = df.sort_values(by="quality_score")

    df.to_excel(output_file, index=False)

    print("✅ Analyse terminée :", output_file)


if __name__ == "__main__":
    analyze_file(
        "cleaned_text.xlsx",
        "analysis_results.xlsx"
    )
