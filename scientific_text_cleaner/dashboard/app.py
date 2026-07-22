import streamlit as st
import pandas as pd

from scientific_text_cleaner.scientific_text_cleaner.quality import compute_quality_score
from scientific_text_cleaner.scientific_text_cleaner.ml_model import load_model, predict_anomalies


st.set_page_config(page_title="Text Quality Dashboard", layout="wide")

st.title("📊 Scientific Text Cleaning Dashboard")


# --- upload fichier ---
file = st.file_uploader("Upload Excel file", type=["xlsx"])

if file is not None:

    df = pd.read_excel(file, engine="openpyxl")

    st.write("✅ Dataset chargé :", df.shape)

    # --- scoring classique ---
    scores = []
    issues_list = []

    for _, row in df.iterrows():

        score, issues = compute_quality_score(
            row["display_name_title"],
            row["display_name_title2"]
        )

        scores.append(score)
        issues_list.append(",".join(issues))

    df["quality_score"] = scores
    df["rule_issues"] = issues_list

    # --- ML ---
    model = load_model()
    ml_scores, anomalies = predict_anomalies(model, df)

    df["ml_score"] = ml_scores
    df["ml_anomaly"] = anomalies

    # --- filtres ---
    st.sidebar.header("Filters")

    min_score = st.sidebar.slider("Min quality score", 0, 100, 50)

    show_anomalies = st.sidebar.checkbox("Only ML anomalies", False)

    filtered = df[df["quality_score"] <= min_score]

    if show_anomalies:
        filtered = filtered[df["ml_anomaly"] == -1]

    # --- affichage ---
    st.write("### 🔍 Problematic rows")
    st.dataframe(filtered.head(200))

    # --- stats ---
    st.write("### 📈 Stats")

    st.metric("Total rows", len(df))
    st.metric("Avg score", int(df["quality_score"].mean()))
    st.metric("Anomalies ML", int((df["ml_anomaly"] == -1).sum()))

    # --- top erreurs ---
    st.write("### ⚠️ Worst rows")

    worst = df.nsmallest(20, "quality_score")

    for _, row in worst.iterrows():
        st.markdown(f"""
        **Score:** {row['quality_score']}  
        **Issues:** {row['rule_issues']}  
        **Original:** {row['display_name_title'][:300]}  
        **Cleaned:** {row['display_name_title2'][:300]}
        ---
        """)
