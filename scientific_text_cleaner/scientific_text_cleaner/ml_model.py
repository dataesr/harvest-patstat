import joblib
import pandas as pd
from sklearn.ensemble import IsolationForest

from .features import extract_features


def prepare_features(df):

    rows = []

    for _, row in df.iterrows():

        feats = extract_features(
            row["display_name_title"],
            row["display_name_title2"]
        )

        rows.append(feats)

    return pd.DataFrame(rows)


# ✅ ENTRAÎNEMENT
def train_model(df):

    X = prepare_features(df)

    model = IsolationForest(
        n_estimators=100,
        contamination=0.1,
        random_state=42
    )

    model.fit(X)

    return model


# ✅ PRÉDICTION
def predict_anomalies(model, df):

    X = prepare_features(df)

    scores = model.decision_function(X)
    anomalies = model.predict(X)  # -1 = anomalie

    return scores, anomalies


# ✅ sauvegarde
def save_model(model, path="models/anomaly_model.pkl"):
    joblib.dump(model, path)


def load_model(path="models/anomaly_model.pkl"):
    return joblib.load(path)
