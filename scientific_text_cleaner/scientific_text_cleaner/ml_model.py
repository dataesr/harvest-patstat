import joblib
import pandas as pd
from sklearn.ensemble import IsolationForest

from scientific_text_cleaner.scientific_text_cleaner.features import extract_features


def prepare_features(df, ori, clea):

    rows = []

    for _, row in df.iterrows():

        feats = extract_features(
            row[ori],
            row[clea]
        )

        rows.append(feats)

    return pd.DataFrame(rows)


# ✅ ENTRAÎNEMENT
def train_model(df, ori, clea):

    X = prepare_features(df, ori, clea)

    model = IsolationForest(
        n_estimators=100,
        contamination=0.1,
        random_state=42
    )

    model.fit(X)

    return model


# ✅ PRÉDICTION
def predict_anomalies(model, df, ori, clea):

    X = prepare_features(df, ori, clea)

    scores = model.decision_function(X)
    anomalies = model.predict(X)  # -1 = anomalie

    return scores, anomalies


# ✅ sauvegarde
def save_model(model, path="models/anomaly_model.pkl"):
    joblib.dump(model, path)


def load_model(path="models/anomaly_model.pkl"):
    return joblib.load(path)
