import re
import pandas as pd
from sklearn.linear_model import LogisticRegression
import joblib

from scientific_text_cleaner.scripts.prepare_spacing_dataset import build_dataset


# =========================================================
# ✅ FEATURES
# =========================================================

def extract_spacing_features(word1, word2):

    return {
        "len1": len(word1),
        "len2": len(word2),
        "is_cap1": int(word1.istitle()),
        "is_cap2": int(word2.istitle()),
        "both_caps": int(word1.istitle() and word2.istitle()),
        "has_vowel1": int(bool(re.search(r"[aeiou]", word1.lower()))),
        "has_vowel2": int(bool(re.search(r"[aeiou]", word2.lower()))),
        "pattern": int(bool(re.match(r"[A-Z][a-z]+", word1)))
    }


# =========================================================
# ✅ TRAINING
# =========================================================

def train_spacing_model(pairs, labels):

    features = [extract_spacing_features(w1, w2) for w1, w2 in pairs]

    X = pd.DataFrame(features)
    y = labels

    model = LogisticRegression()
    model.fit(X, y)

    return model


def save_model(model, path="spacing_model.pkl"):
    joblib.dump(model, path)


def load_model(path="spacing_model.pkl"):
    return joblib.load(path)


# =========================================================
# ✅ PREDICTION
# =========================================================

def should_insert_space(model, word1, word2):

    feats = extract_spacing_features(word1, word2)
    X = pd.DataFrame([feats])

    pred = model.predict(X)[0]

    return pred == 1


# =========================================================
# ✅ APPLY TO TEXT
# =========================================================

def fix_spacing_ml(text, model):

    tokens = re.findall(r"\w+|\W+", text)

    result = []

    for i in range(len(tokens) - 1):

        w1 = tokens[i]
        w2 = tokens[i + 1]

        if w1.isalpha() and w2.isalpha():

            if should_insert_space(model, w1, w2):
                result.append(w1 + " ")
            else:
                result.append(w1)

        else:
            result.append(w1)

    result.append(tokens[-1])

    return "".join(result)


def learning(file):
    df = pd.read_excel(file, engine="openpyxl")

    pairs, labels = build_dataset("/run/media/julia/DATA/fall2025/analysis_results.xlsx")

    model = train_spacing_model(pairs, labels)

    save_model(model, "models/spacing_model.pkl")
