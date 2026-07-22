import pandas as pd
import re


def extract_word_pairs(original, cleaned):

    pairs = []
    labels = []

    # tokenisation simple
    orig_words = re.findall(r"\w+", original)
    clean_words = re.findall(r"\w+", cleaned)

    i = 0
    j = 0

    while i < len(orig_words) and j < len(clean_words):

        w_orig = orig_words[i]
        w_clean = clean_words[j]

        # cas fusion
        if w_orig.lower() == w_clean.lower():
            i += 1
            j += 1
            continue

        # exemple: InVitro → In Vitro
        if (i + 1 < len(orig_words) and
            (orig_words[i] + orig_words[i+1]).lower() == w_clean.lower()):

            pairs.append((orig_words[i], orig_words[i+1]))
            labels.append(1)  # ✅ doit être séparé

            i += 2
            j += 1

        else:
            i += 1
            j += 1

    return pairs, labels


def build_dataset(file):

    df = pd.read_excel(file, engine="openpyxl")
    df = df.loc[df["display_name_title"].notna()]

    all_pairs = []
    all_labels = []

    for _, row in df.iterrows():

        pairs, labels = extract_word_pairs(
            str(row["display_name_title"]),
            str(row["display_name_title2"])
        )

        all_pairs.extend(pairs)
        all_labels.extend(labels)

    return all_pairs, all_labels