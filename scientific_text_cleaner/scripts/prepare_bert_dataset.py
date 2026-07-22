from datasets import Dataset
import pandas as pd


def build_bert_dataset(file):

    df = pd.read_excel(file, engine="openpyxl")
    df = df.loc[df["display_name_title"].notna()]

    inputs = []
    outputs = []

    for _, row in df.iterrows():
        inputs.append(str(row["display_name_title"]))
        outputs.append(str(row["display_name_title2"]))

    return Dataset.from_dict({
        "input_text": inputs,
        "target_text": outputs
    })
