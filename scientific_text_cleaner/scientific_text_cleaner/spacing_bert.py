from transformers import AutoTokenizer, AutoModelForTokenClassification
import torch

MODEL_PATH = "models/bert_model"


tokenizer = None
model = None


def load_bert():

    global tokenizer, model

    if tokenizer is None:
        tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
        model = AutoModelForTokenClassification.from_pretrained(MODEL_PATH)

    return tokenizer, model


def fix_spacing_bert(text):

    tokenizer, model = load_bert()

    tokens = list(text)

    inputs = tokenizer(tokens, return_tensors="pt", is_split_into_words=True)

    outputs = model(**inputs)
    preds = torch.argmax(outputs.logits, dim=-1)[0]

    result = ""

    for t, p in zip(tokens, preds):
        result += t
        if p == 1:
            result += " "

    return result