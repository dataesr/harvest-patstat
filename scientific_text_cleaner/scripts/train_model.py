import pandas as pd

from scientific_text_cleaner.scientific_text_cleaner.ml_model import save_model, train_model

df = pd.read_excel("cleaned_text.xlsx", engine="openpyxl")
df2 = df.loc[(df["display_name_title"]!="") & (df["display_name_title2"]!="")]

model = train_model(df2)

save_model(model)

print("✅ modèle entraîné et sauvegardé")