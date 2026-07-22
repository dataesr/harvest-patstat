import pandas as pd

from scientific_text_cleaner import train_model, save_model

df = pd.read_excel("/run/media/julia/DATA/fall2025/cleaned_text.xlsx", engine="openpyxl")
df = df.loc[(df["display_name_title"].notna()) & (df["display_name_title2"].notna())]

model = train_model(df)

save_model(model)

print("✅ modèle entraîné et sauvegardé")