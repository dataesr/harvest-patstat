import os
import re

import numpy as np

from patstat import dtypes_patstat_declaration as types
import pandas as pd
from unidecode import unidecode

DATA_PATH = "/home/julia/Bureau/"

os.chdir(DATA_PATH)

res = pd.read_csv("res_elastic_structures.csv", sep="|", encoding="utf-8", engine="python",
                  dtype={"sirene": str, "siret": str})

test = pd.read_excel("20221206_res_elastic_structures.xlsx", sheet_name="Feuil1", engine="openpyxl")

pp = pd.read_excel("/home/julia/Documents/data_dataesr/qualif_mauvaises_entre_juew-verif.xlsx",
                   sheet_name="personnes_physiques",
                   usecols=["name_source", "label_fr"],
                   engine="openpyxl")

corriges = pd.read_excel("/home/julia/Documents/data_dataesr/qualif_mauvaises_entre_juew-verif.xlsx",
                         sheet_name="mauvaises_societes", usecols=["name_source", "siren", "label_fr"],
                         engine="openpyxl")

ok = pd.read_excel("/home/julia/Documents/data_dataesr/qualif_mauvaises_entre_juew-verif.xlsx",
                   sheet_name="bonnes_societes", engine="openpyxl")

corriges2 = pd.read_excel("verif_id_personnes_scanr.xlsx", sheet_name="mauvaises_societes",
                          usecols=["name_source", "siren", "label_fr"], engine="openpyxl")
corriges2 = corriges2.loc[corriges2["siren"].notna()]
msiren = corriges2.loc[corriges2["siren"] == "pas de siren"]
corriges2 = corriges2.loc[corriges2["siren"] != "pas de siren"]

pp2 = pd.read_excel("verif_id_personnes_scanr.xlsx", sheet_name="personnes_physiques", engine="openpyxl")

pp = pd.concat([pp, pp2])

ok2 = pd.read_excel("verif_id_personnes_scanr.xlsx", sheet_name="bonnes_societes", engine="openpyxl")

ok = pd.concat([ok, ok2])

del ok2
del pp2

corriges = corriges.loc[corriges["name_source"].notna()].drop_duplicates()
corriges = corriges.loc[corriges["siren"] != "pas de siren"]
corriges["nombres"] = corriges["siren"].apply(lambda a: a.lstrip().rstrip()).copy()
corriges["nombres"] = corriges["nombres"].apply(lambda a: "" if not a.isnumeric() else a)
corriges["sir_nombres"] = corriges["nombres"].apply(lambda a: a[0:9])
corriges["siret"] = corriges["nombres"].apply(lambda a: a if len(a) > 9 else "")
corriges["siren"] = corriges["siren"].str.replace(";", ",", regex=False)
corriges["multi"] = corriges["siren"].apply(lambda a: a if "," in a else "")
corriges["multi"] = corriges["multi"].apply(lambda a: a.split(","))
corriges["multi"] = corriges["multi"].apply(lambda a: [x.lstrip().rstrip() for x in a])
corriges["multi"] = corriges["multi"].apply(lambda a: [x for x in a if x != ""])
corriges["multi_siret"] = corriges["multi"].apply(lambda a: [x for x in a if len(x) > 9])
corriges["multi_siren"] = corriges["multi"].apply(lambda a: [x[0:9] for x in a if len(x) > 9])
corriges["multi_siren"] = corriges["multi_siren"].apply(lambda a: ",".join(a))
corriges["multi_siret"] = corriges["multi_siret"].apply(lambda a: ",".join(a))
corriges = corriges.drop(columns="multi").rename(columns={"siren": "id"})
corriges = corriges.rename(columns={"siret": "siret2"})
corriges.loc[(corriges["multi_siret"] != "") & (corriges["siret2"] == ""), "siret"] = corriges.loc[
    (corriges["multi_siret"] != "") & (corriges["siret2"] == ""), "multi_siret"]
corriges.loc[(corriges["multi_siret"] == "") & (corriges["siret2"] != ""), "siret"] = corriges.loc[
    (corriges["multi_siret"] == "") & (corriges["siret2"] != ""), "siret2"]
corriges = corriges.drop(columns="multi_siret")
corriges.loc[corriges["siret"].isna(), "siret"] = ""
corriges.loc[(corriges["multi_siren"] != "") & (corriges["sir_nombres"] == ""), "siren"] = corriges.loc[
    (corriges["multi_siren"] != "") & (corriges["sir_nombres"] == ""), "multi_siren"]
corriges.loc[(corriges["multi_siren"] == "") & (corriges["sir_nombres"] != ""), "siren"] = corriges.loc[
    (corriges["multi_siren"] == "") & (corriges["sir_nombres"] != ""), "sir_nombres"]
corriges.loc[corriges["siren"].isna(), "siren"] = ""
corriges = corriges.drop(columns=["siret2", "multi_siren", "sir_nombres", "id", "nombres"])
mcorriges = corriges.loc[(corriges["siren"] == "") & (corriges["siret"] == "")].drop(columns=["siren", "siret"])
corriges = corriges.loc[(corriges["siren"] != "") | (corriges["siret"] != "")]

corriges = pd.concat([corriges, corriges2])
del corriges2
msiren = msiren.drop(columns="siren")

missing = pd.concat([msiren, mcorriges])
missing.loc[:, "type"] = "missing"

del msiren
del mcorriges

correct = test.loc[test["test"] == 1]
ok = pd.concat([ok, correct[["name_source", "label_fr", "test"]]])
ok.loc[ok["test"].isna(), "test"] = 1

incorrect = test.loc[test["test"] == 0]
incorrect = pd.merge(incorrect, corriges[["name_source", "siren", "siret"]], on=["name_source"], how="inner").drop(
    columns=["test", "type"])

res_correct = pd.merge(res, ok, on=["name_source", "label_fr"], how="inner").drop(columns="test").drop_duplicates()

res_incorrect = pd.merge(res[["person_id", "name_source", "label_fr"]], incorrect, on=["name_source", "label_fr"],
                         how="inner")

pp.loc[:, "type"] = "pp"
res_personnes = pd.merge(res[["person_id", "name_source", "label_fr"]], pp, on=["name_source", "label_fr"],
                         how="inner").drop(columns="type")

res_missing = pd.merge(res[["person_id", "name_source", "label_fr"]], missing, on=["name_source", "label_fr"],
                       how="inner").drop(columns="type")

res_together = pd.concat([res_correct, res_incorrect, res_missing, res_personnes])

person_id = list(set(res_correct["person_id"]))
for i in list(set(res_incorrect["person_id"])):
    person_id.append(i)

for i in list(set(res_personnes["person_id"])):
    person_id.append(i)

for i in list(set(res_missing["person_id"])):
    person_id.append(i)

missing_id = list(set(res['person_id']).difference(set(person_id)))

df_missing = res.loc[res["person_id"].isin(missing_id)]

test_correct = pd.merge(df_missing, test, on="name_source", how="inner")

names = res[["name_source", "label_fr"]].drop_duplicates()
names.loc[:, "fullname"] = names.loc[:, "name_source"] + names.loc[:, "label_fr"]
test_names = test[["name_source", "label_fr"]].drop_duplicates()
test_names.loc[:, "fullname"] = test_names.loc[:, "name_source"] + test_names.loc[:, "label_fr"]
diff = names.loc[~names["fullname"].isin(test_names["fullname"])]
diff = diff.drop(columns="fullname")

blob = pd.read_excel("20221206_res_elastic_structures.xlsx", sheet_name="20221206_res_elastic_structures",
                     engine="openpyxl")
diff = diff

part = pd.read_csv("/run/media/julia/DATA/test/part_init_p05.csv", sep="|", encoding="utf-8",
                   dtype=types.part_init_types)

pat = pd.read_csv("/run/media/julia/DATA/test/patent.csv", sep="|", dtype=types.patent_types)
test = diff
test = test.loc[test["name_source"].notna()]
part2 = part.loc[part["name_source"].isin(test["name_source"])]
part2.loc[:, "numpub"] = part2.loc[:, "appln_auth"] + part2.loc[:, "publication_number"]

dict = {"name_source": [], "publication-number": []}
for name in list(set(part2["name_source"])):
    pub = list(set(part2.loc[part2["name_source"] == name, "numpub"]))
    dict["name_source"].append(name)
    dict["publication-number"].append(", ".join(pub))

name_missing = pd.DataFrame(data=dict)

df_missing2 = pd.merge(df_missing, name_missing, on="name_source", how="left").drop(
    columns=["person_id", "sirene", "grid", "idref", "siret"]).drop_duplicates()
# df_missing2.to_excel("20230511_missing_names.xlsx", index=False, engine="openpyxl")

retrouve = pd.read_excel("/run/media/julia/DATA/test/20230511_missing_names.xlsx", sheet_name="Sheet1",
                         engine="openpyxl")

retrouve = retrouve.loc[retrouve["name_source"].notna()]
retrouve["test"] = retrouve["test"].astype(int)
retrouve_ok = retrouve.loc[retrouve["siren"] == "correct"]
retrouve_siren = retrouve.loc[~retrouve["siren"].isin(["correct", "pas de siren"])]
retrouve_nok = retrouve.loc[(retrouve["siren"] == "pas de siren") & (retrouve["type"] == "pm")]
retrouve_pp = retrouve.loc[retrouve["type"] == "pp"]

missing_correct = pd.merge(df_missing, retrouve_ok, on=["name_source", "label_fr"], how="inner").drop(
    columns=["siren", "test", "type", "publication-number"]).drop_duplicates()
missing_incorrect = pd.merge(df_missing[["person_id", "name_source", "label_fr"]], retrouve_siren,
                             on=["name_source", "label_fr"], how="inner").drop(
    columns=["test", "type", "publication-number"]).drop_duplicates()
missing_nok = pd.merge(df_missing[["person_id", "name_source", "label_fr"]], retrouve_nok,
                       on=["name_source", "label_fr"], how="inner").drop(
    columns=["test", "type", "publication-number", "siren"]).drop_duplicates()
missing_pp = pd.merge(df_missing[["person_id", "name_source", "label_fr"]], retrouve_pp, on=["name_source", "label_fr"],
                      how="inner").drop(columns=["test", "type", "publication-number", "siren"]).drop_duplicates()

missing_together = pd.concat([missing_correct, missing_incorrect, missing_nok, missing_pp])

together = pd.concat([res_together, missing_together])

together.loc[(together["siren"].notna()) & (together["sirene"].isna()), "siren2"] = together.loc[
    (together["siren"].notna()) & (together["sirene"].isna()), "siren"]
together.loc[(together["siren"].isna()) & (together["sirene"].notna()), "siren2"] = together.loc[
    (together["siren"].isna()) & (together["sirene"].notna()), "sirene"]
together["siren2"] = together["siren2"].apply(lambda a: str(a)[0:9])
together = together.drop(columns=["sirene", "siren"]).rename(columns={"siren2": "siren"})
together_siren = together.loc[together["siren"].notna()]

retrouve_id = list(missing_correct["person_id"])

for i in list(missing_incorrect["person_id"]):
    retrouve_id.append(i)

for i in list(missing_pp["person_id"]):
    retrouve_id.append(i)

for i in list(missing_nok["person_id"]):
    retrouve_id.append(i)

diff2 = list(set(df_missing["person_id"]).difference(set(retrouve_id)))


def remove_punctuation(test_str):
    # Using filter() and lambda function to filter out punctuation characters
    result = ''.join(filter(lambda x: x.isalpha() or x.isdigit() or x.isspace(), test_str))
    return result


df_missing_nok = df_missing.loc[df_missing["person_id"].isin(diff2)]
df_missing_nok["name"] = df_missing_nok["name_source"].apply(lambda a: a.strip())
df_missing_nok["name"] = df_missing_nok["name_source"].apply(lambda a: unidecode(remove_punctuation(a)))
df_missing_nok["name"] = df_missing_nok["name_source"].apply(lambda a: re.sub(r"\s{2,}", "", a).lower())

together_siren["name"] = together_siren["name_source"].apply(lambda a: a.strip())
together_siren["name"] = together_siren["name_source"].apply(lambda a: unidecode(remove_punctuation(a)))
together_siren["name"] = together_siren["name_source"].apply(lambda a: re.sub(r"\s{2,}", "", a).lower())

df_missing_nok2 = pd.merge(df_missing_nok[["name_source", "name", "person_id", "label_fr"]],
                           together_siren.drop(columns=["name_source", "person_id", "label_fr"]),
                           on="name", how="inner")
df_missing_nok2 = df_missing_nok2.drop(columns="name")

together = pd.concat([together, df_missing_nok2])

df_missing3 = df_missing_nok.loc[~df_missing_nok["person_id"].isin(df_missing_nok2["person_id"])].drop(
    columns=["name", "sirene"])

together = pd.concat([together, df_missing3])
together = together.drop_duplicates()
together = together.rename(columns={"grid": "gridt", "idref": "idreft", "siret": "sirett", "siren": "sirent"}).drop(
    columns="label_fr")

for col in ["gridt", "idreft", "sirett", "sirent"]:
    together.loc[together[col] == "", col] = np.NAN


together.to_csv("/home/julia/Documents/data_dataesr/20230512_siren_entp.csv", sep="|", encoding="utf-8", index=False)

part_p05 = pd.read_csv("/home/julia/Documents/data_dataesr/part_p05.csv", sep="|", encoding="utf-8",
                       dtype=types.part_init_types)
part_p05.loc[(part_p05["siren"].isna()) & (part_p05["siret"].notna()), "siren"] = part_p05.loc[
    (part_p05["siren"].isna()) & (part_p05["siret"].notna()), "siret"].apply(lambda a: str(a)[0:9])

part_p2 = pd.merge(part_p05, together, on=["person_id", "name_source"], how="left")
for item in ["gridt", "idreft", "sirett", "sirent"]:
    it = item[0:-1]
    part_p2.loc[(part_p2[item].notna()) & (part_p2[it].isna()), it] = part_p2.loc[
        (part_p2[item].notna()) & (part_p2[it].isna()), item]

part_p2.loc[(part_p2["siren"].isna()) & (part_p2["siret"].notna()), "siren"] = part_p2.loc[
    (part_p2["siren"].isna()) & (part_p2["siret"].notna()), "siret"].apply(lambda a: str(a)[0:9])

part_p2 = part_p2.drop(columns=["gridt", "idreft", "sirett", "sirent"])

part_p2.loc[(part_p2["siren"].isna()) & (part_p2["siret"].notna()), "siren"] = part_p2.loc[
    (part_p2["siren"].isna()) & (part_p2["siret"].notna()), "siret"].apply(lambda a: str(a)[0:9])

part_p2.to_csv("/run/media/julia/DATA/test/p05_traites/part_p05.csv", sep="|", encoding="utf-8", index=False)

part_init = pd.read_csv("/home/julia/Documents/data_dataesr/part_init_p05.csv", sep="|", encoding="utf-8",
                        dtype=types.part_init_types)

part_init.loc[(part_init["siren"].isna()) & (part_init["siret"].notna()), "siren"] = part_init.loc[
    (part_init["siren"].isna()) & (part_init["siret"].notna()), "siret"].apply(lambda a: str(a)[0:9])

part_init2 = pd.merge(part_init, together, on=["person_id", "name_source"], how="left")
for item in ["gridt", "idreft", "sirett", "sirent"]:
    it = item[0:-1]
    part_init2.loc[(part_init2[item].notna()) & (part_init2[it].isna()), it] = part_init2.loc[
        (part_init2[item].notna()) & (part_init2[it].isna()), item]

part_init2.loc[(part_init2["siren"].isna()) & (part_init2["siret"].notna()), "siren"] = part_init2.loc[
    (part_init2["siren"].isna()) & (part_init2["siret"].notna()), "siret"].apply(lambda a: str(a)[0:9])

part_init2 = part_init2.drop(columns=["gridt", "idreft", "sirett", "sirent"])

part_init2.to_csv("/run/media/julia/DATA/test/p05_traites/part_init_p05.csv", sep="|", encoding="utf-8", index=False)
