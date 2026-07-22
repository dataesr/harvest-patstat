# -*- coding: utf-8 -*-

"""
Module de règles pour corriger et protéger les espaces
dans les textes scientifiques.

Rôle :
- protéger expressions latines
- corriger fusions fréquentes (InVitro, InSitu…)
- éviter les fusions incorrectes (TitleCase)
- préserver noms scientifiques
"""

import re


# =========================================================
# ✅ 1. EXPRESSIONS LATINES (CRITIQUES)
# =========================================================

LATIN_PHRASES = [
    "In Vitro",
    "In Vivo",
    "In Situ",
    "Ex Vivo",
    "In Utero"
]


def protect_latin_phrases(text):
    """
    Remplace temporairement les espaces pour éviter toute modification
    """
    for phrase in LATIN_PHRASES:
        text = text.replace(phrase, phrase.replace(" ", "__"))
    return text


def restore_latin_phrases(text):
    """
    Restaure les expressions latines
    """
    return text.replace("__", " ")


# =========================================================
# ✅ 2. CORRECTION DES FUSIONS FRÉQUENTES
# =========================================================

def fix_common_merges(text):

    patterns = {
        r"\bInVitro\b": "In Vitro",
        r"\bInVivo\b": "In Vivo",
        r"\bInSitu\b": "In Situ",
        r"\bExVivo\b": "Ex Vivo",
    }

    for pattern, replacement in patterns.items():
        text = re.sub(pattern, replacement, text)

    return text


# =========================================================
# ✅ 3. PRÉVENTION DES FUSIONS ABUSIVES
# =========================================================

def prevent_bad_merges(text):
    """
    Empêche les fusions type:
    InVitro → In Vitro
    """

    # Préfixes typiques scientifiques
    text = re.sub(
        r"\b(In|Ex|Pre|Post|Anti|Non)([A-Z][a-z]+)\b",
        r"\1 \2",
        text
    )

    return text


# =========================================================
# ✅ 4. NOMENCLATURE SCIENTIFIQUE (latin binomial)
# =========================================================

def fix_latin_binomial(text):
    """
    Corrige les noms scientifiques :
    CandidaAlbicans → Candida albicans
    """

    # Mot Latin Genus + species collé
    text = re.sub(
        r"\b([A-Z][a-z]{2,})([A-Z][a-z]{2,})\b",
        r"\1 \2",
        text
    )

    return text


# =========================================================
# ✅ 5. ÉVITER COLLISION MAJUSCULE
# =========================================================

def fix_title_collisions(text):
    """
    Corrige:
    InVitroActivity → In Vitro Activity
    """

    text = re.sub(
        r"([a-z])([A-Z])",
        r"\1 \2",
        text
    )

    return text


# =========================================================
# ✅ 6. MOTEUR PRINCIPAL
# =========================================================

def apply_spacing_rules(text):

    if not isinstance(text, str):
        return text

    text = protect_latin_phrases(text)

    # corrections sûres
    text = fix_common_merges(text)
    text = prevent_bad_merges(text)
    text = fix_title_collisions(text)
    text = fix_latin_binomial(text)

    text = restore_latin_phrases(text)

    # nettoyage final
    text = re.sub(r"\s+", " ", text)

    return text.strip()
