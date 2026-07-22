# -*- coding: utf-8 -*-

"""
Scientific post-processing module

Objectif :
Corriger les artefacts restants après cleaning :
- espaces autour opérateurs (_, ^)
- unités scientifiques cassées
- collisions de mots
- restes de HTML
- notations LaTeX simplifiées

À appliquer en DERNIÈRE étape du pipeline
"""

import re


# =========================================================
# ✅ 1. CLEAN HTML RESIDUAL TAGS
# =========================================================

def remove_residual_tags(text):
    """
    Supprime les tags HTML restants après parsing
    """
    return re.sub(r"</?[a-zA-Z\-]+[^>]*>", "", text)


# =========================================================
# ✅ 2. SIMPLIFY LATEX NOTATION
# =========================================================

def simplify_latex_notation(text):
    """
    Transforme :
    _{m} → _m
    ^{2} → ^2
    """
    text = re.sub(r"_\{\s*([^}]+)\s*\}", r"_\1", text)
    text = re.sub(r"\^\{\s*([^}]+)\s*\}", r"^\1", text)
    return text


# =========================================================
# ✅ 3. FIX SCIENTIFIC SYMBOL SPACING
# =========================================================

def fix_scientific_notation(text):
    """
    Corrige espaces autour _ et ^
    ex:
    Cu _2 → Cu_2
    mm ^2 → mm^2
    """

    # avant symbole
    text = re.sub(r"\s+_", "_", text)
    text = re.sub(r"\s+\^", "^", text)

    # après symbole
    text = re.sub(r"_\s+", "_", text)
    text = re.sub(r"\^\s+", "^", text)

    return text


# =========================================================
# ✅ 4. FIX SCIENTIFIC UNITS
# =========================================================

def fix_units(text):
    """
    Corrige unités scientifiques
    """

    # μ s → μs
    text = re.sub(r"μ\s+s", "μs", text)

    # mm ^2 → mm^2
    text = re.sub(r"mm\s*\^\s*2", "mm^2", text)

    # Tb/in ^2 → Tb/in^2
    text = re.sub(r"/\s*in\s*\^\s*2", "/in^2", text)

    # μ V → μV
    text = re.sub(r"μ\s+V", "μV", text)

    # μ m → μm
    text = re.sub(r"μ\s+m", "μm", text)

    return text


# =========================================================
# ✅ 5. FIX WORD COLLISIONS
# =========================================================

def fix_word_collisions(text):
    """
    Corrige cas :
    ALow → A Low
    Multi- V → Multi-V
    """

    # AWord → A Word
    text = re.sub(r"\b([A-Z])([A-Z][a-z])", r"\1 \2", text)

    # tiret collé
    text = re.sub(r"-\s+", "-", text)

    return text


# =========================================================
# ✅ 6. FIX SUBSCRIPT / SUPERSCRIPT CASES
# =========================================================

def fix_indices(text):
    """
    Corrige des cas comme :
    p21 ^ Cip1 → p21^Cip1
    V_rms → reste cohérent
    """

    # supprimer espaces autour ^
    text = re.sub(r"\^\s*([A-Za-z0-9]+)", r"^\1", text)

    # supprimer espaces autour _
    text = re.sub(r"_\s*([A-Za-z0-9]+)", r"_\1", text)

    return text


# =========================================================
# ✅ 7. FINAL CLEANING
# =========================================================

def normalize_spaces(text):
    """
    Nettoyage final des espaces
    """
    return re.sub(r"\s+", " ", text).strip()


# =========================================================
# ✅ MAIN FUNCTION
# =========================================================

def scientific_postprocess(text):
    """
    Pipeline final à appliquer après clean_text()

    Étapes :
    1. remove HTML residuals
    2. simplify latex
    3. fix notation (_,^)
    4. fix units
    5. fix collisions
    6. fix indices
    7. normalize spaces
    """

    if not isinstance(text, str):
        return text

    # text = remove_residual_tags(text)

    text = simplify_latex_notation(text)

    text = fix_scientific_notation(text)

    text = fix_units(text)

    text = fix_word_collisions(text)

    text = fix_indices(text)

    text = normalize_spaces(text)

    return text