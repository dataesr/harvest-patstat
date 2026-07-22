import re


def extract_features(original, cleaned):

    return {
        "length_ratio": len(cleaned) / max(len(original), 1),

        "digit_ratio":
            len(re.findall(r"\d", cleaned)) /
            max(len(re.findall(r"\d", original)), 1),

        "has_percent_original": int("%" in original),
        "has_percent_cleaned": int("%" in cleaned),

        "has_angle_original": int("<" in original or ">" in original),
        "has_angle_cleaned": int("<" in cleaned or ">" in cleaned),

        "html_tags_remaining":
            int(bool(re.search(r"<[^>]+>", cleaned))),

        "num_upper_original":
            len(re.findall(r"[A-Z]{2,}", original)),

        "num_upper_cleaned":
            len(re.findall(r"[A-Z]{2,}", cleaned)),

        "num_spaces_change":
            abs(len(original.split()) - len(cleaned.split()))
    }