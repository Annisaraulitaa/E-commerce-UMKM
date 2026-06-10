import re


def clean_text(text):
    if pd_is_missing(text):
        return ""

    text = str(text).lower()
    text = re.sub(r"[^a-zA-Z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text


def tokenize(text):
    text = clean_text(text)
    return text.split()


def pd_is_missing(value):
    return value is None or str(value).lower() == "nan"