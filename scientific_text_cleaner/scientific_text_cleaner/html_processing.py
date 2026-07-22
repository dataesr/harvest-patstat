from bs4 import BeautifulSoup
import re

def parse_html(text):
    text = re.sub(r"⟨!\-+!?\[?\w+\]?\-+⟩", "", text)
    text = re.sub(r"⟨img.+\/⟩", "", text)
    text = re.sub(r"⟨u\/⟩", "", text)

    if "⟨p⟩" or "⟨P⟩" in text:
        text = re.sub(r"⟨\/?p⟩", "", text, flags=re.IGNORECASE)

    if "⟨title⟩" or "⟨TITLE⟩" in text:
        text = re.sub(r"⟨\/?title⟩", "", text, flags=re.IGNORECASE)

    if '⟨ Pub⟨sub⟩newline⟨/sub⟩ ⟩' in text:
        text = text.replace("⟨ Pub⟨sub⟩newline⟨/sub⟩ ⟩", "⟨br/⟩")
    soup = BeautifulSoup(text, "html.parser")

    if "⟨italic⟩" in soup.get_text():
        value = soup.get_text()
        value = value.replace("italic", "i")
        soup = BeautifulSoup(value, "html.parser")

    if "⟨em⟩" in soup.get_text():
        value = soup.get_text()
        value = value.replace("em", "i")
        soup = BeautifulSoup(value, "html.parser")

    if "⟨inf⟩" in soup.get_text():
        value = soup.get_text()
        value = value.replace("inf", "sub")
        if "⟨formula⟩" in value:
            value = value.replace("⟨formula⟩", "")
            value = value.replace("⟨/formula⟩", "")
        if "⟨roman⟩" in value:
            value = value.replace("⟨roman⟩", "")
            value = value.replace("⟨/roman⟩", "")
        soup = BeautifulSoup(value, "html.parser")

    for tag in soup.find_all("br"):
        br = tag.get_text()
        tag.replace_with("⟨br/⟩")

    for tag in soup.find_all("i"):
        italic = tag.get_text()
        tag.replace_with("⟨i⟩" + italic + "⟨/i⟩")

    for tag in soup.find_all("sp"):
        sp = tag.get_text()
        tag.replace_with("⟨sup⟩" + sp + "⟨/sup⟩")

    for tag in soup.find_all("sup"):
        sup = tag.get_text()
        tag.replace_with("⟨sup⟩" + sup + "⟨/sup⟩")

    for tag in soup.find_all("sb"):
        sb = tag.get_text()
        tag.replace_with("⟨sub⟩" + sb + "⟨/sub⟩")

    for tag in soup.find_all("sub"):
        sub = tag.get_text()
        tag.replace_with("⟨sub⟩" + sub + "⟨/sub⟩")

    for tag in soup.find_all("u"):
        tu = tag.get_text()
        tag.replace_with("⟨u⟩" + tu + "⟨/u⟩")

    for tag in soup.find_all("b"):
        tb = tag.get_text()
        tag.replace_with("⟨b⟩" + tb + "⟨/b⟩")

    return soup.get_text(separator=" ")