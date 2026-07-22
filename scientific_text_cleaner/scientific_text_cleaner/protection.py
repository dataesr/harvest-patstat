import re

def protect_angles(text):
    return re.sub(r"<([A-Za-z0-9,_\s]+)>", r"⟨\1⟩", text)

def restore_angles(text):
    return text.replace("⟨", "<").replace("⟩", ">")