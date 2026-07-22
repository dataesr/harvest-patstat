import re

def protect_angles(text):
    text = text.replace("(<P>)", "(⟪P⟫)")
    return text.replace("<", "⟨").replace(">", "⟩")
    return re.sub(r"<(.+)>", r"⟨\1⟩", text)

def restore_angles(text):
    text = text.replace("∫∫∫", "$")
    text = text.replace("(⟪P⟫)", "(<P>)")
    return text.replace("⟨", "<").replace("⟩", ">")