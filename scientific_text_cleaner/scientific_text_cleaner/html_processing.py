from bs4 import BeautifulSoup

def parse_html(text):
    soup = BeautifulSoup(text, "html.parser")
    return soup.get_text(separator=" ")