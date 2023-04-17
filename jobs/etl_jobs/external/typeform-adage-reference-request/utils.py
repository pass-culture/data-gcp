import unicodedata
import re


def clean_question(input_str):
    input_str = re.sub(r"\?|\.|\:", "", input_str).rstrip()
    input_str = re.sub(r"-|\'| ", "_", input_str).lower()
    nfkd_form = unicodedata.normalize("NFKD", input_str)
    only_ascii = nfkd_form.encode("ASCII", "ignore")

    return only_ascii.decode("utf-8")
