import pandas as pd


def description_preprocessing(series: pd.Series, min_length: int) -> pd.Series:
    return (
        series.str.lower()
        .str.strip()
        .str.normalize("NFD")
        .str.encode("ascii", errors="ignore")
        .str.decode("ascii")
        .where(lambda s: s.str.len() >= min_length, other=pd.NA)
    )


def offer_name_preprocessing(series: pd.Series) -> pd.Series:
    return (
        series.str.lower()
        .str.replace(
            r"^(?:concert de poche|apero-concert|concerts?)\W*", "", regex=True
        )
        .str.split(r"/|\+|&|•|\||:| - | à | en concert | x | et | au | avec ")
        .str[0]
        .str.strip()
        .str.normalize("NFD")
        .str.encode("ascii", errors="ignore")
        .str.decode("ascii")
        .replace("", pd.NA)
    )


def full_name_preprocessing(series: pd.Series) -> pd.Series:
    return (
        series.str.lower()
        .str.strip()
        .str.normalize("NFD")
        .str.encode("ascii", errors="ignore")
        .str.decode("ascii")
        .replace("", pd.NA)
    )
