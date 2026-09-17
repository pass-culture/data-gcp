import pytest

from utils.excel import read_table


def test_read_table_finds_header_row_and_returns_strings(insee_like_xlsx):
    df = read_table(insee_like_xlsx, sheet="Composition_communale", header_key="CODGEO")

    assert list(df.columns) == ["CODGEO", "LIBGEO", "EPCI", "LIBEPCI", "DEP", "REG"]
    assert len(df) == 3
    assert df.iloc[0].tolist() == [
        "59350",
        "Lille",
        "200093201",
        "Métropole Européenne de Lille",
        "59",
        "32",
    ]
    assert df["CODGEO"].dtype == object


def test_read_table_keeps_leading_zeros(insee_like_xlsx):
    df = read_table(insee_like_xlsx, sheet="Composition_communale", header_key="CODGEO")

    assert "01001" in df["CODGEO"].tolist()


def test_read_table_raises_when_header_key_missing(insee_like_xlsx):
    with pytest.raises(ValueError, match="header 'NOPE' not found"):
        read_table(insee_like_xlsx, sheet="Composition_communale", header_key="NOPE")
