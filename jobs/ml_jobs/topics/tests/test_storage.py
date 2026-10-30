import pandas as pd
import pytest

from src.storage import read_table, write_table


@pytest.mark.parametrize("name", ["sample", "sample.parquet", "sample.csv"])
def test_write_table_writes_parquet_and_csv(tmp_path, name):
    df = pd.DataFrame({"item_id": ["001"], "offer_description": ['a "b",\nc']})
    parquet_path = write_table(df, tmp_path / "out" / name)
    assert parquet_path == tmp_path / "out" / "sample.parquet"
    assert (tmp_path / "out" / "sample.csv").exists()
    # leading zeros and embedded quotes/newlines survive the round trip
    pd.testing.assert_frame_equal(read_table(tmp_path / "out" / "sample"), df)


def test_read_table_falls_back_to_csv(tmp_path):
    pd.DataFrame({"item_id": ["001"]}).to_csv(tmp_path / "s.csv", index=False)
    assert read_table(tmp_path / "s")["item_id"].tolist() == ["001"]
