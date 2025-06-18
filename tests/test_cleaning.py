from finess import cleaning
import polars as pl


def test_clean_finess_data():
    res = cleaning.clean_finess_data(
        csv_path="data/t-finess.csv",
        keep_only_mco_ssr_psy=True,
    )
    assert res.shape[0] == 3731
    assert isinstance(res, pl.DataFrame)
