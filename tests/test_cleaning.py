import pytest
from finess.cleaning import clean_finess_data
from finess.stats_by_dept import stats_type_lit_by_dept
import polars as pl


@pytest.fixture
def mock_finess_data():
    """Fixture providing mock Finess CSV data for testing."""
    return """finess,rs,san_med,san_chir,san_obs,san_smr,san_psy,statut_jur_etat,etat,statut_jur_niv1_lib,geoloc_legal_projection,geoloc_4326_lat,geoloc_4326_long
123456789,Test Hospital 1,OUI,NON,NON,OUI,NON,O,ACTUEL,Etablissement public,L93_METROPOLE,48.85,2.35
987654321,Test Hospital 2,NON,NON,NON,NON,OUI,O,ACTUEL,Etablissement prive,L93_METROPOLE,43.30,5.40
111222333,Test Hospital 3,OUI,OUI,NON,NON,NON,O,ACTUEL,Etablissement public,L93_METROPOLE,45.76,4.84
"""


@pytest.fixture
def clean_finess_result(mock_finess_data, tmp_path):
    """Fixture that creates a temporary CSV file and calls clean_finess_data."""
    # Create temporary CSV file
    csv_file = tmp_path / "mock_finess.csv"
    csv_file.write_text(mock_finess_data)

    # Call the function with the temporary file
    return clean_finess_data(
        csv_path=str(csv_file),
        keep_only_mco_ssr_psy=True,
    )


def test_clean_finess_data(clean_finess_result):
    """Test using the fixture."""
    assert isinstance(clean_finess_result, pl.DataFrame)
    assert clean_finess_result.shape[0] > 0
    # Add more specific assertions based on your mock data


def test_stats_type_lit_by_dept(clean_finess_result):
    stats_finess_by_dept = stats_type_lit_by_dept(clean_finess_result)
    assert isinstance(stats_finess_by_dept, pl.DataFrame)
    assert stats_finess_by_dept.shape[0] > 0
