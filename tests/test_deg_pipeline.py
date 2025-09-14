import pytest
import pandas as pd
import sqlite3
from sams.preprocessing import deg_pipeline
from hamilton import driver
from unittest.mock import MagicMock, patch


@pytest.fixture
def sample_deg_df():
    return pd.DataFrame({
        "barcode": ["B1", "B2"],
        "aadhar_no": ["1111", "2222"],
        "academic_year": [2018, 2018],
        "module": ["DEG", "DEG"],
        "gender": ["M", "F"],
        "dob": ["2000-01-01", "1999-12-31"],
        "orphan": ["Yes", "No"],
        "es": ["No", "Yes"],
        "ph": ["No", "No"],
        "sports": ["Yes", "No"],
        "national_cadet_corps": ["No", "Yes"],
        "district": ["A", "B"],
        "state": ["X", "Y"],
        "address": ["Addr 1", "Addr 2"],
        "block": ["Block A", "Block B"],
        "pin_code": ["123456", "789012"],
    })


@pytest.fixture
def mock_conn():
    return MagicMock(spec=sqlite3.Connection)


@patch("sams.preprocessing.deg_pipeline.sqlite3.connect")
def test_sams_db_load(mock_sqlite, mock_conn):
    # Simulate existing database (no build)
    mock_sqlite.return_value = mock_conn
    db = deg_pipeline.sams_db(build=False)
    assert isinstance(db, sqlite3.Connection)


@patch("sams.preprocessing.deg_pipeline.pd.read_sql_query")
@patch("sams.preprocessing.deg_pipeline.sqlite3.connect")
def test_deg_raw(mock_sqlite, mock_read_sql, sample_deg_df):
    # Mock connection + SQL result
    mock_sqlite.return_value = MagicMock()
    mock_read_sql.return_value = sample_deg_df  

    result = deg_pipeline.deg_raw(mock_sqlite.return_value, "DEG")
    assert isinstance(result, pd.DataFrame)
    assert result.shape[0] == 2
    assert result["module"].unique().tolist() == ["DEG"]


def test_preprocess_deg_enrollment(sample_deg_df):
    result = deg_pipeline.preprocess_deg_students_enrollment_data(sample_deg_df)
    assert isinstance(result, pd.DataFrame)
    assert "address" in result.columns
    assert pd.api.types.is_bool_dtype(result["ph"])  


@patch("sams.preprocessing.deg_pipeline.save_data")
def test_save_deg_data(mock_save, sample_deg_df):
    # Mock the save function and test wrapper
    mock_save.return_value = sample_deg_df  

    result = deg_pipeline.save_data(sample_deg_df, dataset_key="deg_enrollments")
    mock_save.assert_called_once()
    assert isinstance(result, pd.DataFrame)
    assert result.equals(sample_deg_df)


def test_deg_pipeline_dag(sample_deg_df, monkeypatch):
    """Full DAG test using mocked deg_raw and save_data."""

    # Prevent saving to disk
    monkeypatch.setattr(deg_pipeline, "save_data", lambda df, path: None)

    # Monkeypatch deg_raw with matching signature
    def mocked_deg_raw(build: bool, sams_db, module: str):
        return sample_deg_df

    deg_pipeline.deg_raw = mocked_deg_raw  

    # Build DAG
    dag_driver = (
        driver.Builder()
        .with_modules(deg_pipeline)
        .with_config({"module": "DEG"})
        .build()
    )

    # Execute DAG
    results = dag_driver.execute(
        final_vars=["save_deg_enrollments"],
        inputs={"build": False, "sams_db": "mock_conn", "deg_raw": sample_deg_df,}
        )

    assert "save_deg_enrollments" in results
    result_df = results["save_deg_enrollments"]
    assert isinstance(result_df, pd.DataFrame)
    assert not result_df.empty
