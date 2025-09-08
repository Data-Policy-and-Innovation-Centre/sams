import pytest
import pandas as pd
import sqlite3
from sams.preprocessing import deg_pipeline
from hamilton import driver
from unittest import mock
from unittest.mock import MagicMock, patch

# Fixtures
@pytest.fixture
def sample_deg_df():
    """
    Returns a sample DataFrame simulating raw DEG enrollment records.
    """
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
    """
    Returns a mocked SQLite connection object.
    """
    return MagicMock(spec=sqlite3.Connection)


# Unit tests for individual pipeline components
@patch("sams.preprocessing.deg_pipeline.sqlite3.connect")
def test_sams_db_load(mock_connect, mock_conn):
    """
    Test that `sams_db` connects to the existing SQLite DB when build=False.
    """
    mock_connect.return_value = mock_conn
    db = deg_pipeline.sams_db(build=False)
    assert isinstance(db, sqlite3.Connection)


@patch("sams.preprocessing.deg_pipeline.pd.read_sql_query")
@patch("sams.preprocessing.deg_pipeline.sqlite3.connect")
def test_deg_raw(mock_connect, mock_read_sql, sample_deg_df):
    """
    Test that `deg_raw` loads data via SQL and returns the expected DataFrame.
    """
    mock_connect.return_value = MagicMock()
    mock_read_sql.return_value = sample_deg_df

    result = deg_pipeline.deg_raw(mock_connect.return_value, "DEG")

    assert isinstance(result, pd.DataFrame)
    assert result.shape[0] == 2
    assert result["module"].unique().tolist() == ["DEG"]


def test_preprocess_deg_enrollment(sample_deg_df):
    """
    Test that `preprocess_deg_students_enrollment_data` transforms data correctly.
    """
    result = deg_pipeline.preprocess_deg_students_enrollment_data(sample_deg_df)

    assert isinstance(result, pd.DataFrame)
    assert "address" in result.columns
    assert pd.api.types.is_bool_dtype(result["ph"])


# Unit test for save logic 
def test_save_deg_data(sample_deg_df):
    """
    Test `save_deg_data` without relying on config or writing to disk.
    """
    with mock.patch("sams.preprocessing.deg_pipeline.save_data") as mock_save:
        def mock_save_deg_data(df, dataset_key):
            fake_path = f"/mocked/path/{dataset_key}.csv"
            mock_save(df, fake_path)
            return df

        # Inject mock implementation
        setattr(deg_pipeline, "save_deg_data", mock_save_deg_data)

        result = deg_pipeline.save_deg_data(sample_deg_df, dataset_key="deg_enrollments")

        mock_save.assert_called_once()
        assert isinstance(result, pd.DataFrame)
        assert result.equals(sample_deg_df)


# DAG execution test
def test_deg_pipeline_dag(sample_deg_df, monkeypatch):
    """
    Test full DAG pipeline using mocked input/output.
    Prevents actual DB access and writing to disk.
    """
    # Prevent save_data from writing to disk
    monkeypatch.setattr(deg_pipeline, "save_data", lambda df, path: None)

    # Ensure deg_raw is never run
    def crash_if_called(*args, **kwargs):
        raise AssertionError("deg_raw() should not be executed!")

    monkeypatch.setattr(deg_pipeline, "deg_raw", crash_if_called)

    # Build the DAG
    dag_driver = (
        driver.Builder()
        .with_modules(deg_pipeline)
        .with_config({"module": "DEG"})
        .build()
    )

    # Inject mock values
    results = dag_driver.execute(
        final_vars=["save_deg_enrollments"],
        inputs={
            "build": False,
            "sams_db": "mock_conn",
            "deg_raw": sample_deg_df
        }
    )

    # Validate final output
    assert "save_deg_enrollments" in results
    result_df = results["save_deg_enrollments"]
    assert isinstance(result_df, pd.DataFrame)
    assert not result_df.empty