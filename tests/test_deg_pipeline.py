import pytest
import ibis
import pandas as pd
from sams.preprocessing import deg_pipeline
from hamilton import driver
from unittest.mock import MagicMock, patch
import duckdb

# Fixtures
@pytest.fixture
def sample_deg_table():
    """Minimal mock DEG dataset as ibis.Table for pipeline testing."""
    df = pd.DataFrame({
        "barcode": ["B1", "B2"],
        "aadhar_no": ["1111", "2222"],
        "academic_year": [2018, 2019],
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
    return ibis.memtable(df)


# Tests
@patch("sams.preprocessing.deg_pipeline.Path")
@patch("sams.preprocessing.deg_pipeline.ibis.duckdb.connect")
def test_sams_db_load(mock_ibis_connect, mock_path_class):
    """
    Tests that sams_db returns a connection when build=False and the DB file EXISTS.
    """
    # Arrange: Configure our mocks to simulate the desired conditions.
    
    # 1. Simulate that the database file exists.
    #    - `mock_path_class` is our mocked Path class.
    #    - `return_value` is the mock object returned when `Path()` is called.
    #    - We set its `exists()` method to return True.
    mock_path_instance = mock_path_class.return_value
    mock_path_instance.exists.return_value = True

    # Create a mock connection object that `ibis.duckdb.connect` will return.
    mock_connection_obj = MagicMock()
    mock_ibis_connect.return_value = mock_connection_obj

    # Act: Call the function we are testing.
    db_connection = deg_pipeline.sams_db(build=False)

    # Assert: Check that the function behaved as expected.

    # Verify that the code actually checked if the file exists.
    mock_path_instance.exists.assert_called_once()

    # Verify that the code tried to connect to the database.
    mock_ibis_connect.assert_called_once()

    # Verify that the function returned the connection object we expected
    assert db_connection is mock_connection_obj

# No @patch decorator is needed here!
def test_deg_raw(sample_deg_table):
    """Test that deg_raw returns an Ibis table filtered by module."""
    
    # 1. Create a REAL, but temporary, in-memory database connection
    #    This requires no files and is very fast
    con = ibis.duckdb.connect()

    # 2. Load your sample data into this connection
    #    The `deg_raw` function expects a table named "students"
    con.create_table("students", sample_deg_table.to_pandas())

    # 3. Pass the REAL connection object into the function under test.
    result_table_expr = deg_pipeline.deg_raw(con, "DEG")

    # 4. Now, all your assertions will work on real objects.
    assert isinstance(result_table_expr, ibis.Table)
    assert "module" in result_table_expr.columns

    # .execute() now works correctly because it has a real backend.
    df_out = result_table_expr.execute()

    # The final check will now operate on a real pandas DataFrame.
    assert not df_out.empty
    assert df_out["module"].unique().tolist() == ["DEG"]


def test_preprocess_deg_enrollment(sample_deg_table):
    """Test preprocessing of DEG enrollment data."""
    result = deg_pipeline.preprocess_deg_students_enrollment_data(sample_deg_table)

    assert isinstance(result, ibis.Table)
    assert "address" in result.columns

    df_out = result.execute()
    assert not df_out.empty
    assert pd.api.types.is_bool_dtype(df_out["ph"])


@patch("sams.preprocessing.deg_pipeline.datasets", {"deg_enrollments": {"path": "fake_path/deg_enrollments.pq"}})
def test_save_deg_data(sample_deg_table, monkeypatch):
    """Test that save_deg_data calls the backend's execute method without writing a file."""
    # Create a real in-memory connection and a table that is BOUND to it.
    real_con = ibis.duckdb.connect()
    bound_table = real_con.from_dataframe(sample_deg_table.to_pandas())

    # Get the actual DuckDB connection object from the Ibis backend.
    # This is the object whose `execute` method we want to mock.
    duckdb_connection_to_mock = bound_table._find_backend().con

    # Create a mock for the `execute` method.
    mock_execute = MagicMock()
    monkeypatch.setattr(duckdb_connection_to_mock, "execute", mock_execute)

    # Run the function with our bound table.
    result = deg_pipeline.save_deg_data(bound_table, dataset_key="deg_enrollments")

    # Assert that the function completed and our mock `execute` method was called.
    assert result is None
    mock_execute.assert_called_once()


def test_deg_pipeline_dag(sample_deg_table, monkeypatch):
    """Full DAG test using mocked deg_raw and save_data."""
    mock_db = ibis.duckdb.connect()
    mock_db.create_table("students", sample_deg_table.to_pandas())

    # monkeypatch.setattr(deg_pipeline, "save_deg_data", lambda df, dataset_key: None)
    # deg_pipeline.deg_raw = lambda sams_db, module: sample_deg_table

    dag_driver = (
        driver.Builder()
        .with_modules(deg_pipeline)
        .with_config({"module": "DEG"})
        .build()
    )

    results = dag_driver.execute(
        final_vars=["save_deg_enrollments"],
        inputs={"sams_db": "mock_conn"},
    )

    assert "save_deg_enrollments" in results
    result_tbl = results["save_deg_enrollments"]

    assert isinstance(result_tbl, ibis.Table)
    df_out = result_tbl.execute()
    assert not df_out.empty
    assert pd.api.types.is_bool_dtype(df_out["ph"])

    
