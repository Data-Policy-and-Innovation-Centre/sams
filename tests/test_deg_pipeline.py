import os
import json
from datetime import date
from unittest.mock import patch, MagicMock, call
import pytest
import pandas as pd
import ibis
import ibis.expr.types as ir

from sams.preprocessing import deg_pipeline

# Tests for core logic 
def test_sams_db_uses_existing_db(monkeypatch):
    """Verify sams_db connects correctly when an existing DB is found."""
    # Mock DB existence and ibis connection
    monkeypatch.setattr("pathlib.Path.exists", lambda self: True)
    mock_connect = MagicMock(return_value="fake_db_connection")
    monkeypatch.setattr(ibis.duckdb, "connect", mock_connect)

    # Should return mocked DB connection
    assert deg_pipeline.sams_db(build=False) == "fake_db_connection"

@pytest.fixture
def enroll_data_fixture():
    """Minimal data ONLY for testing enrollment cleaning.
    This data is pre-filtered, just as it would be in the real pipeline.
    """
    df = pd.DataFrame({
        "module": ["DEG", "DEG"],
        "academic_year": [2022, 2021],
        "dob": ["2000-01-01", "na"],
        "orphan": ["Yes", " "],
        "percentage": ["95.5", "80"],
        "nationality": ["Indian", "Other"],
        "empty_col": [None, None], 
    })
    df['empty_col'] = df['empty_col'].astype('float64')
    return ibis.memtable(df)


def test_preprocess_deg_enrollment(enroll_data_fixture):
    """Test cleaning, sorting, and column dropping for enrollments."""
    # The function first filters for 'DEG', leaving 2 rows, then processes.
    result_df = deg_pipeline.preprocess_deg_enrollment(enroll_data_fixture).execute()
    
    # Check cleaned and sorted output
    assert len(result_df) == 2
    assert result_df["academic_year"].tolist() == [2021, 2022] # Check sorting
    assert result_df["orphan"].tolist() == [None, True] # Check cleaning
    assert "nationality" not in result_df.columns # Check column dropping
    assert "empty_col" not in result_df.columns

@pytest.fixture
def apps_data_fixture():
    """Minimal data ONLY for testing application unnesting."""
    apps_json = json.dumps([{"OptionNo": "1", "Stream": "ARTS"}, {"OptionNo": "2", "Stream": "SCIENCE"}])
    df = pd.DataFrame({
        "module": ["DEG", "DEG"],
        "academic_year": [2022, 2022], "aadhar_no": ["1111", "2222"], "barcode": ["B1", "B2"],
        "deg_option_details": [apps_json, '[]']
    })
    return ibis.memtable(df)

def test_preprocess_deg_applications(apps_data_fixture):
    """Test JSON unnesting for applications."""
    result_df = deg_pipeline.preprocess_deg_applications(apps_data_fixture).execute()
    # Only one student has valid options (2 expected rows)
    assert len(result_df) == 2
    assert result_df["aadhar_no"].unique().tolist() == ["1111"]
    assert result_df["num_applications"].tolist() == [2, 2]

@pytest.fixture
def marks_data_fixture():
    """Minimal DEG marks data with compartment info."""
    comps_json = json.dumps([
        {"COMPSubject": "Physics"},
        {"COMPSubject": "Chemistry"}
    ])
    df = pd.DataFrame({
        "module": ["DEG", "DEG"],
        "academic_year": [2022, 2022],
        "aadhar_no": ["1111", "2222"],
        "barcode": ["B1", "B2"],
        "deg_compartments": [comps_json, '[]'],
        "highest_qualification": ["+2 Sci", "+2 Arts"],
        "total_marks": [500, 500],
        "secured_marks": [450, 400],
        "board_exam_name_for_highest_qualification": ["CHSE", "CHSE"],
        "examination_board_of_the_highest_qualification": ["CHSE", "CHSE"],
        "examination_type": ["Annual", "Annual"],
        "year_of_passing": [2022, 2022],
        "percentage": [90.0, 80.0],
        "compartmental_status": [True, False],
    })
    return ibis.memtable(df)

def test_preprocess_deg_marks(marks_data_fixture):
    """Check compartment unnesting and left join correctness."""
    result_df = deg_pipeline.preprocess_deg_marks(marks_data_fixture).execute()

    # Expect 3 rows: 2 for student 1111 + 1 for 2222
    assert len(result_df) == 3
    assert sorted(result_df["aadhar_no"].unique()) == ["1111", "2222"]

    # Ensure student without compartments retains data
    student_2_row = result_df[result_df["aadhar_no"] == "2222"].iloc[0]
    assert pd.isna(student_2_row["comp_subject"])
    assert student_2_row["highest_qualification"] == "+2 Arts"

# Tests for side effects 
def test_save_deg_data(monkeypatch, tmp_path):
    """Test save_deg_data handles file creation and SQL execution."""
    dummy_table = ibis.memtable(pd.DataFrame({"a": [1]}))
    fake_output_path = tmp_path / "data.pq"

    # Mock external dependencies and connection
    monkeypatch.setattr(deg_pipeline, "datasets", {"deg_data": {"path": str(fake_output_path)}})
    monkeypatch.setattr(os, "makedirs", MagicMock())
    mock_con_execute = MagicMock()
    mock_backend = MagicMock(con=MagicMock(execute=mock_con_execute))
    monkeypatch.setattr(ir.Table, "_find_backend", lambda self: mock_backend)
    
    # Execute function
    deg_pipeline.save_deg_data(df=dummy_table, dataset_key="deg_data")    
    
    # Validate two SQL commands executed (PRAGMA, COPY)
    assert mock_con_execute.call_count == 2

    # Verify COPY SQL call
    copy_sql_call = call(f"""
        COPY ({dummy_table.compile()})
        TO '{fake_output_path}'
        (FORMAT PARQUET, ROW_GROUP_SIZE 500000, COMPRESSION 'zstd');
        """)
    mock_con_execute.assert_has_calls([copy_sql_call])