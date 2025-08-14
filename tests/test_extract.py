import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import os
from sams.etl.extract import SamsDataDownloader
from sams.api.exceptions import APIError
import sams.etl.extract as extr
from pydantic import BaseModel
import importlib

# Test suite for SamsDataDownloader# Test suite for SamsDataDownloader
@pytest.fixture
def mock_sams_client():
    with patch("sams.api.client.SAMSClient") as MockSAMSClient:
        mock_client = MockSAMSClient.return_value
        yield mock_client


@pytest.fixture
def data_downloader(mock_sams_client):
    return SamsDataDownloader(mock_sams_client)


@pytest.fixture
def mock_api_client():
    mock_client = MagicMock()
    mock_client.get_student_data.return_value = 100
    mock_client.get_institute_data.return_value = 50
    return mock_client

@pytest.mark.parametrize(
    "year, module, expected_data",
    [
        (2022, "ITI", [{"id": 1, "name": "John Doe", "module": "ITI", "year": 2022}]),
        (
            2023,
            "Diploma",
            [{"id": 2, "name": "Jane Smith", "module": "Diploma", "year": 2023}],
        ),
        (
            2021,
            "PDIS",
            [{"id": 3, "name": "Bob Johnson", "module": "PDIS", "year": 2021}],
        ),
        (
            2024,
            "HSS",
            [{"id": 4, "name": "Taylor", "module": "HSS", "year": 2024}],
        ),
        (
            2018,
            "DEG",
            [{"id": 4, "name": "Sam", "module": "DEG", "year": 2018}]
        )
    ],
)
def test_fetch_students(data_downloader, mock_sams_client, year, module, expected_data):
    mock_sams_client.get_student_data.side_effect = [1, expected_data, []]
    result = data_downloader.fetch_students(module, year)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == len(expected_data)
    for i, row in result.iterrows():
        assert row["name"] == expected_data[i]["name"]
        assert row["module"] == expected_data[i]["module"]
        assert row["academic_year"] == expected_data[i]["year"]

        if "source_of_fund" in expected_data[i]:
            assert row["source_of_fund"] == expected_data[i]["source_of_fund"]


@pytest.mark.parametrize(
    "module, year, admission_type, expected_data",
    [
        (
            "ITI",
            2022,
            None,
            [{"id": 1, "name": "ITI Institute", "module": "ITI", "year": 2022}],
        ),
        (
            "Diploma",
            2023,
            1,
            [
                {
                    "id": 2,
                    "name": "Diploma College",
                    "module": "Diploma",
                    "year": 2023,
                    "admission_type": 1,
                }
            ],
        ),
        (
            "PDIS",
            2021,
            None,
            [{"id": 3, "name": "PDIS Center", "module": "PDIS", "year": 2021}],
        ),
    ],
)
def test_fetch_institutes(
    data_downloader, mock_sams_client, module, year, admission_type, expected_data
):
    mock_sams_client.get_institute_data.side_effect = [1, expected_data]
    result = data_downloader.fetch_institutes(module, year, admission_type)
    assert isinstance(result, list)
    assert len(result) == len(expected_data)
    for i, item in enumerate(result):
        assert item["name"] == expected_data[i]["name"]
        assert item["module"] == expected_data[i]["module"]
        assert item["academic_year"] == expected_data[i]["year"]
        if "admission_type" in expected_data[i]:
            assert item["admission_type"] == expected_data[i]["admission_type"]


def test_update_total_records(mock_api_client, tmp_path, monkeypatch):
    # Mock the LOGS directory
    monkeypatch.setattr("sams.config.LOGS", str(tmp_path))

    # Mock the STUDENT and INSTITUTE dictionaries
    mock_student = {"ITI": {"yearmin": 2020, "yearmax": 2021}}
    mock_institute = {"Diploma": {"yearmin": 2020, "yearmax": 2021}}
    monkeypatch.setattr("sams.config.STUDENT", mock_student)
    monkeypatch.setattr("sams.config.INSTITUTE", mock_institute)

    # Call the method
    importlib.reload(extr)
    downloader = extr.SamsDataDownloader(mock_api_client)
    downloader.update_total_records()

    # Check if the CSV files were created
    assert os.path.exists(os.path.join(tmp_path, "students_count.csv"))
    assert os.path.exists(os.path.join(tmp_path, "institutes_count.csv"))

    # Read the CSV files
    students_df = pd.read_csv(os.path.join(tmp_path, "students_count.csv"))
    institutes_df = pd.read_csv(os.path.join(tmp_path, "institutes_count.csv"))

    # Check the content of the students CSV
    assert len(students_df) == 2  # 2 years for ITI
    assert all(students_df["count"] == 100)
    assert all(students_df["module"] == "ITI")
    assert set(students_df["academic_year"]) == {2020, 2021}

    # Check the content of the institutes CSV
    assert len(institutes_df) == 4  # 2 years * 2 admission types for Diploma
    assert all(institutes_df["count"] == 50)
    assert all(institutes_df["module"] == "Diploma")
    assert set(institutes_df["academic_year"]) == {2020, 2021}
    assert set(institutes_df["admission_type"]) == {1, 2}

    # Check if the log file was created
    assert os.path.exists(os.path.join(tmp_path, "total_records.log"))

def test_fetch_students_with_pydantic_model(data_downloader, mock_sams_client):
    """Test that fetch_students correctly handles Pydantic model instances returned by the API client."""

    year = extr.STUDENT["ITI"]["yearmax"] 
    
    # Define a simple mock Pydantic model that mimics the API's behavior
    class MockStudentModel(BaseModel):
        id: int
        name: str
        module: str
        year: int

    # Prepare a fake Pydantic model instance
    pydantic_record = MockStudentModel(id=1, name="Alice", module="ITI", year=year)

    # Count first, then one page (we pass page_number=1 so no pagination loop)
    mock_sams_client.get_student_data.side_effect = [1, [pydantic_record]]

    # Call fetch_students, which should detect the model and call model_dump()
    df = data_downloader.fetch_students("ITI", year, pandify=True, page_number=1)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.loc[0, "name"] == "Alice"
    assert df.loc[0, "module"] == "ITI"
    assert df.loc[0, "academic_year"] == year


@pytest.mark.parametrize(
    "table_name,module",
    [
        ("students", "ITI"),
        ("institutes", "Diploma"),
    ],
)
def test_update_total_records_api_error(table_name, module, monkeypatch):

    importlib.reload(extr)

    # Mock the logger
    mock_logger = MagicMock()
    monkeypatch.setattr(extr, "logger", mock_logger, raising=False)

    # Mock the API client to raise an APIError
    mock_client = MagicMock()
    if table_name == "students":
        mock_client.get_student_data.side_effect = APIError("API Error")
    else:
        mock_client.get_institute_data.side_effect = APIError("API Error")

    downloader = extr.SamsDataDownloader(mock_client)

    # Call the method
    downloader._update_total_records(pd.DataFrame(), {module: {"yearmin": 2020, "yearmax": 2020}}, table_name)

    # Check if the error was logged
    mock_logger.error.assert_called_with(f"Data download failed for {module} 2020 after 3 retries. Skipping...")

