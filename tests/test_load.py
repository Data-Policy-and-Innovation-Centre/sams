import pytest
import json
import os
from unittest.mock import Mock, patch, mock_open
from sams.etl import load
from sams.etl.load import SamsDataLoader, SamsDataLoaderPandas
import pandas as pd

# Fixture for mock db session
@pytest.fixture
def mock_db_session():
    mock_session = Mock()
    return mock_session

# Fixture for SamsDataLoader with patched sessionmaker
@pytest.fixture
def data_loader(mock_db_session):
    with patch("sams.etl.load.sessionmaker", return_value=lambda: mock_db_session):
        loader = SamsDataLoader("sqlite:///:memory:")
        return loader

# Tests for SamsDataLoader
class TestSamsDataLoader:
    def test_load_institute_data(self, data_loader, mock_db_session):
        institute_data = [
            {
                "SAMSCode": "1234",
                "academic_year": 2022,
                "module": "ITI",
                "InstituteName": "Test Institute",
                "TypeofInstitute": "Govt.",
                "branch": "",
                "trade": "",
                "admission_type": None,
                "strength": {},
                "cutoff": {},
                "enrollment": {},
            }
        ]

        data_loader.load(institute_data, "institutes")
        # Assert that the session's add and commit methods were called
        assert mock_db_session.add.called
        assert mock_db_session.commit.called

    def test_load_hss_student(self, data_loader, mock_db_session):
        hss_data = [
            {
                "barcode": "HSS123",
                "academicYear": 2024,
                "module": "HSS",
                "rollNo": "123456789",
                "percentage": "83.33",
                "student_name": "Taylor",  
                "hssOptionDetails": {}
            }
        ]

        data_loader.load(hss_data, "students")
        assert mock_db_session.add.called
        assert mock_db_session.commit.called

# Test suite for SamsDataLoaderPandas
@pytest.fixture
def mock_engine():
    with patch("sqlalchemy.create_engine") as MockEngine:
        mock_engine = Mock()
        MockEngine.return_value = mock_engine
        yield mock_engine


@pytest.fixture
def data_loader_pandas():
    return SamsDataLoaderPandas("sqlite:///:memory:")

def test_load_checkpoint_file_exists():
    mock_data = {"2019": 3}
    mock_json = json.dumps(mock_data)

    with patch("os.path.exists", return_value=True), \
         patch("builtins.open", mock_open(read_data=mock_json)) as mock_file:

        result = load.load_checkpoint()

        assert result == mock_data
        mock_file.assert_called_once_with(load.CHECKPOINT_FILE, "r")


def test_load_checkpoint_file_missing():
    with patch("os.path.exists", return_value=False):
        result = load.load_checkpoint()
        assert result == {}

    
# Tests for SamsDataLoaderPandas
class TestSamsDataLoaderPandas:
    def test_load_data(self, data_loader_pandas):
        df = pd.DataFrame({"id": [1, 2], "name": ["John", "Jane"]})
        data_loader_pandas.load_data(df, "test_table")

if __name__ == "__main__":
    pytest.main()


