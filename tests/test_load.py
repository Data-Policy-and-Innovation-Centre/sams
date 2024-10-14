import pytest
from unittest.mock import Mock, patch
from sams.etl.load import SamsDataLoader, SamsDataLoaderPandas
import pandas as pd


@pytest.fixture
def mock_db_session():
    with patch("sqlalchemy.orm.sessionmaker") as MockSessionmaker:
        mock_session = Mock()
        MockSessionmaker.return_value = lambda: mock_session
        yield mock_session


@pytest.fixture
def data_loader(mock_db_session):
    return SamsDataLoader("sqlite:///:memory:")


class TestSamsDataLoader:
    def test_load_institute_data(self, data_loader):
        institute_data = [
            {
                "SAMSCode": 1,
                "academic_year": 2022,
                "module": "ITI",
                "InstituteName": "Test Institute",
                "TypeofInstitute": "Govt.",
                "strength": {},
                "cutoff": {},
            }
        ]
        data_loader.load_institute_data(institute_data)
        # Assert that the session's add and commit methods were called
        assert data_loader.Session().add.called
        assert data_loader.Session().commit.called


# Test suite for SamsDataLoaderPandas
@pytest.fixture
def mock_engine():
    with patch("sqlalchemy.create_engine") as MockEngine:
        mock_engine = Mock()
        MockEngine.return_value = mock_engine
        yield mock_engine


@pytest.fixture
def data_loader_pandas(mock_engine):
    return SamsDataLoaderPandas("sqlite:///:memory:")


class TestSamsDataLoaderPandas:
    def test_load_data(self, data_loader_pandas):
        df = pd.DataFrame({"id": [1, 2], "name": ["John", "Jane"]})
        data_loader_pandas.load_data(df, "test_table")
        # Assert that to_sql was called on the engine
        assert data_loader_pandas.engine.connect().execute.called


if __name__ == "__main__":
    pytest.main()
