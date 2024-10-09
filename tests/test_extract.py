import pytest
from unittest.mock import patch
import pandas as pd
import polars as pl

from sams.etl.extract import SamsDataDownloader

# Test suite for SamsDataDownloader# Test suite for SamsDataDownloader
@pytest.fixture
def mock_sams_client():
    with patch('sams.api.client.SAMSClient') as MockSAMSClient:
        mock_client = MockSAMSClient.return_value
        yield mock_client

@pytest.fixture
def data_downloader(mock_sams_client):
    return SamsDataDownloader(mock_sams_client)

class TestSamsDataDownloader: 
    @pytest.mark.parametrize("year, module, expected_data", [
        (2022, 'ITI', [{'id': 1, 'name': 'John Doe', 'module': 'ITI', 'year': 2022}]),
        (2023, 'Diploma', [{'id': 2, 'name': 'Jane Smith', 'module': 'Diploma', 'year': 2023}]),
        (2021, 'PDIS', [{'id': 3, 'name': 'Bob Johnson', 'module': 'PDIS', 'year': 2021}]),
    ])
    def test_fetch_students(self, data_downloader, mock_sams_client, year, module, expected_data):
        mock_sams_client.get_student_data.side_effect = [1,expected_data,[]]
        result = data_downloader.fetch_students(module, year)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(expected_data)
        for i, row in result.iterrows():
            assert row['name'] == expected_data[i]['name']
            assert row['module'] == expected_data[i]['module']
            assert row['academic_year'] == expected_data[i]['year']
            if 'source_of_fund' in expected_data[i]:
                assert row['source_of_fund'] == expected_data[i]['source_of_fund']


    @pytest.mark.parametrize("module, year, admission_type, expected_data", [
        ('ITI', 2022, None, [{'id': 1, 'name': 'ITI Institute', 'module': 'ITI', 'year': 2022}]),
        ('Diploma', 2023, 1, [{'id': 2, 'name': 'Diploma College', 'module': 'Diploma', 'year': 2023, 'admission_type': 1}]),
        ('PDIS', 2021, None, [{'id': 3, 'name': 'PDIS Center', 'module': 'PDIS', 'year': 2021}]),
    ])
    def test_fetch_institutes(self, data_downloader, mock_sams_client, module, year, admission_type, expected_data):
        mock_sams_client.get_institute_data.side_effect = [1,expected_data]
        result = data_downloader.fetch_institutes(module, year, admission_type)
        assert isinstance(result, list)
        assert len(result) == len(expected_data)
        for i, item in enumerate(result):
            assert item['name'] == expected_data[i]['name']
            assert item['module'] == expected_data[i]['module']
            assert item['academic_year'] == expected_data[i]['year']
            if 'admission_type' in expected_data[i]:
                assert item['admission_type'] == expected_data[i]['admission_type']


    