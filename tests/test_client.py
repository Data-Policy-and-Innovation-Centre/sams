import pytest
import json
from unittest.mock import patch, MagicMock
from sams.api.client import SAMSClient
from sams.api.exceptions import APIError
import requests


@pytest.fixture
def mock_client():
    return SAMSClient()


@pytest.fixture
def mock_response():
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "StatusCode": 200,
        "TotalRecordCount": 100,
        "RecordCount": 10,
        "Data": [{"id": i} for i in range(10)],
    }
    return mock_resp


@patch("requests.get")
def test_get_student_data(mock_get, mock_client, mock_response):
    mock_get.return_value = mock_response
    result = mock_client.get_student_data("ITI", 2022, 1)
    assert len(result) == 10
    mock_get.assert_called_once()


@patch("requests.get")
def test_get_student_data_count(mock_get, mock_client, mock_response):
    mock_get.return_value = mock_response
    result = mock_client.get_student_data("ITI", 2022, 1, count=True)
    assert result == 100
    mock_get.assert_called_once()


@pytest.mark.parametrize("module", ["ITI", "Diploma", "PDIS"])
def test_get_student_data_valid_inputs(module, mock_client, mock_response):
    with patch("requests.get", return_value=mock_response):
        result = mock_client.get_student_data(module, 2022)
        assert len(result) == 10


@pytest.mark.parametrize("module", [("ITI", 2), ("Diploma", 2), ("InvalidModule", 1)])
def test_get_student_data_invalid_inputs(module, mock_client):
    with pytest.raises(ValueError):
        mock_client.get_student_data(module, 2022)


@patch("requests.get")
def test_get_institute_data(mock_get, mock_client, mock_response):
    mock_get.return_value = mock_response
    result = mock_client.get_institute_data("PDIS", 2022)
    assert len(result) == 10
    mock_get.assert_called_once()


@patch("requests.get")
def test_get_institute_data_count(mock_get, mock_client, mock_response):
    mock_get.return_value = mock_response
    result = mock_client.get_institute_data("PDIS", 2022, count=True)
    assert result == 100
    mock_get.assert_called_once()


@pytest.mark.parametrize(
    "module,admission_type",
    [("PDIS", None), ("ITI", None), ("Diploma", 1), ("Diploma", 2)],
)
def test_get_institute_data_valid_inputs(
    module, admission_type, mock_client, mock_response
):
    with patch("requests.get", return_value=mock_response):
        result = mock_client.get_institute_data(module, 2022, admission_type)
        assert len(result) == 10


@pytest.mark.parametrize(
    "module,admission_type", [("InvalidModule", None), ("Diploma", 3)]
)
def test_get_institute_data_invalid_inputs(module, admission_type, mock_client):
    with pytest.raises(ValueError):
        mock_client.get_institute_data(module, 2022, admission_type)


@patch("requests.get")
def test_handle_response_api_error(mock_get, mock_client):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"success": False, "message": "API Error"}
    mock_get.return_value = mock_response

    with pytest.raises(APIError, match="API Error"):
        mock_client.get_student_data("ITI", 2022, 1)


@patch("requests.get")
def test_handle_response_missing_fields(mock_get, mock_client):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"StatusCode": 200, "Data": []}
    mock_get.return_value = mock_response

    with pytest.raises(APIError, match="API returned invalid response: Fields"):
        mock_client.get_student_data("ITI", 2022, 1, 1)


@patch("requests.get")
def test_handle_response_mismatch_record_count(mock_get, mock_client):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "StatusCode": 200,
        "TotalRecordCount": 100,
        "RecordCount": 10,
        "Data": [{"id": i} for i in range(5)],
    }
    mock_get.return_value = mock_response

    with pytest.raises(
        APIError, match="API returned invalid response: Expected 10 records, but got 5"
    ):
        mock_client.get_student_data("ITI", 2022, 1, 1)


@patch("requests.get")
def test_handle_response_http_errors(mock_get, mock_client):
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_get.return_value = mock_response

    with pytest.raises(APIError, match="Bad Request: Some inputs are missing."):
        mock_client.get_student_data("ITI", 2022, 1, 1)

    mock_response.status_code = 500
    with pytest.raises(APIError, match="Server Error: Something went wrong."):
        mock_client.get_student_data("ITI", 2022, 1, 1)


@patch("requests.get")
def test_refresh_on_timeout(mock_get, mock_client, mock_response):
    mock_get.side_effect = [
        requests.ConnectTimeout("Connection timed out"),
        mock_response,
    ]

    with patch.object(mock_client, "refresh") as mock_refresh:
        result = mock_client.get_student_data("ITI", 2022, 1)
        assert len(result) == 10
        mock_refresh.assert_called_once()
        assert mock_get.call_count == 2
