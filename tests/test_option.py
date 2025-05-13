import pandas as pd
import pytest
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))) ## Add project root to sys.path to enable module imports during testing
from notebooks.option_pipeline import (
    total_applications_by_year,
    average_applications_per_student,
    demand_by_year
)

@pytest.fixture
def mock_option_data():
    # Sample flattened option_data
    return pd.DataFrame([
        {"barcode": "B1", "aadhar_no": "A1", "year": 2021, "Trade": "Electrician", "module": "ITI"},
        {"barcode": "B1", "aadhar_no": "A1", "year": 2021, "Trade": "Fitter", "module": "ITI"},
        {"barcode": "B2", "aadhar_no": "A2", "year": 2021, "Trade": "Electrician", "module": "Diploma"},
        {"barcode": "B3", "aadhar_no": "A3", "year": 2022, "Trade": "Machinist", "module": "ITI"},
        {"barcode": "B4", "aadhar_no": "A4", "year": 2022, "Trade": "Machinist", "module": "PDIS"},
    ])

def test_total_applications_by_year(mock_option_data):
    result = total_applications_by_year(mock_option_data)
    expected = pd.DataFrame({
        "year": [2021, 2022],
        "total_applications_by_year": [3, 2]
    })
    pd.testing.assert_frame_equal(result.sort_values("year").reset_index(drop=True), expected)

def test_average_applications_per_student(mock_option_data):
    result = average_applications_per_student(mock_option_data)
    expected = pd.DataFrame({
        "year": [2021, 2022],
        "average_applications_per_student": [1.5, 1.0]
    })
    pd.testing.assert_frame_equal(result.sort_values("year").reset_index(drop=True), expected)

def test_demand_by_year(mock_option_data):
    result = demand_by_year(mock_option_data)
    expected_columns = {
        'year', 'total_applications_by_year', 'distinct_trades_by_year',
        'demand_trade', 'demand_trade_count', 'demand_trade_module'
    }
    assert set(result.columns) == expected_columns
    assert len(result) == 2  # Two years: 2021, 2022
    assert result["year"].isin([2021, 2022]).all()
