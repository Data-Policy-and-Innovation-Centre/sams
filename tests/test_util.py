import pytest
from datetime import datetime
import os
import pandas as pd
import sqlite3
from unittest.mock import patch, mock_open
from sams.util import (
    is_valid_date,
    get_existing_modules,
    camel_to_snake_case,
    dict_camel_to_snake_case,
    correct_spelling,
    find_null_column,
    hours_since_creation
)

def test_is_valid_date():
    assert is_valid_date("2024-08-26")[0]
    assert is_valid_date("26-08-2024")[0]
    assert is_valid_date("08/26/2024")[0]
    assert is_valid_date("26 Aug 2024")[0]
    assert is_valid_date("August 26, 2024")[0]
    assert is_valid_date("2024-08-26 15:30:00")[0]
    assert not is_valid_date("invalid date")[0]

@pytest.mark.parametrize("raw_data_dir,logs_dir", [('/mock/raw/data/dir', '/mock/logs')])
@patch('os.path.exists')
@patch('pandas.read_csv')
@patch('sqlite3.connect')
def test_get_existing_modules(mock_sqlite3, mock_read_csv, mock_exists, raw_data_dir, logs_dir):
    with patch('sams.config.RAW_DATA_DIR', raw_data_dir), patch('sams.config.LOGS', logs_dir):
        mock_exists.return_value = True
        mock_read_csv.return_value = pd.DataFrame({
            'module': ['MOD1', 'MOD2'],
            'academic_year': [2024, 2024],
            'count': [10, 20]
        })
        mock_cursor = mock_sqlite3.return_value.__enter__.return_value.cursor.return_value
        mock_cursor.fetchall.return_value = [('MOD1', 2024, 10), ('MOD2', 2024, 21)]

        result = get_existing_modules()
        assert result == [('MOD1', 2024), ('MOD2', 2024)]

@pytest.mark.parametrize("input_str,expected", [
    ("camelCase", "camel_case"),
    ("FOOBar", "foo_bar"),
    ("ThisIsATest", "this_is_a_test"),
    ("ABC", "abc"),
    ("alreadySnakeCase", "already_snake_case")
])
def test_camel_to_snake_case(input_str, expected):
    assert camel_to_snake_case(input_str) == expected

def test_dict_camel_to_snake_case():
    test_dict = {"camelCase": 1, "PascalCase": 2, "snake_case": 3}
    expected = {"camel_case": 1, "pascal_case": 2, "snake_case": 3}
    assert dict_camel_to_snake_case(test_dict) == expected

@pytest.mark.parametrize("input_str,expected", [
    ("Tength", "Tenth"),
    ("tength", "tenth"),
    ("OR", "Or"),
    ("normalText", "normalText")
])
def test_correct_spelling(input_str, expected):
    assert correct_spelling(input_str) == expected

@pytest.mark.parametrize("input_str,expected", [
    ("students.column_name", "column_name"),
    ("institutes.column_name", "column_name"),
    ("invalid_text", None)
])
def test_find_null_column(input_str, expected):
    assert find_null_column(input_str) == expected

@pytest.mark.parametrize("file_exists,mtime,current_time,expected", [
    (True, 1000, 3600 + 1000, 1.0),  # File exists, created 1 hour ago
    (False, 0, 0, float('inf'))  # File doesn't exist
])
def test_hours_since_creation(file_exists, mtime, current_time, expected):
    with patch('os.path.exists', return_value=file_exists), \
         patch('os.path.getmtime', return_value=mtime), \
         patch('time.time', return_value=current_time):
        assert hours_since_creation("dummy_path") == expected

if __name__ == '__main__':
    pytest.main()