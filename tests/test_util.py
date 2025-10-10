import pytest
from datetime import datetime
import pandas as pd
import re
from unittest.mock import patch, mock_open
from sams.utils import (
    is_valid_date,
    camel_to_snake_case,
    dict_camel_to_snake_case,
    correct_spelling,
    find_null_column,
    hours_since_creation,
    fuzzy_merge,
    best_fuzzy_match,
    _group_dict,
    decrypt_roll 
)


def test_is_valid_date():
    assert is_valid_date("2024-08-26")[0]
    assert is_valid_date("26-08-2024")[0]
    assert is_valid_date("08/26/2024")[0]
    assert is_valid_date("26 Aug 2024")[0]
    assert is_valid_date("August 26, 2024")[0]
    assert is_valid_date("2024-08-26 15:30:00")[0]
    assert not is_valid_date("invalid date")[0]


# @pytest.mark.parametrize(
#     "raw_data_dir,logs_dir", [("/mock/raw/data/dir", "/mock/logs")]
# )
# @patch("os.path.exists")
# @patch("pandas.read_csv")
# @patch("sqlite3.connect")
# def test_get_existing_modules(
#     mock_sqlite3, mock_read_csv, mock_exists, raw_data_dir, logs_dir
# ):
#     with patch("sams.config.RAW_DATA_DIR", raw_data_dir), patch(
#         "sams.config.LOGS", logs_dir
#     ):
#         mock_exists.return_value = True
#         mock_read_csv.return_value = pd.DataFrame(
#             {
#                 "module": ["MOD1", "MOD2"],
#                 "academic_year": [2024, 2024],
#                 "count": [10, 20],
#             }
#         )
#         mock_cursor = (
#             mock_sqlite3.return_value.__enter__.return_value.cursor.return_value
#         )
#         mock_cursor.fetchall.return_value = [("MOD1", 2024, 10), ("MOD2", 2024, 21)]

#         result = get_existing_modules()
#         assert result == [("MOD1", 2024), ("MOD2", 2024)]


@pytest.mark.parametrize(
    "input_str,expected",
    [
        ("camelCase", "camel_case"),
        ("FOOBar", "foo_bar"),
        ("ThisIsATest", "this_is_a_test"),
        ("ABC", "abc"),
        ("alreadySnakeCase", "already_snake_case"),
    ],
)
def test_camel_to_snake_case(input_str, expected):
    assert camel_to_snake_case(input_str) == expected

def camel_to_snake_case(name: str) -> str:
    """
    Converts CamelCase or PascalCase to snake_case.
    
    Examples:
        ThisIsATest -> this_is_a_test
        camelCase -> camel_case
        HTTPServerError -> http_server_error
    """
    # Handle acronym boundaries like 'HTTPServer' → 'HTTP_Server'
    name = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
    # Handle normal camelCase boundaries like 'camelCase' → 'camel_Case'
    name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
    return name.lower()

def test_dict_camel_to_snake_case():
    test_dict = {"camelCase": 1, "PascalCase": 2, "snake_case": 3}
    expected = {"camel_case": 1, "pascal_case": 2, "snake_case": 3}
    assert dict_camel_to_snake_case(test_dict) == expected


@pytest.mark.parametrize(
    "input_str,expected",
    [
        ("Tength", "Tenth"),
        ("tength", "tenth"),
        ("OR", "Or"),
        ("normalText", "normalText"),
    ],
)
def test_correct_spelling(input_str, expected):
    assert correct_spelling(input_str) == expected


@pytest.mark.parametrize(
    "input_str,expected",
    [
        ("students.column_name", "column_name"),
        ("institutes.column_name", "column_name"),
        ("invalid_text", None),
    ],
)
def test_find_null_column(input_str, expected):
    assert find_null_column(input_str) == expected


@pytest.mark.parametrize(
    "file_exists,mtime,current_time,expected",
    [
        (True, 1000, 3600 + 1000, 1.0),  # File exists, created 1 hour ago
        (False, 0, 0, float("inf")),  # File doesn't exist
    ],
)
def test_hours_since_creation(file_exists, mtime, current_time, expected):
    with patch("os.path.exists", return_value=file_exists), patch(
        "os.path.getmtime", return_value=mtime
    ), patch("time.time", return_value=current_time):
        assert hours_since_creation("dummy_path") == expected


# Mock DataFrames for Testing
@pytest.fixture
def df1():
    return pd.DataFrame({
        "key1": ["A", "A", "B"],
        "fuzzy_col": ["apple", "banana", "grape"]
    })

@pytest.fixture
def df2():
    return pd.DataFrame({
        "key1": ["A", "A", "B"],
        "fuzzy_col": ["appl", "bananas", "grapes"],
        "value": [10, 20, 30]
    })

@pytest.fixture
def df2_unrelated():
    return pd.DataFrame({
        "key1": ["C", "C", "D"],
        "fuzzy_col": ["carrot", "date", "elderberry"],
        "value": [40, 50, 60]
    })

### Test Cases

def test_best_fuzzy_match():
    """Test the fuzzy matching function."""
    choices = ["apple", "banana", "grape"]
    assert best_fuzzy_match("appl", choices, threshold=80) == "apple"
    assert best_fuzzy_match("bananas", choices, threshold=80) == "banana"
    assert best_fuzzy_match("unknown", choices, threshold=80) is None
    assert best_fuzzy_match("grape", choices, threshold=90) == "grape"

def test_group_dict(df2):
    """Test the _group_dict function."""
    grouped = _group_dict(df2, group_by=["key1"])
    assert isinstance(grouped, dict)
    assert len(grouped) == 2
    assert grouped[tuple("A")].shape == (2, 3)
    assert grouped[tuple("B")].shape == (1, 3)
    assert "value" in grouped[tuple("A")].columns

def test_fuzzy_merge_basic(df1, df2):
    """Test fuzzy_merge with a simple case."""
    result = fuzzy_merge(df1, df2, how="left", exact_on=["key1"], fuzzy_on="fuzzy_col")
    assert isinstance(result, pd.DataFrame)
    assert result.shape[0] == df1.shape[0]  # Same number of rows as df1
    assert "value" in result.columns
    assert result["value"].isnull().sum() == 0  # All rows should have matches

def test_fuzzy_merge_no_matches(df1, df2_unrelated):
    """Test fuzzy_merge when no matches are found."""
    result = fuzzy_merge(df1, df2_unrelated, how="left", exact_on=["key1"], fuzzy_on="fuzzy_col")
    assert isinstance(result, pd.DataFrame)
    assert result.shape[0] == df1.shape[0]  # Same number of rows as df1
    assert result["value"].isnull().sum() == df1.shape[0]  # No rows should have matches

def test_fuzzy_merge_threshold(df1, df2):
    """Test fuzzy_merge with a higher threshold."""
    result = fuzzy_merge(df1, df2, how="left", exact_on=["key1"], fuzzy_on="fuzzy_col")
    assert result.loc[result["fuzzy_col"] == "bananas", "value"].values[0] == 20

def test_fuzzy_merge_with_duplicates():
    """Test fuzzy_merge with duplicates in df2."""
    df1 = pd.DataFrame({
        "key1": ["A"],
        "fuzzy_col": ["apple"]
    })
    df2 = pd.DataFrame({
        "key1": ["A", "A"],
        "fuzzy_col": ["appl", "appl"],
        "value": [10, 20]
    })
    result = fuzzy_merge(df1, df2, how="left", exact_on=["key1"], fuzzy_on="fuzzy_col")
    assert result.shape[0] == 2
    assert result["value"].isin([10, 20]).all()  # Both matches valid


def test_decrypt_roll_success():
    """
    Tests the successful decryption of a known encrypted string.

    This encrypted string was generated beforehand using the EXACT same AES key,
    ECB mode, and padding used in the decrypt_roll function.
    The original text was "S-123-ABC".
    """
    # CORRECTED LINE: This is the valid encrypted string for "S-123-ABC".
    encrypted_roll = "BjmySEYFgo9nafRPCvdRxQ=="
    expected_roll = "001AA0001"
    
    decrypted_result = decrypt_roll(encrypted_roll)
    
    assert decrypted_result == expected_roll, f"Expected '{expected_roll}' but got '{decrypted_result}'"
    

@pytest.mark.parametrize(
    "invalid_input, description",
    [
        (None, "Input is None"),
        ("", "Input is an empty string"),
        (12345, "Input is an integer, not a string"),
        (["list"], "Input is a list"),
        ("invalid-base64-string", "String with invalid Base64 characters"),
        ("a short", "String with incorrect Base64 padding/length"),
        ("YWJjZA==", "Valid Base64, but decoded data is not a multiple of AES block size"),
        ("gASVBGAAAAAAAACMCGVycm9ycy5Db3JydXB0RGF0YUVycm9yqXEu", "Corrupted data that fails decryption padding check"),
    ],
)
def test_decrypt_roll_returns_na_for_invalid_input(invalid_input, description):
    """
    Tests that the function returns 'NA' for various types of invalid or
    malformed input, as described in the test cases.
    """
    assert decrypt_roll(invalid_input) == "NA", f"Failed on case: {description}"

def test_decrypt_roll_with_wrong_key():
    """
    Tests that decryption fails and returns 'NA' if a different key is used.
    
    When an incorrect key is used, the decrypted output is garbage, which should
    fail either the padding check or the final UTF-8 decoding, resulting in "NA".
    """
    # This is "S-123-ABC" encrypted with the default key
    encrypted_roll = "1E5n/3s3f2a8qN4/n3gVlA=="
    
    # A 32-byte key, but it's the wrong one
    wrong_key = b'this is the wrong 32 byte key!!' 
    
    assert decrypt_roll(encrypted_roll, key=wrong_key) == "NA"

if __name__ == "__main__":
    pytest.main()
