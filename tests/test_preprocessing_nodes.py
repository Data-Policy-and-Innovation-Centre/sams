import pytest
import pandas as pd
import numpy as np
import json
from datetime import datetime
from geopy.location import Location
from geopy.point import Point
import sams.preprocessing.nodes as nodes

# Fixtures for common test data
@pytest.fixture
def sample_df():
    return pd.DataFrame({
        'empty_col': ['', ' ', 'NA', 'value'],
        'pin_code': ['123456', '789012'],
        'dob': ['2000-01-01', '1999-12-31'],
        'date_of_application': ['2023-01-01', '2023-12-31'],
        'had_two_year_full_time_work_exp_after_tenth': ['Yes', 'No'],
        'gc': ['Yes', 'No'],
        'ph': ['No', 'Yes'],
        'es': ['Yes', 'No'],
        'sports': ['No', 'Yes'],
        'national_cadet_corps': ['Yes', 'No'],
        'pm_care': ['No', 'Yes'],
        'orphan': ['Yes', 'No'],
        'highest_qualification': ['BA', 'diploma'],
        'mark_data': [
            json.dumps([{"ExamName": "10th", "SecuredMarks": "450", "TotalMarks": "500"},
                       {"ExamName": "12th", "SecuredMarks": "480", "TotalMarks": "500"}]),
            json.dumps([{"ExamName": "10th", "SecuredMarks": "400", "TotalMarks": "500"},
                       {"ExamName": "ITI", "SecuredMarks": "380", "TotalMarks": "400"}])
        ]
    })

# Test _make_date function
def test_make_date():
    series = pd.Series(['2023-01-01', '2023-12-31', 'invalid'])
    result = nodes._make_date(series)
    assert pd.api.types.is_datetime64_dtype(result)
    assert result.iloc[0] == pd.Timestamp('2023-01-01')
    assert pd.isna(result.iloc[2])

# Test _make_null function
def test_make_null(sample_df):
    result = nodes._make_null(sample_df)
    assert pd.isna(result['empty_col'].iloc[0])  # Empty string
    assert pd.isna(result['empty_col'].iloc[1])  # Space
    assert pd.isna(result['empty_col'].iloc[2])  # "NA"
    assert result['empty_col'].iloc[3] == 'value'  # Normal value

# Test _make_bool function
def test_make_bool():
    series = pd.Series(['Yes', 'No', 'Invalid'])
    result = nodes._make_bool(series)
    assert result.iloc[0] is True
    assert result.iloc[1] is False
    assert pd.isna(result.iloc[2])

# Test _fix_qual_names function
def test_fix_qual_names():
    series = pd.Series(['BA', 'diploma', '12th', '10th', 'matric', 'b.tech', 'iti'])
    result = nodes._fix_qual_names(series)
    assert result.iloc[0] == 'Graduate and above'
    assert result.iloc[1] == 'Diploma'
    assert result.iloc[2] == '12th'
    assert result.iloc[3] == '10th'
    assert result.iloc[4] == '10th'  # matric should be converted to 10th
    assert result.iloc[5] == 'Graduate and above'  # b.tech should be graduate
    assert result.iloc[6] == 'ITI'

# Test _extract_highest_qualification function
def test_extract_highest_qualification(sample_df):
    result = nodes._extract_highest_qualification(sample_df['mark_data'])
    assert result.iloc[0] == '12th'  # Should pick 12th over 10th
    assert result.iloc[1] == 'ITI'   # Should pick ITI over 10th

# Test _extract_mark_data function
def test_extract_mark_data(sample_df):
    result = nodes._extract_mark_data(
        sample_df['mark_data'],
        'ExamName',
        '10th',
        ['SecuredMarks', 'TotalMarks']
    )
    assert result.iloc[0]['SecuredMarks'] == '450'
    assert result.iloc[0]['TotalMarks'] == '500'
    assert result.iloc[1]['SecuredMarks'] == '400'
    assert result.iloc[1]['TotalMarks'] == '500'

# Test _preprocess_students function
def test_preprocess_students(sample_df, monkeypatch):
    # Mock _lat_long function to avoid actual geocoding
    def mock_lat_long(df, *args, **kwargs):
        df['longitude'] = [77.0, 78.0]
        df['latitude'] = [28.0, 29.0]
        return df
    
    monkeypatch.setattr(nodes, '_lat_long', mock_lat_long)
    
    result = nodes._preprocess_students(sample_df)
    
    # Check date conversions
    assert isinstance(result['dob'].iloc[0], pd.Timestamp)
    assert isinstance(result['date_of_application'].iloc[0], pd.Timestamp)
    
    # Check boolean conversions
    assert result['had_two_year_full_time_work_exp_after_tenth'].iloc[0] is True
    assert result['gc'].iloc[1] is False
    
    # Check geocoding
    assert result['longitude'].iloc[0] == 77.0
    assert result['latitude'].iloc[0] == 28.0

# Test _get_distance function
def test_get_distance():
    coord_1 = (28.6139, 77.2090)  # Delhi coordinates
    coord_2 = (19.0760, 72.8777)  # Mumbai coordinates
    distance = nodes._get_distance(coord_1, coord_2)
    assert isinstance(distance, float)
    assert round(distance) == 1163  # Approximate distance in km

    # Test with invalid coordinates
    invalid_coord = (91.0, 181.0)  # Invalid lat/long
    assert nodes._get_distance(coord_1, invalid_coord) is None

# Test preprocess_distances function
def test_preprocess_distances():
    df = pd.DataFrame({
        'student_lat': [28.6139, 19.0760],
        'student_long': [77.2090, 72.8777],
        'institute_lat': [19.0760, 28.6139],
        'institute_long': [72.8777, 77.2090]
    })
    
    result = nodes.preprocess_distances(df)
    assert 'distance' in result.columns
    assert isinstance(result['distance'].iloc[0], float)
    assert round(result['distance'].iloc[0]) == 1163  # Approximate distance in km

# Test preprocess_geocodes function
def test_preprocess_geocodes(monkeypatch):
    # Mock _lat_long function
    def mock_lat_long(df, *args, **kwargs):
        df['longitude'] = [77.0] * len(df)
        df['latitude'] = [28.0] * len(df)
        return df
    
    monkeypatch.setattr(nodes, '_lat_long', mock_lat_long)
    
    df1 = pd.DataFrame({'address': ['Address 1', 'Address 2']})
    df2 = pd.DataFrame({'location': ['Location 1', 'Location 2']})
    
    result = nodes.preprocess_geocodes([df1, df2], ['address', 'location'])
    
    assert 'longitude' in result.columns
    assert 'latitude' in result.columns
    assert len(result) == 4  # Combined unique addresses
    assert all(result['longitude'] == 77.0)
    assert all(result['latitude'] == 28.0)

# Test _extract_cutoff_cols function
def test_extract_cutoff_cols():
    cutoffs_df = pd.DataFrame({
        'sams_code': ['SAMS001'],
        'academic_year': ['2023'],
        'trade': ['Trade1'],
        'cutoff': [json.dumps([{
            'SelectionStage': 'Stage1',
            'Category1': 80,
            'Category2': 75
        }])]
    })
    
    result = nodes._extract_cutoff_cols(cutoffs_df)
    assert 'selection_stage' in result.columns
    assert 'applicant_type' in result.columns
    assert 'cutoff' in result.columns
    assert len(result) == 2  # Two categories in the sample data

# Test error handling
def test_preprocess_geocodes_error_handling():
    with pytest.raises(ValueError):
        # Mismatched number of dataframes and column names
        nodes.preprocess_geocodes([pd.DataFrame()], ['col1', 'col2'])
