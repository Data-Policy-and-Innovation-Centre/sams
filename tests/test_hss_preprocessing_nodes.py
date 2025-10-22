import pytest
import pandas as pd
import numpy as np
import json
import sams.preprocessing.hss_nodes as hss


# Fixtures
@pytest.fixture
def hss_sample_df():
    return pd.DataFrame({
        'dob': ['2001-01-01', 'invalid'],
        'ph': ['Yes', 'No'],
        'percentage': ['85.0', '105'],
        'year_of_passing': ['2019', '1965'],
        'secured_marks': ['400', 'invalid'],
        'total_marks': ['500', 'invalid'],
        'address': ['123 St', ''],
        'block': ['Block A', 'Block B'],
        'district': ['District A', 'District B'],
        'state': ['State A', 'State B'],
        'pin_code': ['123456', '654321'],
        'hss_option_details': [json.dumps([{"OptionNo": "1", "Stream": "Arts", "AdmissionStatus": "ADMITTED"}]), None],
        'hss_compartments': [json.dumps([{"COMPSubject": "Math", "COMPFailMark": 12, "COMPPassMark": 35}]), "null"],
        'barcode': ['ABC123', 'XYZ456'],
        'academic_year': ['2023-24', '2023-24'],
        'compartmental_status': ['Yes', 'No'],
        'aadhar_no': ['1111', '2222'],
        'module': ['HSS','HSS'],
        'board_exam_name_for_highest_qualification': ['Board of Secondary Education, Orissa, Cuttack', 'Central Board of Secondary Education, Sikhya Kendra, Delhi'],
        'highest_qualification': ['10th','12th'],
        'examination_board_of_the_highest_qualification': ['BSE, Odisha', 'CBSE, New Delhi'],
        'examination_type': ['Annual', 'Supplementary'],
    })


# Test: _make_null
def test_make_null():
    df = pd.DataFrame({'col': ['NA', '', ' ', 'value']})
    result = hss._make_null(df)
    assert result['col'].isna().sum() == 3
    assert result['col'].iloc[3] == 'value'


# Test: _make_bool
def test_make_bool():
    series = pd.Series(['Yes', 'No', 'Other'])
    result = hss._make_bool(series)
    assert result.tolist() == [True, False, np.nan]


# Test: _make_date
def test_make_date():
    s = pd.Series(['2022-01-01', 'not-a-date'])
    result = hss._make_date(s)
    assert pd.isna(result[1])
    assert result[0] == pd.Timestamp('2022-01-01')


# Test: _clean_year_of_passing
def test_clean_year_of_passing():
    s = pd.Series(['2010', '1960', '2030', 'abcd'])
    result = hss._clean_year_of_passing(s)
    assert result.isna().sum() == 3
    assert result.iloc[0] == 2010


# Test: _clean_percentage
def test_clean_percentage():
    result = hss._clean_percentage(pd.Series(['90', '110', '-5']))
    expected = [90.0, np.nan, np.nan]
    for r, e in zip(result, expected):
        if pd.isna(e):
            assert pd.isna(r)
        else:
            assert r == e


# Test: _coerce_marks
def test_coerce_marks():
    s = pd.Series(['450', 'invalid'])
    result = hss._coerce_marks(s)
    assert result[0] == 450
    assert pd.isna(result[1])


# Test: _correct_addresses
def test_correct_addresses_formatting():
    result = hss._correct_addresses("123 St, Area", "Block", "District", "State", "123456")
    assert "Block" in result and result.endswith("123456")


# Test: extract_hss_options
def test_extract_hss_options(hss_sample_df):
    result = hss.extract_hss_options(hss_sample_df)
    assert "OptionNo" in result.columns
    assert result.iloc[0]["Stream"] == "Arts"


# Test: extract_hss_compartments
def test_extract_hss_compartments(hss_sample_df):
    result = hss.extract_hss_compartments(hss_sample_df)
    assert "COMPSubject" in result.columns
    assert result["barcode"].nunique() == 2


# Test: preprocess_students_compartment_marks
def test_preprocess_students_compartment_marks(hss_sample_df):
    result = hss.preprocess_students_compartment_marks(hss_sample_df)
    assert "comp_subject" in result.columns
    assert result.iloc[0]["comp_subject"] == "Math"


# Test: preprocess_hss_students_enrollment_data
def test_preprocess_hss_students_enrollment_data(hss_sample_df):
    result = hss.preprocess_hss_students_enrollment_data(hss_sample_df)
    assert "barcode" in result.columns
    assert "dob" in result.columns
    assert "student_name" not in result.columns  # dropped


# Test: get_priority_admission_status
def test_get_priority_admission_status(hss_sample_df):
    result = hss.get_priority_admission_status(hss_sample_df)
    assert result.iloc[0]["AdmissionStatus"] == "ADMITTED"


# Test: filter_admitted_on_first_choice
def test_filter_admitted_on_first_choice():
    df = pd.DataFrame({
        'OptionNo': ['1', '2'],
        'AdmissionStatus': ['ADMITTED', 'SELECTED BUT NOT ADMITTED'],
        'barcode': ['ABC123', 'XYZ456'],
        'academic_year': ['2023-24', '2023-24']
    })
    result = hss.filter_admitted_on_first_choice(df)
    assert result.shape[0] == 1
    assert result.iloc[0]['OptionNo'] == '1'


# Test: analyze_stream_trends
def test_analyze_stream_trends(hss_sample_df):
    result = hss.analyze_stream_trends(hss_sample_df)
    assert 'student_count' in result.columns
    assert 'Stream' in result.columns


# Test: compute_local_flag
def test_compute_local_flag():
    df = pd.DataFrame({
        'district': ['Cuttack', 'BBSR', None],
        'InstituteDistrict': ['cuttack', 'bbsr', 'bbsr']
    })
    result = hss.compute_local_flag(df)
    assert result['local'].tolist()[:2] == [True, True]
    assert result['local'].iloc[2] == False
