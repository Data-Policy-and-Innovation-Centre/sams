import pandas as pd
import numpy as np
import ibis
import json
from sams.utils import dict_camel_to_snake_case, flatten
from loguru import logger
from tqdm import tqdm
import time
from ibis import _ as L
import ibis.expr.datatypes as dt  
from sams.utils import (
    dict_camel_to_snake_case, 
    camel_to_snake_case, 
    flatten
)
from sams.preprocessing import normalize_nulls, make_bool, normalize_date

def _clean_year_of_passing(col: ibis.Expr) -> ibis.Expr:
    """Ensure year_of_passing is numeric and within [1970, 2025]."""
    val = col.cast("int32")
    return (
        ibis.case()
        .when((val >= 1970) & (val <= 2025), val) 
        .else_(ibis.null().cast("int32"))          
        .end()
    )

def _clean_percentage(col: ibis.Expr) -> ibis.Expr:
    """Ensure percentage is numeric and within [0, 100]."""
    val = col.cast("float32")
    return (
        ibis.case()
        .when((val >= 0) & (val <= 100), val)      
        .else_(ibis.null().cast("float32"))        
        .end()
    )


def _coerce_marks(col: ibis.Expr) -> ibis.Expr:
    """Coerce marks to float safely."""
    return col.cast("float32")

def _correct_addresses(
    address: ibis.Expr, block: ibis.Expr, district: ibis.Expr,
    state: ibis.Expr, pincode: ibis.Expr
) -> ibis.Expr:
    """Build a normalized address string safely."""
    return (
        ibis.coalesce(address, ibis.literal(""))
        .split(",")[0]  
        .concat(", ")
        .concat(block.fillna(""))
        .concat(", ")
        .concat(district.fillna(""))
        .concat(", ")
        .concat(state.fillna(""))
        .concat(" ")
        .concat(pincode.cast("string").fillna(""))
    )


def _preprocess_hss_students(df: ibis.Table) -> ibis.Table:
    """Preprocess HSS student data with Ibis expressions."""

    # Date cleanup
    if "dob" in df.columns:
        df = df.mutate(dob=normalize_date(df.dob))

    # Boolean fields cleanup
    bool_cols = ["ph", "es", "sports", "national_cadet_corps", "orphan", "compartmental_status"]
    for col in bool_cols:
        if col in df.columns:
            df = df.mutate(**{col: make_bool(df[col])})

    # Percentage cleanup
    if "percentage" in df.columns:
        df = df.mutate(percentage=_clean_percentage(df.percentage))

    # Year of passing cleanup
    if "year_of_passing" in df.columns:
        df = df.mutate(year_of_passing=_clean_year_of_passing(df.year_of_passing))

    # Marks cleanup
    for col in ["secured_marks", "total_marks"]:
        if col in df.columns:
            df = df.mutate(**{col: _coerce_marks(df[col])})

    # Clean text-like columns (normalize null-like values)
    text_cols = ["pin_code", "aadhar_no"]
    for col in [c for c in text_cols if c in df.columns]:
        df = df.mutate(**{col: normalize_nulls(df[col])})

    # Address cleanup
    if all(c in df.columns for c in ["address", "block", "district", "state", "pin_code"]):
        df = df.mutate(
            full_address=_correct_addresses(
                df.address, df.block, df.district, df.state, df.pin_code
            )
        )

    return df


def preprocess_hss_students_enrollment_data(df: ibis.Table) -> ibis.Table:
    """
    Preprocess HSS student enrollment data for downstream use.
    Ensures output is ordered only by academic_year ascending.
    """

    # Filter only HSS module
    df = df.filter(df.module == "HSS")

    # Select key enrollment-related columns 
    keep_cols = [
        "id",
        "barcode",
        "aadhar_no",
        "academic_year",
        "module",
        "student_name",
        "gender",
        "dob",
        "social_category",
        "orphan",
        "es",
        "ph",
        "address",
        "state",
        "district",
        "block",
        "pin_code",
        "annual_income",
        "roll_no",
        "highest_qualification",
        "board_exam_name_for_highest_qualification",
        "examination_board_of_the_highest_qualification",
        "examination_type",
        "year_of_passing",
        "total_marks",
        "secured_marks",
        "percentage",
        "compartmental_status",
        "hss_option_details",
        "hss_compartments",
    ]

    available_cols = [c for c in keep_cols if c in df.columns]
    df = df[available_cols]

    df = _preprocess_hss_students(df)

    # Order strictly by academic_year ascending
    if "academic_year" in df.columns:
        df = df.order_by(df.academic_year.asc())

    return df



def extract_hss_options(students_table: ibis.Table, option_col: str = "hss_option_details", 
                        aadhar_col: str = "aadhar_no", id_col: str = "barcode", 
                        year_col: str = "academic_year",
                    ) -> ibis.Table:
    """
    Cleans and flattens HSS student application data.

    Expands the `hss_option_details` JSON array so each application option
    becomes a separate row. If a student has no options, it adds a default
    empty entry to keep them in the data. Also calculates how many total
    applications each student made.

    Args:
        students_table (ibis.Table): Table containing raw HSS student data.

    Returns:
        ibis.Table: Flattened table with one row per application option.
    """
    logger.info("HSS applications preprocessing started")

    # Filter for HSS students and count total applications
    base_hss_students = students_table.filter(students_table.module == 'HSS').mutate(
        num_applications=L[option_col].cast('json').array.length().fillna(0).cast('int64')
    )

    # Default JSON array for students without options
    default_option_array = ibis.literal('[{}]').cast(dt.string)

    # Replace NULL or empty arrays with the default
    base_hss_with_default = base_hss_students.mutate(
        options_to_unnest=ibis.case()
            .when(base_hss_students[option_col].isnull(), default_option_array)
            .when(base_hss_students[option_col] == '[]', default_option_array)
            .else_(base_hss_students[option_col])
            .end()
    )

    # Expand the JSON array into separate rows
    students_with_options = base_hss_with_default.unnest(
        L.options_to_unnest.cast('json').array.name("option_object")
    )

    # Extract fields from JSON
    json_to_column_map = {
        "ReportedInstitute": "reported_institute",
        "SAMSCode": "sams_code",
        "Stream": "stream",
        "Subject": "subject",
        "InstituteDistrict": "institute_district",
        "InstituteBlock": "institute_block",
        "TypeofInstitute": "type_of_institute",
        "Phase": "phase",
        "Year": "year",
        "AdmissionStatus": "admission_status",
    }

    string_mutations = {
        final_name: L.option_object[json_key].cast('string').replace('"', '')
        for json_key, final_name in json_to_column_map.items()
    }
    
    other_mutations = {
        "option_no": L.option_object["OptionNo"].cast('string').re_extract(r'(\d+)$', 1).nullif('').cast('int32')
    }

    all_mutations = {**other_mutations, **string_mutations}
    df_options = students_with_options.mutate(**all_mutations)

    # Select final cleaned columns
    final_columns = [
        aadhar_col, id_col, year_col, "reported_institute", "sams_code",
        "stream", "subject", "institute_district", "institute_block", "type_of_institute",
        "phase", "year", "admission_status", "option_no", "num_applications"
    ]
    df_options = df_options.select(final_columns)

    # Sort for consistency
    df_options = df_options.order_by([year_col, id_col, ibis.coalesce(df_options.option_no, 0)])

    logger.info("HSS applications preprocessing finished")
    return df_options


def preprocess_students_compartment_marks(students_table: ibis.Table) -> ibis.Table:
    """
    Cleans and flattens student HSS compartment data.

    This function expands the `hss_compartments` JSON array so each subject
    appears as a separate row. If a student has no compartment data, it adds
    a default empty entry to ensure they are not dropped.

    Args:
        students_table (ibis.Table): Input table with `hss_compartments` data.

    Returns:
        ibis.Table: Flattened table with one row per compartment subject.
    """
    logger.info("Starting HSS marks preprocessing")

    # Keep only HSS module records
    hss_students = students_table.filter(students_table.module == 'HSS')

    # Default JSON array for students without compartments
    default_compartment_array = ibis.literal('[{}]').cast(dt.string)

    # Replace NULL or empty arrays with the default
    hss_students_with_default = hss_students.mutate(
        compartments_to_unnest=ibis.case()
        .when(hss_students.hss_compartments.isnull(), default_compartment_array)
        .when(hss_students.hss_compartments == '[]', default_compartment_array)
        .else_(hss_students.hss_compartments)
        .end()
    )

    # Expand the JSON array into separate rows
    flattened_table = hss_students_with_default.unnest(
        L.compartments_to_unnest.cast('json').array.name("compartment_object")
    )

    # Extract compartment details
    fields = {
        v: L.compartment_object[k].cast("string").replace('"', "")
        for k, v in {
            "COMPSubject": "comp_subject",
            "COMPFailMark": "comp_fail_mark",
            "COMPPassMark": "comp_pass_mark",
        }.items()
    }

    # Add extracted fields and select final columns
    df = flattened_table.mutate(**fields).select(
        "aadhar_no", "barcode", "academic_year", "module",
        "board_exam_name_for_highest_qualification", "highest_qualification",
        "examination_board_of_the_highest_qualification", "examination_type",
        "year_of_passing", "total_marks", "secured_marks", "percentage",
        "compartmental_status", "comp_subject", "comp_fail_mark", "comp_pass_mark"
    ).order_by(["academic_year", "barcode", "aadhar_no"])

    logger.info("HSS compartment preprocessing done")
    return df
