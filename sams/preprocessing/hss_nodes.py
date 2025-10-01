import pandas as pd
import numpy as np
import ibis
import json
from sams.utils import dict_camel_to_snake_case, flatten
from loguru import logger
from tqdm import tqdm
import time
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

def extract_hss_options(
    df: ibis.Table,
    option_col: str = "hss_option_details",
    aadhar_col: str = "aadhar_no",
    id_col: str = "barcode",
    year_col: str = "academic_year",
) -> ibis.Table:
    """
    Flatten 'hss_option_details' JSON into long format (via DuckDB SQL).
    Preserves the OptionNo from JSON exactly as stored.
    """

    start_all = time.time()
    logger.info("HSS applications preprocessing started")

    con = ibis.get_backend(df)

    query = f"""
    SELECT
        s.{aadhar_col},
        s.{id_col},
        s.{year_col},
        opt.value::JSON ->> 'ReportedInstitute' AS reported_institute,
        opt.value::JSON ->> 'SAMSCode'          AS sams_code,
        opt.value::JSON ->> 'Stream'            AS stream,
        opt.value::JSON ->> 'InstituteDistrict' AS institute_district,
        opt.value::JSON ->> 'InstituteBlock'    AS institute_block,
        opt.value::JSON ->> 'TypeofInstitute'   AS type_of_institute,
        opt.value::JSON ->> 'Phase'             AS phase,
        opt.value::JSON ->> 'Year'              AS year,
        opt.value::JSON ->> 'AdmissionStatus'   AS admission_status,
        opt.value::JSON ->> 'OptionNo'        AS option_no

    FROM (
        SELECT {aadhar_col}, {id_col}, {year_col}, {option_col}
        FROM students
        WHERE module = 'HSS'
    ) AS s
    LEFT JOIN LATERAL json_each(s.{option_col}) AS opt ON TRUE
    ORDER BY
        s.{year_col} ASC,
        s.{id_col} ASC,
        CAST(opt.key AS INTEGER) ASC  -- use JSON array index for row order
    """

    df_options = con.sql(query)

    # Count applications per student-year
    win = ibis.window(group_by=[aadhar_col, year_col, id_col])
    df_options = df_options.mutate(
        num_applications=df_options.option_no.count().over(win)
    )

    logger.info(
        f"HSS applications preprocessing finished in {time.time() - start_all:.1f}s"
    )
    return df_options


 
def preprocess_students_compartment_marks(df: ibis.Table) -> ibis.Table:
    """
    Flatten and preprocess HSS compartment subject marks using DuckDB SQL via Ibis.
    Preserves the row order as in the raw database (by academic_year, barcode, then JSON array index).
    """

    logger.info("Starting HSS markss preprocessing")
    
    con = ibis.get_backend(df)

    query = """
    SELECT
        s.aadhar_no,
        s.barcode,
        s.academic_year,
        s.module,
        s.board_exam_name_for_highest_qualification,
        s.highest_qualification,
        s.examination_board_of_the_highest_qualification,
        s.examination_type,
        s.year_of_passing,
        s.total_marks,
        s.secured_marks,
        s.percentage,
        s.compartmental_status,
        comp.value::JSON ->> 'COMPSubject'   AS comp_subject,
        comp.value::JSON ->> 'COMPFailMark'  AS comp_fail_mark,
        comp.value::JSON ->> 'COMPPassMark'  AS comp_pass_mark
    FROM (
        SELECT *
        FROM students
        WHERE module = 'HSS'
    ) AS s
    LEFT JOIN LATERAL json_each(s.hss_compartments) AS comp ON TRUE
    ORDER BY
        s.academic_year ASC,
        s.barcode ASC,
        CAST(comp.key AS INTEGER) ASC
    """

    exploded = con.sql(query)

    logger.info("Flattened HSS compartment JSON fields")
    return exploded

