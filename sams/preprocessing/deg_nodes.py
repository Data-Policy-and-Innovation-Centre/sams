import re
import json
import ibis
import math
from loguru import logger
from datetime import date
from typing import Iterable, Optional, Any, Union
import time
from sams.utils import dict_camel_to_snake_case, camel_to_snake_case, flatten
from sams.preprocessing import normalize_nulls, make_bool, normalize_date

def _preprocess_students(df: ibis.Table) -> ibis.Table:
    """
    Apply canonical preprocessing for DEG student fields:
      - Normalize NULL tokens
      - Normalize DOB into DATE
      - Convert YES/NO fields into boolean
      - Cast numerics into proper numeric types
    """
    # Handle DOB with null + date normalization
    df2 = df.mutate(
        dob=normalize_date(normalize_nulls(df.dob))
    )

    # Boolean-like columns
    bool_cols = [c for c in ["ph", "es", "orphan", "sports", "compartmental_status"] if c in df2.columns]
    for col in bool_cols:
        df2 = df2.mutate(**{col: make_bool(normalize_nulls(df2[col]))})

    # Clean text-like columns
    text_cols = [
        "pin_code", "aadhar_no"
    ]
    for col in [c for c in text_cols if c in df2.columns]:
        df2 = df2.mutate(**{col: normalize_nulls(df2[col])})

    # Numeric coercions
    if "percentage" in df2.columns:
        df2 = df2.mutate(percentage=normalize_nulls(df2.percentage).cast("float64"))
    if "year_of_passing" in df2.columns:
        df2 = df2.mutate(year_of_passing=normalize_nulls(df2.year_of_passing).cast("int32"))
    if "secured_marks" in df2.columns:
        df2 = df2.mutate(secured_marks=normalize_nulls(df2.secured_marks).cast("float64"))
    if "total_marks" in df2.columns:
        df2 = df2.mutate(total_marks=normalize_nulls(df2.total_marks).cast("int32"))

    return df2

def preprocess_deg_students_enrollment_data(df: ibis.Table) -> ibis.Table:
    """
    Pure Ibis version of DEG enrollment preprocessing.
    Ensures the final table is ordered by academic_year ascending
    """

    logger.info("Starting DEG enrollment preprocessing")

    # Apply standard preprocessing first
    df2 = _preprocess_students(df)

    # Drop irrelevant columns, keep 'phase' if it exists
    drop_cols = {"nationality", "contact_no", "national_cadet_corps", "year"}
    keep_cols = [c for c in df2.columns if c not in drop_cols or c == "phase"]
    df2 = df2.select(*[df2[c] for c in keep_cols])

    # Drop fully-empty columns, but always keep key identifiers
    counts = df2.aggregate({c: df2[c].count() for c in keep_cols}).execute()
    force_keep = {"dob", "aadhar_no", "barcode", "academic_year"}
    non_all_na_cols = [
        c for c in keep_cols if counts[c].iloc[0] > 0 or c in force_keep
    ]
    df2 = df2.select(*[df2[c] for c in non_all_na_cols])

    # Ensure ordering only by academic_year ascending
    if "academic_year" in df2.columns:
        df2 = df2.order_by(df2.academic_year.asc())

    return df2


def preprocess_deg_options_details(con, df):
    """
    Flatten DEG application options using Ibis + DuckDB.
    Ensures option numbers come directly from JSON (OptionNo field),
    preserving student-wise sequence (1,2,3,...).
    """
    start_all = time.time()
    logger.info("Starting DEG applications preprocessing")

    sql = """
    WITH expanded AS (
        SELECT
            s.aadhar_no,
            s.academic_year,
            s.barcode,
            CAST(opt.value::JSON ->> 'OptionNo' AS INT) AS option_no,
            opt.value::JSON ->> 'Stream'            AS stream,
            opt.value::JSON ->> 'Subject'           AS subject,
            opt.value::JSON ->> 'AdmissionStatus'   AS admission_status,
            opt.value::JSON ->> 'ReportedInstitute' AS reported_institute,
            opt.value::JSON ->> 'SAMSCode'          AS sams_code,
            opt.value::JSON ->> 'InstituteDistrict' AS institute_district,
            opt.value::JSON ->> 'InstituteBlock'    AS institute_block,
            opt.value::JSON ->> 'TypeofInstitute'   AS type_of_institute,
            opt.value::JSON ->> 'Year'              AS year,
            opt.value::JSON ->> 'Phase'             AS phase
        FROM students s
        LEFT JOIN LATERAL json_each(s.deg_option_details) AS opt ON TRUE
        WHERE s.module = 'DEG'
    )
    SELECT *
    FROM expanded
    ORDER BY
        academic_year,
        aadhar_no,
        barcode,
        option_no
    """

    df_options = con.sql(sql)
    logger.info("Flattened JSON fields")

    # Count applications per student-year-barcode
    win = ibis.window(group_by=["aadhar_no", "academic_year", "barcode"])
    df_options = df_options.mutate(
        num_applications=df_options.option_no.count().over(win)
    )

    logger.info(
        f"Done processing DEG applications in {time.time() - start_all:.1f} seconds"
    )
    return df_options


def preprocess_deg_compartments(con, df):
    """
    Preprocess DEG compartment subjects from 'deg_compartments' JSON column.

    Each row in the output corresponds to a single compartment subject.
    If deg_compartments is NULL or empty, the student still appears (no drop).
    """
    start_all = time.time()
    logger.info("Starting DEG compartments preprocessing")

    sql = """
    SELECT
        s.aadhar_no,
        s.academic_year,
        s.barcode,
        s.board_exam_name_for_highest_qualification,
        s.highest_qualification,
        s.module,
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
        WHERE module = 'DEG'
    ) AS s
    LEFT JOIN LATERAL json_each(s.deg_compartments) AS comp ON TRUE
    ORDER BY
        s.academic_year ASC,
        s.barcode ASC,
        s.aadhar_no ASC
    """

    df_comp = con.sql(sql)
    logger.info("Flattened all compartment JSON fields")
    
    logger.info(f"Done processing DEG marks in {time.time() - start_all:.1f} seconds")
    return df_comp
