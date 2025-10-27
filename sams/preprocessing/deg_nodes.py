import json
import ibis
import math
from loguru import logger
from datetime import date
from typing import Iterable, Optional, Any, Union
import time
from ibis import _ as L  
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


def preprocess_deg_options_details(students_table: ibis.Table):
    """Clean and flatten DEG application data from student records.

    This function takes the `deg_option_details` JSON column and:
    - Breaks it into separate rows so each application option has its own record.
    - Counts how many application options each student has.
    - Extracts key details like stream, subject, admission status, and institute info.

    Args:
        students_table (ibis.Table): Ibis Table with student data that includes 
            the `deg_option_details` column.

    Returns:
        ibis.Table: A flattened table with one row per application option, 
            including student IDs, option details, and total application count.
    """

    logger.info("Starting DEG applications preprocessing")

    # Filter for DEG module records
    students_deg = students_table.filter(students_table.module == "DEG")

    # Count number of DEG options for each student in the filtered table
    students_with_counts = students_deg.mutate(
        num_applications=students_deg.deg_option_details.cast('json').array.length().fillna(0).cast('int64')
    )

    # data explosion operation
    options = students_with_counts.unnest(
        students_with_counts.deg_option_details.cast('json').array.name("option_object")
    )


    # define a mapping from the JSON key to the final, clean column name.
    json_to_column_map = {
        "Stream": "stream",
        "Subject": "subject",
        "AdmissionStatus": "admission_status",
        "ReportedInstitute": "reported_institute",
        "SAMSCode": "sams_code",
        "InstituteDistrict": "institute_district",
        "InstituteBlock": "institute_block",
        "TypeofInstitute": "type_of_institute",
        "Phase": "phase"
    }

    # Use a dictionary comprehension to build the cleaning rules for all string columns. 
    string_mutations = {
        final_name: L.option_object[json_key].cast('string').replace('"', '')
        for json_key, final_name in json_to_column_map.items()
    }

    # Define the rules for the non-string columns separately.
    other_mutations = {
        "option_no": L.option_object["OptionNo"].cast("int32"),
        "year": L.option_object["Year"],
    }
    
    # Combine all the mutation rules into a single dictionary.
    all_mutations = {**other_mutations, **string_mutations}

    # Apply all mutations in a single, clean step using dictionary unpacking (**).
    final_options = options.mutate(**all_mutations)
    

    # Select only the final columns we need
    final_options = final_options.select(
        "academic_year", "aadhar_no", "barcode", "option_no", "phase", 
        "stream", "subject", "admission_status", "reported_institute", "sams_code",
        "type_of_institute", "institute_district", "institute_block", "year",  "num_applications"
    )

    # Order the results
    final_options = final_options.order_by(
        [final_options.academic_year.asc(), final_options.aadhar_no.asc(), final_options.barcode.asc(), final_options.option_no.asc()]
    )
    
    logger.info("Done processing DEG applications")
    
    return final_options

def preprocess_deg_compartments(students_table: ibis.Table):
    """Flatten and clean DEG compartment data from student records.

    This function processes the `deg_compartments` JSON array column by:
    - Expanding each array element into separate rows (one per compartment subject).
    - Retaining all students, even those without compartment data, using a left join.
    - Extracting key compartment details (subject, fail mark, pass mark) into new columns.

    Args:
        students_table (ibis.Table): An Ibis Table containing the raw student data, 
            which includes the `deg_compartments` column.

    Returns:
        ibis.Table: Flattened Ibis Table with one row per compartment subject,
            including student identifiers, academic details, and compartment fields.
    """
    logger.info("Starting DEG compartments preprocessing")

    # Filter table for students in the DEG module
    students_deg = students_table.filter(students_table.module == 'DEG')

    # Select key columns and unnest the deg_compartments JSON array
    expanded_compartments = students_deg.select(
        "aadhar_no", "academic_year", "barcode", "deg_compartments"
    ).unnest(
        L.deg_compartments.cast('json').array.name("compartment_object")
    ).drop("deg_compartments")

    # Left join to keep all students, including those with no compartment data
    compartments = students_deg.left_join(
        expanded_compartments,
        ["aadhar_no", "academic_year", "barcode"]
    ) 
      
    # Extract compartment details: subject, fail mark, and pass mark
    compartments_with_fields = compartments.mutate(
        comp_subject=L.compartment_object["COMPSubject"],
        comp_fail_mark=L.compartment_object["COMPFailMark"],
        comp_pass_mark=L.compartment_object["COMPPassMark"]
    )

    # Select and organize final columns for the output table
    final_compartments = compartments_with_fields.select(
        "aadhar_no", "academic_year", "barcode", "board_exam_name_for_highest_qualification",
        "highest_qualification", "module", "examination_board_of_the_highest_qualification",
        "examination_type", "year_of_passing", "total_marks", "secured_marks",
        "percentage", "compartmental_status", "comp_subject", "comp_fail_mark", "comp_pass_mark"
    )
    
    # Sort the final table by academic year, barcode, and aadhar number
    final_compartments = final_compartments.order_by(
        [ibis.asc("academic_year"), ibis.asc("barcode"), ibis.asc("aadhar_no")]
    )

    logger.info("Done processing DEG compartments")
    return final_compartments
