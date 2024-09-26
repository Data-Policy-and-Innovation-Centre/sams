from loguru import logger
import pandas as pd
import os
from config import LOGS

def _pd_check_student_missing_values(df: pd.DataFrame, logfile: str = "student_missing_values.log") -> None:

    # Remove all existing log handlers
    for handler_id in list(logger._core.handlers.keys()):
          logger.remove(handler_id)

    # Add a new log handler
    log_file_id = logger.add(
          os.path.join(LOGS, logfile), mode='w',
          format="{time} {level} {message}", level="INFO"
     )
    
     # Define required keys
    required_keys = ["Barcode", "StudentName", "Gender", "DOB", "ReligionName", "Nationality", "AnnualIncome", "Address", "State", "District", "Block",
                        "PINCode", "SocialCategory", "Domicile", "S_DomicileCategory", "OutsideOdishaApplicantStateName",
                        "OdiaApplicantLivingOutsideOdishaStateName", "ResidenceBarcodeNumber", "TengthExamSchoolAddress", "EighthExamSchoolAddress",
                      "HighestQualification", "HighestQualificationExamBoard", "HighestQualificationBoardExamName", "ExaminationType", "YearofPassing",
                     "RollNo", "TotalMarks", "SecuredMarks", "Percentage", "CompartmentalStatus", "CompartmentalFailMark", "SubjectWiseMarks", "hadTwoYearFullTimeWorkExpAfterTength",
                        "GC", "PH", "ES", "Sports", "NationalCadetCorps", "PMCare", "Orphan", "IncomeBarcode", "TFW", "EWS", "BOC", "BOCRegdNo", "CourseName", "CoursePeriod",
                        "BeautyCultureType", "ReportedInstitute", "ReportedBranchORTrade", "InstituteDistrict", "TypeofInstitute", "Phase", "Year", "AdmissionStatus", "EnrollmentStatus"]
    
    # Check for missing keys
    missing_keys = [key for key in required_keys if key not in df.columns]
    if missing_keys:
        logger.error(f"Missing required keys: {missing_keys}")
    
    # Check for missing values
    missing_values_summary = df.isna().sum()
    missing_values_summary_pct = (missing_values_summary / len(df)) * 100
    missing_values_summary_pct = missing_values_summary_pct.to_frame(name='Percentage of missing values')
    missing_values_summary_pct['Percentage of missing values'] = missing_values_summary_pct['Percentage of missing values'].apply(lambda x: f"{x:.2f}%")
    
    # Log aggregate summary table
    logger.error("Missing values summary:")
    logger.error(missing_values_summary_pct.to_string())
    logger.info(f"Missing values summary logged to '{LOGS}/{logfile}'")


def _check_duplicate_students(data: pd.DataFrame) -> None:
    duplicates, not_unique = _find_duplicate_students(data)
    if not_unique:
        _log_duplicates(duplicates, "student_duplicates.log")

def _find_duplicate_students(data: list) -> tuple[pd.DataFrame, bool]:
    barcodes = [record["Barcode"] for record in data]
    num_duplicate_barcodes = len(barcodes) - len(set(barcodes))
    if num_duplicate_barcodes > 0:
        df = pd.DataFrame(data, columns=["Barcode", "StudentName", "Module", "Year"])
        duplicates = df[df.duplicated(subset=['Barcode'], keep=False)]
        return duplicates, True
    return pd.DataFrame(), False

def _log_duplicates(duplicates: pd.DataFrame, filename) -> None:
    file_id = logger.add(os.path.join(LOGS, filename), mode='w', format="{time} {level} {message}", level="INFO")
    logger.warning(f"Found {len(duplicates)} duplicate barcodes in downloaded student data")
    logger.debug(duplicates)
    logger.remove(file_id)