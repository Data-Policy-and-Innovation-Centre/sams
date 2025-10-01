from typing import Any, List, Dict, Union, Optional
from pydantic import BaseModel, Field, AliasChoices, field_validator, ConfigDict

class BaseStudentDB(BaseModel):
    # be explicit about extra-field policy and name handling
    model_config = ConfigDict(extra='ignore', populate_by_name=True)

    barcode: str = Field(..., validation_alias=AliasChoices("Barcode"))

    @field_validator("barcode", mode="before")
    @classmethod
    def _coerce_barcode(cls, v: Any) -> str:
        # Treat None/blank as invalid for a required field
        if v is None:
            raise ValueError("barcode is required")
        if isinstance(v, (int,)):
            v = str(v)
        elif isinstance(v, float):
            # reject floats so "123.0" doesn't silently become a barcode
            raise ValueError("barcode must be a string or integer, not float")
        elif not isinstance(v, str):
            # any other type will error
            raise ValueError("barcode must be a string")

        s = v.strip()
        if not s:
            raise ValueError("barcode cannot be blank")
        # normalize if you expect hyphens/spaces
        # s = s.replace("-", "").replace(" ", "")
        #enforce digits only
        # if not s.isdigit():
        #     raise ValueError("barcode must contain only digits")
        return s


    # core identity
    student_name: str = Field(None, validation_alias=AliasChoices("StudentName"))
    gender: Optional[str] = Field(None, validation_alias=AliasChoices("Gender"))

    # demographics / contacts
    religion_name: Optional[str] = Field(None, validation_alias=AliasChoices("ReligionName"))
    dob: Optional[str] = Field(None, validation_alias=AliasChoices("DOB"))
    contact_no: Optional[str] = Field(None, validation_alias=AliasChoices("ContactNo"))
    aadhar_no: Optional[str] = Field(None, validation_alias=AliasChoices("AadharNo"))
    nationality: Optional[str] = Field(None, validation_alias=AliasChoices("Nationality"))
    annual_income: Optional[str] = Field(None, validation_alias=AliasChoices("AnnualIncome"))
    address: Optional[str] = Field(None, validation_alias=AliasChoices("Address"))
    state: Optional[str] = Field(None, validation_alias=AliasChoices("State"))
    district: Optional[str] = Field(None, validation_alias=AliasChoices("District"))
    block: Optional[str] = Field(None, validation_alias=AliasChoices("Block"))
    pin_code: Optional[str] = Field(None, validation_alias=AliasChoices("PINCode"))
    social_category: Optional[str] = Field(None, validation_alias=AliasChoices("SocialCategory"))

    # domicile & residence
    domicile: Optional[str] = Field(None, validation_alias=AliasChoices("Domicile"))
    s_domicile_category: Optional[str] = Field(None, validation_alias=AliasChoices("S_DomicileCategory"))
    outside_odisha_applicant_state_name: Optional[str] = Field(None, validation_alias=AliasChoices("OutsideOdishaApplicantStateName"))
    odia_applicant_living_outside_odisha_state_name: Optional[str] = Field(None, validation_alias=AliasChoices("OdiaApplicantLivingOutsideOdishaStateName"))
    residence_barcode_number: Optional[str] = Field(None, validation_alias=AliasChoices("ResidenceBarcodeNumber"))

    # ExamSchoolAddress
    tenth_exam_school_address: Optional[str] = Field(None, validation_alias=AliasChoices("TengthExamSchoolAddress"))
    eighth_exam_school_address: Optional[str] = Field(None, validation_alias=AliasChoices("EighthExamSchoolAddress"))

    # qualification
    highest_qualification: Optional[str] = Field(None, validation_alias=AliasChoices("HighestQualification"))
    highest_qualification_exam_board: Optional[str] = Field(None,validation_alias=AliasChoices("HighestQualificationExamBoard"))
    board_exam_name_for_highest_qualification: Optional[str] = Field(
        None, validation_alias=AliasChoices("BoardExamNameforHighestQualification")
        )    
    examination_board_of_the_highest_qualification: Optional[str] = Field(None, validation_alias=AliasChoices("ExaminationBoardoftheHighestQualification"))

    examination_type: Optional[str] = Field(None, validation_alias=AliasChoices("ExaminationType"))
    year_of_passing: Optional[str] = Field(None, validation_alias=AliasChoices("YearofPassing"))
    roll_no: Optional[str] = Field(None, validation_alias=AliasChoices("RollNo"))
    total_marks: Optional[str] = Field(None, validation_alias=AliasChoices("TotalMarks"))
    secured_marks: Optional[str] = Field(None, validation_alias=AliasChoices("SecuredMarks"))
    percentage: Optional[str] = Field(None, validation_alias=AliasChoices("Percentage"))
    compartmental_status: Optional[str] = Field(None, validation_alias=AliasChoices("CompartmentalStatus"))

    # flags / categories 
    had_two_year_full_time_work_exp_after_tenth: Optional[str] = Field(
        None,
        validation_alias=AliasChoices("hadTwoYearFullTimeWorkExpAfterTength"),
    )
    gc: Optional[str] = Field(None, validation_alias=AliasChoices("GC"))
    ph: Optional[str] = Field(None, validation_alias=AliasChoices("PH"))
    es: Optional[str] = Field(None, validation_alias=AliasChoices("ES"))
    sports: Optional[str] = Field(None, validation_alias=AliasChoices("Sports"))
    national_cadet_corps: Optional[str] = Field(None, validation_alias=AliasChoices("NationalCadetCorps"))
    pm_care: Optional[str] = Field(None, validation_alias=AliasChoices("PMCare"))
    orphan: Optional[str] = Field(None, validation_alias=AliasChoices("Orphan"))
    income_barcode: Optional[str] = Field(None, validation_alias=AliasChoices("IncomeBarcode"))
    tfw: Optional[str] = Field(None, validation_alias=AliasChoices("TFW"))
    ews: Optional[str] = Field(None, validation_alias=AliasChoices("EWS"))
    boc: Optional[str] = Field(None, validation_alias=AliasChoices("BOC"))
    boc_regd_no: Optional[str] = Field(None, validation_alias=AliasChoices("BOCRegdNo"))

    # academic / institute mapping 
    course_name: Optional[str] = Field(None, validation_alias=AliasChoices("CourseName"))
    course_period: Optional[str] = Field(None, validation_alias=AliasChoices("CoursePeriod"))
    beauty_culture_type: Optional[str] = Field(None, validation_alias=AliasChoices("BeautyCultureType"))

    sams_code: Optional[str] = Field(None, validation_alias=AliasChoices("SAMSCode"))
    reported_institute: Optional[str] = Field(None, validation_alias=AliasChoices("ReportedInstitute"))
    reported_branch_or_trade: Optional[str] = Field(None, validation_alias=AliasChoices("ReportedBranchORTrade"))
    institute_district: Optional[str] = Field(None, validation_alias=AliasChoices("InstituteDistrict"))
    type_of_institute: Optional[str] = Field(None, validation_alias=AliasChoices("TypeofInstitute"))
    phase: Optional[str] = Field(None, validation_alias=AliasChoices("Phase"))
    year: Optional[str] = Field(None, validation_alias=AliasChoices("Year"))
    admission_status: Optional[str] = Field(None, validation_alias=AliasChoices("AdmissionStatus"))
    enrollment_status: Optional[str] = Field(None, validation_alias=AliasChoices("EnrollmentStatus"))


    applied_status: Optional[str] = Field(None, validation_alias=AliasChoices("AppliedStatus"))
    date_of_application: Optional[str] = Field(None, validation_alias=AliasChoices("DateOfApplication"))
    application_status: Optional[str] = Field(None, validation_alias=AliasChoices("ApplicationStatus"))
    registration_number: Optional[str] = Field(None, validation_alias=AliasChoices("RegistrationNumber"))

    # ETL-added fields 
    module: Optional[str] = Field(None, validation_alias=AliasChoices("Module"))
    academic_year: Optional[Union[int, str]] = Field(None, validation_alias=AliasChoices("AcademicYear"))    
    
    # JSON columns
    mark_data: Optional[List[Dict[str, Any]]] = Field(None, validation_alias=AliasChoices("MarkData"))
    option_data: Optional[List[Dict[str, Any]]] = Field(None, validation_alias=AliasChoices("OptionData"))

    # HSS / DEG JSON columns 
    hss_option_details: Optional[List[dict]] = Field(
        default=None,
        validation_alias=AliasChoices("HssOptionDetails", "HSSOptionDetails"),
    )
    hss_compartments: Optional[List[dict]] = Field(
        default=None,
        validation_alias=AliasChoices("hssCompartments", "HSSCompartments"),
    )
    deg_option_details: Optional[List[dict]] = Field(
        default=None,
        validation_alias=AliasChoices("DEGOptionDetails"),
    )
    deg_compartments: Optional[List[dict]] = Field(
        default=None,
        validation_alias=AliasChoices("DEGCompartments"),
    )

module_model_map = {
    'ITI': BaseStudentDB,
    'Diploma': BaseStudentDB,
    'PDIS': BaseStudentDB,
    'HSS': BaseStudentDB,
    'DEG': BaseStudentDB,
}
