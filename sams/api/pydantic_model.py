from pydantic import BaseModel
from typing import List, Optional

# SHARED BASE MODEL (COMMON FIELDS)

class BaseStudent(BaseModel):
    Barcode: str
    StudentName: str
    Gender: str
    ReligionName: Optional[str]
    DOB: Optional[str]
    ContactNo: Optional[str]
    AadharNo: Optional[str]
    Nationality: Optional[str]
    AnnualIncome: Optional[str]
    Address: Optional[str]
    State: Optional[str]
    District: Optional[str]
    Block: Optional[str]
    PINCode: Optional[str]
    SocialCategory: Optional[str]
    HighestQualification: Optional[str]
    ExaminationBoardoftheHighestQualification: Optional[str]
    BoardExamNameforHighestQualification: Optional[str]
    ExaminationType: Optional[str]
    YearofPassing: Optional[str]
    RollNo: Optional[str]
    TotalMarks: Optional[str]
    SecuredMarks: Optional[str]
    Percentage: Optional[str]
    CompartmentalStatus: Optional[str]
    PH: Optional[str]
    ES: Optional[str]
    Sports: Optional[str]
    NationalCadetCorps: Optional[str]
    Orphan: Optional[str]

# PDIS MODULE
class PDISOptionDetail(BaseModel):
    InstituteName: str
    SAMSCode: str
    TradeName: str
    TradeCode: str
    InstituteDistrict: str
    InstituteBlock: str
    Phase: str
    Year: str
    AdmissionStatus: str
    OptionNo: str

class PDISStudent(BaseStudent):
    PDISOptionDetails: List[PDISOptionDetail]
    
# DIPLOMA MODULE
class DiplomaOptionDetail(BaseModel):
    InstituteName: str
    SAMSCode: str
    TradeName: str
    TradeCode: str
    InstituteDistrict: str
    InstituteBlock: str
    Phase: str
    Year: str
    AdmissionStatus: str
    OptionNo: str

class DiplomaStudent(BaseStudent):
    AdmissionType: Optional[str]
    DiplomaOptionDetails: List[DiplomaOptionDetail]

# ITI MODULE
class ITIOptionDetail(BaseModel):
    ITIName: str
    TradeName: str
    TradeCode: str
    InstituteDistrict: str
    InstituteBlock: str
    SAMSCode: str
    AdmissionStatus: str
    Phase: str
    Year: str
    OptionNo: str

class ITIStudent(BaseStudent):
    ITIOptionDetails: List[ITIOptionDetail]

# HSS MODULE (+2)
class HSSOptionDetail(BaseModel):
    StreamAppliedFor: str
    AdmissionStatus: str
    SubjectName: str
    InstituteName: str
    InstituteDistrict: str
    InstituteBlock: str
    Phase: str
    OptionNo: str
    Year: str
    SAMSCode: str

class HSSStudent(BaseStudent):
    HSSOptionDetails: List[HSSOptionDetail]

# DEG MODULE
class DEGOptionDetail(BaseModel):
    ReportedInstitute: str
    SAMSCode: str
    Stream: str
    Subject: str
    InstituteDistrict: str
    InstituteBlock: str
    TypeofInstitute: str
    Phase: str
    Year: str
    AdmissionStatus: str
    OptionNo: str

class DEGCompartment(BaseModel):
    COMPSubject: str
    COMPFailMark: str
    COMPPassMark: str

class DEGStudent(BaseStudent):
    DEGOptionDetails: List[DEGOptionDetail]
    DEGCompartments: List[DEGCompartment]

