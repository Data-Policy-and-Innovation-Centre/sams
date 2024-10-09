from sqlalchemy import create_engine, inspect, func, Column, Integer, String, JSON, Enum, ForeignKey, UniqueConstraint, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.exc import OperationalError, IntegrityError, DatabaseError
import pandas as pd
from tqdm import tqdm
import time
from loguru import logger
from sams.config import ERRMAX, RAW_DATA_DIR
from sams.etl.validate import check_null_values

Base = declarative_base()

# Define the Student ORM model
class Student(Base):
    __tablename__ = 'students'

    id = Column(Integer, primary_key=True, autoincrement=True)
    barcode = Column(String, nullable=False)
    student_name = Column(String, nullable=False)
    gender = Column(String, nullable=True)
    religion_name = Column(String, nullable=True)
    dob = Column(String, nullable=True)  # Date of Birth
    nationality = Column(String, nullable=True)
    annual_income = Column(String, nullable=True)
    address = Column(String, nullable=True)
    state = Column(String, nullable=True)
    district = Column(String, nullable=True)
    block = Column(String, nullable=True)
    pin_code = Column(String, nullable=True)
    social_category = Column(String, nullable=True)
    domicile = Column(String, nullable=True)
    s_domicile_category = Column(String, nullable=True)
    outside_odisha_applicant_state_name = Column(String, nullable=True)
    odia_applicant_living_outside_odisha_state_name = Column(String, nullable=True)
    residence_barcode_number = Column(String, nullable=True)
    tenth_exam_school_address = Column(String, nullable=True)
    eighth_exam_school_address = Column(String, nullable=True)
    highest_qualification = Column(String, nullable=True)
    had_two_year_full_time_work_exp_after_tenth = Column(String, nullable=True)  # Assume yes/no or boolean
    gc = Column(String, nullable=True)  # General category
    ph = Column(String, nullable=True)  # Physically Handicapped
    es = Column(String, nullable=True)  # Economically Weaker Section
    sports = Column(String, nullable=True)  # Participation in Sports
    national_cadet_corps = Column(String, nullable=True)
    pm_care = Column(String, nullable=True)
    orphan = Column(String, nullable=True)
    income_barcode = Column(String, nullable=True)
    tfw = Column(String, nullable=True)  # TFW: Tuition Fee Waiver
    ews = Column(String, nullable=True)  # Economically Weaker Section
    boc = Column(String, nullable=True)  # Backward Class
    boc_regd_no = Column(String, nullable=True)
    course_name = Column(String, nullable=True)
    course_period = Column(String, nullable=True)
    beauty_culture_type = Column(String, nullable=True)
    sams_code = Column(String, nullable=True)
    reported_institute = Column(String, nullable=True)
    reported_branch_or_trade = Column(String, nullable=True)
    institute_district = Column(String, nullable=True)
    typeof_institute = Column(String, nullable=True)
    phase = Column(String, nullable=False)
    year = Column(Integer, nullable=False)
    admission_status = Column(String, nullable=False)
    enrollment_status = Column(String, nullable=False)
    applied_status = Column(String, nullable=False)
    date_of_application = Column(String, nullable=True)
    application_status = Column(String, nullable=True)
    aadhar_no = Column(String, nullable=True)
    registration_number = Column(String, nullable=True)
    mark_data = Column(JSON, nullable=True)  # Could be JSON or a specific format
    module = Column(String, nullable=False)
    academic_year = Column(Integer, nullable=False)

    # Example of a unique constraint if needed
    __table_args__ = (
        UniqueConstraint('barcode', 'module', 'academic_year', 'applied_status','enrollment_status','admission_status', 'phase', 'year', name='uq_barcode_module_year'),
    )

class Institute(Base):
    __tablename__ = 'institutes'

    # Primary columns
    id = Column(Integer, primary_key=True, autoincrement=True)
    sams_code = Column(Integer,nullable=False)
    year = Column(Integer, nullable=False)
    module = Column(Enum("ITI", "Diploma", "PDIS"), nullable=False)
    name = Column(String)
    funding_source = Column(Enum("Govt.", "Pvt."))

    # Nested columns
    strength = Column(JSON)
    cutoff = Column(JSON)

    # Polymorphic identity for inheritance
    __mapper_args__ = {
        'polymorphic_on': module
    }

class ITI(Institute):
    __tablename__ = 'itis'
    id = Column(Integer, ForeignKey('institutes.id'), primary_key=True)
    trade = Column(String)  # Trade is specific to non-PDIS

    __mapper_args__ = {
        'polymorphic_identity': 'ITI',
    }

class Diploma(Institute):
    __tablename__ = 'diplomas'
    id = Column(Integer, ForeignKey('institutes.id'), primary_key=True)
    trade = Column(String)  # Trade is specific to non-PDIS

    __mapper_args__ = {
        'polymorphic_identity': 'Diploma',
    }

class PDIS(Institute):
    __tablename__ = 'pdis'
    id = Column(Integer, ForeignKey('institutes.id'), primary_key=True)

    __mapper_args__ = {
        'polymorphic_identity': 'PDIS',
    }


class SamsDataLoader:
    def __init__(self, db_url):
        """
        Initialize the SamsDataLoader.

        Args:
            db_url (str): The SQL Alchemy database URL.

        Returns:
            None
        """
        self.engine = create_engine(db_url, echo=False, pool_size=20, max_overflow=10)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def load_institute_data(self, institute_data):
        """
        Loads the given institute data into the database.

        Args:
            institute_data (list): List of dictionaries containing institute data.

        Returns:
            None
        """
        with tqdm(total=len(institute_data), desc="Loading institute data") as pbar:
            for data in institute_data:
                self._add_institute(data)
                pbar.update(1)

    def load_student_data(self, student_data):
        """
        Loads the given student data into the database.

        Args:
            student_data (list): List of dictionaries containing student data.

        Returns:
            None
        """
        with tqdm(total=len(student_data), desc="Loading student data") as pbar:
            for data in student_data:
                
                # Check for nulls in variables within the uniqueness constraint of the table
                nulls = check_null_values(data)
                if nulls:
                    logger.warning(f"Nulls in unique constraint for {data['Barcode']} - {data['module']} - {data['academic_year']}")
                    continue
                
                # Try to add row to table
                success = self._add_student(data)
                if success:
                    pbar.update(1)

    def _add_student(self, data):
        """
        Adds the given student data to the database.

        Args:
            data (dict): Dictionary containing student data.

        Returns:
            bool: True if student data was successfully added, False otherwise.
        """
        success = True

        session = self.Session()
        try:
            student = Student(
                barcode=data['Barcode'],
                student_name=data['StudentName'],
                gender=data['Gender'],
                dob=data['DOB'],
                nationality=data['Nationality'],
                annual_income=data['AnnualIncome'],
                address=data['Address'],
                state=data['State'],
                district=data['District'],
                block=data['Block'],
                pin_code=data['PINCode'],
                social_category=data['SocialCategory'],             
                religion_name=data['ReligionName'],
                domicile=data['Domicile'],
                s_domicile_category=data['S_DomicileCategory'],
                outside_odisha_applicant_state_name=data['OutsideOdishaApplicantStateName'],
                odia_applicant_living_outside_odisha_state_name=data['OdiaApplicantLivingOutsideOdishaStateName'],
                residence_barcode_number=data['ResidenceBarcodeNumber'],
                tenth_exam_school_address=data['TengthExamSchoolAddress'],
                eighth_exam_school_address=data['EighthExamSchoolAddress'],
                highest_qualification=data['HighestQualification'],
                had_two_year_full_time_work_exp_after_tenth=data['hadTwoYearFullTimeWorkExpAfterTength'],
                gc=data['GC'],
                ph=data['PH'],
                es=data['ES'],
                sports=data['Sports'],
                national_cadet_corps=data['NationalCadetCorps'],
                pm_care=data['PMCare'],
                orphan=data['Orphan'],
                income_barcode=data['IncomeBarcode'],
                tfw=data['TFW'],
                ews=data['EWS'],
                boc=data['BOC'],
                boc_regd_no=data['BOCRegdNo'],
                course_name=data['CourseName'],
                course_period=data['CoursePeriod'],
                beauty_culture_type=data['BeautyCultureType'],
                sams_code=data['SAMSCode'],
                reported_institute=data['ReportedInstitute'],
                reported_branch_or_trade=data['ReportedBranchORTrade'],
                institute_district=data['InstituteDistrict'],
                typeof_institute=data['TypeofInstitute'],
                phase=data['Phase'],
                year=data['Year'],
                admission_status=data['AdmissionStatus'],
                enrollment_status=data['EnrollmentStatus'],
                applied_status=data['AppliedStatus'],
                date_of_application=data['DateOfApplication'],
                application_status=data['ApplicationStatus'],
                aadhar_no=data['AadharNo'],
                registration_number=data['RegistrationNumber'],
                mark_data=data['MarkData'],
                module=data['module'],
                academic_year=data['academic_year'],

            )
            session.add(student)
            session.commit()

        except IntegrityError as e:
            session.rollback()
            logger.warning(f"Skipping duplicate student: {data['Barcode']} - {data['module']} - {data['academic_year']}")
            success = False
        except Exception as e:
            if 'database is locked' in str(e):
                time.sleep(1)
            else:
                session.rollback()
                logger.error(f"Error adding student: {e}")
                success = False
        finally:
            session.close()
            return success 

    def _add_institute(self, data):
        session = self.Session()
        try:
            institute = Institute(
                sams_code=data['SAMSCode'],
                year=int(data['academic_year']),
                module=data['module'],
                name=data['InstituteName'],
                funding_source=data['TypeofInstitute'] ,         
                
                # Add other columns as needed
                strength=data['strength'],
                cutoff=data['cuttoff']
            )
            session.add(institute)
            session.commit()
        except Exception as e:
            if 'database is locked' in str(e):
                time.sleep(1)
            if 'UNIQUE constraint failed' in str(e):
                session.rollback()
                logger.warning(f"Skipping duplicate institute: {data['SAMSCode']}")
            else:
                session.rollback()
                logger.error(f"Error adding institute: {e}")
        finally:
            session.close()

    def remove(self, table_name: str, module:str, year:str, admission_type:int = None) -> None:
        """
        Removes all records from the given table that correspond to the given module,
        year and (if table is "institutes" and module is "Diploma") admission_type.
        """
        session = self.Session()
        try:
            if table_name == "institutes":
                if module == "Diploma":
                    session.query(Institute).filter_by(module=module, year=year, admission_type=admission_type).delete()
                else:
                    session.query(Institute).filter_by(module=module, year=year).delete()
            else:
                session.query(Student).filter_by(module=module, year=year).delete()
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Error removing records from {table_name}: {e}")
        finally:
            session.close()


class SamsDataLoaderPandas(SamsDataLoader):
    def __init__(self, db_url):
        super().__init__(db_url)

    def load_data(self, data: pd.DataFrame, table_name: str) -> None:
        """
        Loads data from a pandas dataframe into a table in the database.

        Args:
            data (pd.DataFrame): The data to load into the database.
            table_name (str): The name of the table to load the data into.

        Returns:
            None
        """
        num_retries = 0

        while num_retries < ERRMAX:
            try:
                data.to_sql(table_name, con=self.engine, if_exists='append', index=False)
                break
            except IntegrityError as e:
                logger.error(f"Error loading data into {table_name}: {e}")
                break
            except (DatabaseError, OperationalError) as e:
                logger.error(f"Error loading data into {table_name}: {e}")
                logger.info(f"Retrying ({num_retries+1}/{ERRMAX})...")
                num_retries += 1
                time.sleep(1)
            
def main():

    loader = SamsDataLoaderPandas(f"sqlite:///{RAW_DATA_DIR}/sams.db")
    loader.remove("students", "ITI", "2017")
    # loader.remove("students", "Diploma", "2019")


if __name__ == "__main__":
    main()
    