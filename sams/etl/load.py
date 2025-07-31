from sqlalchemy import (
    create_engine,
    inspect,
    func,
    Column,
    Integer,
    String,
    JSON,
    Enum,
    ForeignKey,
    UniqueConstraint,
    DateTime,
    Float,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, IntegrityError, DatabaseError
import pandas as pd
import json
import sys
from tqdm import tqdm
import time
from loguru import logger
from sams.etl.extract import SamsDataDownloader 
from sams.config import ERRMAX, RAW_DATA_DIR, LOGS
from sams.utils import (
    dict_camel_to_snake_case,
    find_null_column,
    stop_logging_to_console,
    resume_logging_to_console,
)
import os
from numpy import nan
import warnings

warnings.filterwarnings("ignore")

Base = declarative_base()


# Define the Student ORM model
class Student(Base):
    __tablename__ = "students"

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
    highest_qualification_exam_board = Column(String, nullable=True)
    board_exam_name_for_highest_qualification = Column(String, nullable=True)
    highest_qualification = Column(String, nullable=True)
    had_two_year_full_time_work_exp_after_tenth = Column(
        String, nullable=True
    )  # Assume yes/no or boolean
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
    type_of_institute = Column(String, nullable=True)
    phase = Column(String, nullable=True)
    year = Column(Integer, nullable=False)
    admission_status = Column(String, nullable=True)
    enrollment_status = Column(String, nullable=True)
    applied_status = Column(String, nullable=True)
    date_of_application = Column(String, nullable=True)
    application_status = Column(String, nullable=True)
    aadhar_no = Column(String, nullable=True)
    registration_number = Column(String, nullable=True)
    mark_data = Column(JSON, nullable=True)  # Could be JSON or a specific format
    module = Column(Enum("ITI", "Diploma", "PDIS", "HSS"), nullable=False)
    academic_year = Column(Integer, nullable=False)
    contact_no = Column(String, nullable=True)
    option_data = Column(JSON, nullable=True)

    # new columns for HSS
    examination_board_of_the_highest_qualification = Column(String, nullable=True)
    board_exam_name_for_highest_qualification = Column(String, nullable=True)
    examination_type = Column(String, nullable=True)
    year_of_passing = Column(String, nullable=True)
    roll_no = Column(String, nullable=True)
    total_marks = Column(String, nullable=True)
    secured_marks = Column(String, nullable=True)
    percentage = Column(String, nullable=True)
    compartmental_status = Column(String, nullable=True)

    hss_option_details = Column(JSON, nullable=True)
    hss_compartments = Column(JSON, nullable=True)


    # Example of a unique constraint if needed
    __table_args__ = (
        UniqueConstraint(
            "barcode",
            "module",
            "academic_year",
            "applied_status",
            "enrollment_status",
            "admission_status",
            "phase",
            "year",
            name="uq_barcode_module_year",
        ),
    )


class Institute(Base):
    __tablename__ = "institutes"

    # Primary columns
    id = Column(Integer, primary_key=True, autoincrement=True)
    sams_code = Column(String, nullable=False)
    ncvtmis_code = Column(String, nullable=True)
    academic_year = Column(Integer, nullable=False)
    module = Column(Enum("ITI", "Diploma", "PDIS"), nullable=False)
    institute_name = Column(String, nullable=False)
    type_of_institute = Column(Enum("Govt.", "Pvt."), nullable=False)
    admission_type = Column(Integer, nullable=True)
    branch = Column(String, nullable=True)
    trade = Column(String, nullable=True)

    # Nested columns
    strength = Column(JSON)
    cutoff = Column(JSON)
    enrollment  = Column(JSON)

    __table_args__ = (
        UniqueConstraint(
            "sams_code",
            "module",
            "academic_year",
            "trade",
            "branch",
            "admission_type",
            name="uq_sams_code_module_academic_year_trade_branch_admission_type",
        ),
    )


class SamsDataLoader:
    def __init__(self, db_url):
        """
        Initialize the SamsDataLoader.

        Args:
            db_url (str): The SQL Alchemy database URL.

        Returns:
            None
        """
        if db_url.startswith("sqlite"):
            self.engine = create_engine(db_url, echo = False)
        else:
            self.engine = create_engine(db_url, echo=False, pool_size=20, max_overflow=10)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def load(self, data: list, table_name: str):
        """
        Loads the given data into the database.

        Args:
            data (list): List of dictionaries containing data.
            table_name (str): The name of the table to load the data into.

        Returns:
            None
        """
        if table_name not in ["institutes", "students"]:
            raise ValueError(f"Invalid table name: {table_name}")

        with tqdm(total=len(data), desc=f"Loading {table_name} data") as pbar:
            for unit in data:
                # Try to add row to table
                success = self._add_data(unit, table_name)
                if success:
                    pbar.update(1)

    def bulk_load(self, data: list, table_name: str):
        """
        Adds the given student data to the database in bulk.

        Args:
            data (list): List of dictionaries containing student data.
            table_name (str): The name of the table to load the data into.

        Returns:
            None 
        """
        session = self.Session()

        data = [dict_camel_to_snake_case(unit) for unit in data]
        # If module is HSS, rename fields
        HSS_RENAME_FIELDS = {
            'yearof_passing': 'year_of_passing',
            'examination_boardofthe_highest_qualification': 'examination_board_of_the_highest_qualification',
            'board_exam_namefor_highest_qualification':'board_exam_name_for_highest_qualification'
        }
        # Rename HSS-specific fields to match database schema
        for unit in data:
            module_name = unit.get('module')
            if module_name == 'HSS':
                for old_key, new_key in HSS_RENAME_FIELDS.items():
                    if old_key in unit:
                        unit[new_key] = unit.pop(old_key)
                # Add HSS-specific defaults if needed
                if unit.get('year') is None:
                    unit['year'] = 0

        if table_name == "students":
            Unit = Student
        elif table_name == "institutes":
            Unit = Institute
        else:
            raise ValueError(f"Invalid table name: {table_name}")
        
        # Optimization: skip bulk for HSS students (bulk_save_objects has problems with JSON + Enum)

        if table_name == "students" and any(unit.get("module") == "HSS" for unit in data):
            print("HSS data detected — using individual inserts.")
            self.load(data, table_name)
            return
    
        with tqdm(total=len(data), desc=f"Loading {table_name} data in bulk") as pbar:
            try:
                session.bulk_save_objects([Unit(**unit) for unit in data])
                session.commit()
                pbar.update(len(data))

            except (OperationalError, IntegrityError, DatabaseError) as e:
                resume_logging_to_console()
                logger.error(
                    f"Error while adding data in bulk - will try adding individually!"
                )
                stop_logging_to_console(
                    os.path.join(LOGS, f"{table_name}_data_download.log")
                )
                session.rollback()
                self.load(data, table_name)
            finally:
                session.close()

    def _add_data(self, data: dict, table_name: str) -> bool:
        """
        Adds the given data to the database.

        Args:
            data (dict): Dictionary containing data.
            table_name (str): The name of the table to load the data into.

        Returns:
            bool: True if data was successfully added, False otherwise.
        """
        if table_name == "students":
            return self._add_student(data)
        else:
            return self._add_institute(data)

    def _add_student(self, data: dict) -> bool:
        """
        Adds the given student data to the database.

        Args:
            data (dict): Dictionary containing student data.
            table_name (str): The name of the table to load the data into.

        Returns:
            bool: True if student data was successfully added, False otherwise.
        """
        if not isinstance(data, dict):
            raise TypeError("Data must be a dictionary")
        
        
        data = dict_camel_to_snake_case(data)
        
        session = self.Session()
        success = False
        try:
            student = Student(**data)
            session.add(student)
            session.commit()
            success = True
        except IntegrityError as e:
            session.rollback()
            if "UNIQUE constraint failed" in str(e):
                logger.warning(
                    f"Skipping duplicate student: {data['barcode']} - {data['module']} - {data['academic_year']}"
                )
            elif "NOT NULL constraint failed" in str(e):
                logger.warning(
                    f"Skipping student: {data['barcode']} - {data['module']} - {data['academic_year']} due to null value in '{find_null_column(str(e))}' "
                )
            else:
                logger.error(f"Error adding student: {e}")
            success = False
        except Exception as e:
            if "database is locked" in str(e):
                time.sleep(1)
                success = self._add_student(data)
            else:
                session.rollback()
                logger.error(f"Error adding student: {e}")
                success = False
        finally:
            session.close()
            return success

    def _add_institute(self, data: dict) -> bool:
        if not isinstance(data, dict):
            raise TypeError("Data must be a dictionary")

        session = self.Session()
        data = dict_camel_to_snake_case(data)
        success = False

        try:
            institute = Institute(**data)
            session.add(institute)
            session.commit()
            success = True
        except IntegrityError as e:
            if "UNIQUE constraint failed" in str(e):
                session.rollback()
                logger.warning(f"Skipping duplicate institute: {data['SAMSCode']}")
            elif "NOT NULL constraint failed" in str(e):
                session.rollback()
                logger.warning(
                    f"Skipping institute: {data['SAMSCode']} due to null value in "
                )
            else:
                session.rollback()
                logger.error(f"Error adding institute: {e}")
        except Exception as e:
            if "database is locked" in str(e):
                time.sleep(1)
            else:
                session.rollback()
                logger.error(f"Error adding institute: {e}")

        finally:
            session.close()
            return success

    def get_existing_modules(self, table_name: str) -> list:
        if table_name not in ["students", "institutes"]:
            raise ValueError(f"Table name not supported: {table_name}")

        if table_name == "students":
            return self._get_student_modules()
        else:
            return self._get_institute_modules()

    def _get_student_modules(self):
        session = self.Session()
        expected_counts = self._get_counts("students")

        student_modules = (
            session.query(Student.module, Student.academic_year, func.count(Student.id))
            .group_by(Student.module, Student.academic_year)
            .all()
        )

        existing_modules = [
            (module, year)
            for module, year, count in student_modules
            if count
            >= expected_counts.loc[
                (expected_counts["module"] == module)
                & (expected_counts["academic_year"] == year),
                "count",
            ].iloc[0]
        ]

        excess_modules = [
            (module, year, count)
            for module, year, count in student_modules
            if count
            > expected_counts.loc[
                (expected_counts["module"] == module)
                & (expected_counts["academic_year"] == year),
                "count",
            ].iloc[0]
        ]

        if excess_modules:
            logger.warning(
                f"Modules with excess records than expected found: {excess_modules}"
            )

        return existing_modules

    def _get_institute_modules(self):
        """
        Retrieves institute module records grouped by module, academic year, and admission type,
        compares their counts with expected counts, and identifies modules with excess records.

        Returns:
            list of tuple: A list of tuples (module, academic_year, admission_type) for which
            the actual count is greater than or equal to the expected count.

        Side Effects:
            Logs a warning if any modules have more records than expected.

        Notes:
            - Uses SQLAlchemy session to query the Institute table.
            - Uses pandas DataFrame for data manipulation and comparison.
            - Replaces NaN and None values with 0 in the counts.
        """
        session = self.Session()
        counts = self._get_counts("institutes")
        counts.replace({nan: 0}, inplace=True)

        institute_modules = (
            session.query(
                Institute.module,
                Institute.academic_year,
                Institute.admission_type,
                func.count(Institute.id),
            )
            .group_by(
                Institute.module, Institute.academic_year, Institute.admission_type
            )
            .all()
        )
        institute_modules = pd.DataFrame(
            institute_modules,
            columns=["module", "academic_year", "admission_type", "count"],
        )
        institute_modules.replace({nan: 0, None: 0}, inplace=True)

        existing_modules = [
            (module, year, admission_type)
            for module, year, admission_type, count in institute_modules.itertuples(
                index=False
            )
            if count
            >= counts.loc[
                (counts["module"] == module)
                & (counts["academic_year"] == year)
                & (counts["admission_type"] == admission_type),
                "count",
            ].iloc[0]
        ]

        excess_modules = [
            (module, year, admission_type, count)
            for module, year, admission_type, count in institute_modules.itertuples(
                index=False
            )
            if count
            > counts.loc[
                (counts["module"] == module)
                & (counts["academic_year"] == year)
                & (counts["admission_type"] == admission_type),
                "count",
            ].iloc[0]
        ]

        if excess_modules:
            logger.warning(
                f"Modules with excess records than expected found: {excess_modules}"
            )

        return existing_modules

    def _get_counts(self, table_name: str) -> pd.DataFrame:
        counts_path = os.path.join(LOGS, f"{table_name}_count.csv")

        if not os.path.exists(counts_path):
            raise FileNotFoundError(
                f"{counts_path} not found! Please run update_total_records() first to generate it."
            )

        counts = pd.read_csv(counts_path)

        return counts


    def remove(
        self, table_name: str, module: str, year: str, admission_type: int = None
    ) -> None:
        """
        Removes all records from the given table that correspond to the given module,
        year and (if table is "institutes" and module is "Diploma") admission_type.
        """
        session = self.Session()
        try:
            if table_name == "institutes":
                if module == "Diploma":
                    session.query(Institute).filter_by(
                        module=module, academic_year=year, admission_type=admission_type
                    ).delete()
                else:
                    session.query(Institute).filter_by(
                        module=module, academic_year=year
                    ).delete()
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
                data.to_sql(
                    table_name, con=self.engine, if_exists="append", index=False
                )
                break
            except IntegrityError as e:

                logger.error(f"Error loading data into {table_name}: {e}")
                break
            except (DatabaseError, OperationalError) as e:
                logger.error(f"Error loading data into {table_name}: {e}")
                logger.info(f"Retrying ({num_retries+1}/{ERRMAX})...")
                num_retries += 1
                time.sleep(1)


# def main():
#     loader = SamsDataLoaderPandas(f"sqlite:///{RAW_DATA_DIR}/sams.db")
#     downloader = SamsDataDownloader() 

#     # See what is in the DB already
#     # print(loader.get_existing_modules("students"))

#     loader.remove("students", "HSS", "2022")

#     # Load HSS data into DB
#     df_hss_students = downloader.fetch_students("HSS", 2022, page_number=1, pandify=True)
#     loader.bulk_data(df_hss_students, "students")
#     print("Loaded HSS 2022 into DB")

# if __name__ == "__main__":
#     main()

CHECKPOINT_FILE = 'sams/etl/checkpoint.json'
LOG_FILE = 'sams/etl/hss_load_log.txt'

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_checkpoint(checkpoint):
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f)

def main():
    db_url = f"sqlite:///{RAW_DATA_DIR}/sams.db"
    loader = SamsDataLoader(db_url)
    downloader = SamsDataDownloader()

    target_module = "HSS"
    target_years = [2018]
    checkpoint_every = 10

    checkpoint = load_checkpoint()

    for year in target_years:
        last_page = checkpoint.get(str(year), 0)
        current_page = last_page + 1
        total_records_saved = 0

        print(f"\nResuming {target_module} {year} from Page {current_page}...")

        while True:
            try:
                data = downloader.fetch_students(target_module, year, page_number=current_page, pandify=False)

                if not data:
                    print(f"Page {current_page}: No more data. Stopping.")
                    break

                loader.bulk_load(data, "students")
                total_records_saved += len(data)

                msg = f"{year} Page {current_page}: {len(data)} records saved (Total so far: {total_records_saved})"
                print(msg)

                with open(LOG_FILE, "a") as logf:
                    logf.write(msg + "\n")

                # Save checkpoint every N pages
                if current_page % checkpoint_every == 0:
                    checkpoint[str(year)] = current_page
                    save_checkpoint(checkpoint)

                current_page += 1

            except Exception as e:
                print(f"Page {current_page}: ERROR — {e}")
                break

        checkpoint[str(year)] = current_page - 1
        save_checkpoint(checkpoint)

        msg = f"\n Batch done for {year}. Last page saved: {current_page - 1}, Total records saved: {total_records_saved}\n"
        print(msg)
        with open(LOG_FILE, "a") as logf:
            logf.write(msg + "\n")

    print(" All done.")

if __name__ == "__main__":
    main()