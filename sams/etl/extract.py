from api.client import SAMSClient
from api.exceptions import APIError
from requests import HTTPError
import os
from pathlib import Path
import json
from collections import Counter
from loguru import logger
from config import API_AUTH, RAW_DATA_DIR, ERRMAX, STUDENT, INSTITUTE, SOF, LOGS, NUM_TOTAL_RECORDS
from util import is_valid_date
from tqdm import tqdm
import pprint
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

class SamsDataDownloader:
    def __init__(self):
        self.api_client = SAMSClient(API_AUTH)
        self.executor = ThreadPoolExecutor(max_workers=10)

    def fetch_students_iti_diploma(self, academic_year: int, module: str, source_of_fund: int) -> list:
        """
        Fetches student data from the SAMS API for the given academic year,
        module and source of fund.

        Args:
            academic_year (int): The academic year for which to fetch the data.
            module (str): The module for which to fetch the data.
            source_of_fund (int): The source of fund for which to fetch the data.

        Returns:
            list: A list of dictionaries, where each dictionary represents a student.
        """

        if module not in ["ITI", "Diploma"]:
            logger.error(f"Module {module} is not supported in this method. It must be 'ITI' or 'Diploma'. ")   
            raise ValueError("Module must be either 'ITI' or 'Diploma'. ")
        
        if academic_year < STUDENT[module]['yearmin'] or academic_year > STUDENT[module]['yearmax']:
            logger.warning(f"Data from Academic year {academic_year} is not available for {module}. It must be between {STUDENT[module]['yearmin']} and {STUDENT[module]['yearmax']}. ")
            academic_year = min(STUDENT[module]['yearmax'], max(STUDENT[module]['yearmin'], academic_year))
            logger.warning(f"Adjusting year to {academic_year} for {module}. ")
         
        # Initialize the error count
        errcount = 1
        
        # Initialize the page number
        page = 1
        
        # Initialize the data list
        data = []
        
        # Continue fetching data until there is no more data
        logger.info(f"Fetching all pages for {SOF['tostring'][source_of_fund]} {module} student data for applications in {academic_year}... ")
        while True:
            # Try to fetch a page of data
            try:
                page_data = self.api_client.get_student_data(academic_year=academic_year, 
                    source_of_fund=source_of_fund, module=module, page_number=page)
           
            # If there is an API or HTTP error, log the error and attempt again
            except (APIError,HTTPError) as e:
                if errcount >= ERRMAX:
                    logger.critical("Too many errors. Exiting loop")
                    break
                logger.error(e)
                logger.warning(f"Attempting to continue with page {page}.")
                errcount +=1
                continue
            
            # If there is no more data, log the total page count and total records
            if not page_data:
                try:
                    logger.info(f"\nAll pages downloaded\n. Total page count: {page-1}\n. Total fields: {len(data[0])}\n. Total records: {len(data)}\n\n\n\n")
                except IndexError:
                    logger.warning(f"\nNo data was downloaded!!\n. Total page count: {page-1}\n. Total records: {len(data)}\n\n\n\n")
                break
            
            # Add the data to the list
            data.extend(page_data)
            
            # Reset the error count
            errcount = 0
            
            # Increment the page number
            page += 1

        # Return the list of data
        return data
    
    def fetch_students_pdis(self, academic_year: int) -> list:
        """
        Fetches PDIS student data from the SAMS API for the given academic year.

        Args:
            academic_year (int): The academic year for which to fetch the data.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, where each dictionary represents a student.
        """

        # Define the minimum and maximum academic years for PDIS
        year_min = STUDENT['PDIS']['yearmin']
        year_max = STUDENT['PDIS']['yearmax']

        # If the academic year is outside the accepted range, adjust it
        if academic_year < year_min or academic_year > year_max:
            academic_year = min(year_max, max(year_min, academic_year))
            logger.warning(f"Data from Academic year {academic_year} is not available for PDIS. It must be between {STUDENT['PDIS']['yearmin']} and {STUDENT['PDIS']['yearmax']}. ")
            logger.warning(f"Adjusting year to {academic_year} for PDIS.")

        # Initialize the error count
        error_count = 0

        # Log the start of the fetching process
        logger.info(f"Fetching PDIS student data for applications in {academic_year}...")

        students = []

        # Continue fetching data until there is no more data
        while True:
            # Try to fetch a page of data
            try:
                # Get the student data from the SAMS API
                page_data = self.api_client.get_student_data(module="PDIS", academic_year=academic_year)

                # If there is data, add it to the list of students
                if page_data:
                    students.extend(page_data)
                # If there is no data, log a warning and break the loop
                else:
                    logger.warning(f"No data was fetched for academic year {academic_year} and module PDIS.")
                    break
            # If there is an API or HTTP error, log the error and attempt again
            except (APIError, HTTPError) as e:
                error_count += 1
                if error_count >= ERRMAX:
                    logger.critical("Too many errors. Exiting loop.")
                    break
                logger.warning(f"Attempting again. Error: {e}")
                continue

            # If there is no more data, log the field count and total records
            logger.info(f"\nPDIS data fetched\n. Total fields: {len(students[0])}\n. Total records: {len(students)}\n\n\n\n")
            break

        # Return the list of students
        return students

    def download_all_student_data(self) -> list:
        """
        Downloads student data from SAMS API for all modules and years.

        Returns:
            list: A list of dictionaries, where each dictionary represents a student.
        """
        # Remove all existing log handlers
        for handler_id in list(logger._core.handlers.keys()):
            logger.remove(handler_id)
        
        # Add a new log handler for downloading student data
        log_file_id = logger.add(
            os.path.join(LOGS, "data_download.log"), mode='w',
            format="{time} {level} {message}", level="INFO"
        )

        student_data = []
        futures = []
        
        # Progress bar for downloading student data
        with tqdm(total=NUM_TOTAL_RECORDS, desc="Downloading student data") as pbar:
            for module, metadata in STUDENT.items():
                for year in range(metadata["yearmin"], metadata["yearmax"] + 1):
                    if module == "PDIS":
                        future = self.executor.submit(self.fetch_students_pdis, year)
                        futures.append(future)
                    else:
                        future_1 = self.executor.submit(self.fetch_students_iti_diploma, year, module, 1)
                        future_5 = self.executor.submit(self.fetch_students_iti_diploma, year, module, 5)
                        futures.extend([future_1, future_5])
                    

            for future in as_completed(futures):
                data = future.result()
                student_data.extend(data)
                pbar.update(len(data))

        # Remove the log handler for downloading student data
        logger.remove(log_file_id)

        # Add a new log handler for validation
        logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True)

        # Validate the fetched student data
        self.validate_student_data(student_data)

        return student_data

    def download_all_institute_data(self) -> list:
        pass

    
    # def download_missing_student_data(self, db: Session) -> list:
    #     """Downloads student data that is not already in the database for all modules and years."""

    #     student_data = []
    #     for module, metadata in STUDENT.items():
    #         for year in range(metadata["yearmin"], metadata["yearmax"] + 1):
    #             if module == "PDIS":
    #                 existing = db.query(Student).filter_by(module=module, year=year).all()
    #                 if not existing:
    #                     student_data.extend(self.fetch_students_pdis(year))
    #             else:
    #                 existing_1 = db.query(Student).filter_by(module=module, year=year, source_of_fund=1).all()
    #                 existing_5 = db.query(Student).filter_by(module=module, year=year, source_of_fund=5).all()
    #                 if not existing_1:
    #                     student_data.extend(self.fetch_students_iti_diploma(year, module, 1))
    #                 if not existing_5:
    #                     student_data.extend(self.fetch_students_iti_diploma(year, module, 5))
        
    #     self.validate_student_data(student_data)
    #     return student_data
    
    def validate_student_data(self, data: list) -> None:
        # Remove all handlers
        for handler_id in list(logger._core.handlers.keys()):
            logger.remove(handler_id)

        required_keys = ["Barcode", "StudentName", "Gender", "DOB", "ReligionName", "Nationality", "AnnualIncome", "Address", "State", "District", "Block",
                         "PINCode", "SocialCategory", "Domicile", "S_DomicileCategory", "OutsideOdishaApplicantStateName",
                         "OdiaApplicantLivingOutsideOdishaStateName", "ResidenceBarcodeNumber", "TengthExamSchoolAddress", "EighthExamSchoolAddress",
                         "HighestQualification", "HighestQualificationExamBoard", "HighestQualificationBoardExamName", "ExaminationType", "YearofPassing",
                         "RollNo", "TotalMarks", "SecuredMarks", "Percentage", "CompartmentalStatus", "CompartmentalFailMark", "SubjectWiseMarks", "hadTwoYearFullTimeWorkExpAfterTength",
                         "GC", "PH", "ES", "Sports", "NationalCadetCorps", "PMCare", "Orphan", "IncomeBarcode", "TFW", "EWS", "BOC", "BOCRegdNo", "CourseName", "CoursePeriod",
                         "BeautyCultureType", "ReportedInstitute", "ReportedBranchORTrade", "InstituteDistrict", "TypeofInstitute", "Phase", "Year", "AdmissionStatus", "EnrollmentStatus"]

        total = len(data)
        chunk_size = 1000  # Adjust this based on your needs and available memory

        def validate_chunk(chunk):
            count_missing = Counter()
            for i, record in enumerate(chunk, start=1):
                if not all(key in record for key in required_keys):
                    logger.error(f"Record {i} is missing required keys")
                    logger.debug(f"Record {i} has extra keys: {set(record.keys()) - set(required_keys)}")
                    continue

                if record['Barcode'] in ["NA", None, "-", "--"]:
                    #logger.error(f"Record {i} has missing or invalid barcode.")
                    count_missing['Barcode'] += 1

                if not is_valid_date(record["DOB"]):
                    #logger.error(f"Record {i} has invalid date of birth.")
                    count_missing['DOB'] += 1

                if record["Gender"] not in ["Male", "Female", "Other"]:
                    #logger.error(f"Record {i} has invalid gender.")
                    count_missing['Gender'] += 1

                for key in [k for k in required_keys if k not in ["Barcode", "DOB", "Gender"]]:
                    if record[key] in ["NA", None, "-", "--"]:
                        #logger.error(f"Record {i} has missing or invalid {key}.")
                        count_missing[key] += 1

            return count_missing

        with tqdm(total=total, desc="Validating data") as pbar:
            futures = []
            for i in range(0, total, chunk_size):
                chunk = data[i:min(i+chunk_size,total)]
                future = self.executor.submit(validate_chunk, chunk)
                futures.append(future)

            count_missing = Counter()
            for future in as_completed(futures):
                chunk_results = future.result()
                count_missing.update(chunk_results)
                pbar.update(chunk_size)

        # Log information on missing values
        summary_file_id = logger.add(os.path.join(LOGS, "data_stream_summary.log"), mode='w', format="{time} {level} {message}", level="INFO")
        logger.info(f"\n\n\nTotal records: {total}")
        logger.info(f"Missing values: {dict(count_missing)}\n\n\n")
        logger.info(f"Missing values percentage: {dict((key, 100*value/total) for key, value in count_missing.items())}\n\n\n")

        # Close loggers
        logger.remove(summary_file_id)
        logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True)
       
    
def main():
    """
    Main function that downloads student data from the SAMS API and saves it to an Excel file.
    """
    # Check if the API authentication file exists
    if not Path(API_AUTH).exists():
        logger.critical(f"{API_AUTH} not found. API authentication is required.")
        exit(1)

    # Create a SamsDataDownloader instance
    downloader = SamsDataDownloader()

    # Download all student data
    #iti_data = downloader.fetch_students_iti_diploma(academic_year=2020, module="ITI", source_of_fund=1)
    #pdis_data = downloader.fetch_students_pdis(academic_year=2012)
    #downloader.validate_student_data(pdis_data)
    student_data = downloader.download_all_student_data()

    # Save the student data to an Excel file
    file_path = os.path.join(RAW_DATA_DIR, "student_data.csv")
    logger.info(f"Saving student data to {file_path}")
    pd.DataFrame(student_data).to_csv(file_path, index=False)


if __name__ == '__main__':
    main()


