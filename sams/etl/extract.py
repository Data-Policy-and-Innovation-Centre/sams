from api.client import SAMSClient
from api.exceptions import APIError
from requests import HTTPError
import os
import json
from loguru import logger
from config import API_AUTH, RAW_DATA_DIR, ERRMAX, STUDENT, INSTITUTE, SOF, LOGS, NUM_TOTAL_RECORDS
from util import is_valid_date
from tqdm import tqdm
import pprint
import pandas as pd

class SamsDataDownloader:
    def __init__(self):
        self.api_client = SAMSClient(API_AUTH)

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
            except APIError as e:
                if errcount >= ERRMAX:
                    logger.critical("Too many API errors. Exiting loop")
                    break
                logger.error(e)
                logger.warning(f"Attempting to continue with page {page}.")
                errcount +=1
                continue
            except HTTPError as h:
                if errcount >= ERRMAX:
                    logger.critical("Too many HTTP errors. Exiting loop")
                    break
                logger.error(h)
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
            list: A list of dictionaries, where each dictionary represents a student.
        """

        if academic_year < STUDENT['PDIS']['yearmin'] or academic_year > STUDENT['PDIS']['yearmax']:
            logger.warning(f"Data from Academic year {academic_year} is not available for PDIS. It must be between {STUDENT['PDIS']['yearmin']} and {STUDENT['PDIS']['yearmax']}. ")
            academic_year = min(STUDENT['PDIS']['yearmax'], max(STUDENT['PDIS']['yearmin'], academic_year))
            logger.warning(f"Adjusting year to {academic_year} for {module}. ")

        errcount = 0
        logger.info(f"Fetching all pages for PDIS student data for applications in {academic_year}... ")
        while True:
            try:
                data = self.api_client.get_student_data(module="PDIS", academic_year=academic_year)
                if data:
                    logger.info(f"\nPDIS data fetched\n. Total fields: {len(data[0])}\n. Total records: {len(data)}\n\n\n\n")
                    break
            except APIError as e:
                logger.error(e)
                if errcount >= ERRMAX:
                    logger.critical("Too many API errors. Exiting loop.")
                    break
                
                logger.warning("Attempting again")
                errcount += 1
                continue
            except HTTPError as h:
                logger.error(h)
                if errcount >= ERRMAX:
                    logger.critical("Too many HTTP errors. Exiting loop.")
                    break

                logger.warning("Attempting again")
                errcount += 1
                continue
        
        return data

    def download_all_student_data(self) -> list:
        """Downloads student data from SAMS API for all modules and years."""

        for handler_id in list(logger._core.handlers.keys()):
            logger.remove(handler_id)
        fid = logger.add(os.path.join(LOGS, "data_download.log"), mode='w',format="{time} {level} {message}", level="INFO")

        student_data = []
        with tqdm(total=NUM_TOTAL_RECORDS, desc="Downloading student data") as pbar:
            for module, metadata in STUDENT.items():
                for year in range(metadata["yearmin"], metadata["yearmax"] + 1):
                    if module == "PDIS":
                        data = self.fetch_students_pdis(year)
                        student_data.extend(data)
                    else:
                        data = self.fetch_students_iti_diploma(year, module, 1)
                        data.extend(self.fetch_students_iti_diploma(year, module, 5))
                        student_data.extend(data)
                    pbar.update(len(data))

        logger.remove(fid)
        logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True)
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
        """
        Validates the fetched student data.

        Args:
            data (list): The list of dictionaries, where each dictionary represents a student.
        """
        # Check if the data is empty
        if not data:
            logger.error("No data to validate")
            return
        
        # Remove all handlers
        for handler_id in list(logger._core.handlers.keys()):
            logger.remove(handler_id)

        # Add new logger for validation
        log_file_id = logger.add(os.path.join(LOGS, "data_stream_validation.log"), mode='w',format="{time} {level} {message}", level="INFO")

        # Check if the record has the required keys
        required_keys = ["Barcode", "StudentName", "Gender", "DOB", "ReligionName", "Nationality", "AnnualIncome", "Address", "State", "District", "Block",
                             "PINCode", "SocialCategory", "Domicile", "S_DomicileCategory", "OutsideOdishaApplicantStateName",
                             "OdiaApplicantLivingOutsideOdishaStateName","ResidenceBarcodeNumber", "TengthExamSchoolAddress","EighthExamSchoolAddress",
                             "HighestQualification","HighestQualificationExamBoard","HighestQualificationBoardExamName", "ExaminationType","YearofPassing",
                             "RollNo","TotalMarks","SecuredMarks","Percentage","CompartmentalStatus","CompartmentalFailMark", "SubjectWiseMarks","hadTwoYearFullTimeWorkExpAfterTength",
                             "GC","PH","ES","Sports","NationalCadetCorps","PMCare","Orphan","IncomeBarcode","TFW","EWS","BOC","BOCRegdNo", "CourseName","CoursePeriod",
                             "BeautyCultureType","ReportedInstitute","ReportedBranchORTrade","InstituteDistrict","TypeofInstitute","Phase","Year","AdmissionStatus","EnrollmentStatus"]
        count_missing = {key: 0 for key in required_keys}

        # Total number of records
        total = len(data)

        with tqdm(total=total, desc="Validating data") as pbar:
            # Iterate over the records and check for errors
            for i, record in enumerate(data, start=1):

                if not all(key in record for key in required_keys):
                    logger.error(f"Record {i} is missing required keys")
                    logger.debug(f"Record {i} has extra keys: {set(record.keys()) - set(required_keys)}")
                    continue

                # Validate the barcode
                if record['Barcode'] in ["NA", None, "-", "--"]:
                    logger.error(f"Record {i} has invalid barcode")
                    count_missing['Barcode'] += 1

                # Validate the date of birth
                if not is_valid_date(record["DOB"]):
                    logger.info(f"DOB is {record['DOB']} with format ")
                    logger.error(f"Record {i} with barcode {record['Barcode']} has invalid date of birth")
                    count_missing['DOB'] += 1
                
                # Validate the gender
                if record["Gender"] not in ["Male", "Female", "Other"]: 
                    logger.error(f"Record {i} with barcode {record['Barcode']} has invalid gender")
                    count_missing['Gender'] += 1

                # Validate others for missing values
                for key in [k for k in required_keys if k not in ["Barcode", "DOB", "Gender"]]:
                    if record[key] in ["NA", None, "-", "--"]:
                        logger.error(f"Record {i} with barcode {record['Barcode']} has missing value for {key}")
                        count_missing[key] += 1
                
                # Update the progress bar
                pbar.update(1)
        
        # Log information on missing values
        summary_file_id = logger.add(os.path.join(LOGS, "data_stream_summary.log"), mode='w',format="{time} {level} {message}", level="INFO")
        logger.info(f"\n\n\nTotal records: {total}")
        logger.info(f"Missing values: {pprint.pformat(count_missing)}\n\n\n")
        logger.info(f"Missing values percentage: {pprint.pformat({key: 100*value/total for key, value in count_missing.items()})}\n\n\n")
        

        # Close logger
        logger.remove(summary_file_id)
        logger.remove(log_file_id)
        logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True) 

       
    
def main():
    if not Path(API_AUTH).exists():
        logger.critical(f"ERROR: {API_AUTH} not found. This file is required for API authentication.")
        exit(1)
    downloader = SamsDataDownloader()
    student_data = downloader.download_all_student_data()
    file_path = os.path.join(RAW_DATA_DIR, "student_data.xlsx")
    logger.info(f"Saving student data to {file_path}")
    pd.DataFrame(student_data).to_excel(file_path, index=False)



if __name__ == '__main__':
    main()