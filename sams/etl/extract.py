from api.client import SAMSClient
from api.exceptions import APIError
import os
import json
from loguru import logger
from config import API_AUTH, ERRMAX, STUDENT, INSTITUTE
import datetime

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
        while True:
            # Try to fetch a page of data
            try:
                page_data = self.api_client.get_student_data(academic_year=academic_year, 
                    source_of_fund=source_of_fund, module=module, page_number=page)
            # If there is an API error, log the error and attempt again
            except APIError as e:
                if errcount >= ERRMAX:
                    logger.critical("Too many API errors. Exiting.")
                    exit(1)
                logger.error(e)
                logger.warning(f"Attempting to continue with page {page}.")
                errcount +=1
                continue 
            
            # If there is no more data, log the total page count and total records
            if not page_data:
                logger.info(f"\nAll pages downloaded\n. Total page count: {page-1}\n. Total records: {len(data)}\n\n\n\n")
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

        data = self.api_client.get_student_data(module="PDIS", academic_year=academic_year)
        logger.info(f"PDIS data fetched\n. Total records: {len(data)}\n\n\n\n")

    def download_student_data(self, programs):
        pass

    def download_institute_data(self, programs, years):
        pass
    
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
        
        # Iterate over the records and check for errors
        for i, record in enumerate(data, start=1):
            # Check if the record has the required keys
            required_keys = ["name", "primary_contact_number", "dob", "passing_year", "employer_name", "offer_date", "joined"]
            if not all(key in record for key in required_keys):
                logger.error(f"Record {i} is missing required keys")
                continue
            
            # Validate the date of birth
            try:
                datetime.strptime(record["dob"], "%d-%m-%Y")
            except ValueError:
                logger.error(f"Record {i} has invalid date of birth")
                continue
            
            # Validate the offer date
            try:
                datetime.strptime(record["offer_date"], "%d-%m-%Y")
            except ValueError:
                logger.error(f"Record {i} has invalid offer date")
                continue
            
            # Validate the passing year
            try:
                int(record["passing_year"])
            except ValueError:
                logger.error(f"Record {i} has invalid passing year")
                continue
    
def main():
    downloader = SamsDataDownloader()
    iti_data = downloader.fetch_students_iti_diploma(2012, "ITI", 1)

if __name__ == '__main__':
    main()