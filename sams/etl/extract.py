from sams.api.client import SAMSClient
from sams.api.exceptions import APIError
from requests import HTTPError
import os
from pathlib import Path
import json
from collections import Counter
from loguru import logger
from sams.config import API_AUTH, ERRMAX, STUDENT, RAW_DATA_DIR, \
INSTITUTE, SOF, LOGS, NUM_TOTAL_STUDENT_RECORDS, NUM_TOTAL_INSTITUTE_RECORDS
from sams.util import is_valid_date
from tqdm import tqdm
import polars as pl
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

class SamsDataDownloader:
    def __init__(self, client=None):
        if not client:
            self.api_client = SAMSClient(API_AUTH)
        else:
            self.api_client = client
        self.executor = ThreadPoolExecutor(max_workers=10)

    def fetch_students(self, academic_year: int, module: str, source_of_fund: int = None, pandify = True) -> pd.DataFrame | list:

        """
        Fetches student data from the SAMS API for the given academic year,
        module and source of fund.

        Args:
            academic_year (int): The academic year for which to fetch the data.
            module (str): The module for which to fetch the data.
            source_of_fund (int, optional): The source of fund for which to fetch the data.
            pandify (bool, default True): If True, returns a pandas DataFrame.
                Otherwise, returns a list of dictionaries.

        Returns:
            pd.DataFrame or list: The fetched data.
        """
        academic_year = self._check_student_data_params(academic_year, module, source_of_fund)

        expected_records = self._get_records("students",academic_year, module, source_of_fund, count=True)

        if module in ['ITI', 'Diploma']:
            data = self._get_students_iti_diploma(academic_year, module, source_of_fund)
        else:
            data = self._get_records("students",academic_year,module)

        if len(data) < expected_records:
            logger.warning(f"Expected {expected_records} records, but got {len(data)} records.")

        try:
            info = f"""\nStudent data downloaded for module {module}, academic year {academic_year}, source of fund {SOF['tostring'][source_of_fund]}. 
            \n.Num fields: {len(data[0])} \n.Num records: {len(data)} \n.Num distinct records: { len(set(map(lambda item: tuple(sorted(item.items())), data)))} \n.Expected records: {expected_records}"""
            logger.info(info)
        except IndexError as e:
            logger.error(f"Student data missing for module {module}, academic year {academic_year}, source of fund {SOF['tostring'][source_of_fund]}.")
        except TypeError as t:
            info = f"""\nStudent data downloaded for module {module}, academic year {academic_year}, source of fund {SOF['tostring'][source_of_fund]}. 
            \n.Num fields: {len(data[0])} \n.Num records: {len(data)} \n.Expected records: {expected_records}"""
            logger.info(info)

        if pandify:
            df = pd.DataFrame(data)
            df['module'] = module
            df['academic_year'] = academic_year
            df['source_of_fund'] = source_of_fund
            return df
        else:
            return data
        
    def fetch_institutes(self, module: str, academic_year: int, admission_type: int = None, pandify = False) -> list:

        academic_year = self._check_institute_data_params(academic_year, module, admission_type)

        expected_records = self._get_records("institutes", academic_year, module, admission_type=admission_type, count=True)

        data = self._get_records("institutes", academic_year, module, admission_type=admission_type)

        if len(data) < expected_records:
            logger.warning(f"Expected {expected_records} records, but got {len(data)} records.")

        try:
            info = f"""\nInstitute data downloaded for module {module}, academic year {academic_year}, admission type {admission_type}.
            \n.Num fields: {len(data[0])} \n.Num records: {len(data)} \n.Num distinct records: { len(set(map(lambda item: tuple(sorted(item.items())), data)))} \n.Expected records: {expected_records}"""
            logger.info(info)
        except IndexError as e:
            logger.error(f"Institute data missing for module {module}, academic year {academic_year}, admission type {admission_type}.")
        except TypeError:
            info = f"""\nInstitute data downloaded for module {module}, academic year {academic_year}, admission type {admission_type}.
            \n.Num fields: {len(data[0])} \n.Num records: {len(data)} \n.Expected records: {expected_records}"""
            logger.info(info)

        if pandify:
            df = pd.DataFrame(data)
            df['module'] = module
            df['academic_year'] = academic_year
            df['admission_type'] = admission_type
            return df
        else:
            for item in data:
                item['module'] = module
                item['academic_year'] = academic_year
                item['admission_type'] = admission_type
            return data
         
    
    def _get_students_iti_diploma(self, academic_year: int, module: str, source_of_fund: int) -> list:
        
        """
        Downloads student data for ITI and Diploma from SAMS API.

        Args:
            academic_year (int): The academic year for which to fetch the data.
            module (str): The module for which to fetch the data.
            source_of_fund (int): The source of fund for which to fetch the data.

        Returns:
            list: The downloaded data.
        """
        
        page = 1
        data = []

        while True:
            records = self._get_records(table_name="students", module=module, academic_year=academic_year, source_of_fund=source_of_fund, page_number=page)

            if len(records) == 0:
                break

            data.extend(records)
            page += 1
        
        return data
              

    def _get_records(self, table_name: str, academic_year: int, module: str, source_of_fund: int = None, admission_type: int = None, page_number = 1, count = False) -> int | list:
        
        """
        Fetches the expected number of student records, or the actual list of records from SAMS API for the given academic year, module and source of fund.

        Args:
            table (str): The table for which to fetch the data.
            academic_year (int): The academic year for which to fetch the data.
            module (str): The module for which to fetch the data.
            source_of_fund (int, optional): The source of fund for which to fetch the data.
            page_number (int, optional): The page number to fetch.
            count (bool, optional): If True, returns the expected number of records. Otherwise returns a list consisting of the actual records.
        
        Returns:
            int or list : The expected number of student records or a list of records in JSON format.
        """
        if table_name not in ['students', 'institutes']:
            raise ValueError(f"Invalid table name: {table_name}")

        retries = 0
        records = []

        while retries < ERRMAX:
            try:
                if table_name == 'students':
                    records = self.api_client.get_student_data(module=module, academic_year=academic_year, source_of_fund=source_of_fund, page_number=page_number, count = count)
                else:
                    records = self.api_client.get_institute_data(module=module, academic_year=academic_year, admission_type=admission_type, count = count)
                break 
            except APIError as e:
                logger.error(f"API Error: {e}")
                logger.error(f"Retrying...({retries+1}/{ERRMAX})")
                retries += 1
                continue

        if not records and count:
            records = 0
        
        return records
                 

    def _check_student_data_params(self, academic_year: int, module: str, source_of_fund: int = None) -> int:
        """
        Checks if the given academic year, module and source of fund are valid,
        and if not, adjusts them to the nearest valid values.

        Args:
            academic_year (int): The academic year for which to fetch the data.
            module (str): The module for which to fetch the data.
            source_of_fund (int): The source of fund for which to fetch the data.

        Returns:
            int: The adjusted academic year.
        """
        if module not in ["ITI", "Diploma", "PDIS"]:
            raise ValueError("Module must be either 'ITI', 'PDIS' or 'Diploma'. ")
        
        if module in ["ITI", "Diploma"] and source_of_fund not in [1,5]:
            raise ValueError("Source of fund must be either 1 (for Govt) or 5 (for Private). ")
        
        if academic_year < STUDENT[module]['yearmin'] or academic_year > STUDENT[module]['yearmax']:
            logger.warning(f"Data from Academic year {academic_year} is not available for {module}. It must be between {STUDENT[module]['yearmin']} and {STUDENT[module]['yearmax']}. ")
            academic_year = min(STUDENT[module]['yearmax'], max(STUDENT[module]['yearmin'], academic_year))
            logger.warning(f"Adjusting year to {academic_year} for {module}. ")

        return academic_year
    
    def _check_institute_data_params(self, academic_year: int, module: str, admission_type: int= None) -> int:
        """
        Checks if the given academic year, module and admission type are valid,
        and if not, adjusts them to the nearest valid values.

        Args:
            academic_year (int): The academic year for which to fetch the data.
            module (str): The module for which to fetch the data.
            admission_type (int, optional): The admission type for which to fetch the data. Defaults to None.

        Returns:
            int: The adjusted academic year.
        """
        if module not in ["ITI", "Diploma", "PDIS"]:
            raise ValueError("Module must be either 'ITI', 'PDIS' or 'Diploma'. ")
        
        if module == "Diploma" and admission_type not in [1,2]:
            raise ValueError("Admission type must be either 1 (for Fresh Entry) or 2 (for Lateral Entry). ")
        
        if academic_year < INSTITUTE[module]['yearmin'] or academic_year > INSTITUTE[module]['yearmax']:
            logger.warning(f"Data from Academic year {academic_year} is not available for {module}. It must be between {INSTITUTE[module]['yearmin']} and {INSTITUTE[module]['yearmax']}. ")
            academic_year = min(INSTITUTE[module]['yearmax'], max(INSTITUTE[module]['yearmin'], academic_year))
            logger.warning(f"Adjusting year to {academic_year} for {module}. ")

        return academic_year   
    
    def download_all_student_data(self, save: bool = False) -> pl.DataFrame:
        """
        Downloads student data from SAMS API for all modules and years.

        Returns:
            pl.DataFrame: A polars dataframe, where each row represents a student.
        """
        # Remove all existing log handlers
        for handler_id in list(logger._core.handlers.keys()):
            logger.remove(handler_id)
        
        # Add a new log handler for downloading student data
        log_file_id = logger.add(
            os.path.join(LOGS, "student_data_download.log"), mode='w',
            format="{time} {level} {message}", level="INFO"
        )

        student_data = []
        futures = []
        
        # Progress bar for downloading student data
        with tqdm(total=NUM_TOTAL_STUDENT_RECORDS, desc="Downloading student data") as pbar:
            for module, metadata in STUDENT.items():
                for year in range(metadata["yearmin"], metadata["yearmax"] + 1):
                    if module == "PDIS":
                        future = self.executor.submit(self.fetch_students, year, module)
                        futures.append(future)
                    else:
                        future_govt = self.executor.submit(self.fetch_students, year, module, 1)
                        future_pvt = self.executor.submit(self.fetch_students, year, module, 5)
                        futures.extend([future_govt, future_pvt])
                    

            for future in as_completed(futures):
                data = future.result()

                # Add only if data is not empty
                if data.shape[0] > 0:
                    student_data.append(data)
                pbar.update(len(data))

        # Convert to polars
        student_data = pl.concat([pl.from_pandas(df) for df in student_data])

        # Remove the log handler for downloading student data
        logger.remove(log_file_id)

        # Add a new log handler for validation
        logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True)

        if save:
            student_data.write_parquet(os.path.join(RAW_DATA_DIR, "student_data.parquet"))

        return student_data

    def download_all_institute_data(self) -> list:
        """
        Downloads institute data from SAMS API for all modules and years.

        Returns:
            list: A list of dictionaries, where each dictionary represents an institute.
        """
        # Remove all existing log handlers
        for handler_id in list(logger._core.handlers.keys()):
            logger.remove(handler_id)
        
        # Add a new log handler for downloading institute data
        log_file_id = logger.add(
            os.path.join(LOGS, "institute_data_download.log"), mode='w',
            format="{time} {level} {message}", level="INFO"
        )

        institute_data = []
        futures = []
        
        # Progress bar for downloading institute data
        with tqdm(total=NUM_TOTAL_INSTITUTE_RECORDS, desc="Downloading institute data") as pbar:
            for module, metadata in INSTITUTE.items():
                for year in range(metadata["yearmin"], metadata["yearmax"] + 1):
                    if module == "Diploma":
                        future_fresh = self.executor.submit(self.fetch_institutes, module, year, 1)
                        future_lateral = self.executor.submit(self.fetch_institutes, module, year, 2)
                        futures.extend([future_fresh, future_lateral])
                    else:
                        future = self.executor.submit(self.fetch_institutes, module, year)
                        futures.append(future)

            for future in as_completed(futures):
                data = future.result()
                institute_data.extend(data)
                pbar.update(len(data))

        # Remove the log handler for downloading institute data
        logger.remove(log_file_id)

        # Add a new log handler for validation
        logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True)

        return institute_data

    def update_total_records(self) -> None:

        # Remove all existing log handlers
        for handler_id in list(logger._core.handlers.keys()):
            logger.remove(handler_id)
        
        # Add a new log handler for downloading student data
        log_file_id = logger.add(
            os.path.join(LOGS, "total_records.log"), mode='w',
            format="{time} {level} {message}", level="INFO"
        )

        # Set up counter
        counter = {
            "students": 0,
            "institutes": 0
        }

        # Students
        counter = self._update_total_records(counter, STUDENT, type="students")

        # Institutes
        counter = self._update_total_records(counter, INSTITUTE, type="institutes")
        
        # Dump counts in json file
        with open(os.path.join(LOGS, "total_records.json"), "w") as f:
            json.dump(counter, f)

        # Close the log handler
        logger.remove(log_file_id)

    def _update_total_records(self, counter, metadict, type="students") -> dict:

        if type not in ["students", "institutes"]:
            raise ValueError("type must be either 'students' or 'institutes'")

        for module, metadata in metadict.items():
            for year in range(metadata["yearmin"], metadata["yearmax"] + 1):
                retries = 0
                success = False
                while not success:
                    try:
                        if type == "students":
                            counter[type] += self.api_client.get_student_data(module, year, source_of_fund=1,page_number=1, count=True)
                            if module in ["ITI", "Diploma"]:
                                counter[type] += self.api_client.get_student_data(module, year, source_of_fund=5,page_number=1, count=True)
                        else:
                            counter[type] += self.api_client.get_institute_data(module, year, admission_type=1, count=True)
                            if module == "Diploma":
                                counter[type] += self.api_client.get_institute_data(module, year, admission_type=2, count=True)
                        success = True
                    except APIError as e:
                        retries += 1
                        if retries >= ERRMAX:
                            logger.error(f"Data download failed for {module} {year} after {ERRMAX} retries. Skipping...")
                            break
                        else:
                            continue
        return counter
        
    
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
    data = downloader.fetch_students(2022, "ITI", 1)
    logger.info(data)
    # student_data = downloader.download_all_student_data()
    # institude_data = downloader.download_all_institute_data()

    # # Write to parquet file
    # student_data.write_parquet(os.path.join(RAW_DATA_DIR,'student_data.parquet'))

    # # Write to json file
    # with open(os.path.join(RAW_DATA_DIR,'institute_data.json'), 'w', encoding='utf-8') as f:
    #     json.dump(institude_data, f, ensure_ascii=False, indent=4)
    
    
    
    #downloader.download_all_student_data(save=True)
    


if __name__ == '__main__':

    main()


