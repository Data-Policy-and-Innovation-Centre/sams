import os
from loguru import logger
from sams.etl.extract import SamsDataDownloader
from sams.etl.load import SamsDataLoader
from sams.etl.validate import validate
from sams.config import STUDENT, LOGS, INSTITUTE, SAMS_DB
from sams.utils import stop_logging_to_console, resume_logging_to_console
import pandas as pd


class SamsDataOrchestrator:
    def __init__(self, db_url=f"sqlite:///{SAMS_DB}"):
        self.downloader = SamsDataDownloader()
        self.loader = SamsDataLoader(db_url)

    def download_and_add_student_data(self, module: str, academic_year: int, bulk_add: bool = False):
        stop_logging_to_console(os.path.join(LOGS, "students_data_download.log"))

        # Try to call the API
        try:
            student_data = self.downloader.fetch_students(module, academic_year, pandify=False)
        
        # Check if data is empty
            if not student_data:
                logger.error(f"No student data returned for module: {module}, year: {academic_year}. API may be down or no records exist.")
                return
        
            logger.debug(f"[DEBUG] Fetched student data for {module} {academic_year}: {student_data}")

            # Check for required fields
            required_fields = {"module", "academic_year"}
            missing = required_fields - set(student_data[0].keys())
            if missing:
                logger.warning(f"Student data missing required fields: {missing}. Skipping validation and load.")
                return

            # Continue as normal
            validate(student_data, table_name="students")
            if bulk_add:
                self.loader.bulk_load(student_data, "students")
            else:
                self.loader.load(student_data, "students")

        except Exception as e:
            logger.error(f"API request failed for module: {module}, year: {academic_year}. Error: {e}")

        finally:
            resume_logging_to_console()

    def download_and_add_institute_data(self,module: str,academic_year: int,admission_type: int = None,bulk_add: bool = False,):
        stop_logging_to_console(os.path.join(LOGS, f"institutes_data_download.log"))
        
        try:
            institute_data = self.downloader.fetch_institutes(module, academic_year, admission_type, pandify=False)
            logger.debug(f"[DEBUG] Fetched institute data for {module} {academic_year} type {admission_type}: {institute_data}")
        except Exception as e:
            logger.error(f"API call failed for institute module={module}, year={academic_year}, type={admission_type}: {e}")
            resume_logging_to_console()
            return
        

        # Check if data is empty (API down or no results)
        if not institute_data:
            logger.warning(f"No institute data for module={module}, year={academic_year}, type={admission_type}. API may be down or no records exist.")
            resume_logging_to_console()
            return

        # Check for required fields 
        required_fields = {"module", "academic_year"}
        missing = required_fields - set(institute_data.columns)
        if missing:
            logger.warning(f"Missing required fields {missing} in institute data for module={module}, year={academic_year}. Skipping.")
            resume_logging_to_console()
            return

        # Proceed to load
        if bulk_add:
            self.loader.bulk_load(institute_data, "institutes") 
        else:
            self.loader.load(institute_data, "institutes")

        resume_logging_to_console()

    def process_data(
        self, table_name: str, exclude: bool = True, bulk_add: bool = False
    ):
        # Add a new log handler for downloading student data
        log_file_id = logger.add(
            os.path.join(LOGS, f"{table_name}_data_download.log"),
            mode="w",
            format="{time} {level} {message}",
            level="INFO",
        )

        if exclude:
            excluded_modules = self.loader.get_existing_modules(table_name)
        else:
            excluded_modules = []

        fmt_exlcude = str(excluded_modules).replace("),", ")\n")
        logger.info(
            f"Processing data for table: {table_name}\n excluding modules:\n {fmt_exlcude}.\n Bulk adding: {bulk_add}"
        )

        if table_name == "students":
            for module, metadata in STUDENT.items():
                for year in range(metadata["yearmin"], metadata["yearmax"] + 1):
                    if (module, year) not in excluded_modules:
                        logger.info(
                            f"Downloading student module: {module}, year: {year}"
                        )
                        self.download_and_add_student_data(module, year, bulk_add)

        else:
            for module, metadata in INSTITUTE.items():
                for year in range(metadata["yearmin"], metadata["yearmax"] + 1):
                    if module == "Diploma":
                        if (module, year, 1) not in excluded_modules:
                            logger.info(
                                f"Downloading institute module: {module}, year: {year}, entry: Fresh"
                            )
                            self.download_and_add_institute_data(
                                module, year, 1, bulk_add
                            )

                        if (module, year, 2) not in excluded_modules:
                            logger.info(
                                f"Downloading institute module: {module}, year: {year}, entry: Lateral"
                            )
                            self.download_and_add_institute_data(
                                module, year, 2, bulk_add
                            )
                    else:
                        if (module, year, 0) not in excluded_modules:
                            logger.info(
                                f"Downloading institute module: {module}, year: {year}"
                            )
                            self.download_and_add_institute_data(
                                module, year, None, bulk_add
                            )


def main():
    db_url = f"sqlite:///{SAMS_DB}"
    logger.debug(os.path.exists(SAMS_DB))
    orchestrator = SamsDataOrchestrator(db_url)
    orchestrator.process_data("institutes")


if __name__ == "__main__":
    main()
    