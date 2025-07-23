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

    def download_and_add_student_data(
        self, module: str, academic_year: int, bulk_add: bool = False
    ):
        stop_logging_to_console(os.path.join(LOGS, "students_data_download.log"))
        student_data = self.downloader.fetch_students(
            module, academic_year, pandify=False
        )
        #print(f"Fetched student data for {module} {academic_year}: {student_data}")

        # Check if data is empty 
        if not student_data:
            logger.warning(f"No student data for module: {module}, year: {academic_year} — possibly due to API issues. Skipping.")
            resume_logging_to_console()
            return
        
        # Check if required columns exist
        required_fields = {"module", "academic_year"}
        missing = required_fields - set(student_data[0].keys())
        if missing:
            logger.warning(f"Missing required fields in student data: {missing}. Skipping validation and load.")
            resume_logging_to_console()
            return
        validate(student_data, table_name="students")

        if bulk_add:
            self.loader.bulk_load(student_data, "students")
        else:
            self.loader.load(student_data, "students")
        resume_logging_to_console()

    def download_and_add_institute_data(
        self,
        module: str,
        academic_year: int,
        admission_type: int = None,
        bulk_add: bool = False,
    ):
        stop_logging_to_console(os.path.join(LOGS, f"institutes_data_download.log"))
        institute_data = self.downloader.fetch_institutes(
            module, academic_year, admission_type, pandify=False
        )


        # Check if data is empty (API down or no results)
        if not institute_data:
            logger.warning(
                f"No institute data for module: {module}, year: {academic_year}, type: {admission_type} — possibly due to API issues. Skipping."
            )
            resume_logging_to_console()
            return

        # Check for required fields (if any — adjust as needed)
        required_fields = {"module", "academic_year"}
        missing = required_fields - set(institute_data.columns)
        if missing:
            logger.warning(f"Missing required fields in institute data: {missing}. Skipping load.")
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
    