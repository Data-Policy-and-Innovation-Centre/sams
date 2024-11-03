import os
from loguru import logger
from sams.etl.extract import SamsDataDownloader
from sams.etl.load import SamsDataLoader
from sams.etl.validate import validate
from sams.config import STUDENT, LOGS, INSTITUTE, SAMS_DB
from sams.util import stop_logging_to_console, resume_logging_to_console
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
