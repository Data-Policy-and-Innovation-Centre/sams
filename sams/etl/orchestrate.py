import os
from loguru import logger
from sams.etl.extract import SamsDataDownloader
from sams.etl.load import SamsDataLoader
from sams.etl.validate import validate
from sams.config import STUDENT, LOGS, INSTITUTE, SAMS_DB

class SAMSDataOrchestrator:
    def __init__(self, db_url=f"sqlite:///{SAMS_DB}"):
        self.downloader = SamsDataDownloader()
        self.loader = SamsDataLoader(db_url)

    def download_and_add_student_data(self, module:str, academic_year:int, bulk_add:bool=False):
        student_data = self.downloader.fetch_students(module, academic_year, pandify=False)
        validate(student_data, table_name="students")
        if bulk_add:
            self.loader.bulk_add_students(student_data)
        else:
            self.loader.load_student_data(student_data)

    def download_and_add_institute_data(self, module:str, academic_year:int, admission_type:int = None, bulk_add:bool=False):
        pass

    def process_data(self, table_name:str, exclude=[], bulk_add:bool=False):

        logger.info(f"Processing data for table {table_name}, excluding modules {exclude}. Bulk adding: {bulk_add}")

        # Remove all existing log handlers
        for handler_id in list(logger._core.handlers.keys()):
            logger.remove(handler_id)

        # Add a new log handler for downloading student data
        log_file_id = logger.add(
            os.path.join(LOGS, f"{table_name}_data_download.log"), mode='w',
            format="{time} {level} {message}", level="INFO"
        )

        if table_name == "students":
            for module, metadata in STUDENT.items():
                for year in range(metadata["yearmin"], metadata["yearmax"] + 1):
                    if (module, year) not in exclude:
                        print(f"Downloading module: {module}, year: {year}")
                        self.download_and_add_student_data(module, year, bulk_add)
             
        else:
            for module, metadata in INSTITUTE.items():
                for year in range(metadata["yearmin"], metadata["yearmax"] + 1):
                    if module == "Diploma":
                        print(f"Downloading module: {module}, year: {year}, entry: Fresh")
                        self.download_and_add_institute_data(module, year,1, bulk_add)
                        print(f"Downloading module: {module}, year: {year}, entry: Lateral")
                        self.download_and_add_institute_data(module, year,2, bulk_add)
                    else:
                        print(f"Downloading module: {module}, year: {year}")
                        self.download_and_add_institute_data(module, year, bulk_add)




def main():
    db_url = f"sqlite:///{SAMS_DB}"
    logger.debug(os.path.exists(SAMS_DB))
    orchestrator = SAMSDataOrchestrator(db_url)
    orchestrator.process_data("students")

if __name__ == '__main__':
    main()