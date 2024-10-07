import os
from loguru import logger
from sams.etl.extract import SamsDataDownloader
from sams.etl.load import SamsDataLoader
from sams.config import RAW_DATA_DIR, STUDENT, LOGS, INSTITUTE

class SAMSDataOrchestrator:
    def __init__(self, db_url=f"sqlite:///{RAW_DATA_DIR}/sams.db"):
        self.downloader = SamsDataDownloader()
        self.loader = SamsDataLoader(db_url)

    def download_and_add_student_data(self, module:str, academic_year:int, source_of_fund:int = None):
        student_data = self.downloader.fetch_students(module, academic_year, source_of_fund, pandify=False)
        self.loader.load_student_data(student_data)

    def download_and_add_institute_data(self, module:str, academic_year:int, admission_type:int = None):
        pass

    def process_data(self, table_name:str):
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
                    if module == "PDIS":
                        print(f"Downloading module: {module}, year: {year}")
                        self.download_and_add_student_data(module, year)
                    else:
                        print(f"Downloading module: {module}, year: {year}, type: Govt")
                        self.download_and_add_student_data(module, year, 1)
                        print(f"Downloading module: {module}, year: {year}, type: Pvt")
                        self.download_and_add_student_data(module, year, 5)
        else:
            for module, metadata in INSTITUTE.items():
                for year in range(metadata["yearmin"], metadata["yearmax"] + 1):
                    if module == "Diploma":
                        print(f"Downloading module: {module}, year: {year}, entry: Fresh")
                        self.download_and_add_institute_data(module, year,1)
                        print(f"Downloading module: {module}, year: {year}, entry: Lateral")
                        self.download_and_add_institute_data(module, year,2)




def main():
    db_url = f"sqlite:///{RAW_DATA_DIR}/sams.db"
    logger.debug(os.path.exists(os.path.join(RAW_DATA_DIR, "sams.db")))
    orchestrator = SAMSDataOrchestrator(db_url)
    orchestrator.process_data("students")

if __name__ == '__main__':
    main()