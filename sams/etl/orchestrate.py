import os
from loguru import logger
from sams.etl.extract import SamsDataDownloader
from sams.etl.load import SamsDataLoader
from sams.config import RAW_DATA_DIR

class SAMSDataOrchestrator:
    def __init__(self, db_url=f"sqlite:///{RAW_DATA_DIR}/sams.db"):
        self.downloader = SamsDataDownloader()
        self.loader = SamsDataLoader(db_url)
       
    def process_data(self):
        logger.info("Processing student data...")
        student_data = self.downloader.download_all_student_data()
        self.loader.load_student_data(student_data)

        logger.info("Processing institute data...")
        institute_data = self.downloader.download_all_institute_data()
        self.loader.load_institute_data(institute_data)
        logger.info("Data processing completed")

    def close(self):
        self.loader.close()

def main():
    db_url = f"sqlite:///{RAW_DATA_DIR}/sams.db"
    logger.debug(os.path.exists(db_url))

    orchestrator = SAMSDataOrchestrator(db_url)
    
    try:
        orchestrator.process_data()
    finally:
        orchestrator.close()

if __name__ == '__main__':
    main()