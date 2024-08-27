from tqdm import tqdm
import pandas as pd
from loguru import logger
from etl.extract import SamsDataDownloader
from etl.load import SamsDataLoader
from config import RAW_DATA_DIR

class SAMSDataOrchestrator:
    def __init__(self, db_url):
        self.downloader = SamsDataDownloader()
        self.loader = SamsDataLoader(db_url)
       

    def process_student_data(self):
        logger.info("Data processing started")
        student_data = self.downloader.download_all_student_data()
        self.loader.load_student_data(student_data)
        logger.info("Data processing completed")

    def process_institute_data(self):
        pass

    def close(self):
        self.executor.shutdown()
        self.loader.close()

def main():
    db_url = f"sqlite:///{RAW_DATA_DIR}/sams.db"
    orchestrator = SAMSDataOrchestrator(db_url)
    
    try:
        orchestrator.process_student_data()
    finally:
        orchestrator.close()

if __name__ == '__main__':
    main()