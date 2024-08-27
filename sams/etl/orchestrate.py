from concurrent.futures import ThreadPoolExecutor, as_completed
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
        self.executor = ThreadPoolExecutor(max_workers=2)  # One for download, one for load

    def process_student_data(self):
        download_future = self.executor.submit(self.downloader.download_all_student_data)
        student_data = []  
        for chunk in download_future.result():
            student_data.extend(chunk)
            if len(student_data) >= 1000:  # Process in chunks of 1000
                self.loader.load_student_data(student_data)
                student_data = []
            
        if student_data:  # Load any remaining data
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