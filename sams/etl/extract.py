from api.client import SAMSClient
from sams.config import ROOT_DIR
import os
import json
from logeru import logger

class SamsDataDownloader:
    def __init__(self, api_auth_creds=os.path.join(ROOT_DIR, 'config.json'), api_client=SAMSClient(api_auth_creds)):
        self.api_client = SAMSClient(api_auth_creds)

    def fetch_iti_diploma(self, fetch_function, academic_year, module, source_of_fund):
        page = 1
        data = []
        while True:
            page_data = fetch_function(academic_year=academic_year, 
                source_of_fund=source_of_fund, module=module, page_number=page)
            if not page_data:
                break
            data.extend(page_data['Data'])
            page += 1
        return data
    
    def fetch_pdis(self, fetch_function, academic_year):
        return fetch_function(module="PDIS", academic_year=academic_year)

    def download_student_data(self, programs):
        student_data = {}
        
        return student_data

    def download_institute_data(self, programs, years):
        institute_data = {}
        for year in years:
            institute_data[year] = {}
            for program in programs:
                institute_data[year][program] = self.fetch_data_by_page(
                    self.api_client.get_institute_data_by_year_and_page, year, program
                )
        return institute_data