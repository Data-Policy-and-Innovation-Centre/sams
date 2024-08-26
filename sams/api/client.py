import requests
import json
from .auth import Auth
from .endpoints import Endpoints
from .exceptions import APIError
from config import API_AUTH
from loguru import logger

class SAMSClient:
    def __init__(self, config_path):
        with open(config_path, "r") as f:
            cred = json.load(f)
        
        self.auth = Auth(cred['username'], cred['password'])
        self.endpoints = Endpoints()
        

    def get_student_data(self, module, academic_year, source_of_fund=None,page_number=None):
        logger.info(f"Getting SAMS student module: {module}, year: {academic_year}, SOF: {source_of_fund}, page: {page_number}")
        url = self.endpoints.get_student_data()
        headers = self.auth.get_auth_header()
        
        if module == "PDIS":
            params = {"Module": module, "AcademicYear": academic_year}
        else:
            params = {"Module": module, "AcademicYear": academic_year, 'SourceOfFund': source_of_fund, 'PageNumber': page_number}
        
        response = requests.get(url, headers=headers, data=params)
        return self._handle_response(response)

    def get_institute_data(self, module, academic_year):
        logger.info(f"Getting SAMS institute module: {module}, year: {academic_year}")
        url = self.endpoints.get_institute_data()
        headers = self.auth.get_auth_header()
        params = {"Module": module, "AcademicYear": academic_year}

        response = requests.get(url, headers=headers, json=params)
        return self._handle_response(response)

    def _handle_response(self, response):
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 400:
            raise APIError("Bad Request: Some inputs are missing.")
        elif response.status_code == 500:
            raise APIError("Server Error: Something went wrong.")
        else:
            response.raise_for_status()

def main():
    client = SAMSClient(API_AUTH)

    # Fetch student data
    #pdis_data = client.get_student_data(module="PDIS", academic_year=2022)
    #iti_data = client.get_student_data(module="ITI", academic_year=2022, source_of_fund=1, page_number=1)
    #diploma_data = client.get_student_data(module="Diploma", academic_year=2022, source_of_fund=1, page_number=1)
    #print(pdis_data)
    #print(iti_data)
    #print(diploma_data)

    # Fetch institute data
    institute_data = client.get_institute_data(module="PDIS", academic_year=2021)
    print(institute_data)
    

if __name__ == '__main__':
    main()
