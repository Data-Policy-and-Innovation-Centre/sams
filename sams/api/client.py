import requests
import json
from sams.api.auth import Auth
from sams.api.endpoints import Endpoints
from sams.api.exceptions import APIError
from sams.config import API_AUTH, SOF
from loguru import logger

class SAMSClient:
    """
    A client for interacting with the SAMS API.

    Args:
        config_path (str): The path to the configuration file containing the
            API credentials.

    Attributes:
        auth (Auth): An instance of the Auth class used for authentication.
        endpoints (Endpoints): An instance of the Endpoints class used for
            accessing the API endpoints.
    """
    def __init__(self, config_path):
        """
        Initialize the SAMS client.

        Loads the API credentials from the configuration file and
        initializes the Auth and Endpoints instances.
        """
        with open(config_path, "r") as f:
            cred = json.load(f)
        
        self.auth = Auth(cred['username'], cred['password'])
        self.endpoints = Endpoints()
        
    def refresh(self):
        """
        Refreshes the authentication token by calling the `get_token` method of the `Auth` class.

        This method is called internally by other methods of the `SAMSClient` class to ensure that the authentication token is always up-to-date before making API requests.

        This method does not return anything.
        """
        self.auth.get_token()

    def get_student_data(self, module: str, academic_year: int, source_of_fund: int = None, page_number: int = None, count: bool = False) -> list | int:
        """
        Fetches student data from SAMS API for the given academic year,
        module and source of fund.

        Args:
            module (str): The module for which to fetch the data.
            academic_year (int): The academic year for which to fetch the data.
            source_of_fund (int, optional): The source of fund for which to fetch the data.
            page_number (int, optional): The page number for which to fetch the data.
            count (bool, default False): If True, returns the total number of records.

        Returns:
            list: List of dictionaries contained in the 'Data' field of the JSON response from the SAMS API.
            int: Total number of records if count is True.
        """
        if module not in ["ITI", "Diploma","PDIS"]:
            raise ValueError(f"Module {module} not supported.")
        if module in ["ITI", "Diploma"] and source_of_fund not in [1,5]:
            raise ValueError(f"Source of fund {source_of_fund} not supported.")

        if count:
            logger.info(f"Counting SAMS student records for module: {module},Year: {academic_year}, Source of Funds: {SOF['tostring'][source_of_fund]}")
        else:
            logger.info(f"Getting SAMS student module: {module}, Year: {academic_year},Source of Funds: {SOF['tostring'][source_of_fund]}, Page number: {page_number}")        

        # Set up packet
        url = self.endpoints.get_student_data()
        headers = self.auth.get_auth_header()
        params = {
            "Module": module,
            "AcademicYear": academic_year,
        }
        if module != "PDIS":
            params["SourceOfFund"] = source_of_fund
            params["PageNumber"] = page_number

        # Make HTTP request
        try:
            response = requests.get(url, headers=headers, json=params)
        except requests.ConnectTimeout as e:
            logger.error(f"Connection timeout: {e}")
            logger.info("Resetting connection...")
            self.refresh()
            response = requests.get(url, headers=headers, json=params)
        
        return self._handle_response(response, count)

    def get_institute_data(self, module: str, academic_year: int, admission_type: int = None, count: bool = False) -> list | int:
        """
        Fetches institute data from SAMS API for the given academic year
        and module.

        Args:
            module (str): The module for which to fetch the data.
            academic_year (int): The academic year for which to fetch the data.
            admission_type (int, optional): The admission type for which to fetch the data.
                                            (1 - Fresh entry, 2 - Lateral entry)
            count (bool, default False): If True, returns the total number of records.

        Returns:
            list: List of dictionaries contained in the 'Data' field of the JSON response from the SAMS API.
            int: Total number of records if count is True.
        """

        # Check if module and admission type is valid
        if module not in ["PDIS","ITI","Diploma"]:
            raise ValueError(f"Module {module} not supported.")
        
        if module == "Diploma" and admission_type not in [1,2]:
            raise ValueError(f"Admission type {admission_type} not supported.")
        
        if count:
            logger.info(f"Counting SAMS institute records for module: {module}, Year: {academic_year}, Admission Type: {admission_type}")
        else:
            logger.info(f"Getting SAMS institute module: {module}, Year: {academic_year}, Admission Type: {admission_type}")

        # Set up HTTP request
        url = self.endpoints.get_institute_data()
        headers = self.auth.get_auth_header()
        if module == "Diploma":
            params = {"Module": module, "AcademicYear": academic_year, "AdmissionType": admission_type}
        else:
            params = {"Module": module, "AcademicYear": academic_year}

        # Send request
        try:
            response = requests.get(url, headers=headers, json=params)
        except requests.ConnectTimeout as e:
            logger.error(f"Connection timeout: {e}")
            logger.info("Resetting connection...")
            self.refresh()
            response = requests.get(url, headers=headers, json=params)
        
        return self._handle_response(response, count)


    def _handle_response(self, response: requests.Response, count: bool = False) -> list | int:
        """
        Handles the response from the SAMS API.

        Args:
            response (requests.Response): The response from the SAMS API.

        Returns:
            list: The list of dictionaries, where each dictionary represents
                a student or institute.

        Raises:
            APIError: If the response status code is not 200.
        """
        if response.status_code == 200:
            json_response = response.json()
            
            # Check if the API returned a success message
            if 'success' in json_response and not json_response['success']:
                raise APIError(json_response['message'])
            
            # Check if the API returned a valid response
            reqd_fields = set(['StatusCode','TotalRecordCount','RecordCount','Data'])
            fields = json_response.keys()
            if not reqd_fields.issubset(fields):
                diff = reqd_fields.difference(fields)
                raise APIError(f"API returned invalid response: Fields {diff} are missing.")
            
            # Check if the response size is expected
            if len(json_response['Data']) != json_response['RecordCount']:
                raise APIError(f"API returned invalid response: Expected {json_response['RecordCount']} records, but got {len(json_response['Data'])}.")
            
            if count:
                return json_response['TotalRecordCount']
            else:
                return json_response['Data']

        elif response.status_code == 400:
            raise APIError("Bad Request: Some inputs are missing.")
        elif response.status_code == 500:
            raise APIError("Server Error: Something went wrong.")
        else:
            response.raise_for_status()

def main():
    client = SAMSClient(API_AUTH)

    # Fetch student data
    pdis_data = client.get_student_data(module="PDIS", academic_year=2022,count=False)
    logger.info(pdis_data[5])
    # iti_data = client.get_student_data(module="ITI", academic_year=2022, source_of_fund=1, page_number=5,count=False)
    # logger.info(iti_data)
    # diploma_data = client.get_student_data(module="Diploma", academic_year=2022, source_of_fund=1, page_number=1,count=False)
    # logger.info(diploma_data)

    # # Fetch institute data
    # institute_data = client.get_institute_data(module="PDIS", academic_year=2022,count=False)
    # logger.info(institute_data)
    

if __name__ == '__main__':
    main()
