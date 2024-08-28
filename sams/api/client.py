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

    def get_student_data(self, module: str, academic_year: int, source_of_fund: int = None, page_number: int = None) -> list:
        """
        Fetches student data from SAMS API for the given academic year,
        module and source of fund.

        Args:
            module (str): The module for which to fetch the data.
            academic_year (int): The academic year for which to fetch the data.
            source_of_fund (int, optional): The source of fund for which to fetch the data.
            page_number (int, optional): The page number for which to fetch the data.

        Returns:
            list: List of dictionaries contained in the 'Data' field of the JSON response from the SAMS API.
        """
        if module in ["ITI", "Diploma"]:
            source_of_fund = source_of_fund or 1
            page_number = page_number or 1
            if source_of_fund not in [1,5]:
                raise ValueError(f"Source of fund {source_of_fund} not supported.")

            logger.info(f"Getting SAMS student module: {module},Year: {academic_year}, Source of Funds: {SOF['tostring'][source_of_fund]}, Page number: {page_number}")
        elif module == "PDIS":
            logger.info(f"Getting SAMS student module: {module}, Year: {academic_year}")
        else:
            raise ValueError(f"Module {module} not supported.")
        

        url = self.endpoints.get_student_data()
        headers = self.auth.get_auth_header()
        params = {
            "Module": module,
            "AcademicYear": academic_year,
        }
        if module != "PDIS":
            params["SourceOfFund"] = source_of_fund
            params["PageNumber"] = page_number

        try:
            response = requests.get(url, headers=headers, json=params)
        except requests.ConnectTimeout as e:
            logger.error(f"Connection timeout: {e}")
            logger.info("Resetting connection...")
            self.refresh()
            response = requests.get(url, headers=headers, json=params)
        
        return self._handle_response(response)

    def get_institute_data(self, module: str, academic_year: int) -> list:
        """
        Fetches institute data from SAMS API for the given academic year
        and module.

        Args:
            module (str): The module for which to fetch the data.
            academic_year (int): The academic year for which to fetch the data.

        Returns:
            list: List of dictionaries contained in the 'Data' field of the JSON response from the SAMS API.
        """
        if module not in ["PDIS","ITI","Diploma"]:
            raise ValueError(f"Module {module} not supported.")
        
        logger.info(f"Getting SAMS institute module: {module}, Year: {academic_year}")
        url = self.endpoints.get_institute_data()
        headers = self.auth.get_auth_header()
        params = {"Module": module, "AcademicYear": academic_year}

        try:
            response = requests.get(url, headers=headers, json=params)
        except requests.ConnectTimeout as e:
            logger.error(f"Connection timeout: {e}")
            logger.info("Resetting connection...")
            self.refresh()
            response = requests.get(url, headers=headers, json=params)
        
        return self._handle_response(response)

    def _handle_response(self, response: requests.Response) -> list:
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
            data = response.json()
            
            # Check if the API returned a success message
            if 'success' in data and not data['success']:
                raise APIError(data['message'])
            # Check if the API returned a valid response
            if 'Data' not in data:
                raise APIError("API returned invalid response: 'Data' field not found.")
            return data['Data']
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
    iti_data = client.get_student_data(module="ITI", academic_year=2022, source_of_fund=1, page_number=5)
    #diploma_data = client.get_student_data(module="Diploma", academic_year=2022, source_of_fund=1, page_number=1)

    # Fetch institute data
    institute_data = client.get_institute_data(module="PDIS", academic_year=2022)
    print(institute_data)
    #print(iti_data)

    

if __name__ == '__main__':
    main()
