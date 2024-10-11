import requests
import time
from loguru import logger
from sams.api.exceptions import APIError


class Auth:
    def __init__(self, username, password):
        """
        Initializes the Auth class with the provided username and password.

        Args:
            username (str): The username for authentication.
            password (str): The password for authentication.
        """

        # Store the username and password
        self.username = username
        self.password = password

        # Initialize the token and last token refresh time to None
        self.token = None
        self.last_token_refresh = None

    def get_token(self):
        """
        Retrieves the token from the SAMS API by making a POST request to the
        getDPICtoken endpoint.

        Raises:
            Exception: If the authentication fails and a token is not retrieved.
        """

        # Set the URL for the getDPICtoken endpoint
        url = "https://api.samsodisha.gov.in/api/getDPICtoken"

        # Set the payload containing the username and password
        payload = {"username": self.username, "password": self.password}

        # Make a POST request to the getDPICtoken endpoint with the payload
        response = requests.post(url, json=payload)

        # Check if the response status code is 200 and retrieve the token
        if response.status_code == 200:
            token = response.json().get("Token_No")
            self.token = token
            self.last_token_refresh = time.time()
        else:
            raise APIError("Authentication failed")

    def get_auth_header(self):
        """
        Returns the authentication header required to authorize API requests.

        This function retrieves a token from the SAMS API if it is not already
        present or if it has expired. It then returns the authentication header
        containing the token.

        Returns:
            dict: The authentication header with the token.
        """
        # Check if the token has expired or if it has not been retrieved yet
        if not self.last_token_refresh or time.time() - self.last_token_refresh > 1800:
            self.get_token()

        return {"Authorization": f"Bearer {self.token}"}
