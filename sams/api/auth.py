import requests
import time

class Auth:
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.token = None
        self.last_token_refresh = None

    def get_token(self):
        url = "https://api.samsodisha.gov.in/api/getDPICtoken"
        payload = {"username": self.username, "password": self.password}
        response = requests.post(url, json=payload)

        if response.status_code == 200:
            self.token = response.json().get("Token_No")
            self.last_token_refresh = time.time()
        else:
            raise Exception("Authentication failed")

    def get_auth_header(self):
        if not self.last_token_refresh or time.time() - self.last_token_refresh > 1800:
            self.get_token()
        return {"Authorization": f"Bearer {self.token}"}
