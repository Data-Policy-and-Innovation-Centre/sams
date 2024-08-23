import requests

class Auth:
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.token = None

    def get_token(self):
        url = "https://api.samsodisha.gov.in/api/getDPICtoken"
        payload = {"username": self.username, "password": self.password}
        response = requests.post(url, json=payload)

        if response.status_code == 200:
            self.token = response.json().get("Token_No")
        else:
            raise Exception("Authentication failed")

    def refresh_token(self):
        self.get_token()

    def get_auth_header(self):
        if not self.token:
            self.get_token()
        return {"Authorization": f"Bearer {self.token}"}
