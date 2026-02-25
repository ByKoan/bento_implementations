import requests
import os

PB_URL = os.getenv('POCKETBASE_URL')
PB_USER = os.getenv('POCKETBASE_USER')
PB_PASS = os.getenv('POCKETBASE_PASSWORD')


class PocketBaseClient:

    '''
        A simple client to interact with the PocketBase API, 
        handling authentication and requests. 
        It includes a method to authenticate and obtain a token, 
        a method to make POST requests that automatically re-authenticates if the token is expired, 
        and a method to make GET requests.
    '''

    def __init__(self):
        self.token = None

    # ===============================
    # AUTH
    # ===============================

    def authenticate(self):

        '''Authenticate with PocketBase using the provided credentials and obtain a token.'''

        url = f"{PB_URL}/api/collections/users/auth-with-password"

        payload = {
            "identity": PB_USER,
            "password": PB_PASS
        }

        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()

        self.token = r.json()["token"]
        # If the authentication is successful, we store the token for future requests

        print()
        print("✅ PocketBase autenticado", flush=True)
        print()

    # ===============================
    # POST (NO rompe en 400)
    # ===============================

    def post(self, endpoint, data):

        '''Make a POST request to the PocketBase API with the given endpoint and data.'''

        if not self.token:
            self.authenticate()

        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
            # We set the content type to application/json since we are sending JSON data
        }

        url = f"{PB_URL}{endpoint}"

        r = requests.post(url, json=data, headers=headers, timeout=10) # We make the POST request with a timeout of 10 seconds

        if r.status_code == 401:
            print("\nToken expirado, reautenticando...\n", flush=True)
            self.authenticate()
            headers["Authorization"] = f"Bearer {self.token}"
            r = requests.post(url, json=data, headers=headers, timeout=10)
            # If the token was expired, we re-authenticate and try the request again with the new token

        print("\nSTATUS:", r.status_code, flush=True)
        print("BODY:", r.text, flush=True)
        print()

        if r.status_code >= 500:
            r.raise_for_status()
            # If the error is a server error (5xx), we raise an exception to trigger the retry mechanism in the batch writer

        return r

    # ===============================
    # GET
    # ===============================

    def get(self, path, params=None):

        '''Make a GET request to the PocketBase API with the given path and query parameters.'''

        if not self.token:
            self.authenticate()

        url = f"{PB_URL}{path}"

        response = requests.get(
            url,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json"
            },
            params=params,
            timeout=10,
            # We make the GET request with the token in the headers, the query parameters and a timeout of 10 seconds
        )

        return response