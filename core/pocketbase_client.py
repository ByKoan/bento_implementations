import requests
import os

PB_URL = os.getenv('POCKETBASE_URL')
PB_USER = os.getenv('POCKETBASE_USER')
PB_PASS = os.getenv('POCKETBASE_PASSWORD')

class PocketBaseClient:
    def __init__(self):
        self.token = None

    def authenticate(self):
        url = f"{PB_URL}/api/collections/users/auth-with-password"

        payload = {
            "identity": PB_USER,
            "password": PB_PASS
        }

        r = requests.post(url, json=payload)
        r.raise_for_status()

        self.token = r.json()["token"]
        print(flush=True)
        print("✅ PocketBase autenticado", flush=True)
        print(flush=True)

    def post(self, endpoint, data):
        if not self.token:
            self.authenticate()

        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

        url = f"{PB_URL}{endpoint}"
        r = requests.post(url, json=data, headers=headers)

        # Si el token expiró, reautenticamos automáticamente
        if r.status_code == 401:
            print("\nToken expirado, reautenticando...\n", flush=True)
            self.authenticate()
            headers["Authorization"] = f"Bearer {self.token}"
            r = requests.post(url, json=data, headers=headers)

        print("\nSTATUS:", r.status_code, flush=True)
        print("BODY:", r.text, flush=True)
        print()

        r.raise_for_status()

        return r


    def get(self, path, params=None):
        url = f"{PB_URL}{path}"

        response = requests.get(
            url,
            headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
            },
            params=params,
            timeout=10,
        )

        return response