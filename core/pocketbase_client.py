import requests

PB_URL = "http://host.docker.internal:8090"
PB_USER = "test@test.com"
PB_PASS = "test_1234"


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
            print(flush=True)
            print("Token expirado, reautenticando...", flush=True)
            print(flush=True)
            self.authenticate()
            headers["Authorization"] = f"Bearer {self.token}"
            r = requests.post(url, json=data, headers=headers)

        print(flush=True)
        print("STATUS:", r.status_code, flush=True)
        print("BODY:", r.text, flush=True)
        print(flush=True)

        r.raise_for_status()
        return r.json()
