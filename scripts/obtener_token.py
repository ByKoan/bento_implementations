import requests
import dotenv
import os

dotenv.load_dotenv()

'''
    This is a simple script to obtain the token of a user in PocketBase
'''

# Configuration
BASE_URL = os.getenv("DB_URL")
COLLECTION = os.getenv("AUTHENTICATION_COLLECTION")
EMAIL = os.getenv("POCKETBASE_SUPERUSER")
PASSWORD = os.getenv("POCKETBASE_SUPERPASSWORD")

# Authentication endpoint
url = f"{BASE_URL}/api/collections/{COLLECTION}/auth-with-password"

# Payload
payload = {
    "identity": EMAIL,
    "password": PASSWORD
}

# POST request to obtain the token
response = requests.post(url, json=payload)

if response.status_code == 200:
    data = response.json()
    token = data.get("token")
    if token:
        print("Token de usuario:", token)
    else:
        print("No se obtuvo token. Revisa que el usuario exista y la contraseña sea correcta.")
else:
    print("Error:", response.status_code, response.text)