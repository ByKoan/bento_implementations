import requests

'''
    This is a simple script to obtain the token of a user in PocketBase
'''

# Configuration
BASE_URL = "http://127.0.0.1:8090"
COLLECTION = "_superusers"  # Must be the unique id value of the collection
EMAIL = "koan@koan.com"  # User email
PASSWORD = "koan_12345"  # User password

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