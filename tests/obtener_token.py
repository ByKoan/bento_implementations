import requests

# Configuración
BASE_URL = "http://127.0.0.1:8090"
COLLECTION = "_superusers"  # Debe ser el ID interno de la colección que creaste
EMAIL = "koan@koan.com"  # Email del usuario que creaste
PASSWORD = "koan_12345"  # Contraseña del usuario

# Endpoint de autenticación de la colección
url = f"{BASE_URL}/api/collections/{COLLECTION}/auth-with-password"

# Payload
payload = {
    "identity": EMAIL,
    "password": PASSWORD
}

# Hacer POST
response = requests.post(url, json=payload)

# Mostrar resultado
if response.status_code == 200:
    data = response.json()
    token = data.get("token")
    if token:
        print("Token de usuario:", token)
    else:
        print("No se obtuvo token. Revisa que el usuario exista y la contraseña sea correcta.")
else:
    print("Error:", response.status_code, response.text)