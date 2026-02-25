import random
import subprocess
import time
import json
import os 
import dotenv

dotenv.load_dotenv()

DEVICE_ID = os.getenv("DEVICE_ID")
if not DEVICE_ID:
    raise ValueError("DEVICE_ID no está definido en las variables de entorno")
TOPIC = f"devices/{DEVICE_ID}/readings"
BROKER = "host.docker.internal"
PORT = 1883

TOTAL_MESSAGES = 120

print(f"\nEnviando {TOTAL_MESSAGES} mensajes randomizados (válidos e inválidos)...\n")

for i in range(TOTAL_MESSAGES):
    
    # randomly decide whether the message is valid (70%) or invalid (30%)
    is_valid = random.random() < 0.7

    if is_valid:
        temp = random.randint(60, 100)  # Valid value
        payload = json.dumps({"temp": temp})
        tipo = "VALIDO"
    else:
        # Invalid random payload
        # Could be a string, null, empty, or even a completely different structure
        invalid_values = ["INVALID", None, "", {}, []]
        payload = json.dumps({"temp": random.choice(invalid_values)})
        tipo = "INVALIDO"

    print(f"[{i+1}] {tipo} -> {payload}")

    # Publish the message using mosquitto_pub in a Docker container
    subprocess.run([
        "docker", "run", "--rm",
        "eclipse-mosquitto:2",
        "mosquitto_pub",
        "-h", BROKER,
        "-p", str(PORT),
        "-t", TOPIC,
        "-m", payload
    ])

    time.sleep(1)

print("\nSimulación finalizada.\n")