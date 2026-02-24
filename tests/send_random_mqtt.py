import random
import subprocess
import time
import json

DEVICE_ID = "kzbh4my108zvefi"
TOPIC = f"devices/{DEVICE_ID}/readings"
BROKER = "host.docker.internal"
PORT = 1883

TOTAL_MESSAGES = 120

print(f"\nEnviando {TOTAL_MESSAGES} mensajes randomizados (válidos e inválidos)...\n")

for i in range(TOTAL_MESSAGES):
    # Decidir aleatoriamente si el mensaje es válido (70%) o inválido (30%)
    is_valid = random.random() < 0.7

    if is_valid:
        temp = random.randint(60, 100)  # valor válido
        payload = json.dumps({"temp": temp})
        tipo = "VALIDO"
    else:
        # payload inválido aleatorio
        invalid_values = ["INVALID", None, "", {}, []]
        payload = json.dumps({"temp": random.choice(invalid_values)})
        tipo = "INVALIDO"

    print(f"[{i+1}] {tipo} -> {payload}")

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