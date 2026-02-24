import random
import subprocess
import time
import json

DEVICE_ID = "kzbh4my108zvefi"

TOPIC = f"devices/{DEVICE_ID}/readings"
BROKER = "host.docker.internal"
PORT = 1883

print("\nEnviando 1 inválido + 10 válidos...\n")

# -------------------------
# 1️⃣ MENSAJE INVÁLIDO
# -------------------------

invalid_payload = json.dumps({"temp": "INVALID"})

print(f"[1] INVALIDO -> {invalid_payload}")

subprocess.run([
    "docker", "run", "--rm",
    "eclipse-mosquitto:2",
    "mosquitto_pub",
    "-h", BROKER,
    "-p", str(PORT),
    "-t", TOPIC,
    "-m", invalid_payload
])

time.sleep(1)

# -------------------------
# 2️⃣ 10 MENSAJES VÁLIDOS
# -------------------------

for i in range(10):
    temp = random.randint(60, 100)
    payload = json.dumps({"temp": temp})

    print(f"[{i+2}] VALIDO -> {payload}")

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