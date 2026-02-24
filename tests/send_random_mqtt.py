import random
import subprocess
import time
import json

DEVICE_ID = "kzbh4my108zvefi"

TOPIC = f"devices/{DEVICE_ID}/readings"
BROKER = "host.docker.internal"
PORT = 1883

MESSAGES = 20

print("Enviando mensajes MQTT aleatorios...\n")

for i in range(MESSAGES):
    temp = random.randint(60, 100)
    payload = json.dumps({"temp": temp})

    print(f"[{i+1}] Enviando a {TOPIC}: {payload}")

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

print("\nPrueba finalizada.")