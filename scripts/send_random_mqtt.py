import random
import subprocess
import time
import json
import os
import dotenv

dotenv.load_dotenv()

TEMP_ID = os.getenv("TEMP_ID")
BATTERY_ID = os.getenv("BATTERY_ID")
STATUS_ID = os.getenv("STATUS_ID")

if not TEMP_ID or not BATTERY_ID or not STATUS_ID:
    raise ValueError("Faltan variables de entorno de sensores")

BROKER = "host.docker.internal"
PORT = 1883
TOTAL_MESSAGES = 21

TEMP_TOPIC = f"devices/{TEMP_ID}/readings"
BATTERY_TOPIC = f"devices/{BATTERY_ID}/readings"
STATUS_TOPIC = f"devices/{STATUS_ID}/readings"

battery = 100.0
battery_step = 0.5
status_states = {1: "IDLE", 2: "MOVING", 3: "CHARGING", 4: "ERROR"}
status = 1
moving_ticks = 0
ticks_per_moving = 5

for i in range(TOTAL_MESSAGES):
    # --- Temperature ---
    temp = random.randint(60, 100)
    payload_temp = json.dumps({"sensor": TEMP_ID, "value": temp})
    subprocess.run([
        "docker", "run", "--rm",
        "eclipse-mosquitto:2",
        "mosquitto_pub",
        "-h", BROKER,
        "-p", str(PORT),
        "-t", TEMP_TOPIC,
        "-m", payload_temp
    ])
    print(f"[{i+1}] Temperatura -> {temp}")

    # --- Battery ---
    battery -= battery_step
    if battery < 0: battery = 0
    payload_batt = json.dumps({"sensor": BATTERY_ID, "value": round(battery, 2)})
    subprocess.run([
        "docker", "run", "--rm",
        "eclipse-mosquitto:2",
        "mosquitto_pub",
        "-h", BROKER,
        "-p", str(PORT),
        "-t", BATTERY_TOPIC,
        "-m", payload_batt
    ])
    print(f"[{i+1}] Batería -> {battery:.2f}")

    # --- Status ---
    if battery < 20:
        status = 3
        moving_ticks = 0
    else:
        if moving_ticks < ticks_per_moving:
            status = 2
            moving_ticks += 1
        else:
            status = 1
            moving_ticks = 0

    payload_status = json.dumps({"sensor": STATUS_ID, "value": status})
    subprocess.run([
        "docker", "run", "--rm",
        "eclipse-mosquitto:2",
        "mosquitto_pub",
        "-h", BROKER,
        "-p", str(PORT),
        "-t", STATUS_TOPIC,
        "-m", payload_status
    ])
    print(f"[{i+1}] Estado -> {status} ({status_states[status]})")

    time.sleep(1)

print("Simulación finalizada.")