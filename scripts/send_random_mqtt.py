import random
import subprocess
import time
import json
import os
import dotenv

dotenv.load_dotenv()

# --- Configuración de Rutas ---
# Usamos r"" para evitar problemas con las barras de Windows
PATH_LOG = r"C:\Users\AIR\Desktop\bento_implementations\data\pending_readings.log"

TEMP_ID = os.getenv("TEMP_ID")
BATTERY_ID = os.getenv("BATTERY_ID")
STATUS_ID = os.getenv("STATUS_ID")

if not TEMP_ID or not BATTERY_ID or not STATUS_ID:
    raise ValueError("Faltan variables de entorno de sensores")

BROKER = "host.docker.internal"
PORT = 1883
TOTAL_MESSAGES = 21

# --- Función para contar líneas actuales ---
def contar_lineas(ruta):
    if not os.path.exists(ruta):
        return 0
    with open(ruta, 'r', encoding='utf-8') as f:
        return sum(1 for _ in f)

# Inicializamos el contador con lo que ya tenga el archivo
contador_actual = contar_lineas(PATH_LOG)
print(f"Líneas iniciales en el log: {contador_actual}")

battery = 100.0
battery_step = 0.5
status_states = {1: "IDLE", 2: "MOVING", 3: "CHARGING", 4: "ERROR"}
status = 1
moving_ticks = 0
ticks_per_moving = 5

for i in range(TOTAL_MESSAGES):
    # --- 1. Temperature ---
    temp = random.randint(60, 100)
    payload_temp = json.dumps({"sensor": TEMP_ID, "value": temp})
    subprocess.run([
        "docker", "run", "--rm", "eclipse-mosquitto:2",
        "mosquitto_pub", "-h", BROKER, "-p", str(PORT),
        "-t", f"devices/{TEMP_ID}/readings", "-m", payload_temp
    ], capture_output=True) # capture_output para no ensuciar la consola
    
    # Supongamos que cada mensaje enviado exitosamente cuenta como una inserción lógica
    contador_actual += 1 
    print(f"[{i+1}] Temperatura -> {temp} | Total líneas: {contador_actual}")

    # --- 2. Battery ---
    battery -= battery_step
    if battery < 0: battery = 0
    payload_batt = json.dumps({"sensor": BATTERY_ID, "value": round(battery, 2)})
    subprocess.run([
        "docker", "run", "--rm", "eclipse-mosquitto:2",
        "mosquitto_pub", "-h", BROKER, "-p", str(PORT),
        "-t", f"devices/{BATTERY_ID}/readings", "-m", payload_batt
    ], capture_output=True)
    
    contador_actual += 1
    print(f"[{i+1}] Batería -> {battery:.2f} | Total líneas: {contador_actual}")

    # --- 3. Status ---
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
        "docker", "run", "--rm", "eclipse-mosquitto:2",
        "mosquitto_pub", "-h", BROKER, "-p", str(PORT),
        "-t", f"devices/{STATUS_ID}/readings", "-m", payload_status
    ], capture_output=True)
    
    contador_actual += 1
    print(f"[{i+1}] Estado -> {status} ({status_states[status]}) | Total líneas: {contador_actual}")

    time.sleep(1)

print("-" * 30)
print(f"Simulación finalizada. Total final de líneas: {contador_actual}")