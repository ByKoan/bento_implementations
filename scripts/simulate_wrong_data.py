import os
import json
import time
import paho.mqtt.publish as publish
from dotenv import load_dotenv

load_dotenv()

MQTT_BROKER = os.getenv("MQTT_BROKER_LOCAL")
MQTT_PORT = int(os.getenv("MQTT_PORT"))

BATTERY_ID = os.getenv("BATTERY_ID")
TEMP_ID = os.getenv("TEMP_ID")
HAS_PALLET_ID = os.getenv("HAS_PALLET_ID")
STATUS_ID = os.getenv("STATUS_ID")

TOPIC_TEMPLATE = "devices/{}/readings"

test_readings = [
    #{"sensor": BATTERY_ID, "value": 10, "type": "battery"},   # URGENTE: bajo
    #{"sensor": TEMP_ID, "value": 75, "type": "temperature"},  # URGENTE: alto
    #{"sensor": BATTERY_ID, "value": -5, "type": "battery"},   # INVÁLIDO
    #{"sensor": TEMP_ID, "value": 150, "type": "temperature"}, # INVÁLIDO
    #{"sensor": HAS_PALLET_ID, "value": -2, "type": "has_pallet"}, # INVÁLIDO
    #{"sensor": STATUS_ID, "value": -2, "type": "status"}, # INVÁLIDO
    #{"sensor": TEMP_ID, "value": 150, "type": "temperature"}, # INVÁLIDO
    {"sensor": BATTERY_ID, "value": 50, "type": "battery"},   # NORMAL
    {"sensor": TEMP_ID, "value": 65, "type": "temperature"},  # NORMAL
    {"sensor": HAS_PALLET_ID, "value": 1, "type": "has_pallet"},  # NORMAL
    {"sensor": STATUS_ID, "value": 3, "type": "status"},  # NORMAL
]

for i, reading in enumerate(test_readings, 1):
    payload = {
        "sensor": reading["sensor"],
        "value": reading["value"],
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }

    topic = TOPIC_TEMPLATE.format(reading["sensor"])

    publish.single(
        topic=topic,
        payload=json.dumps(payload),
        hostname=MQTT_BROKER,
        port=MQTT_PORT
    )
    print(f"[{i}] Enviado {reading['type']} -> {reading['value']}")
    time.sleep(0.5)

print("Simulación completada. Revisa logs y PocketBase.")