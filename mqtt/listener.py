import json
import os
import paho.mqtt.client as mqtt

from core.utils import fahrenheit_a_celsius, build_ingestion_metadata

MQTT_HOST = os.getenv("MQTT_HOST", "mqtt")

def on_connect(client, userdata, flags, reason_code, properties):
    print("✅ Conectado a MQTT:", reason_code)
    client.subscribe("v1/avg/+/telemetry")

def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode()

    parts = topic.split("/")
    device_id = parts[2]

    data = json.loads(payload)

    temp_f = float(data["temp"])
    temp_c = fahrenheit_a_celsius(temp_f)

    enriched = {
        **build_ingestion_metadata(),
        "device_id": device_id,
        "temp_f": temp_f,
        "temp_c": round(temp_c, 2),
    }

    print("[INGESTED]", enriched)

def start():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_HOST, 1883, 60)
    client.loop_forever()
