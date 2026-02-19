import json
import os
import paho.mqtt.client as mqtt

from core.batch_writer import batch_writer
from core.utils import enrich_message

MQTT_HOST = os.getenv("MQTT_HOST")


def on_connect(client, userdata, flags, reason_code, properties):
    print(flush=True)
    print("✅ Conectado a MQTT:", reason_code, flush=True)
    print(flush=True)
    client.subscribe("#")
    print("✅ SUSCRITO A #", flush=True)
    print(flush=True)


def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode()

    print(flush=True)
    print(">>> MENSAJE RECIBIDO\n", flush=True)
    print("TOPIC:", topic, flush=True)
    print("PAYLOAD:", payload, flush=True)
    print(flush=True)

    parts = topic.split("/")

    if len(parts) < 3:
        print(flush=True)
        print(f"Topic inválido: {topic}", flush=True)
        print(flush=True)
        return

    device_id = parts[2]

    try:
        data = json.loads(payload)
        temp_f = float(data["temp"])
    except Exception as e:
        print(flush=True)
        print(f"Error parseando payload: {e}", flush=True)
        print(flush=True)
        return

    enriched = enrich_message(device_id, temp_f)

    print("[INGESTED]", enriched, flush=True)

    try:
        sensor_id = os.getenv('SENSOR_ID')  # ID real de tu sensor en PocketBase
        batch_writer.add(enriched, sensor_id)
    except Exception as e:
        print(flush=True)
        print(f"Error enviando a batch_writer: {e}", flush=True)
        print(flush=True)


def start():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_HOST, 1883, 60)
    client.loop_forever()
