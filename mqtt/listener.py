import json
import os
import paho.mqtt.client as mqtt

from core.batch_writer import BatchWriter
from core.utils import enrich_message

MQTT_HOST = os.getenv("MQTT_HOST")

# 👇 CLIENTE GLOBAL EXPORTABLE
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

# 👇 Creamos batch_writer aquí pasando el cliente
batch_writer = BatchWriter(mqtt_client=mqtt_client)


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
        print(f"Topic inválido: {topic}", flush=True)
        return

    device_id = parts[1]

    try:
        data = json.loads(payload)
        temp_f = float(data["temp"])
    except Exception as e:
        print(f"Error parseando payload: {e}", flush=True)
        return

    enriched = enrich_message(device_id, temp_f)

    print("[INGESTED]", enriched, flush=True)

    try:
        batch_writer.add(enriched, device_id)
    except Exception as e:
        print(f"Error enviando a batch_writer: {e}", flush=True)


def start():
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(MQTT_HOST, 1883, 60)
    mqtt_client.loop_forever()