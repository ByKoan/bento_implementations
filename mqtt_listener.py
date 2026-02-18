import os
import json
import uuid
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

MQTT_HOST = os.getenv("MQTT_HOST", "mqtt")
TOPIC = "sensores/#"

def build_ingestion_metadata():
    return {
        "message_id": str(uuid.uuid4()),
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat()
    }


def fahrenheit_a_celsius(fahrenheit: float) -> float:
    return (fahrenheit - 32) * 5 / 9

def extraer_device_id(topic: str) -> str | None:
    partes = topic.split("/")

    try:
        idx = partes.index("avg")
        return partes[idx + 1]
    except (ValueError, IndexError):
        return None

def on_connect(client, userdata, flags, rc):
    print("✅ Conectado a MQTT con código:", rc, flush=True)
    client.subscribe("#")

def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        payload = msg.payload.decode().strip()

        # mosquitto_pub -h localhost -p 1883 -t sensores/temp -m "{\"temp\":77}"
        print(f"[MQTT RAW] topic={topic} payload={payload}", flush=True)

        if not payload:
            print("[WARN] Payload vacío, ignorado")
            return

        device_id = extraer_device_id(topic)

        if not device_id:
            print(f"[WARN] Topic inválido para device_id: {topic}")
            return

        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            data = {"temp": float(payload)}

        temp_f = float(data["temp"])
        temp_c = fahrenheit_a_celsius(temp_f)

        enriched_message = {
            **build_ingestion_metadata(),
            "device_id": device_id,
            "temp_f": temp_f,
            "temp_c": round(temp_c, 2)
        }

        # mosquitto_pub -h localhost -p 1883 -t v1/avg/AGV_05/telemetry -m "{\"temp\":77}"
        print("[INGESTED]", json.dumps(enriched_message), flush=True)

    except Exception as e:
        print("[ERROR]", e, flush=True)

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_HOST, 1883, 60)
client.loop_forever()
