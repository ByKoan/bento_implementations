import json
import os
import paho.mqtt.client as mqtt

from core.batch_writer import BatchWriter
from core.utils import enrich_message

MQTT_HOST = os.getenv("MQTT_HOST")

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

def start (batch_writer):

    def on_connect(client, userdata, flags, reason_code, properties):
        print(flush=True)
        print("✅ Conectado a MQTT:", reason_code, flush=True)
        print(flush=True)
        topic2 = client.subscribe("devices/+/readings")
        print("✅ SUSCRITO A {topic2}", flush=True)
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
            
            error_payload = {
                "original_topic": topic,
                "original_payload": payload
            }
            reason = f"invalid_payload: {str(e)}"

            batch_writer._send_to_error_topic(error_payload, reason)
            return

        enriched = enrich_message(device_id, temp_f)

        print("[INGESTED]", enriched, flush=True)

        try:
            batch_writer.add(enriched, device_id)
        except Exception as e:
            print(f"Error enviando a batch_writer: {e}", flush=True)
    
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.reconnect_delay_set(1, 30)
    mqtt_client.connect(MQTT_HOST, 1883, 60)
    mqtt_client.loop_start()