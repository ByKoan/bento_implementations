import os
import json
import logging
import threading
import paho.mqtt.client as mqtt
import datetime

from core.batch_writer import batch_writer

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC")

'''
This class will start the MQTT listener to listen the records
'''

# ===============================
# CALLBACKS
# ===============================
def on_connect(client, userdata, flags, rc):

    '''
    This function will connect to the topic
    '''

    if rc == 0:
        logger.info("Conectado a MQTT broker")
        client.subscribe(MQTT_TOPIC)
        logger.info(f"Suscrito a topic: {MQTT_TOPIC}")
    else:
        logger.error(f"Error al conectar a MQTT broker: {rc}")

def on_message(client, userdata, msg):

    '''
    This function execute automatically every time a record is sended
    It decode the message validate the fields add 'ingestion_timestamp' if it dont came
    Add 'temp_c' if it's necessary and send it to the batchwriter 
    '''

    try:
        payload = json.loads(msg.payload.decode())
        # Minimun fields required
        if "sensor" not in payload or "value" not in payload:
            print(f"Mensaje MQTT incompleto: {payload}", flush=True)
            return

        # Add ingestion_timestamp if needed
        if "ingestion_timestamp" not in payload:
            payload["ingestion_timestamp"] = datetime.datetime.utcnow().isoformat() + "Z"

        # Add temp_c if needed
        if "temp_c" not in payload:
            payload["temp_c"] = float(payload["value"])

        batch_writer.add(payload, payload["sensor"])
    except Exception as e:
        print(f"Error procesando mensaje MQTT: {e}", flush=True)

# ===============================
# START LISTENER
# ===============================
def start(batch_writer_instance=None):
    """
    Start the MQTT client on anothe thread and connect it to the MQTT Broker
    """
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_BROKER, MQTT_PORT, 60)

    # loop_forever run the actual thread
    thread = threading.Thread(target=client.loop_forever, daemon=True)
    thread.start()
    logger.info("MQTT listener iniciado en segundo plano")