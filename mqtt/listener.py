import json
import os
import paho.mqtt.client as mqtt

from core.batch_writer import BatchWriter
from core.utils import enrich_message

MQTT_HOST = os.getenv("MQTT_HOST")

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

'''
MQTT Listener module that connects to the MQTT broker, subscribes to the topic and listens for incoming messages from the devices.
When a message is received, it is parsed, enriched and sent to the batch writer for further processing and storage in the database. If there is an error in the payload or topic, the original message is sent to the errors topic with a reason for the failure.
'''

def start (batch_writer):

    '''
    Main function to initialize the MQTT Listener.
    It connects to the broker, suscribes to the topics and defines the callbacks to handle the connection and incoming messages
    '''

    def on_connect(client, userdata, flags, reason_code, properties):

        '''
           Callback function that is called when the client connects to the MQTT broker successfully
           It subscribes to the topic to receive the messages from the devices
        '''
        
        print(flush=True)
        print("✅ Conectado a MQTT:", reason_code, flush=True)
        print(flush=True)
        topic2 = os.getenv("MQTT_TOPIC") # Añadir a .env
        client.subscribe(topic2)
        print("✅ SUSCRITO A", topic2, flush=True)
        print(flush=True)


    def on_message(client, userdata, msg):

        '''Callback function that is called when a message is received in the subscribed topic'''
        
        topic = msg.topic
        payload = msg.payload.decode()

        print(flush=True)
        print(">>> MENSAJE RECIBIDO\n", flush=True)
        print("TOPIC:", topic, flush=True)
        print("PAYLOAD:", payload, flush=True)
        print(flush=True)

        parts = topic.split("/") # Split the topic to extract the device_id (assuming topic structure is devices/{device_id}/readings)

        if len(parts) < 3:
            print(f"Topic inválido: {topic}", flush=True)
            return

        device_id = parts[1]

        try:
            data = json.loads(payload)
            temp_f = float(data["temp"])
            # If the temp is out of a reasonable range, we can consider it an error (for example, less than -100F or greater than 150F)
        except Exception as e:
            print(f"Error parseando payload: {e}", flush=True)
            
            error_payload = {
                "original_topic": topic,
                "original_payload": payload
            }
            reason = f"invalid_payload: {str(e)}"

            batch_writer._send_to_error_topic(error_payload, reason)
            # If the payload is invalid (wrong data) or the topic is wrong, we send the original message to the errors topic with a reason
            return

        enriched = enrich_message(device_id, temp_f) # Enrich the message with the required format for the batch writer (device_id, temp_f, temp_c, message_id, ingestion_timestamp)

        print("[INGESTED]", enriched, flush=True)

        try:
            batch_writer.add(enriched, device_id)
        except Exception as e:
            print(f"Error enviando a batch_writer: {e}", flush=True)
    
    mqtt_client.on_connect = on_connect # Define the on_connect callback to handle successful connections and subscribe to the topic
    mqtt_client.on_message = on_message # Define the on_message callback to handle incoming messages, parse them, enrich them and send them to the batch writer
    mqtt_client.reconnect_delay_set(1, 30) # Set a reconnection delay in case the connection to the broker is lost (from 1 second to 30 seconds)
    mqtt_client.connect(MQTT_HOST, 1883, 60) # Connect to the MQTT broker using the host defined in the environment variables, port 1883 and a keepalive of 60 seconds
    mqtt_client.loop_start() # Start the MQTT client loop in a separate thread to handle the network traffic and callbacks asynchronously, allowing the main thread to continue executing other tasks (like the batch writer)
