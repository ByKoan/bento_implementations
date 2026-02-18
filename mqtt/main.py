import os
import json
import requests
import paho.mqtt.client as mqtt
from dotenv import load_dotenv

# ------------------------
# Parameters
# ------------------------

load_dotenv()

'''User credentials and configuration'''
PB_URL = os.getenv("POCKETBASE_URL")
PB_EMAIL = os.getenv("POCKETBASE_EMAIL")
PB_PASSWORD = os.getenv("POCKETBASE_PASSWORD")

'''MQTT configuration'''
MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC")

'''PocketBase collection names'''
COLLECTION_DEVICE = "devices"
COLLECTION_SENSOR_CONTEXT = "sensor_contexts"
COLLECTION_SENSOR_TYPE = "sensor_types"
COLLECTION_SENSOR = "sensors"
COLLECTION_READING = "readings"

'''Cache local: (device, sensor_type, context) -> sensor_no'''
sensor_cache = {}

# ------------------------
# PocketBase login
# ------------------------

def pb_login():
    url = f"{PB_URL}/api/collections/_superusers/auth-with-password"

    r = requests.post(url, json= {
        "identity": PB_EMAIL,
        "password": PB_PASSWORD
    })

    r.raise_for_status()
    return r.json()["token"]

PB_TOKEN = pb_login()

''' Debug print token
print("Tu Token")
print()
print(PB_TOKEN)
print()
'''

HEADERS = {
    '''Authorization header for PocketBase API requests'''
    "Authorization": f"Bearer {PB_TOKEN}",
    "Content-Type": "application/json"
}

# ------------------------
# PocketBase helpers
# ------------------------

'''Helper functions to interact with PocketBase API'''

def pb_get(collection, filter_query):
    url = f"{PB_URL}/api/collections/{collection}/records"
    params = {"filter": filter_query}
    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()["items"]


def pb_create(collection, data):
    url = f"{PB_URL}/api/collections/{collection}/records"
    r = requests.post(url, headers=HEADERS, json=data)
    r.raise_for_status()
    return r.json()

# ------------------------
# Resolver sensor
# ------------------------

'''Given device name, sensor type and context, return the corresponding sensor_no from PocketBase. Uses caching to minimize API calls.'''

def get_sensor_no(device_name, sensor_type, context):

    cache_key = f"{device_name}:{sensor_type}:{context}"
    if cache_key in sensor_cache:
        return sensor_cache[cache_key]

    # buscar device
    devices = pb_get(
        COLLECTION_DEVICE,
        f'name="{device_name}"'
    )

    if not devices:
        raise Exception(f"Device not found: {device_name}")

    device_id = devices[0]["id"]

    # buscar context primero
    contexts = pb_get(
        COLLECTION_SENSOR_CONTEXT,
        f'context="{context}"'
    )

    if not contexts:
        raise Exception(f"Context not found: {context}")

    context_id = contexts[0]["id"]

    # buscar sensor_type
    types = pb_get(
        COLLECTION_SENSOR_TYPE,
        f'magnitude="{sensor_type}" && sensor_context="{context_id}"'
    )

    if not types:
        raise Exception(
            f"Sensor type not found: {sensor_type} ({context})"
        )

    sensor_type_id = types[0]["id"]

    # buscar sensor
    sensors = pb_get(
        COLLECTION_SENSOR,
        f'device="{device_id}" && sensor_type="{sensor_type_id}"'
    )

    if not sensors:
        raise Exception("Sensor not found")

    sensor_id = sensors[0]["id"]

    sensor_cache[cache_key] = sensor_id
    return sensor_id


# ------------------------
# MQTT callback
# ------------------------

'''Callback function for incoming MQTT messages. Parses the payload, resolves the sensor number, and stores the reading in PocketBase.'''

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())

        sensor_no = get_sensor_no(
            payload["device"],
            payload["sensor_type"],
            payload["context"]
        )

        reading = {
            "sensor_no": sensor_no,
            "time": payload["timestamp"],
            "value": payload["value"]
        }

        pb_create(COLLECTION_READING, reading)

        print("Reading stored:", reading)

    except Exception as e:
        print("Error:", e)


# ------------------------
# MQTT setup
# ------------------------

'''Set up MQTT client, connect to broker, and subscribe to topic. The client will run indefinitely, processing incoming messages with the on_message callback.'''

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_message = on_message

client.connect(MQTT_BROKER, MQTT_PORT, 60)
client.subscribe(MQTT_TOPIC)

print("MQTT connector running...")
print()
client.loop_forever()
