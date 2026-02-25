import uuid
from datetime import datetime, timezone

def fahrenheit_a_celsius(fahrenheit: float) -> float:
    '''Convert a temperature from Fahrenheit to Celsius.'''
    return (fahrenheit - 32) * 5 / 9

def build_ingestion_metadata():
    '''Build the metadata for the ingestion, including a unique message_id and an ingestion_timestamp in ISO format.'''
    return {
        "message_id": str(uuid.uuid4()),
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat()
    }

def enrich_message(device_id: str, temp_f: float):
    '''Enrich the message with the required format for the batch writer (device_id, temp_f, temp_c, message_id, ingestion_timestamp)'''
    temp_c = fahrenheit_a_celsius(temp_f)

    return {
        **build_ingestion_metadata(),
        "device_id": device_id,
        "temp_f": temp_f,
        "temp_c": round(temp_c, 2)
    }