import os
import time
import pytest
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv(".env")
load_dotenv("token.env")


TEMP_ID = os.getenv("TEMP_ID")
BATTERY_ID = os.getenv("BATTERY_ID")
STATUS_ID = os.getenv("STATUS_ID")
HAS_PALLET_ID = os.getenv("HAS_PALLET_ID")


BENTHOS_URL = os.getenv("BENTHOS_URL")
POCKETBASE_TOKEN = os.getenv("POCKETBASE_TOKEN")

def current_iso_timestamp():
    """Timestamp UTC compatible con PocketBase tipo date."""
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

@pytest.mark.integration
def test_benthos_pipeline_integration():
    """
    Test de integración:
    Envía datos de sensores al Benthos y verifica que la petición HTTP es aceptada.
    """
    agv_id = "AGV_001"

    payload = [
        {
            "sensor": TEMP_ID,
            "value": 25,
            "_collection": "readings",
            "agv_id": agv_id,
            "message_id": "msg-001",
            "time": current_iso_timestamp()
        },
        {
            "sensor": BATTERY_ID,
            "value": -5,
            "_collection": "urgent_alerts",
            "agv_id": agv_id,
            "message_id": "msg-002",
            "time": current_iso_timestamp()
        },
        {
            "sensor": STATUS_ID,
            "value": 1,
            "_collection": "readings",
            "agv_id": agv_id,
            "message_id": "msg-003",
            "time": current_iso_timestamp()
        },
        {
            "sensor": HAS_PALLET_ID,
            "value": 0,
            "_collection": "readings",
            "agv_id": agv_id,
            "message_id": "msg-004",
            "time": current_iso_timestamp()
        },
    ]

    # Espera corta para que Benthos esté levantado
    for _ in range(20):
        try:
            resp = requests.get(BENTHOS_URL)
            if resp.status_code in [200, 405]:
                break
        except requests.ConnectionError:
            time.sleep(1)
    else:
        pytest.fail(f"Benthos no está disponible en {BENTHOS_URL}")

    headers = {
        "Authorization": f"Bearer {POCKETBASE_TOKEN}",
        "Content-Type": "application/json"
    }

    # Enviar payload al endpoint de Benthos
    try:
        resp = requests.post(BENTHOS_URL, json=payload, headers=headers)
        resp.raise_for_status()
    except requests.RequestException as e:
        pytest.fail(f"No se pudo enviar datos al Benthos: {e}")

    # Comprobar que la petición fue aceptada
    assert resp.status_code in [200, 202], f"Respuesta inesperada: {resp.status_code}"