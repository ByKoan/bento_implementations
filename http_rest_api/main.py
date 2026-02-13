from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
import subprocess

from database import engine, SessionLocal
from models import Base, SensorData
from schemas import SensorDataCreate
from bento_runner import start_bento


# Crear tablas
Base.metadata.create_all(bind=engine)

app = FastAPI()


bento_process = None

# ===============================
# ARRANQUE AUTOMATICO DE BENTO
# ===============================

@app.on_event("startup")
def start_bento():
    global bento_process

    bento_process = subprocess.Popen(
        [
            "C:\\Users\\AIR\\Desktop\\bento\\codes\\http_rest_api\\bento.exe",
            "-c",
            "bento.yaml"
        ],
        cwd="C:\\Users\\AIR\\Desktop\\bento\\codes\\http_rest_api"
    )

    print("Bento iniciado automáticamente")

# ===============================
# PARAR AUTOMATICO DE BENTO
# ===============================

@app.on_event("shutdown")
def stop_bento():
    global bento_process
    if bento_process:
        bento_process.terminate()
        print("Bento detenido")

# ===============================
# DEPENDENCIA BD
# ===============================

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ===============================
# ENDPOINT INGESTA (RECIBE BENTO)
# ===============================

@app.post("/ingest")
def ingest_sensor_data(
    data: SensorDataCreate,
    db: Session = Depends(get_db)
):

    print("Datos recibidos desde Bento:", data)
    print(data.device_id)


    sensor_row = SensorData(**data.dict())

    db.add(sensor_row)
    db.commit()
    db.refresh(sensor_row)

    return {"status": "stored", "id": sensor_row.id}


# ===============================
# VER DATOS
# ===============================

@app.get("/data")
def read_data(db: Session = Depends(get_db)):

    rows = db.query(SensorData).all()

    result = [
        {
            "id": r.id,
            "device_id": r.device_id,
            "temperature": r.temperature,
            "humidity": r.humidity,
            "timestamp": r.timestamp
        }
        for r in rows
    ]

    print("Datos en BD:", result)

    return result
