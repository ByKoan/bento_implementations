import bentoml
from fastapi import FastAPI

svc = bentoml.Service("mqtt_service")
app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}