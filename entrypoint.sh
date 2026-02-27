#!/bin/bash
set -e

echo "Iniciando servicio BentoML..."
exec bentoml serve api.service:MQTTService --host 0.0.0.0 --port 3000