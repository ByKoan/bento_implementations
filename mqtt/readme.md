# Implementacion con MQTT

- Deberemos de tener descargado el binario de ***Mosquitto***

- Una vez instalado y configurado el binario de ***Mosquitto*** configuramos las rutas que aparecen comentadas en **main.py**

- Lanzamos el siguiente comando para comprobar que a ***Mosquitto*** le llegan los mensajes que introduciremos manualmente para hacer pruebas

```bash
mosquitto_sub -h localhost -t sensors/# -v
```

- Lanzamos el siguiente comando (Con **main.py** corriendo y podremos comprobar que ***Mosquitto*** lo recibio y se guardo en la base de datos formateado)

```bash
mosquitto_pub -h localhost -t sensors/test -m "{\"id\":\"sensor_01\",\"temp\":23.5,\"hum\":60,\"time\":\"2026-02-13T10:17:02Z\"}"
```