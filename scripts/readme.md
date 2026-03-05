## How to use this project?

 - ***Firs of all you need to get the project UP, run the project in docker, open the Database with:***

 ```bash
 go run main.go serve
 ```

 - ***Then you now can run the scripts to test the program***

### How the JSON is formulated:

 - ***State of every JSON the project made:***

 - This is the JSON before sended to Benthos

 ```json
  {
    "message_id": "c2c9c1c2-8b7e-4b0c-9e5c-8c4d3b1c6f1a",
    "_collection": "readings",
    "sensor": "5bqfwcv9g6g1tm6",
    "value": 24.7,
    "time": "2026-03-05T18:02:10Z"
  }

  {
    "message_id": "e1f7c3f2-3f74-4e4e-a4a2-1d2c7f9eaa00",
    "_collection": "urgent_alerts",
    "sensor": "5bqfwcv9g6g1tm6",
    "value": "Temperatura crítica: 34.2°C",
    "time": "2026-03-05T18:02:10Z"
  }

 ``` 

 - Benthos procceses it, and upload to DataBase choosing the collection we send it in "_collection"

```json
{
  "message_id": "c2c9c1c2-8b7e-4b0c-9e5c-8c4d3b1c6f1a",
  "sensor": "5bqfwcv9g6g1tm6",
  "value": 24.7,
  "time": "2026-03-05T18:02:10Z"
}

{
  "message_id": "e1f7c3f2-3f74-4e4e-a4a2-1d2c7f9eaa00",
  "sensor": "5bqfwcv9g6g1tm6",
  "value": "Temperatura crítica: 34.2°C",
  "time": "2026-03-05T18:02:10Z"
}
```

## Explanation of the scripts:

- ***At the momento we have 4 Scripts:***

### `obtener_token.py`:

- ***I made this script to obtain the token of every user you need, too authenticate it in DataBase***

### `send_random_mqtt.py`:

- ***This script is mostly for testing, you need to create the device, locations etc and this script will upload data, like: random temperatures, decremental battery, status of the device and if the device has a pallet.
This is a minimum simulation only to test funcionalities***

### `simulate_wrong_data.py`:

- ***This script is mostly like the previous, it is dedicated only to test wrong data and alerts.***

### `script.go`:

- ***This script will run a simulation, as if the project where in production. This script automatically create everything. Devices, locations, sensors (1 of every sensor (temperature, battery, status, has_pallet) for every device) and upload the data. For this script is important to grant access to create rule in the readings collection for everyone because if this isnt granted it cant upload the data and will have errors will running.*** 