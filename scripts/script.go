package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"time"
)

const (
	baseURL       = "http://127.0.0.1:8090/api/collections"
	adminToken    = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjb2xsZWN0aW9uSWQiOiJwYmNfMzE0MjYzNTgyMyIsImV4cCI6MTc3MjI4MDM3OCwiaWQiOiJidGg5bXMwMXk5YXc3ZWciLCJyZWZyZXNoYWJsZSI6dHJ1ZSwidHlwZSI6ImF1dGgifQ.t-OqBVPZsE47-dnBejrs-TzYt-p1EIat8xadfvxJ91o" // reemplaza con tu token admin
	emailAdmin    = "admin@example.com"
	password      = "Admin123!"
	updatePeriod  = 5 * time.Second // intervalo de envío de lecturas
	batteryMin    = 20              // % batería mínima antes de charging
	hasPalletTick = 5               // ticks que mantiene pallet
)

// AGV para simular lecturas
type AGV struct {
	ID          string
	Temperature float64
	Battery     float64
	Status      int
	HasPallet   int
	PalletTicks  int
	SensorIDs   map[string]string // "temperature","battery","has_pallet","status"
}

// Lectura enviada a Benthos
type Reading struct {
	Sensor             string  `json:"sensor"`
	Value              float64 `json:"value"`
	HasPallet          int     `json:"has_pallet"`
	Status             int     `json:"status"`
	IngestionTimestamp string  `json:"ingestion_timestamp"`
	TempC              float64 `json:"temp_c"`
}

// Función helper para crear un registro en PocketBase
func createRecord(collection string, data map[string]interface{}) string {
	url := fmt.Sprintf("%s/%s/records", baseURL, collection)
	jsonData, _ := json.Marshal(data)

	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+adminToken)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		log.Fatalf("Error creando %s: %s\n%s", collection, resp.Status, string(bodyBytes))
	}

	var res map[string]interface{}
	json.Unmarshal(bodyBytes, &res)
	return res["id"].(string)
}

// Buscar usuario por email
func getUserIDByEmail(email string) string {
	url := fmt.Sprintf("%s/users/records?filter=email='%s'", baseURL, email)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", "Bearer "+adminToken)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	var res map[string]interface{}
	json.Unmarshal(bodyBytes, &res)

	items, ok := res["items"].([]interface{})
	if ok && len(items) > 0 {
		user := items[0].(map[string]interface{})
		return user["id"].(string)
	}
	return ""
}

func main() {
	rand.Seed(time.Now().UnixNano())
	timestamp := time.Now().Unix()

	// 1️⃣ Crear usuario admin
	userID := getUserIDByEmail(emailAdmin)
	if userID == "" {
		userID = createRecord("users", map[string]interface{}{
			"email":           emailAdmin,
			"password":        password,
			"passwordConfirm": password,
			"name":            "Admin",
			"emailVisibility": true,
		})
		fmt.Println("Usuario creado con ID:", userID)
	} else {
		fmt.Println("Usuario ya existe con ID:", userID)
	}

	// 2️⃣ Crear localizaciones
	locations := []struct {
		name string
		lat  float64
		lng  float64
	}{
		{"Zona A", 33.4, 45.2},
		{"Zona B", 10.5, 5.2},
		{"Zona C", -3.3, 8.8},
	}
	locIDs := make([]string, 0, len(locations))
	for _, loc := range locations {
		id := createRecord("locations", map[string]interface{}{
			"name": loc.name,
			"point": map[string]float64{ // <-- debe ser un mapa, no string
				"lat": loc.lat,
				"lng": loc.lng, // no lon
			},
		})
		locIDs = append(locIDs, id)
	}

	// 3️⃣ Crear devices (AGVs)
	agvs := []string{
		fmt.Sprintf("AGV-01-%d", timestamp),
		fmt.Sprintf("AGV-02-%d", timestamp),
		fmt.Sprintf("AGV-03-%d", timestamp),
		fmt.Sprintf("AGV-04-%d", timestamp),
		fmt.Sprintf("AGV-05-%d", timestamp),
	}
	agvIDs := make([]string, 0, len(agvs))
	for _, name := range agvs {
		id := createRecord("devices", map[string]interface{}{
			"user": userID,
			"name": name,
		})
		agvIDs = append(agvIDs, id)
	}

	// 4️⃣ Crear devices_locations
	assignments := []struct {
		agvIndex int
		locIndex int
	}{
		{0, 0}, {1, 1}, {2, 2}, {3, 0}, {4, 1},
	}
	for _, a := range assignments {
		createRecord("devices_locations", map[string]interface{}{
			"device":    agvIDs[a.agvIndex],
			"location":  locIDs[a.locIndex],
			"placed_at": time.Now().Format(time.RFC3339),
		})
	}

	// 5️⃣ Crear sensor_context
	sensorContextID := createRecord("sensor_contexts", map[string]interface{}{
		"context": "data",
	})

	// 6️⃣ Crear sensor_types
	sensorTypes := []struct {
		magnitude string
		unit      string
	}{
		{"temperature", "°C"},
		{"battery", "%"},
		{"has_pallet", "0/1"},
		{"status", "0/1/2/3"},
	}
	sensorTypeIDs := make([]string, 0, len(sensorTypes))
	for _, st := range sensorTypes {
		id := createRecord("sensor_types", map[string]interface{}{
			"sensor_context": sensorContextID,
			"magnitude":      st.magnitude,
			"unit":           st.unit,
		})
		sensorTypeIDs = append(sensorTypeIDs, id)
	}

	// 7️⃣ Crear sensores
	sensorsMap := map[string]map[string]string{} // agvID -> sensor_type -> sensorID
	for _, agvID := range agvIDs {
		sensorsMap[agvID] = map[string]string{}
		for i, stID := range sensorTypeIDs {
			sensorID := createRecord("sensors", map[string]interface{}{
				"device":      agvID,
				"sensor_type": stID,
			})
			switch i {
			case 0:
				sensorsMap[agvID]["temperature"] = sensorID
			case 1:
				sensorsMap[agvID]["battery"] = sensorID
			case 2:
				sensorsMap[agvID]["has_pallet"] = sensorID
			case 3:
				sensorsMap[agvID]["status"] = sensorID
			}
		}
	}

	fmt.Println("Inicialización completa. Empezando simulación de lecturas...")

	// 8️⃣ Simulación de lecturas en tiempo real para Benthos
	agvObjs := []*AGV{}
	for _, agvID := range agvIDs {
		agvObjs = append(agvObjs, &AGV{
			ID:          agvID,
			Temperature: 20 + rand.Float64()*10,
			Battery:     100,
			Status:      1,
			HasPallet:   0,
			PalletTicks: 0,
			SensorIDs:   sensorsMap[agvID],
		})
	}

	ticker := time.NewTicker(updatePeriod)
	defer ticker.Stop()

	for range ticker.C {
		for _, agv := range agvObjs {
			// temperatura aleatoria
			agv.Temperature += rand.Float64()*2 - 1
			if agv.Temperature < 15 {
				agv.Temperature = 15
			}
			if agv.Temperature > 35 {
				agv.Temperature = 35
			}

			// batería
			agv.Battery -= rand.Float64() * 2
			if agv.Battery < 0 {
				agv.Battery = 100
			}

			// estado
			agv.Status = 1
			if rand.Intn(10) < 3 || agv.HasPallet == 1 {
				agv.Status = 2
			}
			if agv.Battery < batteryMin {
				agv.Status = 3
			}
			if rand.Intn(100) < 2 {
				agv.Status = 4
			}

			// pallet
			if agv.PalletTicks == 0 && rand.Intn(5) == 0 {
				agv.HasPallet = 1
				agv.PalletTicks = hasPalletTick
			}
			if agv.PalletTicks > 0 {
				agv.PalletTicks--
				if agv.PalletTicks == 0 {
					agv.HasPallet = 0
				}
			}

			// generar lecturas
			readings := []Reading{
				{Sensor: agv.SensorIDs["temperature"], Value: agv.Temperature, HasPallet: agv.HasPallet, Status: agv.Status, IngestionTimestamp: time.Now().UTC().Format(time.RFC3339Nano), TempC: agv.Temperature},
				{Sensor: agv.SensorIDs["battery"], Value: agv.Battery, HasPallet: agv.HasPallet, Status: agv.Status, IngestionTimestamp: time.Now().UTC().Format(time.RFC3339Nano), TempC: agv.Temperature},
				{Sensor: agv.SensorIDs["has_pallet"], Value: float64(agv.HasPallet), HasPallet: agv.HasPallet, Status: agv.Status, IngestionTimestamp: time.Now().UTC().Format(time.RFC3339Nano), TempC: agv.Temperature},
				{Sensor: agv.SensorIDs["status"], Value: float64(agv.Status), HasPallet: agv.HasPallet, Status: agv.Status, IngestionTimestamp: time.Now().UTC().Format(time.RFC3339Nano), TempC: agv.Temperature},
			}

			// enviar a stdout para Benthos
			for _, r := range readings {
				j, _ := json.Marshal(r)
				fmt.Println(string(j))
			}
		}
	}
}