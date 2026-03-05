package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	baseURL       = "http://127.0.0.1:8090/api/collections"
	emailAdmin    = "admin@example.com"
	password      = "Admin123!"
	updatePeriod  = 5 * time.Second
	batteryMin    = 20
	hasPalletTick = 5
)

// ===============================
// DataStruct with the content of the AGV
// ===============================
type AGV struct {
	ID          string
	Temperature float64
	Battery     float64
	Status      int
	HasPallet   int
	PalletTicks int
	SensorIDs   map[string]string // "temperature","battery","has_pallet","status"
}

// ===============================
// Reading sended to benthos
// ===============================
type Reading struct {
	MessageID  string  `json:"message_id"`
	Collection string  `json:"_collection"`
	Sensor     string  `json:"sensor"`
	Value      float64 `json:"value"`
	Time       string  `json:"time"`
	Message    string  `json:"message,omitempty"`
}

// ===============================
// Execute the python script and return the token
// ===============================
func getAdminToken() string {
	cmd := exec.Command("python", "obtener_token.py")
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	if err != nil {
		log.Fatalf("Error ejecutando el script de Python: %v", err)
	}

	output := strings.TrimSpace(out.String())
	parts := strings.Split(output, ": ")
	if len(parts) != 2 {
		log.Fatalf("Salida inesperada del script de Python: %s", output)
	}
	return strings.TrimSpace(parts[1])
}

// ===============================
// Send a readings batch to benthos
// ===============================
func sendBatchToBenthos(batch []Reading) {
    url := "http://localhost:4197/ingest"

    payload := []map[string]interface{}{}

    for _, r := range batch {
        if r.Collection == "urgent_alerts" {
			payload = append(payload, map[string]interface{}{
				"_collection": r.Collection,
				"value":       r.Message,
				"sensor": 	   r.Sensor,
				"time":        r.Time,
			})
		} else {
            // Normal readings
            payload = append(payload, map[string]interface{}{
                "_collection": r.Collection,
                "message_id":  r.MessageID,
                "sensor":      r.Sensor,
                "value":       r.Value,
                "time":        r.Time,
            })
        }
    }

    jsonData, err := json.Marshal(payload) // Here we transform the payload to JSON for benthos
    if err != nil {
        log.Println("Error serializando batch:", err)
        return
    }

	fmt.Println("JSON enviado a Benthos:")
	fmt.Println(string(jsonData))
	fmt.Println("------")

    req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
    if err != nil {
        log.Println("Error creando request:", err)
        return
    }
    req.Header.Set("Content-Type", "application/json")

    client := &http.Client{Timeout: 5 * time.Second}
    resp, err := client.Do(req)
    if err != nil {
        log.Println("Error enviando a Benthos:", err)
        return
    }
    defer resp.Body.Close()

    if resp.StatusCode != 200 && resp.StatusCode != 201 {
        body, _ := io.ReadAll(resp.Body)
        log.Printf("Benthos respondió error: %s\n%s\n", resp.Status, string(body))
    }
}

// ===============================
// Helper to create records on DB
// ===============================
func createRecord(collection string, data map[string]interface{}, token string) string {
	url := fmt.Sprintf("%s/%s/records", baseURL, collection)
	jsonData, _ := json.Marshal(data)

	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		log.Fatalf("Error creando %s: %s\n%s", collection, resp.Status, string(bodyBytes))
	}

	var res map[string]interface{}
	json.Unmarshal(bodyBytes, &res)
	return res["id"].(string)
}

// ===============================
// Find user by mail
// ===============================
func getUserIDByEmail(email, token string) string {
	url := fmt.Sprintf("%s/users/records?filter=email='%s'", baseURL, email)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	var res map[string]interface{}
	json.Unmarshal(bodyBytes, &res)

	items, ok := res["items"].([]interface{})
	if ok && len(items) > 0 {
		user := items[0].(map[string]interface{})
		return user["id"].(string)
	}
	return ""
}

// ===============================
// Generate alerts according to thresholds 
// ===============================
func generateAlerts(agv *AGV) []Reading {
	alerts := []Reading{}
	now := time.Now().UTC().Format(time.RFC3339)

	temperatureRounded := math.Round(agv.Temperature*10) / 10
	batteryRounded := math.Round(agv.Battery*10) / 10

	// Overheat alert
	if temperatureRounded > 32 {
		alerts = append(alerts, Reading{
			MessageID:  uuid.New().String(),
			Collection: "urgent_alerts",
			Sensor:     agv.SensorIDs["temperature"],
			Value:      temperatureRounded,
			Time:       now,
			Message:    fmt.Sprintf("Temperatura crítica: %.1f°C", temperatureRounded),
		})
	}

	// Low battery alert 
	if batteryRounded < 20 {
		alerts = append(alerts, Reading{
			MessageID:  uuid.New().String(),
			Collection: "urgent_alerts",
			Sensor:     agv.SensorIDs["battery"],
			Value:      batteryRounded,
			Time:       now,
			Message:    fmt.Sprintf("Batería baja: %.1f%%", batteryRounded),
		})
	}

	// Critical status alert
	if agv.Status >= 3 {
		alerts = append(alerts, Reading{
			MessageID:  uuid.New().String(),
			Collection: "urgent_alerts",
			Sensor:     agv.SensorIDs["status"],
			Value:      float64(agv.Status),
			Time:       now,
			Message:    fmt.Sprintf("Estado crítico: %d", agv.Status),
		})
	}

	return alerts
}

// ===============================
// Main function
// ===============================
func main() {
	rand.Seed(time.Now().UnixNano())
	timestamp := time.Now().Unix()

	adminToken := getAdminToken()
	fmt.Fprintln(os.Stderr, "Token obtenido:", adminToken)

	// Create o recover admin user 
	userID := getUserIDByEmail(emailAdmin, adminToken)
	if userID == "" {
		userID = createRecord("users", map[string]interface{}{
			"email":           emailAdmin,
			"password":        password,
			"passwordConfirm": password,
			"name":            "Admin",
			"emailVisibility": true,
		}, adminToken)
		fmt.Fprintln(os.Stderr, "Usuario creado con ID:", userID)
	} else {
		fmt.Fprintln(os.Stderr, "Usuario ya existe con ID:", userID)
	}

	// Create ubications
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
			"point": map[string]interface{}{
				"lat": loc.lat,
				"lon": loc.lng,
			},
		}, adminToken)
		locIDs = append(locIDs, id)
	}

	// Create devices
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
		}, adminToken)
		agvIDs = append(agvIDs, id)
	}

	// Allocate devices to locations
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
		}, adminToken)
	}

	// Create sensor contexts
	sensorContextID := createRecord("sensor_contexts", map[string]interface{}{
		"context": "data",
	}, adminToken)

	// Create sensor types
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
		}, adminToken)
		sensorTypeIDs = append(sensorTypeIDs, id)
	}

	// Create sensors
	sensorsMap := map[string]map[string]string{}
	for _, agvID := range agvIDs {
		sensorsMap[agvID] = map[string]string{}
		for i, stID := range sensorTypeIDs {
			sensorID := createRecord("sensors", map[string]interface{}{
				"device":      agvID,
				"sensor_type": stID,
			}, adminToken)
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

	fmt.Fprintln(os.Stderr, "Inicialización completa. Empezando simulación de lecturas...")

	// Real time readings simulation
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
			// Randomize temperature between 15 - 35 
			agv.Temperature += rand.Float64()*2 - 1
			if agv.Temperature < 15 {
				agv.Temperature = 15
			}
			if agv.Temperature > 35 {
				agv.Temperature = 35
			}

			// Battery
			agv.Battery -= rand.Float64() * 2
			if agv.Battery < 0 {
				agv.Battery = 100
			}

			// Status
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

			// Pallet
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

			// ===============================
			// Generate normal readings and alerts
			// ===============================
			temperatureRounded := math.Round(agv.Temperature*10) / 10
			batteryRounded := math.Round(agv.Battery*10) / 10

			// Normal readings
			normalReadings := []Reading{
				{
					MessageID:  uuid.New().String(),
					Collection: "readings",
					Sensor:     agv.SensorIDs["temperature"],
					Value:      temperatureRounded,
					Time:       time.Now().UTC().Format(time.RFC3339),
				},
				{
					MessageID:  uuid.New().String(),
					Collection: "readings",
					Sensor:     agv.SensorIDs["battery"],
					Value:      batteryRounded,
					Time:       time.Now().UTC().Format(time.RFC3339),
				},
				{
					MessageID:  uuid.New().String(),
					Collection: "readings",
					Sensor:     agv.SensorIDs["has_pallet"],
					Value:      float64(agv.HasPallet),
					Time:       time.Now().UTC().Format(time.RFC3339),
				},
				{
					MessageID:  uuid.New().String(),
					Collection: "readings",
					Sensor:     agv.SensorIDs["status"],
					Value:      float64(agv.Status),
					Time:       time.Now().UTC().Format(time.RFC3339),
				},
			}

			// Send normal readings
			sendBatchToBenthos(normalReadings)

			// Generate alerts if corresponds
			alerts := generateAlerts(agv)
			if len(alerts) > 0 {
				sendBatchToBenthos(alerts)
			}
		}
	}
}