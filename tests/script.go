package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

// ----------------------------
// CONFIG
// ----------------------------
const (
	NumAGVs      = 5
	UpdatePeriod = 1 * time.Second

	MinX = 0.0
	MaxX = 100.0
	MinY = 0.0
	MaxY = 50.0

	MaxBattery          = 100.0
	MinBattery          = 0.0
	BatteryDrainPerStep = 0.5

	PocketBaseURL = "http://127.0.0.1:8090/api/collections"
	AdminToken    = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjb2xsZWN0aW9uSWQiOiJwYmNfMzE0MjYzNTgyMyIsImV4cCI6MTc3MjAyNzMzNSwiaWQiOiJvc3BnZzR2MG5ncDJjamEiLCJyZWZyZXNoYWJsZSI6dHJ1ZSwidHlwZSI6ImF1dGgifQ.v0cyMnEu0_tks4eRr1AmV7p7Pyb0kCmDJdQxyAFPgII" // <- Pon tu token de _superusers
)

// ----------------------------
// TIPOS
// ----------------------------
type MissionState string

const (
	Idle     MissionState = "IDLE"
	Moving   MissionState = "MOVING"
	Charging MissionState = "CHARGING"
	Error    MissionState = "ERROR"
)

type AGV struct {
	ID       string
	DeviceID string
	X        float64
	Y        float64
	Battery  float64
	Mission  MissionState
	SensorID string
}

// ----------------------------
// FUNCIONES GENERALES
// ----------------------------
func createRecord(collection string, payload map[string]interface{}) (string, error) {
	url := fmt.Sprintf("%s/%s/records", PocketBaseURL, collection)
	data, _ := json.Marshal(payload)

	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(data))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+AdminToken)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	idStr, ok := result["id"].(string)
	if !ok {
		return "", fmt.Errorf("no id returned for %s, body: %v", collection, result)
	}
	return idStr, nil
}

// ----------------------------
// INICIALIZACIÓN DE DATOS
// ----------------------------
func initializeBaseData() ([]*AGV, error) {
	// 1️⃣ Crear usuarios
	fmt.Println("Creando usuarios...")
	adminID, _ := createRecord("users", map[string]interface{}{
		"email":            fmt.Sprintf("admin_%d@example.com", time.Now().UnixNano()),
		"password":         "admin123",
		"passwordConfirm":  "admin123",
		"emailVisibility":  true,
		"name":             "Admin User",
	})
	operatorID, _ := createRecord("users", map[string]interface{}{
		"email":            fmt.Sprintf("operator_%d@example.com", time.Now().UnixNano()),
		"password":         "operator123",
		"passwordConfirm":  "operator123",
		"emailVisibility":  true,
		"name":             "Operator User",
	})

	// 2️⃣ Crear ubicaciones con coordenadas
	fmt.Println("Creando ubicaciones...")
	locEntrada, _ := createRecord("locations", map[string]interface{}{
		"name": "Entrada Planta",
		"point": map[string]interface{}{
			"x": 0.0,
			"y": 0.0,
		},
	})

	// 3️⃣ Crear contextos de sensor
	fmt.Println("Creando contextos de sensor...")
	ctxAGV, _ := createRecord("sensor_contexts", map[string]interface{}{"context": "AGV"})

	// 4️⃣ Crear tipos de sensor
	fmt.Println("Creando tipos de sensor...")
	batteryType, _ := createRecord("sensor_types", map[string]interface{}{
		"sensor_context": ctxAGV,
		"magnitude":         "battery",
		"unit":              "%",
	})
	tempType, _ := createRecord("sensor_types", map[string]interface{}{
		"sensor_context": ctxAGV,
		"magnitude":         "temperature",
		"unit":              "°C",
	})
	
	has_palletType, _ := createRecord("sensor_types", map[string]interface{}{
		"sensor_context": ctxAGV,
		"magnitude":         "has_pallet",
		"unit":              "true/false",
	})

	// 5️⃣ Crear devices, sensores y ubicación inicial
	fmt.Println("Creando devices, sensores y ubicación inicial...")
	agvs := make([]*AGV, NumAGVs)
	for i := 0; i < NumAGVs; i++ {
		userID := operatorID
		if i%2 == 0 {
			userID = adminID
		}

		devName := fmt.Sprintf("AGV-%d-%d", i+1, time.Now().UnixNano())
		devID, err := createRecord("devices", map[string]interface{}{
			"user": userID,
			"name": devName,
		})
		if err != nil {
			fmt.Println("Error creando device:", err)
			continue
		}

		// Sensor de batería
		sensorID, err := createRecord("sensors", map[string]interface{}{
			"device":      devID,
			"sensor_type": batteryType,
		})
		if err != nil {
			fmt.Println("Error creando sensor:", err)
			continue
		}

		// Sensor de temperatura (opcional)
		_, _ = createRecord("sensors", map[string]interface{}{
			"device":      devID,
			"sensor_type": tempType,
		})

		_, _ = createRecord("sensors", map[string]interface{}{
			"device":      devID,
			"sensor_type": has_palletType,
		})

		// AGV struct
		agvs[i] = &AGV{
			ID:       devName,
			DeviceID: devID,
			X:        rand.Float64()*(MaxX-MinX) + MinX,
			Y:        rand.Float64()*(MaxY-MinY) + MinY,
			Battery:  MaxBattery,
			Mission:  Idle,
			SensorID: sensorID,
		}

		// Vincular device a ubicación inicial
		_, _ = createRecord("devices_locations", map[string]interface{}{
			"location": locEntrada,
			"device":   devID,
			"placed_at":   time.Now().Format(time.RFC3339),
		})
	}

	// 6️⃣ Crear readings iniciales
	fmt.Println("Creando readings iniciales...")
	for _, agv := range agvs {
		if agv == nil {
			continue
		}
		_, err := createRecord("readings", map[string]interface{}{
			"value":     agv.Battery,
			"time":      time.Now().Format(time.RFC3339),
			"sensor": agv.SensorID,
		})
		if err != nil {
			fmt.Println("Error creando reading:", err)
			continue
		}
	}

	return agvs, nil
}

// ----------------------------
// SIMULACIÓN
// ----------------------------
func (agv *AGV) Update() {
	if agv.Battery < 20 {
		agv.Mission = Charging
	} else if agv.Mission == Idle && rand.Float64() < 0.5 {
		agv.Mission = Moving
	} else if agv.Mission == Moving && rand.Float64() < 0.05 {
		agv.Mission = Error
	}

	if agv.Mission == Moving {
		dx := rand.Float64()*2 - 1
		dy := rand.Float64()*2 - 1
		agv.X += dx
		agv.Y += dy
		if agv.X < MinX {
			agv.X = MinX
		}
		if agv.X > MaxX {
			agv.X = MaxX
		}
		if agv.Y < MinY {
			agv.Y = MinY
		}
		if agv.Y > MaxY {
			agv.Y = MaxY
		}
	}

	if agv.Mission != Charging && agv.Battery > MinBattery {
		agv.Battery -= BatteryDrainPerStep
		if agv.Battery < 0 {
			agv.Battery = 0
		}
	}

	if agv.Mission == Charging {
		agv.Battery += 1.5
		if agv.Battery >= MaxBattery {
			agv.Battery = MaxBattery
			agv.Mission = Idle
		}
	}
}

// ----------------------------
// ENVIAR DATOS
// ----------------------------
func (agv *AGV) SendReading() {
	_, _ = createRecord("readings", map[string]interface{}{
		"sensor": agv.SensorID,
		"time":      time.Now().Format(time.RFC3339),
		"value":     agv.Battery,
	})
}

// ----------------------------
// MAIN
// ----------------------------
func main() {
	rand.New(rand.NewSource(time.Now().UnixNano()))

	agvs, err := initializeBaseData()
	if err != nil {
		fmt.Println("Error inicializando datos:", err)
		return
	}

	var wg sync.WaitGroup
	for _, agv := range agvs {
		if agv == nil {
			continue
		}
		wg.Add(1)
		go func(a *AGV) {
			defer wg.Done()
			ticker := time.NewTicker(UpdatePeriod)
			for range ticker.C {
				a.Update()
				a.SendReading()
			}
		}(agv)
	}
	wg.Wait()
}