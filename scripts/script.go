package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

// ----------------------------
// CONFIG
// ----------------------------
const (
	NumAGVs              = 5
	UpdatePeriod         = 1 * time.Second
	MaxBattery           = 100.0
	MinBattery           = 0.0
	BatteryDrainPerStep  = 0.5
	StepsPerPalletUpdate = 10
	TicksPerPalletMove   = 10 // número de ticks que el AGV se mantiene en movimiento con pallet

	PocketBaseURL = "http://127.0.0.1:8090/api/collections"
	AdminToken    = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjb2xsZWN0aW9uSWQiOiJwYmNfMzE0MjYzNTgyMyIsImV4cCI6MTc3MjExMjI5OCwiaWQiOiJvc3BnZzR2MG5ncDJjamEiLCJyZWZyZXNoYWJsZSI6dHJ1ZSwidHlwZSI6ImF1dGgifQ.CEWx_LOBYyHSCii48qlHPeA1JGy30VzvcYVD5tRoyiU"
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
	ID                   string
	DeviceID             string
	X                    float64
	Y                    float64
	Battery              float64
	Mission              MissionState
	SensorID             string
	StateSensorID        string
	HasPalletSensorID    string
	TargetX              float64
	TargetY              float64
	stepsSinceLastPallet int
	batteryOffset        float64
	lastHasPallet        int
	ticksWithPallet      int // cuenta ticks en movimiento con pallet
}

// ----------------------------
// GENERAL FUNCTIONS
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
// HELPER: CREATE GEOPOINT
// ----------------------------
func createLocationGeo(name string, lat, lng float64) (string, error) {
	return createRecord("locations", map[string]interface{}{
		"name":  name,
		"point": map[string]interface{}{"lat": lat, "lng": lng},
	})
}

// ----------------------------
// DATA INITIALIZATION
// ----------------------------
func initializeBaseData() ([]*AGV, map[string][2]float64, []string, error) {
	adminID, _ := createRecord("users", map[string]interface{}{
		"email":           fmt.Sprintf("admin_%d@example.com", time.Now().UnixNano()),
		"password":        "admin123",
		"passwordConfirm": "admin123",
		"emailVisibility": true,
		"name":            "Admin User",
	})
	operatorID, _ := createRecord("users", map[string]interface{}{
		"email":           fmt.Sprintf("operator_%d@example.com", time.Now().UnixNano()),
		"password":        "operator123",
		"passwordConfirm": "operator123",
		"emailVisibility": true,
		"name":            "Operator User",
	})

	locations := make(map[string][2]float64)
	locIDs := make([]string, 0)

	// Crear locations correctamente
	locData := []struct {
		Name string
		Lat  float64
		Lng  float64
	}{
		{"Entrada Planta", 10.0, 10.0},
		{"Carga Planta", 50.0, 40.0},
		{"Salida Planta", 90.0, 20.0},
	}

	for _, loc := range locData {
		locID, err := createLocationGeo(loc.Name, loc.Lat, loc.Lng)
		if err != nil {
			fmt.Println("Error creando location:", err)
			continue
		}
		locations[locID] = [2]float64{loc.Lat, loc.Lng}
		locIDs = append(locIDs, locID)
	}

	ctxDATA, _ := createRecord("sensor_contexts", map[string]interface{}{"context": "DATA"})
	batteryType, _ := createRecord("sensor_types", map[string]interface{}{"sensor_context": ctxDATA, "magnitude": "battery", "unit": "%"})
	hasPalletType, _ := createRecord("sensor_types", map[string]interface{}{"sensor_context": ctxDATA, "magnitude": "has_pallet", "unit": "0/1"})
	stateType, _ := createRecord("sensor_types", map[string]interface{}{"sensor_context": ctxDATA, "magnitude": "mission_state", "unit": "1/2/3/4"})

	agvs := make([]*AGV, NumAGVs)
	for i := 0; i < NumAGVs; i++ {
		userID := operatorID
		if i%2 == 0 {
			userID = adminID
		}
		devName := fmt.Sprintf("AGV-%d-%d", i+1, time.Now().UnixNano())
		devID, _ := createRecord("devices", map[string]interface{}{"user": userID, "name": devName})

		sensorID, _ := createRecord("sensors", map[string]interface{}{"device": devID, "sensor_type": batteryType})
		hasPalletSensorID, _ := createRecord("sensors", map[string]interface{}{"device": devID, "sensor_type": hasPalletType})
		stateSensorID, _ := createRecord("sensors", map[string]interface{}{"device": devID, "sensor_type": stateType})

		// Vincular device a la location inicial
		_, _ = createRecord("devices_locations", map[string]interface{}{
			"device":   devID,
			"location": locIDs[0], // Entrada Planta
			"placed_at": time.Now().Format(time.RFC3339),
		})

		agvs[i] = &AGV{
			ID:                   devName,
			DeviceID:             devID,
			X:                    locations[locIDs[0]][0],
			Y:                    locations[locIDs[0]][1],
			Battery:              MaxBattery,
			Mission:              Idle,
			SensorID:             sensorID,
			HasPalletSensorID:    hasPalletSensorID,
			StateSensorID:        stateSensorID,
			TargetX:              locations[locIDs[1]][0],
			TargetY:              locations[locIDs[1]][1],
			stepsSinceLastPallet: rand.Intn(StepsPerPalletUpdate),
			batteryOffset:        rand.Float64() * BatteryDrainPerStep,
			lastHasPallet:        0,
			ticksWithPallet:      0,
		}
	}

	return agvs, locations, locIDs, nil
}

// ----------------------------
// SIMULATION
// ----------------------------
func (agv *AGV) Update(locations map[string][2]float64, locIDs []string) {
	// Primero batería < 90 => Charging
	if agv.Battery < 90 {
		agv.Mission = Charging
	}

	// Movimiento forzado por pallet
	if agv.lastHasPallet == 1 && agv.Mission != Charging {
		agv.Mission = Moving
		agv.TargetX = locations[locIDs[1]][0] // Carga Planta
		agv.TargetY = locations[locIDs[1]][1]
		agv.ticksWithPallet++
		if agv.ticksWithPallet >= TicksPerPalletMove {
			// Fin del movimiento con pallet, reset
			agv.lastHasPallet = 0
			agv.ticksWithPallet = 0
			agv.Mission = Idle
		}
	}

	// Movimiento random si Idle
	if agv.Mission == Idle && rand.Float64() < 0.2 {
		agv.Mission = Moving
		agv.TargetX = locations[locIDs[1]][0] // Carga Planta
		agv.TargetY = locations[locIDs[1]][1]
	}

	// Movimiento
	if agv.Mission == Moving {
		dx := agv.TargetX - agv.X
		dy := agv.TargetY - agv.Y
		dist := math.Sqrt(dx*dx + dy*dy)
		step := 1.0
		if dist > step {
			agv.X += dx / dist * step
			agv.Y += dy / dist * step
		} else {
			agv.X = agv.TargetX
			agv.Y = agv.TargetY
			if agv.lastHasPallet == 0 { // Si no lleva pallet, vuelve a Idle
				agv.Mission = Idle
			}
		}
	}

	// Batería
	if agv.Mission != Charging && agv.Battery > MinBattery {
		agv.Battery -= BatteryDrainPerStep - agv.batteryOffset
		if agv.Battery < MinBattery {
			agv.Battery = MinBattery
			agv.Mission = Error
		}
	} else if agv.Mission == Charging {
		agv.Battery += 1.5
		if agv.Battery >= MaxBattery {
			agv.Battery = MaxBattery
			agv.Mission = Idle
		}
	}
}

// ----------------------------
// SEND DATA
// ----------------------------
func (agv *AGV) SendReading() {
	// Batería
	_, _ = createRecord("readings", map[string]interface{}{
		"sensor": agv.SensorID,
		"time":   time.Now().Format(time.RFC3339),
		"value":  agv.Battery,
	})

	// HasPallet (0/1)
	hasPallet := 0
	agv.stepsSinceLastPallet++
	if agv.stepsSinceLastPallet >= StepsPerPalletUpdate {
		hasPallet = 1
		agv.stepsSinceLastPallet = 0
	}
	agv.lastHasPallet = hasPallet

	_, _ = createRecord("readings", map[string]interface{}{
		"sensor": agv.HasPalletSensorID,
		"time":   time.Now().Format(time.RFC3339),
		"value":  hasPallet,
	})

	// Estado como número
	stateNum := 1
	switch agv.Mission {
	case Idle:
		stateNum = 1
	case Moving:
		stateNum = 2
	case Charging:
		stateNum = 3
	case Error:
		stateNum = 4
	}
	_, _ = createRecord("readings", map[string]interface{}{
		"sensor": agv.StateSensorID,
		"time":   time.Now().Format(time.RFC3339),
		"value":  stateNum,
	})
}

// ----------------------------
// MAIN
// ----------------------------
func main() {
	rand.Seed(time.Now().UnixNano())

	agvs, locations, locIDs, err := initializeBaseData()
	if err != nil {
		fmt.Println("Error inicializando datos:", err)
		return
	}

	var wg sync.WaitGroup
	for _, agv := range agvs {
		wg.Add(1)
		go func(a *AGV) {
			defer wg.Done()
			ticker := time.NewTicker(UpdatePeriod)
			for range ticker.C {
				a.Update(locations, locIDs)
				a.SendReading()
			}
		}(agv)
	}
	wg.Wait()
}