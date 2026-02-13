CREATE TABLE sensor_data (
    id SERIAL PRIMARY KEY,
    device_id TEXT,
    temperature NUMERIC,
    humidity NUMERIC,
    timestamp TIMESTAMPTZ
);
