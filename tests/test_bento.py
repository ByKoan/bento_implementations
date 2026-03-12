import pytest
import yaml


def test_bento_yaml_pipeline():

    # -------------------------
    # Load YAML
    # -------------------------
    with open("tests/test_bento.yml", "r") as f:
        data = yaml.safe_load(f)

    # -------------------------
    # Validate HTTP service
    # -------------------------
    assert "http" in data
    assert "address" in data["http"]
    assert data["http"]["address"] == "0.0.0.0:4197"

    # -------------------------
    # Validate input
    # -------------------------
    assert "input" in data
    assert "http_server" in data["input"]

    http_input = data["input"]["http_server"]

    assert "path" in http_input
    assert http_input["path"] == "/ingest"

    assert "allowed_verbs" in http_input
    assert "POST" in http_input["allowed_verbs"]

    # -------------------------
    # Validate pipeline
    # -------------------------
    assert "pipeline" in data
    assert "processors" in data["pipeline"]

    processors = data["pipeline"]["processors"]
    assert len(processors) >= 2

    # Processor 1 -> unarchive
    assert "unarchive" in processors[0]
    assert processors[0]["unarchive"]["format"] == "json_array"

    # Processor 2 -> mapping
    assert "mapping" in processors[1]

    mapping_script = processors[1]["mapping"]

    assert "meta collection" in mapping_script
    assert "this._collection" in mapping_script
    assert "root = this.without" in mapping_script

    # -------------------------
    # Validate output
    # -------------------------
    assert "output" in data
    assert "switch" in data["output"]

    switch = data["output"]["switch"]

    assert "cases" in switch
    cases = switch["cases"]

    assert len(cases) >= 2

    # -------------------------
    # Validate readings case
    # -------------------------
    readings_case = cases[0]

    assert "check" in readings_case
    assert "readings" in readings_case["check"]

    readings_output = readings_case["output"]
    assert "http_client" in readings_output

    http_client = readings_output["http_client"]

    assert http_client["verb"] == "POST"
    assert "url" in http_client
    assert "/readings/records" in http_client["url"]

    assert "headers" in http_client
    assert "Content-Type" in http_client["headers"]

    # -------------------------
    # Validate urgent_alerts case
    # -------------------------
    alerts_case = cases[1]

    assert "check" in alerts_case
    assert "urgent_alerts" in alerts_case["check"]

    alerts_output = alerts_case["output"]
    assert "http_client" in alerts_output

    http_client = alerts_output["http_client"]

    assert http_client["verb"] == "POST"
    assert "url" in http_client
    assert "/urgent_alerts/records" in http_client["url"]

    assert "headers" in http_client
    assert "Content-Type" in http_client["headers"]

    # -------------------------
    # Validate example input format
    # -------------------------
    example_input = [
        {
            "message_id": "abc",
            "_collection": "readings",
            "sensor": "sensor1",
            "value": 25.5,
            "time": "2026-03-04T12:00:00Z"
        }
    ]

    msg = example_input[0]

    assert "_collection" in msg
    assert "sensor" in msg
    assert "value" in msg
    assert "time" in msg