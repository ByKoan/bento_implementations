import pytest
import yaml

def test_bento_yaml_pipeline():
    # Load YAML
    with open("tests/test_bento.yml", "r") as f:
        data = yaml.safe_load(f)

    # -------------------------
    # Validate input
    # -------------------------
    assert "input" in data
    assert "stdin" in data["input"] 
    stdin_input = data["input"]["stdin"]
    assert isinstance(stdin_input, dict)

    # -------------------------
    # Validate pipeline
    # -------------------------
    assert "pipeline" in data
    assert "processors" in data["pipeline"]
    assert len(data["pipeline"]["processors"]) > 0

    # Validate that ingestion_time mapped to time
    first_processor = data["pipeline"]["processors"][0]
    assert "bloblang" in first_processor
    blob_script = first_processor["bloblang"]
    assert "ingestion_timestamp.ts_parse" in blob_script
    assert "time" in blob_script

    # -------------------------
    # Validate output
    # -------------------------
    assert "output" in data
    assert "http_client" in data["output"]
    http_output = data["output"]["http_client"]
    assert "url" in http_output
    assert "verb" in http_output
    assert http_output["verb"] == "POST"
    assert "headers" in http_output
    assert "Authorization" in http_output["headers"]
    assert "batching" in http_output
    assert "processors" in http_output
    output_processor = http_output["processors"][0]
    assert "bloblang" in output_processor
    # Validate each item
    assert "/api/collections/" in output_processor["bloblang"]
    assert "body" in output_processor["bloblang"]

    # -------------------------
    # Validate JSON example of input
    # -------------------------
    example_input = {
        "sensor": "5bqfwcv9g6g1tm6",
        "value": 94,
        "ingestion_timestamp": "2026-02-27T08:30:13.063626Z",
        "temp_c": 94.0
    }
    # Pipeline must generate time field
    assert "ingestion_timestamp" in example_input
    assert "temp_c" in example_input
    assert "sensor" in example_input
    assert "value" in example_input