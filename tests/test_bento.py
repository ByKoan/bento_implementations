import pytest
import yaml

def test_bento_yaml_pipeline():
    # Loading YAML
    with open("tests/test-bento.yml", "r") as f:
        data = yaml.safe_load(f)

    # Validate input section
    assert "input" in data
    assert "mqtt" in data["input"]
    mqtt_input = data["input"]["mqtt"]
    assert "urls" in mqtt_input
    assert "topics" in mqtt_input
    assert "client_id" in mqtt_input
    assert "qos" in mqtt_input

    # Validate pipeline
    assert "pipeline" in data
    assert "processors" in data["pipeline"]
    assert len(data["pipeline"]["processors"]) > 0

    # Validate output
    assert "output" in data
    assert "mqtt" in data["output"]
    mqtt_output = data["output"]["mqtt"]
    assert "urls" in mqtt_output
    assert "topic" in mqtt_output
    assert "client_id" in mqtt_output
    assert "qos" in mqtt_output

    # Validate tests
    assert "tests" in data
    for test_case in data["tests"]:
        assert "name" in test_case
        assert "target_processors" in test_case
        assert "input_batch" in test_case
        assert "output_batches" in test_case

        # Validate input_batch y output_batches dont lose fields
        for input_item in test_case["input_batch"]:
            assert "json_content" in input_item
            json_input = input_item["json_content"]
            assert "device" in json_input
            assert "temperature_c" in json_input
            assert "battery" in json_input
            assert "status" in json_input

        for batch in test_case["output_batches"]:
            for output_item in batch:
                assert "json_equals" in output_item
                json_out = output_item["json_equals"]
                # Check mapped fields
                assert "device" in json_out
                assert "temperature_c" in json_out
                assert "temperature_f" in json_out
                assert "device_id" in json_out
                assert "original_value" in json_out
                assert "battery" in json_out
                assert "status" in json_out