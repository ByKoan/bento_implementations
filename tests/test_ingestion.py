from utils import fahrenheit_a_celsius, enrich_message


def test_fahrenheit_to_celsius_exact():
    assert fahrenheit_a_celsius(32) == 0
    assert fahrenheit_a_celsius(77) == 25
    assert fahrenheit_a_celsius(212) == 100


def test_mapping_does_not_lose_information():
    msg = enrich_message("AGV_05", 77)

    assert msg["device_id"] == "AGV_05"
    assert msg["temp_f"] == 77
    assert msg["temp_c"] == 25.0

    assert "message_id" in msg
    assert "ingestion_timestamp" in msg
