import sys
import os
import json
import pytest

# Ensure loader/app is importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "loader")))

from app.kafka_consumer import validate_payload, InvalidPayloadError

class DummyRecord:
    def __init__(self, value: bytes, offset: int = 0, topic: str = "csv-rows", partition: int = 0):
        self.value = value
        self.offset = offset
        self.topic = topic
        self.partition = partition

def test_1_malformed_json():
    raw = b"{ malformed json string... "
    with pytest.raises(json.JSONDecodeError):
        json.loads(raw.decode('utf-8'))

def test_2_missing_dataset_id():
    payload = {"row_index": 1, "row_data": {"Customer": "Alice"}}
    with pytest.raises(InvalidPayloadError, match="Missing required field 'dataset_id'"):
        validate_payload(payload)

def test_3_missing_row_index():
    payload = {"dataset_id": "ds-100", "row_data": {"Customer": "Alice"}}
    with pytest.raises(InvalidPayloadError, match="Missing required field 'row_index'"):
        validate_payload(payload)

@pytest.mark.parametrize("wrong_row_index", ["1", 1.5, True, False, [1], {"index": 1}])
def test_4_wrong_row_index_type(wrong_row_index):
    payload = {"dataset_id": "ds-100", "row_index": wrong_row_index, "row_data": {"Customer": "Alice"}}
    with pytest.raises(InvalidPayloadError, match="Field 'row_index' must be an integer"):
        validate_payload(payload)

def test_5_row_data_is_list():
    payload = {"dataset_id": "ds-100", "row_index": 1, "row_data": ["Alice", "Order1"]}
    with pytest.raises(InvalidPayloadError, match="Field 'row_data' must be a JSON object/map"):
        validate_payload(payload)

def test_6_row_data_is_string():
    payload = {"dataset_id": "ds-100", "row_index": 1, "row_data": "Alice, Order1, 250"}
    with pytest.raises(InvalidPayloadError, match="Field 'row_data' must be a JSON object/map"):
        validate_payload(payload)

def test_7_empty_row_data():
    payload = {"dataset_id": "ds-100", "row_index": 1, "row_data": {}}
    with pytest.raises(InvalidPayloadError, match="Field 'row_data' cannot be empty"):
        validate_payload(payload)

def test_8_unknown_extra_fields():
    payload = {
        "dataset_id": "ds-100",
        "row_index": 1,
        "row_data": {"Customer": "Alice"},
        "extra_field": "unseen_metadata",
        "nested_unknown": {"foo": "bar"}
    }
    validated = validate_payload(payload)
    assert validated["dataset_id"] == "ds-100"
    assert validated["row_index"] == 1
    assert validated["row_data"] == {"Customer": "Alice"}

def test_9_completely_empty_payload():
    payload = {}
    with pytest.raises(InvalidPayloadError):
        validate_payload(payload)

def test_10_valid_payload():
    payload = {
        "dataset_id": "ds-100",
        "row_index": 42,
        "row_data": {
            "Customer": "Alice",
            "Order": "O101",
            "Amount": 250
        },
        "filename": "customers.csv",
        "uploaded_at": "2026-09-11T10:00:00Z"
    }
    validated = validate_payload(payload)
    assert validated["dataset_id"] == "ds-100"
    assert validated["row_index"] == 42
    assert validated["row_data"] == {"Customer": "Alice", "Order": "O101", "Amount": 250}
    assert validated["filename"] == "customers.csv"
    assert validated["uploaded_at"] == "2026-09-11T10:00:00Z"
