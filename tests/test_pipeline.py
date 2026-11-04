import json
import os

import pytest

# boto3 clients need a region at import time; no AWS calls are made in these tests.
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import ingest  # noqa: E402
import process  # noqa: E402
from validation import ValidationError, validate_event  # noqa: E402


class FakeQueue:
    def __init__(self):
        self.sent = []

    def send_message(self, QueueUrl, MessageBody):
        self.sent.append((QueueUrl, MessageBody))


class FakeTable:
    def __init__(self, fail_for=None, duplicate_for=None):
        self.items = []
        self.fail_for = fail_for
        self.duplicate_for = duplicate_for

    def put_item(self, Item, ConditionExpression):
        if Item["device_id"] == self.fail_for:
            raise RuntimeError("boom")
        if Item["device_id"] == self.duplicate_for:
            error = Exception("duplicate")
            error.response = {"Error": {"Code": "ConditionalCheckFailedException"}}
            raise error
        self.items.append(Item)


def sqs_record(message_id, **body):
    return {"messageId": message_id, "body": json.dumps(body)}


def test_validate_event_normalizes_and_defaults_timestamp():
    event = validate_event({"device_id": " sensor-1 ", "reading": 42})
    assert event["device_id"] == "sensor-1"
    assert event["reading"] == 42.0
    assert event["event_ts"]


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"device_id": "", "reading": 1},
        {"device_id": "a" * 65, "reading": 1},
        {"device_id": "d1", "reading": "hot"},
        {"device_id": "d1", "reading": True},
        {"device_id": "d1", "reading": 1, "event_ts": "yesterday"},
        ["not", "an", "object"],
    ],
)
def test_validate_event_rejects_bad_payloads(payload):
    with pytest.raises(ValidationError):
        validate_event(payload)


def test_ingest_accepts_valid_event_and_enqueues(monkeypatch):
    queue = FakeQueue()
    monkeypatch.setattr(ingest, "_sqs", queue)
    monkeypatch.setenv("QUEUE_URL", "https://sqs.example/queue")
    response = ingest.handler({"body": json.dumps({"device_id": "d1", "reading": 10})}, None)
    assert response["statusCode"] == 202
    assert len(queue.sent) == 1


def test_ingest_rejects_invalid_json_without_enqueuing(monkeypatch):
    queue = FakeQueue()
    monkeypatch.setattr(ingest, "_sqs", queue)
    monkeypatch.setenv("QUEUE_URL", "https://sqs.example/queue")
    response = ingest.handler({"body": "{not json"}, None)
    assert response["statusCode"] == 400
    assert queue.sent == []


def test_process_flags_anomalies_above_threshold(monkeypatch):
    table = FakeTable()
    monkeypatch.setattr(process, "_table", table)
    monkeypatch.setenv("ANOMALY_THRESHOLD", "90")
    records = [
        sqs_record("m1", device_id="d1", reading=95.5, event_ts="2026-01-01T00:00:00+00:00"),
        sqs_record("m2", device_id="d2", reading=20, event_ts="2026-01-01T00:00:01+00:00"),
    ]
    result = process.handler({"Records": records}, None)
    assert result == {"batchItemFailures": []}
    anomalies = {item["device_id"]: item["anomaly"] for item in table.items}
    assert anomalies == {"d1": True, "d2": False}


def test_process_reports_only_the_failed_record(monkeypatch):
    table = FakeTable(fail_for="bad")
    monkeypatch.setattr(process, "_table", table)
    records = [
        sqs_record("ok", device_id="good", reading=1, event_ts="2026-01-01T00:00:00+00:00"),
        sqs_record("fail", device_id="bad", reading=1, event_ts="2026-01-01T00:00:00+00:00"),
    ]
    result = process.handler({"Records": records}, None)
    assert result == {"batchItemFailures": [{"itemIdentifier": "fail"}]}
    assert len(table.items) == 1


def test_process_treats_duplicate_delivery_as_success(monkeypatch):
    table = FakeTable(duplicate_for="dup")
    monkeypatch.setattr(process, "_table", table)
    records = [sqs_record("m1", device_id="dup", reading=1, event_ts="2026-01-01T00:00:00+00:00")]
    assert process.handler({"Records": records}, None) == {"batchItemFailures": []}
