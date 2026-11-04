"""Ingest Lambda: validates device events and enqueues them for asynchronous processing."""
import json
import os

import boto3

from validation import ValidationError, validate_event

_sqs = boto3.client("sqs")


def _response(status: int, body: dict) -> dict:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def handler(event, context):
    try:
        payload = json.loads(event.get("body") or "{}")
        clean = validate_event(payload)
    except (json.JSONDecodeError, ValidationError) as exc:
        return _response(400, {"error": str(exc)})

    # Decoupling the API from processing means bursts are absorbed by the queue.
    _sqs.send_message(QueueUrl=os.environ["QUEUE_URL"], MessageBody=json.dumps(clean))
    return _response(202, {"status": "accepted", "device_id": clean["device_id"]})
