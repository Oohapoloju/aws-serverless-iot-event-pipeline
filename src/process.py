"""Process Lambda: consumes queued events, flags anomalies and stores them in DynamoDB.

Uses SQS partial batch responses so one bad record is retried (and eventually sent to
the dead-letter queue) without reprocessing the healthy records in the same batch.
"""
import json
import logging
import os
import time
from decimal import Decimal

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

RETENTION_SECONDS = 30 * 24 * 3600
_table = None


def _get_table():
    global _table
    if _table is None:
        _table = boto3.resource("dynamodb").Table(os.environ["TABLE_NAME"])
    return _table


def build_item(event: dict, threshold: float) -> dict:
    """Turn a validated event into a DynamoDB item, flagging readings above the threshold."""
    return {
        "device_id": event["device_id"],
        "event_ts": event["event_ts"],
        "reading": Decimal(str(event["reading"])),
        "anomaly": event["reading"] > threshold,
        "expires_at": int(time.time()) + RETENTION_SECONDS,
    }


def handler(event, context):
    threshold = float(os.environ.get("ANOMALY_THRESHOLD", "90"))
    failures = []
    for record in event.get("Records", []):
        try:
            item = build_item(json.loads(record["body"]), threshold)
            # Conditional write makes redelivery of the same message idempotent.
            _get_table().put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(device_id) AND attribute_not_exists(event_ts)",
            )
            if item["anomaly"]:
                logger.warning("Anomaly detected for device %s", item["device_id"])
        except Exception as exc:  # noqa: BLE001 - report the record and keep going
            if getattr(exc, "response", {}).get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                continue  # already stored: duplicate delivery, nothing to do
            logger.exception("Failed to process message %s", record.get("messageId"))
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}
