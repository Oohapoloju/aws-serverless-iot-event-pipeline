# AWS Serverless IoT Event Pipeline

![CI](https://github.com/Oohapoloju/aws-serverless-iot-event-pipeline/actions/workflows/ci.yml/badge.svg)

A reference implementation of a **serverless, event-driven ingestion pipeline on AWS**, defined entirely as code with AWS SAM and covered by unit tests. It uses synthetic device data and is a demo project inspired by my IoT and event-processing work, not a copy of any employer's system.

## Architecture

```
Device --POST /events--> API Gateway --> IngestFunction (validate)
                                              |
                                              v
                                        SQS queue  --(3 failed receives)--> Dead-letter queue
                                              |
                                              v
                                       ProcessFunction (batch of 10)
                                              |
                                              v
                                  DynamoDB (device_id, event_ts, anomaly, TTL)
```

## Design decisions

| Decision | Why |
| --- | --- |
| API and processing are decoupled by SQS | Traffic bursts are absorbed by the queue instead of overwhelming downstream code. |
| Partial batch responses (`ReportBatchItemFailures`) | One bad record is retried without reprocessing healthy ones in the same batch. |
| Dead-letter queue after 3 receives | Poison messages are isolated and can be inspected without blocking the pipeline. |
| Conditional DynamoDB writes | Redelivered messages are idempotent: duplicates are detected and skipped. |
| Composite key `device_id` + `event_ts` plus TTL | Efficient per-device time-range queries and automatic data expiry. |
| Pay-per-request billing | Cost scales with traffic, with no capacity planning. |
| Pure validation module | Business rules are unit tested without any AWS dependency. |

## Project layout

```
template.yaml            SAM template (API, 2 Lambdas, SQS + DLQ, DynamoDB)
src/ingest.py            API handler: validate and enqueue
src/validation.py        Input validation (pure functions)
src/process.py           Queue consumer: anomaly flag and idempotent write
tests/test_pipeline.py   pytest suite (validation, ingest, processing, failure paths)
.github/workflows/ci.yml cfn-lint and pytest on every push
```

## Run the tests

```
pip install -r requirements-dev.txt
cfn-lint template.yaml
pytest -q
```

## Deploy (optional)

Requires the AWS SAM CLI and an AWS account. Resources created here may incur small costs.

```
sam build
sam deploy --guided
curl -X POST "$API_URL" -d '{"device_id":"sensor-1","reading":97.2}'
```

## Possible extensions

- [ ] Authentication on the API (Cognito or API keys)
- [ ] CloudWatch alarms on DLQ depth and Lambda errors
- [ ] Powertools for AWS Lambda (structured logging, metrics, tracing)
- [ ] Stream to a data lake with Kinesis Firehose

## Author

Ooha Poloju - Software Engineer (Java, Kafka, Azure, AWS). LinkedIn: https://www.linkedin.com/in/ooha-poloju-3b7b42435/
