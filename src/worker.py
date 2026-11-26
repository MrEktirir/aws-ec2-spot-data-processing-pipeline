import os
import json
import time
import boto3

from urllib.parse import unquote_plus
from decimal import Decimal

from checkpoint import (
    load_checkpoint,
    save_checkpoint
)

from interruption import (
    stop_event,
    start_interruption_monitor
)


AWS_REGION = os.getenv("AWS_REGION", "eu-central-1")
QUEUE_URL = os.environ["QUEUE_URL"]

CHUNK_SIZE = 20000
PROCESSING_DELAY = 45

s3 = boto3.client("s3", region_name=AWS_REGION)
sqs = boto3.client("sqs", region_name=AWS_REGION)

def receive_job():
    response = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=20,
        VisibilityTimeout=300
    )

    messages = response.get("Messages", [])

    return messages[0] if messages else None

def extract_s3_locations(message):
    body = json.loads(message["Body"])

    if body.get("Event") == "s3:TestEvent":
        return []

    locations = []

    for record in body["Records"]:
        bucket = record["s3"]["bucket"]["name"]

        key = unquote_plus(
            record["s3"]["object"]["key"]
        )

        locations.append((bucket, key))

    return locations

def read_raw_data(bucket, key):
    response = s3.get_object(
        Bucket=bucket,
        Key=key
    )

    records = json.loads(response["Body"].read())

    print(
        f"Loaded {len(records)} records from {key}",
        flush=True
    )

    return records

def split_into_chunks(records):
    for start in range(0, len(records), CHUNK_SIZE):
        chunk_number = start // CHUNK_SIZE + 1

        chunk = records[start:start + CHUNK_SIZE]

        yield chunk_number, chunk

def process_chunk(chunk):
    processed_records = []

    for record in chunk:
        processed_record = record.copy()

        total_amount = (
            Decimal(str(record["unit_price"]))
            * record["quantity"]
        )

        processed_record["total_amount"] = float(
            total_amount.quantize(Decimal("0.01"))
        )

        processed_records.append(processed_record)

    time.sleep(PROCESSING_DELAY)

    return processed_records

def process_batch(bucket, key, message):
    records = read_raw_data(bucket, key)

    processed_records = []

    for chunk_number, chunk in split_into_chunks(records):

        if stop_event.is_set():
            print("Shutdown requested. Batch paused.", flush=True)
            return None

        extend_job_visibility(message)

        checkpoint = load_checkpoint(
            bucket, key, chunk_number
        )

        if checkpoint is not None:
            print(
                f"Resuming: chunk {chunk_number} already completed",
                flush=True
            )

            processed_records.extend(checkpoint)
            continue

        print(f"Processing chunk {chunk_number}", flush=True)

        result = process_chunk(chunk)

        save_checkpoint(
            bucket, key, chunk_number, result
        )

        processed_records.extend(result)

    return processed_records

def write_processed_data(bucket, raw_key, records):
    processed_key = raw_key.replace(
        "raw/", "processed/", 1
    )

    s3.put_object(
        Bucket=bucket,
        Key=processed_key,
        Body=json.dumps(records).encode("utf-8"),
        ContentType="application/json"
    )

    print(
        f"Completed: {processed_key} "
        f"({len(records)} records)",
        flush=True
    )

def delete_job(message):
    sqs.delete_message(
        QueueUrl=QUEUE_URL,
        ReceiptHandle=message["ReceiptHandle"]
    )


def extend_job_visibility(message):
    sqs.change_message_visibility(
        QueueUrl=QUEUE_URL,
        ReceiptHandle=message["ReceiptHandle"],
        VisibilityTimeout=300
    )


def release_job(message):
    sqs.change_message_visibility(
        QueueUrl=QUEUE_URL,
        ReceiptHandle=message["ReceiptHandle"],
        VisibilityTimeout=0
    )

def main():
    start_interruption_monitor()

    print("Worker started.", flush=True)

    while not stop_event.is_set():
        message = None

        try:
            message = receive_job()

            if message is None:
                continue

            locations = extract_s3_locations(message)

            for bucket, key in locations:

                if stop_event.is_set():
                    release_job(message)
                    return

                extend_job_visibility(message)

                result = process_batch(bucket, key, message)

                if result is None:
                    release_job(message)
                    return

                write_processed_data(bucket, key, result)

            delete_job(message)

        except Exception as error:
            print(f"Worker error: {error}", flush=True)

            stop_event.wait(5)

    print("Worker stopped.", flush=True)


if __name__ == "__main__":
    main()