import json
import boto3

from pathlib import PurePosixPath
from botocore.exceptions import ClientError


s3 = boto3.client("s3")


def get_checkpoint_key(raw_key, chunk_number):
    batch_name = PurePosixPath(raw_key).stem

    return (
        f"checkpoints/{batch_name}/"
        f"chunk_{chunk_number:04d}.json"
    )

def load_checkpoint(bucket, raw_key, chunk_number):
    checkpoint_key = get_checkpoint_key(
        raw_key, chunk_number
    )

    try:
        response = s3.get_object(
            Bucket=bucket,
            Key=checkpoint_key
        )

        return json.loads(response["Body"].read())

    except ClientError as error:
        if error.response["Error"]["Code"] == "NoSuchKey":
            return None

        raise

def save_checkpoint(bucket, raw_key, chunk_number, result):
    checkpoint_key = get_checkpoint_key(
        raw_key, chunk_number
    )

    s3.put_object(
        Bucket=bucket,
        Key=checkpoint_key,
        Body=json.dumps(result).encode("utf-8"),
        ContentType="application/json"
    )

    print(f"Checkpoint saved: {checkpoint_key}", flush=True)