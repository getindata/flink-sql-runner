import logging
import os
from datetime import datetime
from typing import Callable, Optional, Tuple

import boto3
import botocore
from botocore.exceptions import ClientError


def get_content(bucket_name: str, object_key: str) -> Optional[str]:
    s3 = _get_s3_client()
    try:
        data = s3.get_object(Bucket=bucket_name, Key=object_key)
        contents = data["Body"].read()
        return contents.decode("utf-8")
    except botocore.exceptions.ClientError as err:
        status = err.response["ResponseMetadata"]["HTTPStatusCode"]
        if status == 404:
            return None
        else:
            raise
    except Exception:
        raise


def upload_content(content: str, bucket: str, object_name: str) -> bool:
    s3 = _get_s3_client()
    try:
        s3.put_object(Body=content.encode(), Bucket=bucket, Key=object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def get_latest_object(
    bucket: str, prefix: str, filter_predicate: Callable[[str], bool] = lambda x: True
) -> Optional[Tuple[str, datetime]]:
    s3 = _get_s3_client()

    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in response:
        return None
    else:
        objs = response["Contents"]
        sorted_objs_by_ts_desc = sorted(objs, key=(lambda obj: -1 * int(obj["LastModified"].strftime("%s"))))
        sorted_keys_by_ts_desc = [
            (obj["Key"], obj["LastModified"]) for obj in sorted_objs_by_ts_desc if filter_predicate(obj["Key"])
        ]
        if len(sorted_keys_by_ts_desc) == 0:
            return None
        else:
            return sorted_keys_by_ts_desc[0]


def _get_s3_client():
    if "AWS_S3_ENDPOINT" in os.environ:
        return boto3.client("s3", endpoint_url=os.environ["AWS_S3_ENDPOINT"])
    else:
        return boto3.client("s3")
