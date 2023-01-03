import time


def put_object(bucket, key: str, value: str = "dummy") -> None:
    bucket.put_object(Key=key, Body=value)
    # Need to sleep for a few seconds so that each object has different timestamp.
    time.sleep(1.2)
