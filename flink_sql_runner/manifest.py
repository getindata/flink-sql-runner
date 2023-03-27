import logging
import os
from typing import List, Optional

import yaml

from flink_sql_runner.job_configuration import JobConfiguration
from flink_sql_runner.s3 import get_content, list_objects, upload_content


class ManifestManager:
    def __init__(self, external_job_config_bucket: str, external_job_config_prefix: str):
        self.external_job_config_bucket = external_job_config_bucket
        self.external_job_config_prefix = external_job_config_prefix

    def list_manifests(self) -> List[str]:
        return list_objects(self.external_job_config_bucket, self.external_job_config_prefix)

    def fetch_job_manifest(self, job_name: str) -> Optional[JobConfiguration]:
        object_key = os.path.join(self.external_job_config_prefix, f"{job_name}.yaml")
        logging.info(f"Looking for config at s3://{self.external_job_config_bucket}/{object_key}.")
        raw_manifest = get_content(self.external_job_config_bucket, object_key)
        return JobConfiguration(yaml.safe_load(raw_manifest)) if raw_manifest else None

    def upload_job_manifest(self, job_conf):
        upload_path = os.path.join(self.external_job_config_prefix, f"{job_conf.get_name()}.yaml")
        logging.info(f"Uploading the new config file to 's3://{self.external_job_config_bucket}/{upload_path}'.")
        upload_content(yaml.dump(job_conf.to_dict()), self.external_job_config_bucket, upload_path)
        logging.info("The config file has been uploaded.")
