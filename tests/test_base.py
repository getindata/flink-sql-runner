import logging
import os
import tempfile
import unittest
from typing import Optional
from unittest.mock import MagicMock

import boto3

from flink_sql_runner.job_configuration import (JobConfiguration,
                                                JobConfigurationBuilder)
from tests.test_s3_utils import put_object


class TestBase(unittest.TestCase):
    TEST_BUCKET_NAME = "test_bucket"
    TEST_JOB_NAME = "test-query"

    def _create_s3_bucket_for_flink_data(self):
        self.s3 = boto3.resource("s3", region_name="us-east-1")
        self.s3_bucket = self.s3.create_bucket(Bucket=self.TEST_BUCKET_NAME)

    def _write_to_local_file(self, content: str) -> str:
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as tf:
            tf.write(content.encode())
            return tf.name

    def _put_state_to_s3(self, state_base_path: str) -> None:
        logging.info(f"[TEST] Adding state into '{state_base_path}'.")
        put_object(self.s3_bucket, os.path.join(state_base_path, "_metadata"))

    def _load_manifest_from_s3(self, s3_key: str = None) -> "JobConfiguration":
        if s3_key is None:
            s3_key = f"test-prefix/{self.TEST_JOB_NAME}.yaml"
        obj = self._get_object(s3_key)
        return JobConfiguration.from_yaml(obj)

    def _upload_manifest_to_s3(self, job_manifest: str) -> None:
        put_object(
            self.s3_bucket,
            key=f"test-prefix/{self.TEST_JOB_NAME}.yaml",
            value=job_manifest,
        )

    def _get_object(self, key: str) -> str:
        return self.s3.Object(self.TEST_BUCKET_NAME, key).get()["Body"].read().decode()

    def assert_that_dict_call_argument_contains(
            self, mock_object: MagicMock, argument_name: str, key: str, expected_value: str
    ) -> None:
        call_args = mock_object.call_args[1]
        dict_argument_value = call_args[argument_name]
        actual_value = dict_argument_value[key]
        self.assertEqual(expected_value, actual_value)

    def assert_that_list_call_argument_contains(
            self, mock_object: MagicMock, argument_name: str, expected_value: str
    ) -> None:
        call_args = mock_object.call_args[1]
        list_argument_value = call_args[argument_name]
        self.assertTrue(expected_value in list_argument_value)

    def assert_that_call_argument_equals(
            self, mock_object: MagicMock, argument_name: str, expected_value: Optional[str]
    ) -> None:
        call_args = mock_object.call_args[1]
        actual_value = call_args[argument_name]
        self.assertEqual(expected_value, actual_value)

    def a_valid_sql_job_manifest(self, job_name: str = None) -> "JobConfigurationBuilder":
        if job_name is None:
            job_name = self.TEST_JOB_NAME
        return (
            JobConfigurationBuilder()
            .with_name(job_name)
            .with_description("Some description")
            .with_sql("SELECT * FROM test_table")
            .with_property("target-table", "output_table")
            .with_meta_query_version(2)
            .with_meta_query_id("e080791a-80e7-43a6-9966-4d6dd0786543")
            .with_meta_query_create_timestamp("2022-11-23T11:36:11.434123")
            .with_flink_savepoints_dir(f"s3://{self.TEST_BUCKET_NAME}/savepoints/{job_name}/")
            .with_flink_checkpoints_dir(f"s3://{self.TEST_BUCKET_NAME}/checkpoints/{job_name}/")
        )

    def a_valid_code_job_manifest(self, job_name: str = None) -> "JobConfigurationBuilder":
        if job_name is None:
            job_name = self.TEST_JOB_NAME
        return (
            JobConfigurationBuilder()
            .with_name(job_name)
            .with_description("Some description")
            .with_code(
                """
    execution_output = stream_env.from_collection(
        collection=[(1, 'aaa'), (2, 'bb'), (3, 'cccc')],
        type_info=Types.ROW([Types.INT(), Types.STRING()])
    )"""
            )
            .with_property("target-table", "output_table")
            .with_meta_query_version(2)
            .with_meta_query_id("e080791a-80e7-43a6-9966-4d6dd0786543")
            .with_meta_query_create_timestamp("2022-11-23T11:36:11.434123")
            .with_flink_savepoints_dir(f"s3://{self.TEST_BUCKET_NAME}/savepoints/{job_name}/")
            .with_flink_checkpoints_dir(f"s3://{self.TEST_BUCKET_NAME}/checkpoints/{job_name}/")
        )
