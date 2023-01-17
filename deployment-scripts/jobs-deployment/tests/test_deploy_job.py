import logging
import os.path
import sys
import tempfile
import unittest
from typing import Optional
from unittest.mock import MagicMock

import boto3
from moto import mock_s3

sys.path.insert(0, "../")
from deploy_job import EmrJobRunner, JinjaTemplateResolver
from flink_clients import FlinkYarnRunner
from job_configuration import JobConfiguration, JobConfigurationBuilder
from .test_s3_utils import put_object

logging.basicConfig(level=logging.INFO)


class TestEmrFlinkRunner(unittest.TestCase):
    TEST_BUCKET_NAME = "test_bucket"
    TEST_JOB_NAME = "test-query"

    def setUp(self):
        self.flink_cli_runner = FlinkYarnRunner(session_app_id="test_app_id")
        self.flink_cli_runner.is_job_running = MagicMock()
        self.flink_cli_runner.get_job_id = MagicMock()
        self.flink_cli_runner.stop_with_savepoint = MagicMock()
        self.flink_cli_runner.start = MagicMock()
        # and: JinjaTemplateResolver mock
        self.jinja_template_resolver = JinjaTemplateResolver()
        self.jinja_template_resolver.resolve = MagicMock()
        # and: an empty list of table definitions
        self.static_table_definitions_paths = self._write_to_local_file("")

    @mock_s3
    def test_should_start_sql_job_with_clean_state_if_not_running_and_no_previous_state(self):
        # given: an empty S3 bucket for state
        self._create_s3_bucket_for_flink_data()

        # and: FlinkCliRunner mock
        self.flink_cli_runner.is_job_running = MagicMock(return_value=False)
        self.flink_cli_runner.get_job_id = MagicMock(return_value="test_job_id")
        self.flink_cli_runner.stop_with_savepoint = MagicMock()
        self.flink_cli_runner.start = MagicMock()

        # and: a SQL job manifest
        job_manifest = self.a_valid_sql_job_manifest().build().to_yaml()
        config_file_path = self._write_to_local_file(job_manifest)

        # when
        self._run_job(config_file_path)

        # then
        self.flink_cli_runner.stop_with_savepoint.assert_not_called()
        self.flink_cli_runner.start.assert_called_once()
        self.assert_that_call_argument_equals(self.flink_cli_runner.start, "savepoint_path", None)

    @mock_s3
    def test_should_not_restart_sql_job_if_manifest_has_not_changed(self):
        # given: an empty S3 bucket for state
        self._create_s3_bucket_for_flink_data()

        # and: FlinkCliRunner mock
        self.flink_cli_runner.is_job_running = MagicMock(return_value=True)
        self.flink_cli_runner.get_job_id = MagicMock(return_value="test_job_id")
        self.flink_cli_runner.stop_with_savepoint = MagicMock()
        self.flink_cli_runner.start = MagicMock()

        # and: SQL job manifest
        job_manifest = self.a_valid_sql_job_manifest().build().to_yaml()
        config_file_path = self._write_to_local_file(job_manifest)
        self._upload_manifest_to_s3(job_manifest)

        # when
        self._run_job(config_file_path)

        # then
        self.flink_cli_runner.is_job_running.assert_called_with(self.TEST_JOB_NAME)
        self.flink_cli_runner.stop_with_savepoint.assert_not_called()
        self.flink_cli_runner.start.assert_not_called()

    @mock_s3
    def test_should_restart_sql_job_with_latest_savepoint(self):
        # given: an empty S3 bucket for state
        self._create_s3_bucket_for_flink_data()

        # and: FlinkCliRunner mock
        self.flink_cli_runner.is_job_running = MagicMock(return_value=True)
        self.flink_cli_runner.get_job_id = MagicMock(return_value="test_job_id")
        self.flink_cli_runner.stop_with_savepoint = MagicMock(
            side_effect=lambda *args, **kwargs: self._put_state_to_s3(
                f"savepoints/{self.TEST_JOB_NAME}/2/savepoint-438ed8-5a70b22243a2/"
            )
        )
        self.flink_cli_runner.start = MagicMock()

        # and: SQL job manifest
        old_job_manifest = self.a_valid_sql_job_manifest().build().to_yaml()
        self._upload_manifest_to_s3(old_job_manifest)
        # and: a new modified job manifest
        new_job_manifest = (
            self.a_valid_sql_job_manifest()
            .with_flink_property("pipeline.object-reuse", "false")
            .with_no_meta()
            .build()
            .to_yaml()
        )
        config_file_path = self._write_to_local_file(new_job_manifest)

        # when
        self._run_job(config_file_path)

        # then
        self.flink_cli_runner.is_job_running.assert_called_with(self.TEST_JOB_NAME)
        self.flink_cli_runner.stop_with_savepoint.assert_called_once()
        self.flink_cli_runner.start.assert_called_once()
        self.assert_that_call_argument_equals(
            self.flink_cli_runner.start,
            "savepoint_path",
            f"s3://{self.TEST_BUCKET_NAME}/savepoints/{self.TEST_JOB_NAME}/2/savepoint-438ed8-5a70b22243a2/",
        )

    @mock_s3
    def test_should_start_stopped_sql_job_with_latest_checkpoint(self):
        # given: a S3 bucket for state
        self._create_s3_bucket_for_flink_data()
        self._put_state_to_s3(f"checkpoints/{self.TEST_JOB_NAME}/2/aa18345e22b4b5c0e49051d1369bd24f/chk-123")
        self._put_state_to_s3(f"savepoints/{self.TEST_JOB_NAME}/2/savepoint-438ed8-5a70b22243a2/")
        self._put_state_to_s3(f"checkpoints/{self.TEST_JOB_NAME}/2/aa18345e22b4b5c0e49051d1369bd24f/chk-124")

        # and: FlinkCliRunner mock
        self.flink_cli_runner.is_job_running = MagicMock(return_value=False)
        self.flink_cli_runner.get_job_id = MagicMock(return_value="test_job_id")
        self.flink_cli_runner.stop_with_savepoint = MagicMock()
        self.flink_cli_runner.start = MagicMock()

        # and: SQL job manifest
        job_manifest = self.a_valid_sql_job_manifest().build().to_yaml()
        config_file_path = self._write_to_local_file(job_manifest)
        self._upload_manifest_to_s3(job_manifest)

        # when
        self._run_job(config_file_path)

        # then
        self.flink_cli_runner.is_job_running.assert_called_with(self.TEST_JOB_NAME)
        self.flink_cli_runner.stop_with_savepoint.assert_not_called()
        self.flink_cli_runner.start.assert_called_once()
        self.assert_that_call_argument_equals(
            self.flink_cli_runner.start,
            "savepoint_path",
            f"s3://{self.TEST_BUCKET_NAME}/checkpoints/{self.TEST_JOB_NAME}/2/aa18345e22b4b5c0e49051d1369bd24f/chk-124/",
        )

    @mock_s3
    def test_should_start_sql_job_with_clean_state_if_query_has_changed(self):
        # given: an empty S3 bucket for state
        self._create_s3_bucket_for_flink_data()

        # and: FlinkCliRunner mock
        self.flink_cli_runner.is_job_running = MagicMock(return_value=True)
        self.flink_cli_runner.get_job_id = MagicMock(return_value="test_job_id")
        self.flink_cli_runner.stop_with_savepoint = MagicMock(
            side_effect=lambda *args, **kwargs: self._put_state_to_s3(
                f"savepoints/{self.TEST_JOB_NAME}/2/savepoint-438ed8-5a70b22243a2/"
            )
        )
        self.flink_cli_runner.start = MagicMock()

        # and: the old SQL job manifest
        old_job_manifest = (
            self.a_valid_sql_job_manifest()
            .with_sql("SELECT * FROM test_table")
            .with_meta_query_version(2)
            .with_meta_query_id("e080791a-80e7-43a6-9966-4d6dd0786543")
            .with_meta_query_create_timestamp("2022-11-23T11:36:11.434123")
            .build()
            .to_yaml()
        )
        self._upload_manifest_to_s3(old_job_manifest)
        # and: the new SQL job manifest
        new_job_manifest = (
            self.a_valid_sql_job_manifest()
            .with_sql("SELECT * FROM test_table WHERE id > 5")
            .with_no_meta()
            .build()
            .to_yaml()
        )
        config_file_path = self._write_to_local_file(new_job_manifest)

        # when
        self._run_job(config_file_path)

        # then
        self.flink_cli_runner.is_job_running.assert_called_with(self.TEST_JOB_NAME)
        self.flink_cli_runner.stop_with_savepoint.assert_called_once()
        self.flink_cli_runner.start.assert_called_once()
        self.assert_that_call_argument_equals(self.flink_cli_runner.start, "savepoint_path", None)
        job_manifest_in_s3 = self._load_manifest_from_s3()
        self.assertEqual(3, job_manifest_in_s3.get_meta_query_version())

    @mock_s3
    def test_should_start_code_job_with_clean_state_if_not_running_and_no_previous_state(self):
        # given: an empty S3 bucket for state
        self._create_s3_bucket_for_flink_data()

        # and: FlinkCliRunner mock
        self.flink_cli_runner.is_job_running = MagicMock(return_value=False)
        self.flink_cli_runner.get_job_id = MagicMock(return_value="test_job_id")
        self.flink_cli_runner.stop_with_savepoint = MagicMock()
        self.flink_cli_runner.start = MagicMock()

        # and: SQL job manifest
        job_manifest = self.a_valid_code_job_manifest().build().to_yaml()
        config_file_path = self._write_to_local_file(job_manifest)

        # when
        self._run_job(config_file_path)

        # then
        self.flink_cli_runner.stop_with_savepoint.assert_not_called()
        self.flink_cli_runner.start.assert_called_once()
        self.jinja_template_resolver.resolve.assert_called_once()
        self.assert_that_call_argument_equals(self.flink_cli_runner.start, "savepoint_path", None)
        self.assert_that_list_call_argument_contains(
            self.flink_cli_runner.start, "python_flink_params", f"--python /tmp/run-{self.TEST_JOB_NAME}.py"
        )

    def _create_s3_bucket_for_flink_data(self):
        self.s3 = boto3.resource("s3", region_name="us-east-1")
        self.s3_bucket = self.s3.create_bucket(Bucket=self.TEST_BUCKET_NAME)

    def _run_job(self, config_file_path):
        EmrJobRunner(
            job_config_path=config_file_path,
            pyflink_runner_dir="/some/dummy/path/",
            external_job_config_bucket=self.TEST_BUCKET_NAME,
            external_job_config_prefix="test-prefix/",
            table_definition_paths=self.static_table_definitions_paths,
            flink_cli_runner=self.flink_cli_runner,
            jinja_template_resolver=self.jinja_template_resolver,
            pyexec_path="/some/path/python3",
            passthrough_args=[],
        ).run()

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
        put_object(self.s3_bucket, key=f"test-prefix/{self.TEST_JOB_NAME}.yaml", value=job_manifest)

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

    def a_valid_sql_job_manifest(self) -> "JobConfigurationBuilder":
        return (
            JobConfigurationBuilder()
            .with_name(self.TEST_JOB_NAME)
            .with_description("Some description")
            .with_sql("SELECT * FROM test_table")
            .with_property("target-table", "output_table")
            .with_meta_query_version(2)
            .with_meta_query_id("e080791a-80e7-43a6-9966-4d6dd0786543")
            .with_meta_query_create_timestamp("2022-11-23T11:36:11.434123")
            .with_flink_savepoints_dir(f"s3://{self.TEST_BUCKET_NAME}/savepoints/{self.TEST_JOB_NAME}/")
            .with_flink_checkpoints_dir(f"s3://{self.TEST_BUCKET_NAME}/checkpoints/{self.TEST_JOB_NAME}/")
        )

    def a_valid_code_job_manifest(self) -> "JobConfigurationBuilder":
        return (
            JobConfigurationBuilder()
            .with_name(self.TEST_JOB_NAME)
            .with_description("Some description")
            .with_code(
                f"""
    execution_output = stream_env.from_collection(
        collection=[(1, 'aaa'), (2, 'bb'), (3, 'cccc')],
        type_info=Types.ROW([Types.INT(), Types.STRING()])
    )"""
            )
            .with_property("target-table", "output_table")
            .with_meta_query_version(2)
            .with_meta_query_id("e080791a-80e7-43a6-9966-4d6dd0786543")
            .with_meta_query_create_timestamp("2022-11-23T11:36:11.434123")
            .with_flink_savepoints_dir(f"s3://{self.TEST_BUCKET_NAME}/savepoints/{self.TEST_JOB_NAME}/")
            .with_flink_checkpoints_dir(f"s3://{self.TEST_BUCKET_NAME}/checkpoints/{self.TEST_JOB_NAME}/")
        )
