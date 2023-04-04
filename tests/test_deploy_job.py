import logging
from typing import Optional
from unittest.mock import MagicMock

import boto3
from moto import mock_s3

from flink_sql_runner.deploy_job import FlinkJobRunner, JinjaTemplateResolver
from flink_sql_runner.flink_clients import FlinkYarnRunner
from flink_sql_runner.job_configuration import JobConfiguration
from flink_sql_runner.manifest import ManifestManager
from tests.test_base import TestBase

logging.basicConfig(level=logging.INFO)


class TestFlinkJobRunner(TestBase):
    TEST_BUCKET_NAME = "test_bucket"
    TEST_JOB_NAME = "test-query"

    def setUp(self):
        self.flink_cli_runner = FlinkYarnRunner(session_app_id="test_app_id")
        self.flink_cli_runner.is_job_running = MagicMock()
        self.flink_cli_runner.get_job_id = MagicMock()
        self.flink_cli_runner.stop_with_savepoint = MagicMock()
        self.flink_cli_runner.start = MagicMock()
        self.flink_cli_runner.get_job_status = MagicMock(return_value="RUNNING")
        # and: JinjaTemplateResolver mock
        self.jinja_template_resolver = JinjaTemplateResolver()
        self.jinja_template_resolver.resolve = MagicMock()
        # and: an empty list of table definitions
        self.static_table_definitions_paths = self._write_to_local_file("")

    @mock_s3
    def test_should_start_sql_job_with_clean_state_if_not_running_and_no_previous_state(
            self,
    ):
        # given: an empty S3 bucket for state
        self._create_s3_bucket_for_flink_data()

        # and: FlinkCliRunner mock
        self.flink_cli_runner.is_job_running = MagicMock(return_value=False)
        self.flink_cli_runner.get_job_id = MagicMock(return_value="test_job_id")

        # and: a SQL job manifest
        job_manifest = self.a_valid_sql_job_manifest().build()

        # when
        self._run_job(job_manifest)

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

        # and: SQL job manifest
        job_manifest = self.a_valid_sql_job_manifest().build()
        self._upload_manifest_to_s3(job_manifest.to_yaml())

        # when
        self._run_job(job_manifest)

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

        # and: SQL job manifest
        old_job_manifest = self.a_valid_sql_job_manifest().build().to_yaml()
        self._upload_manifest_to_s3(old_job_manifest)
        # and: a new modified job manifest
        new_job_manifest = (
            self.a_valid_sql_job_manifest()
            .with_flink_property("pipeline.object-reuse", "false")
            .with_no_meta()
            .build()
        )

        # when
        self._run_job(new_job_manifest)

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

        # and: SQL job manifest
        job_manifest = self.a_valid_sql_job_manifest().build()
        self._upload_manifest_to_s3(job_manifest.to_yaml())

        # when
        self._run_job(job_manifest)

        # then
        self.flink_cli_runner.is_job_running.assert_called_with(self.TEST_JOB_NAME)
        self.flink_cli_runner.stop_with_savepoint.assert_not_called()
        self.flink_cli_runner.start.assert_called_once()
        self.assert_that_call_argument_equals(
            self.flink_cli_runner.start,
            "savepoint_path",
            f"s3://{self.TEST_BUCKET_NAME}/checkpoints/{self.TEST_JOB_NAME}/2/"
            f"aa18345e22b4b5c0e49051d1369bd24f/chk-124/",
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
        )

        # when
        self._run_job(new_job_manifest)

        # then
        self.flink_cli_runner.is_job_running.assert_called_with(self.TEST_JOB_NAME)
        self.flink_cli_runner.stop_with_savepoint.assert_called_once()
        self.flink_cli_runner.start.assert_called_once()
        self.assert_that_call_argument_equals(self.flink_cli_runner.start, "savepoint_path", None)
        job_manifest_in_s3 = self._load_manifest_from_s3()
        self.assertEqual(3, job_manifest_in_s3.get_meta_query_version())

    @mock_s3
    def test_should_start_code_job_with_clean_state_if_not_running_and_no_previous_state(
            self,
    ):
        # given: an empty S3 bucket for state
        self._create_s3_bucket_for_flink_data()

        # and: FlinkCliRunner mock
        self.flink_cli_runner.is_job_running = MagicMock(return_value=False)
        self.flink_cli_runner.get_job_id = MagicMock(return_value="test_job_id")
        self.flink_cli_runner.stop_with_savepoint = MagicMock()

        # and: SQL job manifest
        job_manifest = self.a_valid_code_job_manifest().build()

        # when
        self._run_job(job_manifest)

        # then
        self.flink_cli_runner.stop_with_savepoint.assert_not_called()
        self.flink_cli_runner.start.assert_called_once()
        self.jinja_template_resolver.resolve.assert_called_once()
        self.assert_that_call_argument_equals(self.flink_cli_runner.start, "savepoint_path", None)
        self.assert_that_list_call_argument_contains(
            self.flink_cli_runner.start,
            "python_flink_params",
            f"--python /tmp/run-{self.TEST_JOB_NAME}.py",
        )

    @mock_s3
    def test_should_start_job_with_job_status_running_whole_time(self):
        # given: an empty S3 bucket for state
        self._create_s3_bucket_for_flink_data()

        # and: FlinkCliRunner mock
        self.flink_cli_runner.is_job_running = MagicMock(return_value=False)
        self.flink_cli_runner.get_job_id = MagicMock(return_value="test_job_id")
        self.flink_cli_runner.stop_with_savepoint = MagicMock()
        self.flink_cli_runner.get_job_status = MagicMock(
            side_effect=["CREATED", "RUNNING", "RUNNING", "FAILING", "RESTARTING"]
        )

        # and: a SQL job manifest
        job_manifest = self.a_valid_sql_job_manifest().build()

        # when
        try:
            self._run_job(job_manifest)

        # then
        except RuntimeError as e:
            self.assertEqual("Unexpected job state. Recent status FAILING.", str(e))
        self.assertEqual(4, self.flink_cli_runner.get_job_status.call_count)

    @mock_s3
    def test_should_fail_job_with_changing_job_status(self):
        # given: an empty S3 bucket for state
        self._create_s3_bucket_for_flink_data()

        # and: FlinkCliRunner mock
        self.flink_cli_runner.is_job_running = MagicMock(return_value=False)
        self.flink_cli_runner.get_job_id = MagicMock(return_value="test_job_id")
        self.flink_cli_runner.stop_with_savepoint = MagicMock()

        # and: a SQL job manifest
        job_manifest = self.a_valid_sql_job_manifest().build()

        # when
        self._run_job(job_manifest)

        # then
        self.assertEqual(10, self.flink_cli_runner.get_job_status.call_count)

    def _create_s3_bucket_for_flink_data(self):
        self.s3 = boto3.resource("s3", region_name="us-east-1")
        self.s3_bucket = self.s3.create_bucket(Bucket=self.TEST_BUCKET_NAME)

    def _run_job(self, job_conf: JobConfiguration) -> None:
        manifest_manager = ManifestManager(
            external_job_config_bucket=self.TEST_BUCKET_NAME,
            external_job_config_prefix="test-prefix/",
        )
        FlinkJobRunner(
            job_name=job_conf.get_name(),
            new_job_conf=job_conf,
            pyflink_runner_dir="/some/dummy/path/",
            manifest_manager=manifest_manager,
            table_definition_paths=self.static_table_definitions_paths,
            flink_cli_runner=self.flink_cli_runner,
            jinja_template_resolver=self.jinja_template_resolver,
            pyexec_path="/some/path/python3",
            passthrough_args=[],
        ).run()

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
