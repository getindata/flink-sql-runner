import logging
import os
import tempfile
from unittest.mock import MagicMock

from moto import mock_s3

from flink_sql_runner.deploy import FlinkRunner
from flink_sql_runner.deploy_job import JinjaTemplateResolver
from flink_sql_runner.flink_clients import FlinkYarnRunner
from flink_sql_runner.manifest import ManifestManager
from tests.test_base import TestBase
from tests.test_s3_utils import put_object

logging.basicConfig(level=logging.INFO)


class TestFlinkRunner(TestBase):

    def setUp(self):
        self.flink_cli_runner = FlinkYarnRunner(session_app_id="test_app_id")
        self.flink_cli_runner.is_job_running = MagicMock()
        self.flink_cli_runner.get_job_id = MagicMock()
        self.flink_cli_runner.stop_with_savepoint = MagicMock()
        self.flink_cli_runner.start = MagicMock()
        self.flink_cli_runner.get_job_status = MagicMock(return_value="RUNNING")
        # and: JinjaTemplateResolver mock
        self.jinja_template_resolver = JinjaTemplateResolver()
        self.config_dir = tempfile.TemporaryDirectory(suffix="flink_runner_test_queries")

    def tearDown(self):
        if self.config_dir:
            self.config_dir.cleanup()

    @mock_s3
    def test_should_cancel_job_for_deleted_query(self):
        # given: an empty S3 bucket for state
        self._create_s3_bucket_for_flink_data()

        # and: FlinkCliRunner mock
        self.flink_cli_runner.is_job_running = MagicMock(return_value=True)
        self.flink_cli_runner.get_job_id = MagicMock(side_effect=lambda name: "id_1" if name == "job1" else "id_2")

        # and: a SQL job manifests in S3
        manifest_1 = (self.a_valid_sql_job_manifest(job_name="job1")
                          .with_flink_property("pipeline.object-reuse", True)
                          .build())
        manifest_2 = (self.a_valid_sql_job_manifest(job_name="job2")
                          .with_flink_property("pipeline.object-reuse", True)
                          .build())
        put_object(self.s3_bucket, "manifests/job1.yaml", manifest_1.to_yaml())
        put_object(self.s3_bucket, "manifests/job2.yaml", manifest_2.to_yaml())

        # and: new configs
        with open(os.path.join(self.config_dir.name, "job1.yaml"), "w") as f:
            f.write(str(manifest_1.to_yaml()))

        # and: template file
        template_file_path = self._write_to_local_file("flinkProperties:\n  'pipeline.object-reuse': True\n")

        # and: savepoints for running jobs
        self._put_state_to_s3(
            f"savepoints/{manifest_1.get_name()}/{manifest_1.get_meta_query_version()}/savepoint-438ed8-5a70b22243a2/")
        self._put_state_to_s3(
            f"savepoints/{manifest_2.get_name()}/{manifest_2.get_meta_query_version()}/savepoint-4b5c0e-9051d13369bd/")

        # when
        FlinkRunner(
            queries_base_path=self.config_dir.name,
            table_definition_path="",
            pyflink_runner_dir="/some/dummy/path/",
            template_file=template_file_path,
            pyexec_path="/some/path/python3",
            flink_cli_runner=self.flink_cli_runner,
            manifest_manager=ManifestManager(TestBase.TEST_BUCKET_NAME, "manifests/"),
            jinja_template_resolver=self.jinja_template_resolver,
            passthrough_args=[]
        ).run()

        # then: stop with savepoint is called only once for job2
        self.assertEqual(self.flink_cli_runner.stop_with_savepoint.call_count, 1)
        print(self.flink_cli_runner.stop_with_savepoint.call_args.args[0])
        self.assertEqual(self.flink_cli_runner.stop_with_savepoint.call_args[0][0], "id_2")
