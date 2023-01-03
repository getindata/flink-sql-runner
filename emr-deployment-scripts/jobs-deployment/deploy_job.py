#!/usr/bin/env python3
import argparse
import copy
import datetime
import logging
import os.path
import uuid
from typing import Any, Dict, List, Optional, Tuple

import yaml
from jinja2 import Environment, FileSystemLoader

from cmd_utils import run_cmd
from job_configuration import JobConfiguration
from s3 import get_content, get_latest_object, upload_content

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--job-config-path",
        required=True,
        help="Path to the new job configuration file.")
    parser.add_argument(
        "--pyflink-runner-dir",
        required=True,
        help="Path to the directory containing PyFlink job runners.")
    parser.add_argument(
        "--external-job-config-bucket",
        required=True,
        help="S3 bucket where job configuration is stored.")
    parser.add_argument(
        "--external-job-config-prefix",
        required=True,
        help="S3 prefix where job configuration is stored.")
    parser.add_argument(
        "--table-definition-path",
        nargs='+',
        required=True,
        help="Paths to files containing common Flink table definitions.")
    parser.add_argument(
        "--pyexec-path",
        required=True,
        help="Path of the Python interpreter used to execute client code and Flink Python UDFs.")
    return parser.parse_known_args()


class FlinkCliRunner(object):
    """
    A Python wrapper for Flink Command-Line Interface. It supports only YARN as the deployment target.
    """

    def __init__(self,
                 session_app_id: str = None,
                 session_cluster_name: str = "Flink session cluster"):
        self.session_app_id = session_app_id if session_app_id is not None else self.__get_session_app_id()
        self.session_cluster_name = session_cluster_name

    @staticmethod
    def __get_session_app_id() -> str:
        """
        Returns YARN applicationId of the running Flink session cluster. The ID has the following format:
        "application_1669122056871_0002".
        :return: YARN applicationId
        """
        _, output, _ = run_cmd(f"""yarn application -list | grep 'Flink session cluster' | cut -f1 -d$'\t' """,
                               throw_on_error=True)
        yarn_application_id = output.strip()
        if yarn_application_id:
            logging.info(f"SESSION APP ID '{yarn_application_id}'.")
            return yarn_application_id
        else:
            raise ValueError("No Flink session cluster running.")

    def is_job_running(self, job_name: str) -> bool:
        """
        Checks if a Flink job with given name exists.
        :param job_name: Flink job name
        :return: 'True' if the job exists, 'False' otherwise
        """
        _, output, _ = run_cmd(
            f"""flink list -t yarn-session -Dyarn.application.id={self.session_app_id} | grep {job_name} | wc -l """,
            throw_on_error=True)
        return "1" == output.strip()

    def get_job_id(self, job_name: str) -> str:
        """
        Gets ID of a Flink job with given job name.
        :param job_name: Flink job name
        :return: Flink job ID
        """
        _, output, _ = run_cmd(
            f"""flink list -t yarn-session -Dyarn.application.id={self.session_app_id} | grep {job_name} | cut -f 4 -d ' ' """,
            throw_on_error=True)
        return output

    def stop_with_savepoint(self, job_id: str, savepoint_path: str) -> None:
        """
        Creates job's final snapshot and stops the job gracefully.
        :param job_id: Flink job ID
        :param savepoint_path: Location where the savepoint should be saved
        """
        run_cmd(
            f"""flink stop \
            -t yarn-session \
            -Dyarn.application.id={self.session_app_id} \
            --savepointPath {savepoint_path} \
            {job_id} """,
            throw_on_error=True)

    def start(self,
              flink_properties: Dict[str, Any],
              python_flink_params: List[str],
              job_arguments: List[str],
              savepoint_path: str = None) -> None:
        """
        Starts Flink job.
        :param flink_properties: A dictionary of Flink configuration properties specific for this job.
        :param python_flink_params: A list of Python-specific parameters of PyFlink.
        :param job_arguments: A list of job parameters.
        :param savepoint_path: The path where the savepoint of the previously executed job is stored. If 'None', the
        job will be started with clean state.
        """
        run_cmd(
            f"""flink run \
            -t yarn-session \
            -Dyarn.application.id={self.session_app_id} \
            {self.__concat_flink_properties(flink_properties)} \
            {" ".join(python_flink_params)} \
            --detached \
            {"" if not savepoint_path else "--fromSavepoint " + savepoint_path} \
            {" ".join(job_arguments)} """,
            throw_on_error=True)

    @staticmethod
    def __concat_flink_properties(flink_properties: Dict[str, Any]) -> str:
        result = ""
        for k, v in flink_properties.items():
            if v is True:
                v = "true"
            elif v is False:
                v = "false"
            result += f"-D{k}={v} "
        return result


class JinjaTemplateResolver(object):
    def resolve(self, template_dir: str, template_file: str, vars: Dict[str, str], output_file_path: str) -> None:
        environment = Environment(loader=FileSystemLoader(template_dir))
        template = environment.get_template(template_file)
        content = template.render(**vars)
        with open(output_file_path, mode="w", encoding="utf-8") as run_file:
            run_file.truncate()
            run_file.write(content)


class EmrJobRunner(object):
    def __init__(self,
                 job_config_path: str,
                 pyflink_runner_dir: str,
                 external_job_config_bucket: str,
                 external_job_config_prefix: str,
                 table_definition_paths: str,
                 pyexec_path: str,
                 flink_cli_runner: FlinkCliRunner,
                 jinja_template_resolver: JinjaTemplateResolver):
        self.job_config_path = job_config_path
        self.pyflink_runner_dir = pyflink_runner_dir
        self.external_job_config_bucket = external_job_config_bucket
        self.external_job_config_prefix = external_job_config_prefix
        self.table_definition_paths = table_definition_paths
        self.pyexec_path = pyexec_path
        self.pyclientexec_path = pyexec_path
        self.flink_cli_runner = flink_cli_runner
        self.jinja_template_resolver = jinja_template_resolver

    def run(self) -> None:
        new_job_conf = JobConfiguration(self.__read_config(self.job_config_path))
        logging.info(f"Deploying '{new_job_conf.get_name()}'.")
        if new_job_conf.is_sql():
            logging.info(f"Deploying query: |{new_job_conf.get_sql()}|")
        else:
            logging.info(f"Deploying code:\n{new_job_conf.get_code()}")

        external_config = self.__fetch_job_manifest(self.external_job_config_bucket,
                                                    self.external_job_config_prefix,
                                                    new_job_conf.get_name())
        logging.info(f"External config:\n{external_config}")

        if not external_config:
            # The job manifest did not exist. Starting a newly created job.
            self.__start_new_job(new_job_conf)
            self.__upload_job_manifest(new_job_conf)
        elif external_config and not self.__has_job_manifest_changed(external_config, new_job_conf):
            # The job manifest has not been modified. There is no need to restart the job. Just ensure it's running.
            if self.__is_job_running(new_job_conf.get_name()):
                logging.info("Job manifest has not changed. Skipping job restart.")
            else:
                self.__start_job_with_unchanged_query(external_config, new_job_conf)
        else:
            # The job manifest has been modified. Job needs to be restarted.
            if self.__is_job_running(new_job_conf.get_name()):
                # Stop the job using the old config (query-version in particular).
                self.__stop_with_savepoint(external_config)

            if external_config and not self.__has_job_definition_changed(external_config, new_job_conf):
                self.__start_job_with_unchanged_query(external_config, new_job_conf)
            else:
                self.__start_new_job_with_changed_query(external_config, new_job_conf)
            self.__upload_job_manifest(new_job_conf)

    @staticmethod
    def __read_config(config_file: str):
        with open(config_file) as qf:
            return yaml.load(qf, yaml.FullLoader)

    def __is_job_running(self, job_name: str) -> bool:
        return self.flink_cli_runner.is_job_running(job_name)

    def __start_new_job(self, job_conf):
        job_conf.set_meta_query_version(1)
        job_conf.set_meta_query_id(str(uuid.uuid1()))
        job_conf.set_meta_query_create_timestamp(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self.__start_with_clean_state(job_conf)

    def __start_job_with_unchanged_query(self, external_config, job_conf):
        job_conf.set_meta_query_version(external_config.get_meta_query_version())
        job_conf.set_meta_query_id(external_config.get_meta_query_id())
        job_conf.set_meta_query_create_timestamp(external_config.get_meta_query_create_timestamp())
        self.__start_with_state(job_conf)

    def __start_new_job_with_changed_query(self, external_config, job_conf):
        job_conf.set_meta_query_version(external_config.get_meta_query_version() + 1)
        job_conf.set_meta_query_id(str(uuid.uuid1()))
        job_conf.set_meta_query_create_timestamp(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self.__start_with_clean_state(job_conf)

    def __upload_job_manifest(self, job_conf):
        upload_path = os.path.join(self.external_job_config_prefix, f"{job_conf.get_name()}.yaml")
        logging.info(f"Uploading the new config file to 's3://{self.external_job_config_bucket}/{upload_path}'.")
        upload_content(yaml.dump(job_conf.to_dict()), self.external_job_config_bucket, upload_path)
        logging.info("The config file has been uploaded.")

    def __stop_with_savepoint(self, job_conf: JobConfiguration) -> None:
        job_id = self.flink_cli_runner.get_job_id(job_conf.get_name())
        savepoint_path = os.path.join(job_conf.get_flink_savepoints_dir(), job_conf.get_meta_query_version_str())
        logging.info(f"Stopping job {job_conf.get_name()} with savepoint at '{savepoint_path}'.")
        self.flink_cli_runner.stop_with_savepoint(job_id, savepoint_path)

    def __start_with_clean_state(self, job_conf: JobConfiguration) -> None:
        logging.info(f"Starting job {job_conf.get_name()} with clean state.")
        self.__start(job_conf, savepoint_path=None)

    def __start_with_state(self, job_conf: JobConfiguration) -> None:
        # Find the latest savepoint if any.
        savepoint_base_path = os.path.join(job_conf.get_flink_savepoints_dir(), job_conf.get_meta_query_version_str())
        latest_savepoint = self.__find_latest_savepoint(savepoint_base_path)

        # Find the latest checkpoint if any.
        checkpoint_base_path = os.path.join(job_conf.get_flink_checkpoints_dir(), job_conf.get_meta_query_version_str())
        latest_checkpoint = self.__find_latest_checkpoint(checkpoint_base_path)

        # Run the latest saved state.
        if latest_savepoint is None and latest_checkpoint is None:
            raise RuntimeError(f"Unexpected state. No checkpoint or savepoint found for {job_conf.get_name()}.")
        if latest_checkpoint is not None and (latest_savepoint is None or latest_checkpoint[1] > latest_savepoint[1]):
            logging.info(f"Starting job {job_conf.get_name()} from checkpoint {latest_checkpoint[0]}.")
            self.__start(job_conf, savepoint_path=latest_checkpoint[0])
        else:
            logging.info(f"Starting job {job_conf.get_name()} from savepoint {latest_savepoint[0]}.")
            self.__start(job_conf, savepoint_path=latest_savepoint[0])

    def __start(self, job_conf: JobConfiguration, savepoint_path=None):
        job_arguments = [f"--table-definition-path {' '.join(self.table_definition_paths)}",
                         f"--target-table '{job_conf.get('target-table')}'",
                         f"--metadata-query-name '{job_conf.get_name()}'",
                         f"--metadata-query-description '{job_conf.get_description()}'",
                         f"--metadata-query-id '{job_conf.get_meta_query_id()}'",
                         f"--metadata-query-version {job_conf.get_meta_query_version()}",
                         f"--metadata-query-create-timestamp '{job_conf.get_meta_query_create_timestamp()}'"]

        if job_conf.is_sql():
            job_arguments.append(f"--query \"{self.__escape_query(job_conf.get_sql())}\"")
            python_flink_params = [
                f"--python {self.pyflink_runner_dir}/run-flink-sql.py",
                f"-pyexec {self.pyexec_path}",
                f"-pyclientexec {self.pyclientexec_path}",
            ]
        else:
            run_file_path = self.__create_run_file_from_template(job_conf)
            python_flink_params = [
                f"--python {run_file_path}",
                f"-pyexec {self.pyexec_path}",
                f"-pyclientexec {self.pyclientexec_path}",
            ]

        self.flink_cli_runner.start(
            flink_properties=self._get_flink_properties(job_conf),
            python_flink_params=python_flink_params,
            job_arguments=job_arguments,
            savepoint_path=savepoint_path
        )

    def _get_flink_properties(self, job_conf: JobConfiguration) -> Dict[str, Any]:
        # We need to append query version to checkpoint and savepoint path, but we don't want to modify the manifest.
        cloned_conf = JobConfiguration(copy.deepcopy(job_conf.job_definition))
        cloned_conf.set_flink_checkpoints_dir(
            os.path.join(cloned_conf.get_flink_checkpoints_dir(), cloned_conf.get_meta_query_version_str()))
        cloned_conf.set_flink_savepoints_dir(
            os.path.join(cloned_conf.get_flink_savepoints_dir(), cloned_conf.get_meta_query_version_str()))
        return cloned_conf.get_flink_properties()

    def __create_run_file_from_template(self, job_conf: JobConfiguration) -> str:
        output_file_path = f"/tmp/run-{job_conf.get_name()}.py"
        self.jinja_template_resolver.resolve(
            template_dir=self.pyflink_runner_dir,
            template_file="run-flink-code.py.jinja2",
            vars={"code": job_conf.get_code()},
            output_file_path=output_file_path
        )
        return output_file_path

    def __find_latest_savepoint(self, savepoint_base_path: str) -> Optional[Tuple[str, datetime.datetime]]:
        # Each job instance keeps savepoints in a separate location. Savepoints DO NOT have increasing numbers assigned.
        # Sample path:
        #   base-location/savepoints/sample-query/1/savepoint-5c9ee3-8575306fd774/_metadata
        # Basically, the path consists of the following parts:
        #   <savepoints-base-path>/<savepoint-id>/_metadata
        # In our case, the checkpoints-base-path consists of the following parts:
        #   <base-path>/savepoints/<query-name>/<query-version>
        # In order to find the latest savepoint, we search for the latest .*_metadata object for given query and
        # version, then we take its "parent directory".
        url_parts = savepoint_base_path.split("/", 3)
        bucket_name = url_parts[2]
        prefix = url_parts[3]
        return self.__find_latest_state_internal(bucket_name, prefix)

    def __find_latest_checkpoint(self, checkpoint_base_path: str) -> Optional[Tuple[str, datetime.datetime]]:
        # Each job instance keeps checkpoint in a separate location. Checkpoints have increasing numbers assigned.
        # Sample path:
        #   base-location/checkpoints/sample-query/1/aa18345e22b4b5c0e49051d1369bd24f/chk-19973/_metadata
        # Basically, the path consists of the following parts:
        #   <checkpoints-base-path>/<flink-job-id>/chk-<checkpoint-number>/_metadata
        # In our case, the checkpoints-base-path consists of the following parts:
        #   <base-path>/checkpoints/<query-name>/<query-version>
        # In order to find the latest checkpoint, we search for the latest .*_metadata object for given query and
        # version, then we take its "parent directory".
        url_parts = checkpoint_base_path.split("/", 3)
        bucket_name = url_parts[2]
        prefix = url_parts[3]
        return self.__find_latest_state_internal(bucket_name, prefix)

    def __find_latest_state_internal(self,
                                     state_bucket: str,
                                     state_prefix: str) -> Optional[Tuple[str, datetime.datetime]]:
        last_created = get_latest_object(state_bucket, state_prefix, lambda k: k.endswith("_metadata"))
        if last_created is None:
            logging.info(f"No state found at '{state_prefix}'.")
            return None
        else:
            last_created_path, last_created_ts = last_created
            # Remove "_metadata" suffix
            state_prefix = last_created_path[0:-9]
            state_path = f"s3://{state_bucket}/{state_prefix}"
            logging.info(f"State found at '{state_path}'.")
            return state_path, last_created_ts

    def __fetch_job_manifest(self, bucket_name: str, prefix: str, job_name: str) -> Optional[JobConfiguration]:
        object_key = os.path.join(prefix, f"{job_name}.yaml")
        logging.info(f"Looking for config at s3://{bucket_name}/{object_key}.")
        raw_manifest = get_content(bucket_name, object_key)
        return JobConfiguration(yaml.safe_load(raw_manifest)) if raw_manifest else None

    def __has_job_manifest_changed(self, old_job_conf: JobConfiguration, new_job_conf: JobConfiguration) -> bool:
        return self.__has_job_definition_changed(old_job_conf, new_job_conf) \
               or self.__have_flink_properties_changed(old_job_conf, new_job_conf)

    def __have_flink_properties_changed(self, old_job_conf: JobConfiguration, new_job_conf: JobConfiguration) -> bool:
        has_changed = old_job_conf.get_flink_properties() != new_job_conf.get_flink_properties()
        logging.info(f"Have Flink properties changed? {has_changed}")
        return has_changed

    def __has_job_definition_changed(self, old_job_conf: JobConfiguration, new_job_conf: JobConfiguration) -> bool:
        """
        Check whether the job definition, either Flink SQL or Flink code block, has changed.
        :param old_job_conf: The job definition already deployed.
        :param new_job_conf: The job definition to be deployed.
        :return: 'True' if the job definition has changed.
        """
        old_definition = old_job_conf.get_sql() if old_job_conf.is_sql() else old_job_conf.get_code()
        new_definition = new_job_conf.get_sql() if new_job_conf.is_sql() else new_job_conf.get_code()
        has_changed = old_definition != new_definition
        logging.info(f"OLD:\n{old_definition}")
        logging.info(f"NEW:\n{new_definition}")
        logging.info(f"Has job definition changed? {has_changed}")
        return has_changed

    @staticmethod
    def __escape_query(query: str) -> str:
        return query.replace("`", "\\`")


if __name__ == "__main__":
    args, _ = parse_args()
    flink_cli_runner = FlinkCliRunner()
    jinja_template_resolver = JinjaTemplateResolver()
    EmrJobRunner(args.job_config_path, args.pyflink_runner_dir, args.external_job_config_bucket,
                 args.external_job_config_prefix, args.base_output_path, args.pyexec_path,
                 flink_cli_runner, jinja_template_resolver).run()
