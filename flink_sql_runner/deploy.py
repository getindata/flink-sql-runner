#!/usr/bin/env python3
import argparse
import logging
import os
from typing import List

import yaml

from flink_sql_runner.deploy_job import FlinkJobRunner, JinjaTemplateResolver
from flink_sql_runner.flink_clients import (FlinkStandaloneClusterRunner,
                                            FlinkYarnRunner, FlinkCli)
from flink_sql_runner.job_configuration import JobConfiguration
from flink_sql_runner.manifest import ManifestManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")


def parse_args():
    # Parse cmd line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True, help="Path where query definition files are stored.")
    parser.add_argument("--template-file", required=True, help="Path to the job configuration defaults.")
    parser.add_argument(
        "--pyflink-runner-dir",
        required=True,
        help="Path to the directory containing PyFlink job runners.",
    )
    parser.add_argument(
        "--external-job-config-bucket",
        required=True,
        help="S3 bucket where job configuration is stored.",
    )
    parser.add_argument(
        "--external-job-config-prefix",
        required=True,
        help="S3 prefix where job configuration is stored.",
    )
    parser.add_argument(
        "--table-definition-path",
        nargs="+",
        required=True,
        help="Paths to files containing common Flink table definitions.",
    )
    parser.add_argument(
        "--pyexec-path",
        required=True,
        help="Path of the Python interpreter used to execute client code and Flink Python UDFs.",
    )
    parser.add_argument(
        "--deployment-target",
        required=True,
        choices=("yarn", "remote"),
        help="Flink deployment target. Currently only yarn and remote are supported.",
    )
    parser.add_argument(
        "--jobmanager-address",
        help="JobManager address. Applicable only when remote deployment target has been chosen.",
    )
    return parser.parse_known_args()


class FlinkRunner:
    def __init__(self,
                 queries_base_path: str,
                 table_definition_path: str,
                 pyflink_runner_dir: str,
                 template_file: str,
                 pyexec_path: str,
                 flink_cli_runner: FlinkCli,
                 manifest_manager: ManifestManager,
                 jinja_template_resolver: JinjaTemplateResolver,
                 passthrough_args,
                 ):
        self.queries_base_path = queries_base_path
        self.table_definition_path = table_definition_path
        self.pyflink_runner_dir = pyflink_runner_dir
        self.template_file = template_file
        self.pyexec_path = pyexec_path
        self.flink_cli_runner = flink_cli_runner
        self.manifest_manager = manifest_manager
        self.jinja_template_resolver = jinja_template_resolver
        self.passthrough_args = passthrough_args

    def run(self):
        query_files = self.__list_query_files(self.queries_base_path)
        existing_query_manifests = self.manifest_manager.list_manifests()
        queries_to_cancel = self.__get_queries_to_cancel(query_files, existing_query_manifests)

        logging.info("Queries to start or update: {}", query_files)
        logging.info("Queries to cancel: {}", queries_to_cancel)

        self.__add_or_update_jobs(query_files)
        self.__cancel_deleted_jobs(queries_to_cancel)

    def __get_queries_to_cancel(self, query_files: List[str], existing_query_manifests: List[str]) -> List[str]:
        running_query_names = [n.split("/")[-1] for n in existing_query_manifests]
        current_query_names = [n.split("/")[-1] for n in query_files]
        difference = set(running_query_names).difference(set(current_query_names))
        return [q.replace(".yaml", "") for q in difference]

    def __add_or_update_jobs(self, query_files):
        for query_file in query_files:
            final_config = self.__read_config(query_file, self.template_file)
            new_job_conf = JobConfiguration(final_config)
            FlinkJobRunner(
                job_name=new_job_conf.get_name(),
                new_job_conf=new_job_conf,
                pyflink_runner_dir=self.pyflink_runner_dir,
                table_definition_paths=self.table_definition_path,
                pyexec_path=self.pyexec_path,
                flink_cli_runner=self.flink_cli_runner,
                jinja_template_resolver=self.jinja_template_resolver,
                manifest_manager=self.manifest_manager,
                passthrough_args=self.passthrough_args,
            ).run()

    def __cancel_deleted_jobs(self, queries_to_cancel):
        for query_to_remove in queries_to_cancel:
            FlinkJobRunner(
                job_name=query_to_remove,
                new_job_conf=None,
                pyflink_runner_dir=self.pyflink_runner_dir,
                table_definition_paths=self.table_definition_path,
                pyexec_path=self.pyexec_path,
                flink_cli_runner=self.flink_cli_runner,
                jinja_template_resolver=self.jinja_template_resolver,
                manifest_manager=self.manifest_manager,
                passthrough_args=self.passthrough_args,
            ).run()

    def __list_query_files(self, base_path: str) -> List[str]:
        result = []
        for root, dirs, files in os.walk(base_path):
            for f in files:
                if f.endswith(".yaml"):
                    result.append(os.path.abspath(os.path.join(root, f)))
        return result

    def __read_config(self, query_file, template_file):
        # FIXME: refactor variables resolutions and yaml merge
        with open(query_file) as qf:
            query_specification = yaml.load(qf, yaml.FullLoader)
            if "sql" in query_specification:
                query_specification["sql"] = query_specification["sql"].replace("\n", " ")
            with open(template_file) as tf:
                raw_defaults = tf.read().format(job_name=query_specification["name"])
                default_config = yaml.safe_load(raw_defaults)
                final_flink_props = {
                    **default_config["flinkProperties"],
                    **query_specification["flinkProperties"],
                }
                final_config = {**default_config, **query_specification}
                final_config["flinkProperties"] = final_flink_props
                logging.info(f"Final configuration:\n{final_config}")
                return final_config


if __name__ == "__main__":
    args, passthrough_args = parse_args()

    FlinkRunner(
        queries_base_path=args.path,
        table_definition_path=args.table_definition_path,
        pyflink_runner_dir=args.pyflink_runner_dir,
        template_file=args.template_file,
        pyexec_path=args.pyexec_path,
        flink_cli_runner=(
            FlinkYarnRunner()
            if args.deployment_target == "yarn"
            else FlinkStandaloneClusterRunner(args.jobmanager_address)
        ),
        manifest_manager=ManifestManager(args.external_job_config_bucket, args.external_job_config_prefix),
        jinja_template_resolver=JinjaTemplateResolver(),
        passthrough_args=passthrough_args,
    ).run()
