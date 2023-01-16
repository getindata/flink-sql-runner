import argparse
import logging
import os
import sys
from typing import Any, Dict, List

import s3fs
import sqlparse
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

TEMPORARY_INPUT_VIEW_NAME = "__temporary_input_view"


def execute_table_definitions(definitions: List[str], params: Dict[str, Any]) -> None:
    for definition in definitions:
        definition = definition.format(**params)
        logging.info("Executing DDL: \n" + definition + "\n\n")
        t_env.execute_sql(definition)


parser = argparse.ArgumentParser()
parser.add_argument(
    "--table-definition-path", nargs="+", required=True, help="Path to flink table DDL. Can be path to a file or s3"
)
parser.add_argument("--query", "-q", required=True, help="SQL query to execute.")
parser.add_argument("--target-table", "-tt", required=True, help="Target table where to write results of the query.")
parser.add_argument("--metadata-query-name", "-qname", required=True, help="Human readable SQL query name.")
parser.add_argument("--metadata-query-description", "-qdesc", required=True, help="SQL query description.")
parser.add_argument("--metadata-query-id", "-qid", required=True, help="Unique SQL query id.")
parser.add_argument(
    "--metadata-query-version",
    "-qv",
    type=int,
    required=True,
    help="SQL query version, monotonously increasing, starts from 1.",
)
parser.add_argument(
    "--metadata-query-create-timestamp", "-qtime", required=True, help="When has the SQL query been deployed."
)
parser.add_argument(
    "--template-params",
    required=False,
    nargs="+",
    help="Extra parameters that will be used when resolving template variables. Each should have form 'key=value'",
)

args = parser.parse_args(sys.argv[1:])

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

sql_paths = args.table_definition_path

table_definitions_params = {
    # Each job should have a unique Kafka group.id.
    "group_id": f"rta-{args.metadata_query_name}-{args.metadata_query_version}",
}

for template_param in args.template_params:
    parts = template_param.strip().split("=", 2)
    table_definitions_params[parts[0].strip()] = parts[1].strip()

for path in sql_paths:
    if path.startswith("s3://"):
        # Reading table definitions from S3 bucket
        _, path = path.split("://", 1)
        client_args = {"enpoint_url": os.environ["AWS_S3_ENDPOINT"]} if "AWS_S3_ENDPOINT" in os.environ else {}
        fs = s3fs.S3FileSystem(client_kwargs=client_args)
        with fs.open(path, "rb") as file:
            execute_table_definitions(sqlparse.split(file.read()), table_definitions_params)
    else:
        # Reading table definitions from local filesystem
        with open(path, "r") as file:
            execute_table_definitions(sqlparse.split(file.read()), table_definitions_params)

load_query = f"""INSERT INTO {args.target_table}
SELECT
    *,
    NOW() AS __create_timestamp,
    CAST('{args.metadata_query_name}' AS STRING) AS __query_name,
    CAST('{args.metadata_query_description}' AS STRING) AS __query_description,
    CAST('{args.metadata_query_id}' AS STRING) AS __query_id,
    CAST({args.metadata_query_version} AS INT) AS __query_version,
    CAST('{args.metadata_query_create_timestamp}' AS TIMESTAMP) AS __query_create_timestamp
FROM
    {TEMPORARY_INPUT_VIEW_NAME}
;"""

logging.info(f"Creating temporary view {TEMPORARY_INPUT_VIEW_NAME}: \n" + args.query + "\n\n")
table = t_env.sql_query(args.query)
t_env.create_temporary_view(TEMPORARY_INPUT_VIEW_NAME, table)

logging.info("Running generated insert query: \n" + load_query)
t_env.execute_sql(load_query)
