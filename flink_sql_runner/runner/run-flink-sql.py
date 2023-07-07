import logging
import os
import sys
from typing import Any, Dict, List
import yaml

import s3fs
import sqlparse
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

TEMPORARY_INPUT_VIEW_NAME = "__temporary_input_view"


def execute_table_definitions(definitions: List[str], params: Dict[str, Any]) -> None:
    for definition in definitions:
        definition = definition.format(**params)
        logging.info(f"Executing DDL: \n{definition}\n\n")
        t_env.execute_sql(definition)


args = sys.argv
config_yaml_path = args[1]

with open(config_yaml_path, "r") as stream:
    try:
        data = yaml.load(stream, yaml.FullLoader)

        query = data["query"]
        target_table = data["target_table"]
        table_definition_path = data["table_definition_path"]
        metadata_query_name = data["metadata_query_name"]
        metadata_query_description = data["metadata_query_description"]
        metadata_query_id = data["metadata_query_id"]
        metadata_query_version = data["metadata_query_version"]
        metadata_query_create_timestamp = data["metadata_query_create_timestamp"]
        template_params = data["template_params"]
        timestamp_field_name = data["timestamp_field_name"]
        include_query_metadata = data["include_query_metadata"]

    except yaml.YAMLError as exc:
        print(exc)

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

sql_paths = table_definition_path
template_params = template_params if template_params else []

table_definitions_params = {
    # Each job should have a unique Kafka group.id.
    "group_id": f"rta-{metadata_query_name}-{metadata_query_version}",
}

for template_param in template_params:
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

result_fields = ["*"]
if timestamp_field_name:
    result_fields.append(f"NOW() AS {timestamp_field_name}")
if include_query_metadata:
    result_fields.extend(
        [
            f"CAST('{metadata_query_name}' AS STRING) AS __query_name",
            f"CAST('{metadata_query_description}' AS STRING) AS __query_description",
            f"CAST('{metadata_query_id}' AS STRING) AS __query_id",
            f"CAST({metadata_query_version} AS INT) AS __query_version",
            f"CAST('{metadata_query_create_timestamp}' AS TIMESTAMP) AS __query_create_timestamp",
        ]
    )

load_query = f"""INSERT INTO {target_table}
SELECT
{"    ,".join(result_fields)}
FROM
    {TEMPORARY_INPUT_VIEW_NAME}
;"""

logging.info(f"Creating temporary view {TEMPORARY_INPUT_VIEW_NAME}: \n{query}\n\n")
table = t_env.sql_query(query)
t_env.create_temporary_view(TEMPORARY_INPUT_VIEW_NAME, table)

logging.info(f"Running generated insert query:\n{load_query}")
t_env.execute_sql(load_query)
