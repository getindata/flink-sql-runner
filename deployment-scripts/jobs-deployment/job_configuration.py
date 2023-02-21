import typing
from typing import Any, Dict

import yaml


class JobConfiguration(object):
    def __init__(self, job_definition: Dict[str, Any]):
        self.job_definition = job_definition

    @staticmethod
    def from_yaml(content: str) -> "JobConfiguration":
        return JobConfiguration(yaml.safe_load(content))

    def get(self, property_name: str) -> Any:
        return self.job_definition[property_name]

    def get_name(self) -> str:
        return self.get("name")

    def get_description(self) -> str:
        return self.get("description")

    def is_sql(self) -> bool:
        return "sql" in self.job_definition

    def is_code(self) -> bool:
        return "code" in self.job_definition

    def get_sql(self) -> str:
        return self.job_definition["sql"]

    def get_code(self) -> str:
        return self.job_definition["code"]

    # ===== Flink properties ===== #

    def get_flink_properties(self) -> Dict[str, Any]:
        if "flinkProperties" in self.job_definition:
            return self.job_definition["flinkProperties"]
        return {}

    def get_flink_property(self, property_name: str) -> str:
        return typing.cast(str, self.get_flink_properties().get(property_name))

    def set_flink_property(self, property_name: str, value: str) -> None:
        self.get_flink_properties()[property_name] = value

    def get_flink_savepoints_dir(self) -> str:
        return self.get_flink_property("state.savepoints.dir")

    def set_flink_savepoints_dir(self, value: str) -> None:
        self.set_flink_property("state.savepoints.dir", value)

    def get_flink_checkpoints_dir(self) -> str:
        return self.get_flink_property("state.checkpoints.dir")

    def set_flink_checkpoints_dir(self, value: str) -> None:
        self.set_flink_property("state.checkpoints.dir", value)

    # ===== METADATA ===== #

    def get_meta(self) -> Dict[str, Any]:
        if "meta" not in self.job_definition:
            self.job_definition["meta"] = {}
        return self.job_definition["meta"]

    def get_meta_query_version(self) -> int:
        return typing.cast(int, self.get_meta().get("query-version"))

    def get_meta_query_version_str(self) -> str:
        return str(self.get_meta().get("query-version"))

    def get_meta_query_id(self) -> str:
        return typing.cast(str, self.get_meta().get("query-id"))

    def get_meta_query_create_timestamp(self) -> str:
        return typing.cast(str, self.get_meta().get("query-create-timestamp"))

    def set_meta_query_version(self, version: int) -> None:
        self.get_meta()["query-version"] = version

    def set_meta_query_id(self, id: str) -> None:
        self.get_meta()["query-id"] = id

    def set_meta_query_create_timestamp(self, timestamp: str) -> None:
        self.get_meta()["query-create-timestamp"] = timestamp

    def to_yaml(self):
        return yaml.dump(self.job_definition)

    def to_dict(self):
        return dict(self.job_definition)

    def __str__(self):
        return str(self.job_definition)


class JobConfigurationBuilder(object):
    def __init__(self):
        self.job_definition = {"meta": {}, "flinkProperties": {}}

    def with_property(self, key: str, value: Any) -> "JobConfigurationBuilder":
        self.job_definition[key] = value
        return self

    def with_name(self, name: str) -> "JobConfigurationBuilder":
        return self.with_property("name", name)

    def with_description(self, description: str) -> "JobConfigurationBuilder":
        return self.with_property("description", description)

    def with_sql(self, sql: str) -> "JobConfigurationBuilder":
        return self.with_property("sql", sql)

    def with_code(self, code: str) -> "JobConfigurationBuilder":
        return self.with_property("code", code)

    def with_flink_savepoints_dir(self, value: str) -> "JobConfigurationBuilder":
        return self.with_flink_property("state.savepoints.dir", value)

    def with_flink_checkpoints_dir(self, value: str) -> "JobConfigurationBuilder":
        return self.with_flink_property("state.checkpoints.dir", value)

    def with_flink_property(self, key: str, value: Any) -> "JobConfigurationBuilder":
        self.job_definition["flinkProperties"][key] = value
        return self

    def with_no_meta(self) -> "JobConfigurationBuilder":
        del self.job_definition["meta"]
        return self

    def with_meta_property(self, key: str, value: Any) -> "JobConfigurationBuilder":
        self.job_definition["meta"][key] = value
        return self

    def with_meta_query_version(self, version: int) -> "JobConfigurationBuilder":
        return self.with_meta_property("query-version", version)

    def with_meta_query_id(self, id: str) -> "JobConfigurationBuilder":
        return self.with_meta_property("query-id", id)

    def with_meta_query_create_timestamp(self, ts: str) -> "JobConfigurationBuilder":
        return self.with_meta_property("query-create-timestamp", ts)

    def build(self) -> JobConfiguration:
        return JobConfiguration(self.job_definition)
