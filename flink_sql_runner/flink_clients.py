import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List

from flink_sql_runner.cmd_utils import run_cmd


class FlinkCli(ABC):
    JOB_RUNNING_CHECK_RETRIES_COUNT = 10
    JOB_RUNNING_CHECK_RETRIES_TIMEOUT = 3

    def ensure_job_is_running(self, job_name: str) -> None:
        """
        Makes sure that a Flink job has status CREATED or RUNNING shortly after being run.
        :param job_name: Flink job name
        """
        for check_index in range(self.JOB_RUNNING_CHECK_RETRIES_COUNT):
            logging.info(f"Checking the state of the job: {check_index}")
            status = self.get_job_status(job_name)
            if status not in ["RUNNING", "CREATED"]:
                raise RuntimeError(f"Unexpected job state. Recent status {status}.")
            time.sleep(self.JOB_RUNNING_CHECK_RETRIES_TIMEOUT)

    @abstractmethod
    def get_job_status(self, job_name: str) -> str:
        """
        Gets the status of a Flink job.
        :param job_name: Flink job name
        :return: Status of a Flink job
        """
        pass

    @abstractmethod
    def is_job_running(self, job_name: str) -> bool:
        pass

    @abstractmethod
    def get_job_id(self, job_name: str) -> str:
        """
        Gets ID of a Flink job with given job name.
        :param job_name: Flink job name
        :return: Flink job ID
        """
        pass

    @abstractmethod
    def stop_with_savepoint(self, job_id: str, savepoint_path: str) -> None:
        """
        Creates job's final snapshot and stops the job gracefully.
        :param job_id: Flink job ID
        :param savepoint_path: Location where the savepoint should be saved
        """
        pass

    @abstractmethod
    def start(
            self,
            flink_properties: Dict[str, Any],
            python_flink_params: List[str],
            job_arguments: List[str],
            savepoint_path: str = None,
    ) -> None:
        """
        Starts Flink job.
        :param flink_properties: A dictionary of Flink configuration properties specific for this job.
        :param python_flink_params: A list of Python-specific parameters of PyFlink.
        :param job_arguments: A list of job parameters.
        :param savepoint_path: The path where the savepoint of the previously executed job is stored. If 'None', the
        job will be started with clean state.
        """
        pass


class FlinkYarnRunner(FlinkCli):
    """
    A Python wrapper for Flink Command-Line Interface. YARN is the deployment target.
    """

    def __init__(
            self,
            session_app_id: str = None,
            session_cluster_name: str = "Flink session cluster",
    ):
        self.session_app_id = session_app_id if session_app_id is not None else self.__get_session_app_id()
        self.session_cluster_name = session_cluster_name

    def get_job_status(self, job_name: str) -> str:
        _, job_status, _ = run_cmd(
            f"""flink list -t yarn-session -Dyarn.application.id={self.session_app_id} | grep {job_name} | \
            cut -f 7 -d ' ' | sed 's/.//;s/.$//' | tr -d '\\n' """,
            throw_on_error=True,
        )
        return job_status

    @staticmethod
    def __get_session_app_id() -> str:
        """
        Returns YARN applicationId of the running Flink session cluster. The ID has the following format:
        "application_1669122056871_0002".
        :return: YARN applicationId
        """
        _, output, _ = run_cmd(
            f"""yarn application -list | grep 'Flink session cluster' | cut -f1 -d$'\t' """,  # noqa: F541
            throw_on_error=True,
        )
        yarn_application_id = output.strip()
        if yarn_application_id:
            logging.info(f"SESSION APP ID '{yarn_application_id}'.")
            return yarn_application_id
        else:
            raise ValueError("No Flink session cluster running.")

    def is_job_running(self, job_name: str) -> bool:
        _, output, _ = run_cmd(
            f"""flink list -t yarn-session -Dyarn.application.id={self.session_app_id} | grep {job_name} | wc -l """,
            throw_on_error=True,
        )
        return "1" == output.strip()

    def get_job_id(self, job_name: str) -> str:
        _, output, _ = run_cmd(
            f"""flink list -t yarn-session -Dyarn.application.id={self.session_app_id} | \
            grep {job_name} | cut -f 4 -d ' ' """,
            throw_on_error=True,
        )
        return output

    def stop_with_savepoint(self, job_id: str, savepoint_path: str) -> None:
        run_cmd(
            f"""flink stop \
            -t yarn-session \
            -Dyarn.application.id={self.session_app_id} \
            --savepointPath {savepoint_path} \
            {job_id} """,
            throw_on_error=True,
        )

    def start(
            self,
            flink_properties: Dict[str, Any],
            python_flink_params: List[str],
            job_arguments: List[str],
            savepoint_path: str = None,
    ) -> None:
        run_cmd(
            f"""flink run \
            -t yarn-session \
            -Dyarn.application.id={self.session_app_id} \
            {concat_properties(flink_properties)} \
            {" ".join(python_flink_params)} \
            --detached \
            {"" if not savepoint_path else "--fromSavepoint " + savepoint_path} \
            {" ".join(job_arguments)} """,
            throw_on_error=True,
        )


class FlinkStandaloneClusterRunner(FlinkCli):
    """
    A Python wrapper for Flink Command-Line Interface. A Flink Standalone cluster is the deployment target.
    """

    def __init__(self, jobmanager_address: str):
        self.jobmanager_address = jobmanager_address

    def get_job_status(self, job_name: str) -> str:
        _, job_status, _ = run_cmd(
            f"""flink list --jobmanager "{self.jobmanager_address}" | grep "{job_name}" | \
            cut -f 7 -d ' ' | sed 's/.//;s/.$//' | tr -d '\\n' """,
            throw_on_error=True,
        )
        return job_status

    def is_job_running(self, job_name: str) -> bool:
        _, output, _ = run_cmd(
            f"""flink list --jobmanager "{self.jobmanager_address}" | grep {job_name} | wc -l """,
            throw_on_error=True,
        )
        return "1" == output.strip()

    def get_job_id(self, job_name: str) -> str:
        _, output, _ = run_cmd(
            f"""flink list --jobmanager "{self.jobmanager_address}" | grep {job_name} | cut -f 4 -d ' ' """,
            throw_on_error=True,
        )
        return output

    def stop_with_savepoint(self, job_id: str, savepoint_path: str) -> None:
        run_cmd(
            f"""flink stop \
            --jobmanager "{self.jobmanager_address}" \
            --savepointPath {savepoint_path} \
            {job_id} """,
            throw_on_error=True,
        )

    def start(
            self,
            flink_properties: Dict[str, Any],
            python_flink_params: List[str],
            job_arguments: List[str],
            savepoint_path: str = None,
    ) -> None:
        run_cmd(
            f"""flink run \
            --jobmanager "{self.jobmanager_address}" \
            {concat_properties(flink_properties)} \
            {" ".join(python_flink_params)} \
            --detached \
            {"" if not savepoint_path else "--fromSavepoint " + savepoint_path} \
            {" ".join(job_arguments)} """,
            throw_on_error=True,
        )


def concat_properties(flink_properties: Dict[str, Any]) -> str:
    result = ""
    for k, v in flink_properties.items():
        if v is True:
            v = "true"
        elif v is False:
            v = "false"
        result += f"-D{k}={v} "
    return result
