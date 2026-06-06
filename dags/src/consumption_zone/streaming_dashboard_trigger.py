from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

from src.utils import get_logger, logical_date_iso

from .tweet_classifier import CONSUMPTION_SCHEMA_VERSION


class StreamingDashboardTrigger:
    """Hourly supervisor for the live Spark streaming dashboard job."""

    SOURCE_DB = "exploitation_analytics"
    LOG_TABLE = "streaming_dashboard_launch_log"
    SOURCE_SYSTEM = "streaming_path"
    SOURCE_ASSETS = "kafka.weather-barcelona,kafka.airquality-barcelona"
    TOPICS = ("weather-barcelona", "airquality-barcelona")
    CONSUMPTION_TASK = "spark_streaming_dashboard"
    DASHBOARD_NAME = "spark_streaming_with_kafka_dash"
    DEFAULT_NOTEBOOK_PATH = "/opt/airflow/notebooks/Consumption Zone/2. spark_streaming_with_kafka_dash.ipynb"
    DEFAULT_SCRIPT_PATH = "/opt/airflow/scripts/consumption_streaming_dashboard.py"
    DEFAULT_LOG_PATH = "/opt/airflow/logs/consumption_streaming_dashboard.log"
    DEFAULT_PID_PATH = "/tmp/consumption_streaming_dashboard.pid"
    DEFAULT_SPARK_MASTER_UI_URL = "http://spark-master:8080/json/"
    DEFAULT_SPARK_APP_NAME = "consumption_streaming_dashboard"
    LOG_COLUMNS = [
        "source_system",
        "source_assets",
        "created_at",
        "schema_version",
        "consumption_task",
        "dashboard_name",
        "trigger_status",
        "message",
        "kafka_bootstrap_servers",
        "spark_master_url",
        "notebook_path",
        "script_path",
        "spark_application_id",
        "supervisor_action",
        "local_pid",
    ]

    def __init__(self) -> None:
        self.logger = get_logger(self.__class__.__module__)
        self.client: Any | None = None
        self.log_columns = list(self.LOG_COLUMNS)

    def run(self, logical_date: Any) -> dict[str, Any]:
        created_at = logical_date_iso(logical_date)
        self.logger.info("Starting streaming dashboard supervisor created_at=%s", created_at)
        client = self.get_client()
        try:
            self.client = client
            self.ensure_launch_log_table()
            try:
                check_message = self.validate_runtime()
                supervision = self.supervise_dashboard()
                ready_record = self.build_launch_record(
                    created_at=created_at,
                    trigger_status=supervision["status"],
                    message=f"{check_message}; {supervision['message']}",
                    spark_application_id=supervision.get("spark_application_id", ""),
                    supervisor_action=supervision.get("action", ""),
                    local_pid=supervision.get("local_pid", ""),
                )
                self.insert_launch_record(ready_record)
                self.logger.info("Streaming dashboard supervisor completed result=%s", ready_record)
                return ready_record
            except Exception as exc:
                failure_record = self.build_launch_record(
                    created_at=created_at,
                    trigger_status="failed",
                    message=str(exc)[:1000],
                    spark_application_id="",
                    supervisor_action="failed",
                    local_pid=self.read_pid(),
                )
                self.insert_launch_record(failure_record)
                self.logger.exception("Streaming dashboard supervisor failed")
                raise
        finally:
            client.close()
            self.client = None

    def get_client(self) -> Any:
        import clickhouse_connect

        username = os.getenv("CLICKHOUSE_CONSUMPTION_USER")
        password = os.getenv("CLICKHOUSE_CONSUMPTION_PASSWORD")
        if not username or not password:
            # NOTE: analytics is kept only as the local maintenance fallback.
            username = os.getenv("CLICKHOUSE_USER", "analytics")
            password = os.getenv("CLICKHOUSE_PASSWORD", "analytics_secret")
            self.logger.warning("CLICKHOUSE_CONSUMPTION_* is missing; using ClickHouse maintenance fallback user")

        return clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
            username=username,
            password=password,
            database=os.getenv("CLICKHOUSE_DATABASE", "default"),
        )

    @staticmethod
    def quote_identifier(identifier: str) -> str:
        return "`" + identifier.replace("`", "``") + "`"

    @staticmethod
    def sql_literal(value: str) -> str:
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"

    def table_ref(self, table_name: str) -> str:
        return f"{self.quote_identifier(self.SOURCE_DB)}.{self.quote_identifier(table_name)}"

    def _require_client(self) -> Any:
        if self.client is None:
            raise RuntimeError("ClickHouse client is not initialized")
        return self.client

    def ensure_launch_log_table(self) -> None:
        client = self._require_client()
        self.logger.info("Ensuring streaming dashboard launch log table %s.%s", self.SOURCE_DB, self.LOG_TABLE)
        client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_ref(self.LOG_TABLE)}
            (
                source_system String,
                source_assets String,
                created_at String,
                schema_version String,
                consumption_task String,
                dashboard_name String,
                trigger_status String,
                message String,
                kafka_bootstrap_servers String,
                spark_master_url String,
                notebook_path String,
                script_path String,
                spark_application_id String,
                supervisor_action String,
                local_pid String
            )
            ENGINE = MergeTree()
            ORDER BY (created_at, consumption_task, dashboard_name, trigger_status)
            """
        )
        for column_name in ("script_path", "spark_application_id", "supervisor_action", "local_pid"):
            try:
                client.command(f"ALTER TABLE {self.table_ref(self.LOG_TABLE)} ADD COLUMN IF NOT EXISTS {column_name} String")
            except Exception as exc:
                self.logger.warning("Could not add launch log column %s; using available columns only: %s", column_name, exc)

        self.log_columns = self.available_launch_log_columns()

    def available_launch_log_columns(self) -> list[str]:
        client = self._require_client()
        try:
            result = client.query(
                f"""
                SELECT name
                FROM system.columns
                WHERE database = {self.sql_literal(self.SOURCE_DB)}
                  AND table = {self.sql_literal(self.LOG_TABLE)}
                ORDER BY position
                """
            )
            available = {row[0] for row in result.result_rows}
            columns = [column for column in self.LOG_COLUMNS if column in available]
            if columns:
                return columns
        except Exception as exc:
            self.logger.warning("Could not inspect launch log columns; falling back to the legacy schema: %s", exc)
        return self.LOG_COLUMNS[:11]

    def validate_runtime(self) -> str:
        kafka_bootstrap_servers = self.kafka_bootstrap_servers()
        spark_master_url = self.spark_master_url()
        script_path = self.script_path()

        self.logger.info("Validating dashboard script path=%s", script_path)
        if not script_path.exists():
            raise FileNotFoundError(f"Dashboard Python script is not mounted or does not exist: {script_path}")

        self.logger.info("Validating Kafka topics=%s bootstrap=%s", ",".join(self.TOPICS), kafka_bootstrap_servers)
        self.validate_kafka_topics(kafka_bootstrap_servers)

        self.logger.info("Validating Spark master URL=%s", spark_master_url)
        self.validate_spark_master(spark_master_url)
        self.validate_spark_master_ui()
        self.validate_postgres_sink()

        return (
            "ready: dashboard script exists, Kafka topics are reachable, "
            "Spark master is reachable, and PostgreSQL sink accepted a TCP health check"
        )

    def supervise_dashboard(self) -> dict[str, str]:
        active_app = self.find_active_spark_app()
        if active_app:
            app_id = str(active_app.get("id", ""))
            message = f"healthy: Spark application {app_id or self.spark_app_name()} is active"
            return {
                "status": "healthy",
                "action": "none",
                "message": message,
                "spark_application_id": app_id,
                "local_pid": self.read_pid(),
            }

        old_pid = self.read_pid()
        if old_pid and self.is_pid_alive(old_pid):
            self.logger.warning("Spark app is not active but local dashboard pid=%s is alive; stopping stale process", old_pid)
            self.stop_local_process(old_pid)

        pid = self.start_dashboard_process()
        message = f"restarted: no active Spark app named {self.spark_app_name()}; launched local pid={pid}"
        return {
            "status": "restarted",
            "action": "restart",
            "message": message,
            "spark_application_id": "",
            "local_pid": pid,
        }

    def find_active_spark_app(self) -> dict[str, Any] | None:
        app_name = self.spark_app_name()
        response = requests.get(self.spark_master_ui_url(), timeout=5)
        response.raise_for_status()
        payload = response.json()
        for app in payload.get("activeapps", []):
            if app.get("name") == app_name:
                return app
        return None

    def start_dashboard_process(self) -> str:
        script_path = self.script_path()
        log_path = self.log_path()
        pid_path = self.pid_path()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        pid_path.parent.mkdir(parents=True, exist_ok=True)

        env = os.environ.copy()
        existing_pythonpath = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = ":".join(part for part in [existing_pythonpath, "/opt/airflow/dags"] if part)
        env.setdefault("DASHBOARD_SPARK_APP_NAME", self.spark_app_name())
        env.setdefault("CONSUMPTION_DASHBOARD_SCRIPT_PATH", str(script_path))

        with log_path.open("ab") as output:
            process = subprocess.Popen(
                [sys.executable, str(script_path)],
                cwd=str(script_path.parent),
                env=env,
                stdout=output,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )

        pid = str(process.pid)
        pid_path.write_text(pid, encoding="utf-8")
        self.logger.info("Started streaming dashboard process pid=%s script_path=%s log_path=%s", pid, script_path, log_path)
        return pid

    def stop_local_process(self, pid: str) -> None:
        try:
            pid_int = int(pid)
        except ValueError:
            return

        try:
            os.killpg(pid_int, 15)
        except Exception:
            try:
                os.kill(pid_int, 15)
            except Exception as exc:
                self.logger.warning("Failed to terminate dashboard pid=%s: %s", pid, exc)
                return

        deadline = time.time() + 10
        while time.time() < deadline:
            if not self.is_pid_alive(pid):
                break
            time.sleep(1)

        if self.is_pid_alive(pid):
            self.logger.warning("Dashboard pid=%s did not stop after SIGTERM; sending SIGKILL", pid)
            try:
                os.killpg(pid_int, 9)
            except Exception:
                try:
                    os.kill(pid_int, 9)
                except Exception as exc:
                    self.logger.warning("Failed to kill dashboard pid=%s: %s", pid, exc)

    @staticmethod
    def is_pid_alive(pid: str) -> bool:
        try:
            os.kill(int(pid), 0)
            return True
        except Exception:
            return False

    def validate_kafka_topics(self, bootstrap_servers: str) -> None:
        from kafka.admin import KafkaAdminClient

        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="consumption-dashboard-trigger",
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000,
        )
        try:
            available_topics = set(admin_client.list_topics())
        finally:
            admin_client.close()

        missing_topics = sorted(set(self.TOPICS) - available_topics)
        if missing_topics:
            raise RuntimeError(f"Missing Kafka topics for streaming dashboard: {missing_topics}")
        self.logger.info("Kafka topic validation passed available_required_topics=%s", sorted(self.TOPICS))

    @staticmethod
    def validate_spark_master(spark_master_url: str) -> None:
        parsed = urlparse(spark_master_url)
        if parsed.scheme != "spark" or not parsed.hostname or not parsed.port:
            raise ValueError(f"SPARK_MASTER_URL must look like spark://host:port, got: {spark_master_url}")
        with socket.create_connection((parsed.hostname, parsed.port), timeout=3):
            return

    def validate_spark_master_ui(self) -> None:
        response = requests.get(self.spark_master_ui_url(), timeout=5)
        response.raise_for_status()

    def validate_postgres_sink(self) -> None:
        host, port = self.postgres_host_port()
        with socket.create_connection((host, port), timeout=3):
            return

    @staticmethod
    def kafka_bootstrap_servers() -> str:
        value = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").strip()
        if not value:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required for streaming dashboard supervision")
        return value

    @staticmethod
    def spark_master_url() -> str:
        value = os.getenv("SPARK_MASTER_URL", "").strip()
        if not value:
            raise ValueError("SPARK_MASTER_URL is required for streaming dashboard supervision")
        return value

    def notebook_path(self) -> Path:
        return Path(os.getenv("CONSUMPTION_DASHBOARD_NOTEBOOK_PATH", self.DEFAULT_NOTEBOOK_PATH))

    def script_path(self) -> Path:
        return Path(os.getenv("CONSUMPTION_DASHBOARD_SCRIPT_PATH", self.DEFAULT_SCRIPT_PATH))

    def log_path(self) -> Path:
        return Path(os.getenv("CONSUMPTION_DASHBOARD_LOG_PATH", self.DEFAULT_LOG_PATH))

    def pid_path(self) -> Path:
        return Path(os.getenv("CONSUMPTION_DASHBOARD_PID_PATH", self.DEFAULT_PID_PATH))

    def read_pid(self) -> str:
        pid_path = self.pid_path()
        if not pid_path.exists():
            return ""
        return pid_path.read_text(encoding="utf-8").strip()

    def spark_master_ui_url(self) -> str:
        return os.getenv("SPARK_MASTER_UI_URL", self.DEFAULT_SPARK_MASTER_UI_URL)

    def spark_app_name(self) -> str:
        return os.getenv("DASHBOARD_SPARK_APP_NAME", self.DEFAULT_SPARK_APP_NAME)

    @staticmethod
    def postgres_host_port() -> tuple[str, int]:
        jdbc_url = os.getenv("DASHBOARD_POSTGRES_JDBC_URL", "jdbc:postgresql://postgres-analytics:5432/analytics")
        parsed = urlparse(jdbc_url.removeprefix("jdbc:"))
        if not parsed.hostname:
            raise ValueError(f"DASHBOARD_POSTGRES_JDBC_URL must include a host, got: {jdbc_url}")
        return parsed.hostname, parsed.port or 5432

    def build_launch_record(
        self,
        *,
        created_at: str,
        trigger_status: str,
        message: str,
        spark_application_id: str,
        supervisor_action: str,
        local_pid: str,
    ) -> dict[str, str]:
        return {
            "source_system": self.SOURCE_SYSTEM,
            "source_assets": self.SOURCE_ASSETS,
            "created_at": created_at,
            "schema_version": CONSUMPTION_SCHEMA_VERSION,
            "consumption_task": self.CONSUMPTION_TASK,
            "dashboard_name": self.DASHBOARD_NAME,
            "trigger_status": trigger_status,
            "message": message,
            "kafka_bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").strip(),
            "spark_master_url": os.getenv("SPARK_MASTER_URL", "").strip(),
            "notebook_path": str(self.notebook_path()),
            "script_path": str(self.script_path()),
            "spark_application_id": spark_application_id,
            "supervisor_action": supervisor_action,
            "local_pid": local_pid,
        }

    def insert_launch_record(self, record: dict[str, str]) -> None:
        client = self._require_client()
        payload = [[record[column] for column in self.log_columns]]
        client.insert(
            table=self.LOG_TABLE,
            data=payload,
            column_names=self.log_columns,
            database=self.SOURCE_DB,
        )
        self.logger.info(
            "Inserted streaming dashboard launch log trigger_status=%s columns=%s",
            record["trigger_status"],
            self.log_columns,
        )
