"""Static consistency checks for the infrastructure files (no Docker needed)."""

import re
from pathlib import Path

import yaml

from streamlab import generator
from streamlab.kafka_io import DLQ_TOPIC, RAW_TOPIC

ROOT = Path(__file__).resolve().parents[1]


def _compose() -> dict:
    return yaml.safe_load((ROOT / "docker-compose.yml").read_text())


def _env_example() -> dict[str, str]:
    out = {}
    for line in (ROOT / ".env.example").read_text().splitlines():
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def test_required_services_exist():
    services = _compose()["services"]
    for name in (
        "kafka",
        "kafka-init",
        "schema-registry",
        "postgres",
        "minio",
        "minio-init",
        "flink-jobmanager",
        "flink-taskmanager",
        "flink-job-clean",
        "flink-job-agg",
        "grafana",
    ):
        assert name in services, name


def test_host_ports_are_unique():
    ports = []
    for svc in _compose()["services"].values():
        for p in svc.get("ports", []):
            ports.append(str(p).split(":")[0])
    assert len(ports) == len(set(ports)), ports


def test_every_service_has_a_memory_limit():
    for name, svc in _compose()["services"].items():
        assert "mem_limit" in svc, f"{name} has no mem_limit"


def test_depends_on_targets_exist():
    services = _compose()["services"]
    for name, svc in services.items():
        deps = svc.get("depends_on", {})
        for dep in deps if isinstance(deps, list) else deps.keys():
            assert dep in services, f"{name} depends on unknown service {dep}"


def test_long_running_services_have_healthchecks_when_depended_on_as_healthy():
    services = _compose()["services"]
    for name, svc in services.items():
        deps = svc.get("depends_on", {})
        if not isinstance(deps, dict):
            continue
        for dep, cond in deps.items():
            if cond.get("condition") == "service_healthy":
                assert "healthcheck" in services[dep], f"{name} waits on {dep} which has none"


def test_docker_memory_budget_under_10_gib():
    total_mb = 0
    for svc in _compose()["services"].values():
        if svc.get("profiles"):
            continue
        limit = str(svc["mem_limit"]).lower()
        total_mb += int(limit[:-1]) * (1024 if limit.endswith("g") else 1)
    assert total_mb <= 10 * 1024, f"{total_mb} MB of limits"


def test_topics_are_consistent_across_code_sql_and_compose():
    compose_text = (ROOT / "docker-compose.yml").read_text()
    clean_sql = (ROOT / "flink/sql/10_job_clean.sql").read_text()
    agg_sql = (ROOT / "flink/sql/20_job_agg_1m.sql").read_text()
    for topic in (RAW_TOPIC, DLQ_TOPIC, "telemetry.clean.v1"):
        assert topic in compose_text, topic
    assert RAW_TOPIC in clean_sql
    assert "telemetry.clean.v1" in clean_sql and "telemetry.clean.v1" in agg_sql


def test_watermark_delay_matches_generator_constant():
    agg_sql = (ROOT / "flink/sql/20_job_agg_1m.sql").read_text()
    m = re.search(r"INTERVAL '(\d+)' SECOND", agg_sql)
    assert m and int(m.group(1)) == generator.WATERMARK_DELAY_S


def test_window_size_matches_generator_constant():
    agg_sql = (ROOT / "flink/sql/20_job_agg_1m.sql").read_text()
    assert "INTERVAL '1' MINUTE" in agg_sql and generator.WINDOW_S == 60


def test_dev_credentials_match_env_example():
    env = _env_example()
    agg_sql = (ROOT / "flink/sql/20_job_agg_1m.sql").read_text()
    assert f"'username' = '{env['POSTGRES_USER']}'" in agg_sql
    assert f"'password' = '{env['POSTGRES_PASSWORD']}'" in agg_sql
    assert f"jdbc:postgresql://postgres:5432/{env['POSTGRES_DB']}" in agg_sql
    client = yaml.safe_load((ROOT / "flink/client-conf/config.yaml").read_text())
    assert client["s3.access-key"] == env["MINIO_ROOT_USER"]
    assert client["s3.secret-key"] == env["MINIO_ROOT_PASSWORD"]


def test_kafka_transaction_timeout_is_below_broker_max():
    sql = (ROOT / "flink/sql/10_job_clean.sql").read_text()
    m = re.search(r"'properties.transaction.timeout.ms' = '(\d+)'", sql)
    assert m and int(m.group(1)) <= 15 * 60 * 1000


def test_postgres_schema_matches_flink_sink_columns():
    ddl = (ROOT / "db/init/001_schema.sql").read_text()
    sql = (ROOT / "flink/sql/20_job_agg_1m.sql").read_text()
    sink_block = sql.split("CREATE TABLE agg_1m (")[1].split(") WITH")[0]
    flink_cols = re.findall(r"^\s+(\w+)\s+(?:TIMESTAMP|STRING|BIGINT|DOUBLE)", sink_block, re.M)
    for col in flink_cols:
        assert re.search(rf"^\s+{col}\s", ddl, re.M), f"{col} missing in db/init/001_schema.sql"


def test_dashboard_is_valid_json_and_uses_provisioned_datasource():
    import json

    dash = json.loads((ROOT / "grafana/dashboards/streamlab-overview.json").read_text())
    ds = yaml.safe_load((ROOT / "grafana/provisioning/datasources/postgres.yaml").read_text())
    uid = ds["datasources"][0]["uid"]
    assert all(p["datasource"]["uid"] == uid for p in dash["panels"])


def test_kafka_uses_the_kraft_4x_quorum_key():
    """Kafka 4.0 deprecated controller.quorum.voters; a dynamic quorum must not set it."""
    env = _compose()["services"]["kafka"]["environment"]
    assert "KAFKA_CONTROLLER_QUORUM_VOTERS" not in env
    assert env["KAFKA_CONTROLLER_QUORUM_BOOTSTRAP_SERVERS"] == "kafka:9093"
    assert env["KAFKA_PROCESS_ROLES"] == "broker,controller"


def test_flink_properties_use_keys_that_still_exist_in_flink_2x():
    """Flink 2.0 removed state.backend.type and flink-conf.yaml; only config.yaml is read."""
    props = _compose()["services"]["flink-jobmanager"]["environment"]["FLINK_PROPERTIES"]
    keys = {line.split(":")[0].strip() for line in props.strip().splitlines() if ":" in line}
    assert "state.backend.type" not in keys
    assert "state.backend" in keys


def test_flink_and_connector_minor_versions_agree():
    """Connector artifacts are published per Flink minor (e.g. 5.0.0-2.2 for Flink 2.2)."""

    def default(value: str) -> str:
        # Build args are written as ${VAR:-1.2.3}; tests only guard the fallback value.
        match = re.fullmatch(r"\$\{[^:}]+:-([^}]+)\}", value.strip())
        return match.group(1) if match else value.strip()

    args = _compose()["x-flink-build"]["args"]
    flink_version = default(args["FLINK_VERSION"])
    short_minor = ".".join(flink_version.split(".")[:2])
    for key in ("FLINK_KAFKA_CONNECTOR_VERSION", "FLINK_JDBC_CONNECTOR_VERSION"):
        assert default(args[key]).endswith(f"-{short_minor}"), (key, args[key], short_minor)


def test_jdbc_connector_4x_ships_core_and_dialect_jars():
    """Since 4.x there is no single shaded JDBC jar; the Dockerfile must fetch both."""
    dockerfile = (ROOT / "flink/Dockerfile").read_text()
    assert "flink-connector-jdbc-core" in dockerfile
    assert "flink-connector-jdbc-postgres" in dockerfile
