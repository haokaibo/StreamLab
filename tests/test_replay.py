from pathlib import Path

from streamlab import schemas
from streamlab.replay import read_jsonl

SAMPLE = Path(__file__).resolve().parents[1] / "sample_data" / "hwmon_sample.jsonl"


def test_replay_produces_valid_records():
    records = list(read_jsonl(SAMPLE))
    assert len(records) == 10  # 3 readings x 3 lines + 1 long-form line
    for r in records:
        schemas.validate_record(r)


def test_replay_is_idempotent_ids():
    a = [r["event_id"] for r in read_jsonl(SAMPLE)]
    b = [r["event_id"] for r in read_jsonl(SAMPLE)]
    assert a == b
    assert len(set(a)) == len(a)


def test_units_inferred_from_suffix():
    by_metric = {r["metric"]: r["unit"] for r in read_jsonl(SAMPLE)}
    assert by_metric["gpu.junction_c"] == "C"
    assert by_metric["gpu.power_w"] == "W"
