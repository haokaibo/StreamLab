from datetime import UTC, datetime, timedelta

from streamlab.validate import validate_sample

GOOD = {
    "event_id": "id-1",
    "event_time": datetime(2026, 1, 1, tzinfo=UTC),
    "host": "h",
    "source": "s",
    "metric": "m",
    "value": 1.0,
}


def test_good_record():
    assert validate_sample(GOOD) == []


def test_nan_and_inf_rejected():
    assert validate_sample({**GOOD, "value": float("nan")})
    assert validate_sample({**GOOD, "value": float("inf")})


def test_empty_host_rejected():
    assert validate_sample({**GOOD, "host": ""})


def test_naive_datetime_rejected():
    assert validate_sample({**GOOD, "event_time": datetime(2026, 1, 1)})


def test_future_event_rejected_only_when_now_given():
    future = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(hours=1)
    rec = {**GOOD, "event_time": future}
    assert validate_sample(rec) == []
    assert validate_sample(rec, now=datetime(2026, 1, 1, tzinfo=UTC))
