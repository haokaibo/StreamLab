"""Compare the PostgreSQL aggregates with what the generator says they should be."""

from __future__ import annotations

from typing import Any

from streamlab.generator import expected_aggregates


def compare(
    truth: list[dict[str, Any]],
    rows: list[tuple[Any, ...]],
    *,
    avg_tolerance: float = 1e-6,
) -> list[str]:
    """``rows``: (window_start, host, metric, sample_count, avg_value) from agg_1m.

    Returns a list of mismatch descriptions; empty means the pipeline agrees with the truth.
    """
    expected = expected_aggregates(truth)
    actual = {(r[0], r[1], r[2]): (int(r[3]), float(r[4])) for r in rows}
    problems: list[str] = []

    for key, exp in sorted(expected.items()):
        got = actual.get(key)
        if got is None:
            problems.append(f"missing window {key}: expected count={int(exp['count'])}")
            continue
        if got[0] != int(exp["count"]):
            problems.append(f"count mismatch {key}: expected {int(exp['count'])}, got {got[0]}")
        if abs(got[1] - exp["avg"]) > avg_tolerance:
            problems.append(f"avg mismatch {key}: expected {exp['avg']:.6f}, got {got[1]:.6f}")
    return problems


def fetch_rows(dsn: str) -> list[tuple[Any, ...]]:
    import psycopg

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT window_start, host, metric, sample_count, avg_value FROM agg_1m")
        return list(cur.fetchall())
