from streamlab.generator import expected_aggregates, generate
from streamlab.verify import compare


def _rows_from_expected(expected):
    return [(k[0], k[1], k[2], int(v["count"]), v["avg"]) for k, v in expected.items()]


def test_compare_ok_when_rows_equal_expected(cfg):
    truth = generate(cfg).truth
    rows = _rows_from_expected(expected_aggregates(truth))
    assert compare(truth, rows) == []


def test_compare_detects_missing_wrong_count_and_wrong_avg(cfg):
    truth = generate(cfg).truth
    rows = _rows_from_expected(expected_aggregates(truth))
    missing = rows[1:]
    assert any("missing window" in p for p in compare(truth, missing))

    wrong_count = [(*rows[0][:3], rows[0][3] + 1, rows[0][4]), *rows[1:]]
    assert any("count mismatch" in p for p in compare(truth, wrong_count))

    wrong_avg = [(*rows[0][:4], rows[0][4] + 1.0), *rows[1:]]
    assert any("avg mismatch" in p for p in compare(truth, wrong_avg))
