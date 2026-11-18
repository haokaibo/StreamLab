from collections import Counter

from streamlab import schemas
from streamlab.generator import expected_aggregates, generate
from streamlab.validate import validate_sample


def test_deterministic_for_same_seed(cfg):
    a = generate(cfg)
    b = generate(cfg)
    ids_a = [e.record["event_id"] for e in a.emissions]
    ids_b = [e.record["event_id"] for e in b.emissions]
    assert ids_a == ids_b


def test_different_seed_differs(cfg):
    from dataclasses import replace

    a = generate(cfg)
    b = generate(replace(cfg, seed=cfg.seed + 1))
    assert a.emissions[0].record["event_id"] != b.emissions[0].record["event_id"]


def test_emissions_sorted_by_arrival(cfg):
    offsets = [e.emit_offset_s for e in generate(cfg).emissions]
    assert offsets == sorted(offsets)


def test_all_mess_kinds_present(cfg):
    kinds = Counter(e.kind for e in generate(cfg).emissions)
    for kind in ("normal", "out_of_order", "late", "duplicate", "bad"):
        assert kinds[kind] > 0, kind


def test_duplicates_share_event_id_with_an_original(cfg):
    run = generate(cfg)
    originals = {e.record["event_id"] for e in run.emissions if e.kind != "duplicate"}
    dups = [e for e in run.emissions if e.kind == "duplicate"]
    assert dups
    assert all(d.record["event_id"] in originals for d in dups)


def test_truth_has_one_row_per_unique_event(cfg):
    run = generate(cfg)
    ids = [t["event_id"] for t in run.truth]
    assert len(ids) == len(set(ids))


def test_bad_records_fail_validation_and_good_ones_pass(cfg):
    run = generate(cfg)
    for e in run.emissions:
        problems = validate_sample(e.record)
        if e.kind == "bad":
            assert problems
        else:
            assert not problems


def test_non_bad_records_match_avro_schema(cfg):
    run = generate(cfg)
    for e in run.emissions[:500]:
        if e.kind != "bad":
            schemas.validate_record(e.record)


def test_late_events_arrive_after_watermark_and_window(cfg):
    run = generate(cfg)
    late = [e for e in run.emissions if e.kind == "late"]
    for e in late:
        lateness = e.emit_offset_s - (e.record["event_time"] - cfg.start).total_seconds()
        assert lateness >= cfg.late_min_s


def test_out_of_order_events_stay_within_watermark_delay(cfg):
    run = generate(cfg)
    for e in run.emissions:
        if e.kind == "out_of_order":
            lateness = e.emit_offset_s - (e.record["event_time"] - cfg.start).total_seconds()
            assert lateness < 30


def test_counted_flag(cfg):
    run = generate(cfg)
    by_kind = {t["kind"]: t["counted"] for t in run.truth}
    assert by_kind["normal"] and by_kind["out_of_order"]
    assert not by_kind["late"] and not by_kind["bad"]


def test_expected_aggregates_bruteforce(cfg):
    run = generate(cfg)
    agg = expected_aggregates(run.truth)
    assert agg
    total_expected = sum(v["count"] for v in agg.values())
    counted_total = sum(1 for t in run.truth if t["counted"])
    # closed windows are a subset of all counted events, and not trivially small
    assert 0 < total_expected < counted_total
    assert total_expected > 0.7 * counted_total
    for (window_start, _host, _metric), v in agg.items():
        assert window_start.second == 0
        assert v["min"] <= v["avg"] <= v["max"]
