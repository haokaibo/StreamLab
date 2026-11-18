"""Command line entry point: ``streamlab <command>``."""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import UTC, datetime, timedelta

from streamlab import schemas
from streamlab.generator import GeneratorConfig, generate
from streamlab.truth import read_truth, write_truth

log = logging.getLogger("streamlab")

DEFAULT_DSN = "postgresql://streamlab:streamlab_dev@127.0.0.1:55432/streamlab"


def _cmd_check_schemas(_: argparse.Namespace) -> int:
    for name in (schemas.TELEMETRY_V1, schemas.TELEMETRY_V2_CANDIDATE):
        schemas.load_schema(name)
        print(f"ok  {name}")
    return 0


def _cmd_generate(args: argparse.Namespace) -> int:
    from streamlab.kafka_io import TelemetryProducer

    total = args.duration + args.settle
    realtime = args.mode == "realtime"
    # burst: pretend the data is historical so event times land in the past
    start = datetime.now(UTC) - (timedelta(seconds=total) if not realtime else timedelta(0))
    cfg = GeneratorConfig(
        seed=args.seed,
        start=start.replace(microsecond=0),
        duration_s=args.duration,
        settle_s=args.settle,
        interval_s=args.interval,
        duplicate_ratio=args.dup,
        out_of_order_ratio=args.ooo,
        late_ratio=args.late,
        bad_ratio=args.bad,
    )
    run = generate(cfg)
    if args.truth:
        write_truth(args.truth, run.truth)
        log.info("wrote ground truth to %s", args.truth)

    producer = TelemetryProducer(args.bootstrap, args.registry)
    t0 = time.monotonic()
    for em in run.emissions:
        if realtime:
            wait = em.emit_offset_s - (time.monotonic() - t0)
            if wait > 0:
                time.sleep(wait)
        producer.send(em.record)
    producer.flush()
    log.info(
        "done: sent=%d dead_lettered=%d delivery_errors=%d",
        producer.sent,
        producer.dead_lettered,
        producer.delivery_errors,
    )
    return 1 if producer.delivery_errors else 0


def _cmd_replay(args: argparse.Namespace) -> int:
    from streamlab.kafka_io import TelemetryProducer
    from streamlab.replay import read_jsonl

    producer = TelemetryProducer(args.bootstrap, args.registry)
    for record in read_jsonl(args.file, source=args.source):
        producer.send(record)
    producer.flush()
    log.info("replayed: sent=%d dead_lettered=%d", producer.sent, producer.dead_lettered)
    return 1 if producer.delivery_errors else 0


def _cmd_verify(args: argparse.Namespace) -> int:
    from streamlab.verify import compare, fetch_rows

    truth = read_truth(args.truth)
    problems = compare(truth, fetch_rows(args.dsn))
    if problems:
        for p in problems[:50]:
            print("FAIL", p)
        print(f"{len(problems)} mismatch(es)")
        return 1
    print("OK: database aggregates match ground truth for all closed windows")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="streamlab")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("check-schemas", help="parse all Avro schemas offline").set_defaults(
        func=_cmd_check_schemas
    )

    def add_kafka_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("--bootstrap", default="localhost:9092")
        p.add_argument("--registry", default="http://localhost:8081")

    gen = sub.add_parser("generate", help="send synthetic telemetry (with duplicates, lateness)")
    add_kafka_args(gen)
    gen.add_argument("--seed", type=int, default=42)
    gen.add_argument("--duration", type=int, default=300, help="seconds of disturbed data")
    gen.add_argument("--settle", type=int, default=90, help="seconds of clean tail")
    gen.add_argument("--interval", type=float, default=1.0, help="seconds between samples")
    gen.add_argument("--dup", type=float, default=0.02)
    gen.add_argument("--ooo", type=float, default=0.10)
    gen.add_argument("--late", type=float, default=0.01)
    gen.add_argument("--bad", type=float, default=0.005)
    gen.add_argument("--mode", choices=["burst", "realtime"], default="burst")
    gen.add_argument("--truth", help="write ground truth JSONL here (used by `verify`)")
    gen.set_defaults(func=_cmd_generate)

    rep = sub.add_parser("replay", help="replay recorded measurements from a JSONL file")
    add_kafka_args(rep)
    rep.add_argument("--file", required=True)
    rep.add_argument("--source", default="hw_monitor")
    rep.set_defaults(func=_cmd_replay)

    ver = sub.add_parser("verify", help="compare agg_1m in PostgreSQL with the ground truth")
    ver.add_argument("--truth", required=True)
    ver.add_argument("--dsn", default=DEFAULT_DSN)
    ver.set_defaults(func=_cmd_verify)
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
