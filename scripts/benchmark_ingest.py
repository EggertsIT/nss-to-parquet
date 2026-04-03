#!/usr/bin/env python3
"""
TCP ingest benchmark for nss-ingestor.

It sends NSS-like lines to the listener and computes measured throughput from
/api/stats deltas, including an estimated safe sustained logs/min figure.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class Snapshot:
    ingested: int
    parsed_ok: int
    parsed_error: int
    written_rows: int
    dlq_rows: int
    status: str


@dataclass
class WorkerResult:
    sent: int = 0
    connect_failures: int = 0
    send_failures: int = 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Benchmark nss-ingestor TCP ingest throughput")
    p.add_argument("--host", default="127.0.0.1", help="NSS ingest host")
    p.add_argument("--port", type=int, default=514, help="NSS ingest TCP port")
    p.add_argument(
        "--line-file",
        default="sample.log",
        help="File containing sample NSS log lines (first non-empty line is used)",
    )
    p.add_argument(
        "--stats-url",
        default="http://127.0.0.1:9090/api/stats",
        help="Stats API URL",
    )
    p.add_argument("--duration-secs", type=int, default=60, help="Load phase duration")
    p.add_argument(
        "--drain-secs",
        type=int,
        default=10,
        help="Extra wait after sending so writer can flush",
    )
    p.add_argument("--connections", type=int, default=8, help="Parallel TCP connections")
    p.add_argument(
        "--target-lpm",
        type=float,
        default=0.0,
        help="Target total send rate in logs/min (0 = unthrottled)",
    )
    p.add_argument(
        "--timeout-secs",
        type=float,
        default=5.0,
        help="HTTP timeout for stats fetch",
    )
    p.add_argument(
        "--max-loss-ratio",
        type=float,
        default=0.02,
        help="Fail if (sent - ingested) / sent exceeds this threshold",
    )
    p.add_argument(
        "--max-parse-error-ratio",
        type=float,
        default=0.001,
        help="Fail if parse_error / ingested exceeds this threshold",
    )
    return p.parse_args()


def load_line(line_file: str) -> str:
    path = Path(line_file)
    if not path.exists():
        raise FileNotFoundError(f"line file not found: {path}")
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line:
            return line
    raise ValueError(f"line file has no non-empty lines: {path}")


def fetch_snapshot(url: str, timeout_secs: float) -> Snapshot:
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout_secs) as resp:
            payload = resp.read().decode("utf-8")
    except urllib.error.URLError as exc:
        raise RuntimeError(f"failed to fetch stats from {url}: {exc}") from exc

    data = json.loads(payload)
    totals = data["totals"]
    return Snapshot(
        ingested=int(totals["ingested"]),
        parsed_ok=int(totals["parsed_ok"]),
        parsed_error=int(totals["parsed_error"]),
        written_rows=int(totals["written_rows"]),
        dlq_rows=int(totals["dlq_rows"]),
        status=str(data.get("status", "unknown")),
    )


async def worker(
    worker_id: int,
    host: str,
    port: int,
    payload: bytes,
    stop_at: float,
    target_lps_per_conn: float,
) -> WorkerResult:
    res = WorkerResult()
    writer: Optional[asyncio.StreamWriter] = None
    next_send = time.perf_counter()

    while time.perf_counter() < stop_at:
        if writer is None:
            try:
                _reader, writer = await asyncio.open_connection(host, port)
            except Exception:
                res.connect_failures += 1
                await asyncio.sleep(0.2)
                continue

        try:
            writer.write(payload)
            res.sent += 1
            if res.sent % 200 == 0:
                await writer.drain()
        except Exception:
            res.send_failures += 1
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            writer = None
            await asyncio.sleep(0.1)
            continue

        if target_lps_per_conn > 0:
            next_send += 1.0 / target_lps_per_conn
            sleep_for = next_send - time.perf_counter()
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
            else:
                next_send = time.perf_counter()

    if writer is not None:
        try:
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
    return res


def pct(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return (numerator / denominator) * 100.0


async def run() -> int:
    args = parse_args()
    if args.connections <= 0:
        print("connections must be > 0", file=sys.stderr)
        return 2
    if args.duration_secs <= 0:
        print("duration-secs must be > 0", file=sys.stderr)
        return 2

    line = load_line(args.line_file)
    payload = (line + "\n").encode("utf-8")

    start_snapshot = fetch_snapshot(args.stats_url, args.timeout_secs)
    t0 = time.perf_counter()
    stop_at = t0 + float(args.duration_secs)

    target_lps_per_conn = 0.0
    if args.target_lpm > 0:
        target_lps_per_conn = (args.target_lpm / 60.0) / float(args.connections)

    print(
        f"benchmark: host={args.host}:{args.port} connections={args.connections} "
        f"duration={args.duration_secs}s target_lpm={args.target_lpm:.0f}"
    )
    sys.stdout.flush()

    tasks = [
        asyncio.create_task(
            worker(
                worker_id=i,
                host=args.host,
                port=args.port,
                payload=payload,
                stop_at=stop_at,
                target_lps_per_conn=target_lps_per_conn,
            )
        )
        for i in range(args.connections)
    ]
    results = await asyncio.gather(*tasks)

    sent_total = sum(r.sent for r in results)
    connect_failures = sum(r.connect_failures for r in results)
    send_failures = sum(r.send_failures for r in results)

    if args.drain_secs > 0:
        await asyncio.sleep(float(args.drain_secs))

    end_snapshot = fetch_snapshot(args.stats_url, args.timeout_secs)
    t1 = time.perf_counter()
    elapsed = max(t1 - t0, 1e-6)

    d_ingested = end_snapshot.ingested - start_snapshot.ingested
    d_parsed_ok = end_snapshot.parsed_ok - start_snapshot.parsed_ok
    d_parsed_error = end_snapshot.parsed_error - start_snapshot.parsed_error
    d_written_rows = end_snapshot.written_rows - start_snapshot.written_rows
    d_dlq = end_snapshot.dlq_rows - start_snapshot.dlq_rows

    sent_lpm = sent_total * 60.0 / elapsed
    ingested_lpm = d_ingested * 60.0 / elapsed
    written_lpm = d_written_rows * 60.0 / elapsed
    parsed_ok_lpm = d_parsed_ok * 60.0 / elapsed
    safe_lpm = min(ingested_lpm, written_lpm)

    estimated_loss = max(sent_total - d_ingested, 0)
    loss_ratio = (estimated_loss / sent_total) if sent_total > 0 else 0.0
    parse_error_ratio = (d_parsed_error / d_ingested) if d_ingested > 0 else 0.0

    print()
    print(f"elapsed_seconds: {elapsed:.2f}")
    print(f"sent_total: {sent_total}")
    print(f"sent_rate_lpm: {sent_lpm:.0f}")
    print(f"ingested_delta: {d_ingested}")
    print(f"ingested_rate_lpm: {ingested_lpm:.0f}")
    print(f"written_rows_delta: {d_written_rows}")
    print(f"written_rate_lpm: {written_lpm:.0f}")
    print(f"parsed_ok_delta: {d_parsed_ok}")
    print(f"parsed_ok_rate_lpm: {parsed_ok_lpm:.0f}")
    print(f"parsed_error_delta: {d_parsed_error}")
    print(f"dlq_delta: {d_dlq}")
    print(f"estimated_loss_count: {estimated_loss}")
    print(f"estimated_loss_ratio_pct: {pct(estimated_loss, sent_total):.3f}")
    print(f"parse_error_ratio_pct: {pct(d_parsed_error, d_ingested):.3f}")
    print(f"connect_failures: {connect_failures}")
    print(f"send_failures: {send_failures}")
    print(f"final_service_status: {end_snapshot.status}")
    print()
    print(f"estimated_safe_sustained_logs_per_min: {safe_lpm:.0f}")

    failed = False
    if sent_total == 0:
        print("FAIL: no lines were sent", file=sys.stderr)
        failed = True
    if loss_ratio > args.max_loss_ratio:
        print(
            f"FAIL: estimated loss ratio {loss_ratio:.4f} exceeds threshold {args.max_loss_ratio:.4f}",
            file=sys.stderr,
        )
        failed = True
    if parse_error_ratio > args.max_parse_error_ratio:
        print(
            f"FAIL: parse error ratio {parse_error_ratio:.4f} exceeds threshold {args.max_parse_error_ratio:.4f}",
            file=sys.stderr,
        )
        failed = True

    return 1 if failed else 0


def main() -> None:
    try:
        code = asyncio.run(run())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
    raise SystemExit(code)


if __name__ == "__main__":
    main()
