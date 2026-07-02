from __future__ import annotations

import argparse
import asyncio
import statistics
import time
from typing import Any

import httpx


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((pct / 100.0) * (len(ordered) - 1)))))
    return ordered[idx]


async def _request(
    client: httpx.AsyncClient,
    *,
    url: str,
    token: str,
    stages: list[str],
    iterations: int,
    hold_ms: int,
    response_kb: int,
    index: int,
) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        response = await client.post(
            url,
            headers={"x-studio-load-test-token": token},
            json={
                "stages": stages,
                "iterations": iterations,
                "hold_ms": hold_ms,
                "response_kb": response_kb,
            },
        )
        elapsed = (time.perf_counter() - started) * 1000.0
        ok = 200 <= response.status_code < 300
        payload = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
        return {
            "index": index,
            "ok": ok,
            "status": response.status_code,
            "elapsed_ms": elapsed,
            "server_elapsed_ms": float(payload.get("elapsed_ms") or 0.0) if isinstance(payload, dict) else 0.0,
            "error": "" if ok else response.text[:300],
        }
    except Exception as exc:
        elapsed = (time.perf_counter() - started) * 1000.0
        return {
            "index": index,
            "ok": False,
            "status": 0,
            "elapsed_ms": elapsed,
            "server_elapsed_ms": 0.0,
            "error": str(exc)[:300],
        }


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    url = args.base_url.rstrip("/") + "/api/studio-agent/load-test/render-simulation"
    timeout = httpx.Timeout(args.timeout, connect=15.0)
    limits = httpx.Limits(max_connections=max(args.concurrency * 2, 20), max_keepalive_connections=args.concurrency)
    semaphore = asyncio.Semaphore(args.concurrency)
    results: list[dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
        async def one(i: int) -> None:
            async with semaphore:
                results.append(
                    await _request(
                        client,
                        url=url,
                        token=args.token,
                        stages=args.stages,
                        iterations=args.iterations,
                        hold_ms=args.hold_ms,
                        response_kb=args.response_kb,
                        index=i,
                    )
                )

        started = time.perf_counter()
        await asyncio.gather(*(one(i) for i in range(args.requests)))
        wall_ms = (time.perf_counter() - started) * 1000.0

    latencies = [float(row["elapsed_ms"]) for row in results]
    ok = [row for row in results if row["ok"]]
    failed = [row for row in results if not row["ok"]]
    return {
        "url": url,
        "requests": len(results),
        "concurrency": args.concurrency,
        "success": len(ok),
        "failed": len(failed),
        "success_rate_pct": round((len(ok) / max(len(results), 1)) * 100.0, 2),
        "wall_ms": round(wall_ms, 2),
        "rps": round((len(results) / max(wall_ms / 1000.0, 0.001)), 2),
        "latency_ms": {
            "min": round(min(latencies), 2) if latencies else 0.0,
            "mean": round(statistics.mean(latencies), 2) if latencies else 0.0,
            "p50": round(_percentile(latencies, 50), 2),
            "p95": round(_percentile(latencies, 95), 2),
            "p99": round(_percentile(latencies, 99), 2),
            "max": round(max(latencies), 2) if latencies else 0.0,
        },
        "statuses": {str(code): sum(1 for row in results if row["status"] == code) for code in sorted({row["status"] for row in results})},
        "first_errors": failed[:5],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Provider-free Studio render queue load test.")
    parser.add_argument("--base-url", default="https://studio.nyptid.com")
    parser.add_argument("--token", required=True)
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=25)
    parser.add_argument("--stages", nargs="+", default=["render", "stills", "i2v", "audio", "compose"])
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument("--hold-ms", type=int, default=250)
    parser.add_argument("--response-kb", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=120.0)
    args = parser.parse_args()
    report = asyncio.run(_run(args))
    import json

    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
