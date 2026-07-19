"""Collect cached Upbit histories under /tmp and run the attention replay."""

from __future__ import annotations

import argparse
import asyncio
import datetime
import gzip
import json
import os
import tempfile
from pathlib import Path
from typing import Dict, Iterable, Mapping, Sequence

import aiohttp

import config
import common.upbit_client as upbit_client
from common.models import CandleData
from common.replay import (
    ReplayReport,
    replay_10m_bar_count,
    replay_daily_bar_count,
    run_point_in_time_replay,
)
from common.sector_loader import load_and_process_sectors
from common.storage_client import StateError
from common.upbit_client import CandleTimeUnit, get_all_krw_tickers, get_candles


CACHE_SCHEMA_VERSION = 1
DEFAULT_CACHE_DIR = Path("/tmp/crypto-rank-tracker-replay")


async def collect_dataset(
    evaluation_days: int, as_of: datetime.datetime
) -> tuple[Dict[str, list[CandleData]], Dict[str, list[CandleData]], dict]:
    """Fetch the current KRW universe plus replay and daily warm-up histories."""
    # A replay is a long bulk job and may share the IP with the scheduled scanner.
    upbit_client.GLOBAL_RATE_LIMIT_PER_SECOND = config.REPLAY_RATE_LIMIT_PER_SECOND
    async with aiohttp.ClientSession() as session:
        tickers = await get_all_krw_tickers(session)
        markets = [ticker.market for ticker in tickers]
        ten_minute_count = replay_10m_bar_count(evaluation_days)
        daily_count = replay_daily_bar_count(evaluation_days)
        ten_minute, daily = await asyncio.gather(
            get_candles(
                session,
                markets,
                CandleTimeUnit.MINUTES,
                count=ten_minute_count,
                minutes_unit=config.PRIMARY_EXECUTION_TIMEFRAME_MINUTES,
                as_of=as_of,
                synthesize_no_trade_intervals=True,
            ),
            get_candles(
                session,
                markets,
                CandleTimeUnit.DAYS,
                count=daily_count,
                as_of=as_of,
            ),
        )
        missing_ten_minute = sorted(set(markets) - set(ten_minute))
        if missing_ten_minute:
            await asyncio.sleep(5)
            recovered = await get_candles(
                session,
                missing_ten_minute,
                CandleTimeUnit.MINUTES,
                count=ten_minute_count,
                minutes_unit=config.PRIMARY_EXECUTION_TIMEFRAME_MINUTES,
                as_of=as_of,
                synthesize_no_trade_intervals=True,
            )
            ten_minute.update(recovered)

    coverage = len(ten_minute) / len(markets) if markets else 0.0
    if "KRW-BTC" not in ten_minute:
        raise RuntimeError("KRW-BTC replay history is unavailable")
    manifest = {
        "schema_version": CACHE_SCHEMA_VERSION,
        "as_of": as_of.isoformat(),
        "evaluation_days": evaluation_days,
        "requested_market_count": len(markets),
        "ten_minute_market_count": len(ten_minute),
        "daily_market_count": len(daily),
        "ten_minute_bar_count": ten_minute_count,
        "daily_bar_count": daily_count,
        "ten_minute_coverage_ratio": coverage,
        "coverage_below_minimum": coverage < config.CANDLE_SUCCESS_RATE_MINIMUM,
        "requested_markets": sorted(markets),
        "markets": sorted(ten_minute),
    }
    return ten_minute, daily, manifest


def save_dataset(
    cache_dir: Path,
    candles_10m: Mapping[str, Sequence[CandleData]],
    candles_daily: Mapping[str, Sequence[CandleData]],
    manifest: dict,
) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    stored_manifest = {
        **manifest,
        "markets": sorted(candles_10m),
        "daily_markets": sorted(candles_daily),
        "complete": False,
    }
    _atomic_write_json(cache_dir / "manifest.json", stored_manifest)
    _save_candle_directory(cache_dir / "candles-10m", candles_10m)
    _save_candle_directory(cache_dir / "candles-daily", candles_daily)
    _atomic_write_json(
        cache_dir / "manifest.json", {**stored_manifest, "complete": True}
    )


def load_dataset(
    cache_dir: Path,
    evaluation_days: int,
    as_of: datetime.datetime | None = None,
) -> tuple[Dict[str, list[CandleData]], Dict[str, list[CandleData]], dict] | None:
    manifest_path = cache_dir / "manifest.json"
    ten_minute_path = cache_dir / "candles-10m"
    daily_path = cache_dir / "candles-daily"
    if not all(path.exists() for path in [manifest_path, ten_minute_path, daily_path]):
        return None
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    cached_as_of = manifest.get("as_of")
    try:
        cached_as_of = (
            _parse_as_of(cached_as_of)
            if isinstance(cached_as_of, str)
            else None
        )
    except ValueError:
        return None
    if (
        manifest.get("schema_version") != CACHE_SCHEMA_VERSION
        or manifest.get("evaluation_days") != evaluation_days
        or manifest.get("complete", True) is not True
        or cached_as_of is None
        or (
            as_of is not None
            and cached_as_of != as_of.astimezone(datetime.timezone.utc)
        )
    ):
        return None
    return (
        _load_candle_directory(ten_minute_path, manifest.get("markets", [])),
        _load_candle_directory(daily_path, manifest.get("daily_markets", [])),
        manifest,
    )


async def run(args: argparse.Namespace) -> ReplayReport:
    cache_dir = _tmp_path(Path(args.cache_dir))
    requested_as_of = _parse_as_of(args.as_of) if args.as_of else None
    dataset = (
        None
        if args.refresh
        else load_dataset(cache_dir, args.evaluation_days, requested_as_of)
    )
    if dataset is None:
        as_of = requested_as_of or _parse_as_of(None)
        print(
            f"Collecting all-market Upbit candles as of {as_of.isoformat()} "
            f"into {cache_dir}",
            flush=True,
        )
        dataset = await collect_dataset(args.evaluation_days, as_of)
        save_dataset(cache_dir, *dataset)
    else:
        print(
            f"Using cached dataset from {dataset[2]['as_of']} in {cache_dir}",
            flush=True,
        )

    candles_10m, candles_daily, manifest = dataset
    if manifest.get("coverage_below_minimum"):
        print(
            "Warning: replay candle coverage is below the production scan minimum; "
            "treat ranking metrics as partial evidence.",
            flush=True,
        )
    try:
        sectors, reverse_sector_map = await load_and_process_sectors()
    except (StateError, config.StorageConfigError) as error:
        print(
            f"Sector context unavailable ({error.code.value}); replaying without sector tags.",
            flush=True,
        )
        sectors, reverse_sector_map = {}, {}

    def progress(current: int, total: int, observed_at: datetime.datetime) -> None:
        if current == 1 or current == total or current % 144 == 0:
            print(
                f"Replay {current}/{total}: {observed_at.isoformat()}",
                flush=True,
            )

    observations_path = cache_dir / "observations.ndjson"
    descriptor, temporary = tempfile.mkstemp(
        dir=cache_dir, prefix=".observations.", suffix=".ndjson.tmp", text=True
    )
    temporary_path = Path(temporary)
    try:
        with os.fdopen(descriptor, mode="w", encoding="utf-8") as handle:
            report = run_point_in_time_replay(
                candles_10m,
                candles_daily,
                sectors,
                reverse_sector_map,
                evaluation_days=args.evaluation_days,
                top_k=args.top_k,
                requested_market_count=manifest["requested_market_count"],
                progress=progress,
                observation_sink=lambda record: handle.write(
                    json.dumps(record, ensure_ascii=False, separators=(",", ":"))
                    + "\n"
                ),
            )
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, observations_path)
    finally:
        temporary_path.unlink(missing_ok=True)
    report_json = cache_dir / "report.json"
    report_markdown = cache_dir / "report.md"
    _atomic_write_json(report_json, report.model_dump(mode="json"))
    _atomic_write_text(report_markdown, report.to_markdown())
    print(
        f"Replay complete for {manifest['ten_minute_market_count']} markets: "
        f"{report_json}, {report_markdown}, and {observations_path}",
        flush=True,
    )
    return report


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Cache all-market Upbit candles under /tmp and replay the production "
            "attention features without look-ahead."
        )
    )
    parser.add_argument(
        "--evaluation-days",
        type=int,
        default=config.REPLAY_DEFAULT_EVALUATION_DAYS,
        help=(
            f"evaluation window ({config.REPLAY_MIN_EVALUATION_DAYS}-"
            f"{config.REPLAY_MAX_EVALUATION_DAYS} days; warm-up is added separately)"
        ),
    )
    parser.add_argument(
        "--top-k", type=int, default=config.REPLAY_DEFAULT_TOP_K
    )
    parser.add_argument("--cache-dir", default=str(DEFAULT_CACHE_DIR))
    parser.add_argument(
        "--as-of", help="timezone-aware ISO timestamp; defaults to current UTC"
    )
    parser.add_argument(
        "--refresh", action="store_true", help="replace a matching cached dataset"
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    asyncio.run(run(args))
    return 0


def _parse_as_of(value: str | None) -> datetime.datetime:
    if value is None:
        return datetime.datetime.now(datetime.timezone.utc)
    normalized = f"{value[:-1]}+00:00" if value.endswith("Z") else value
    parsed = datetime.datetime.fromisoformat(normalized)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("--as-of must include a timezone")
    return parsed.astimezone(datetime.timezone.utc)


def _tmp_path(path: Path) -> Path:
    resolved = path.expanduser().resolve()
    tmp_root = Path("/tmp").resolve()
    if resolved != tmp_root and tmp_root not in resolved.parents:
        raise ValueError("replay cache and outputs must be stored under /tmp")
    return resolved


def _save_candle_directory(
    directory: Path,
    candles_by_market: Mapping[str, Sequence[CandleData]],
) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for market, candles in candles_by_market.items():
        _atomic_write_gzip_json(
            directory / f"{market}.json.gz",
            [candle.model_dump(mode="json") for candle in candles],
        )


def _load_candle_directory(
    directory: Path, markets: Sequence[str]
) -> Dict[str, list[CandleData]]:
    result = {}
    for market in markets:
        payload = _read_gzip_json(directory / f"{market}.json.gz")
        if not isinstance(payload, list):
            raise ValueError(f"cached candle payload for {market} must be a list")
        candles = [CandleData.model_validate(candle) for candle in payload]
        if any(candle.market != market for candle in candles):
            raise ValueError(f"cached candle market mismatch for {market}")
        result[market] = candles
    return result


def _read_gzip_json(path: Path):
    with gzip.open(path, mode="rt", encoding="utf-8") as handle:
        return json.load(handle)


def _atomic_write_gzip_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
    )
    os.close(descriptor)
    temporary_path = Path(temporary)
    try:
        with gzip.open(temporary_path, mode="wt", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, separators=(",", ":"))
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _atomic_write_json(path: Path, value: dict) -> None:
    _atomic_write_text(
        path, json.dumps(value, ensure_ascii=False, indent=2) + "\n"
    )


def _atomic_write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", text=True
    )
    os.close(descriptor)
    temporary_path = Path(temporary)
    try:
        temporary_path.write_text(value, encoding="utf-8")
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
