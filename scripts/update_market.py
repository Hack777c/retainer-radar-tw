#!/usr/bin/env python3
"""Build static 30-day retainer market snapshots for every TW world."""
from __future__ import annotations

import json
import argparse
import math
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
API = "https://universalis.app/api/v2"
WINDOW_DAYS = 30
WINDOW_SECONDS = WINDOW_DAYS * 86400
WORLD_NAMES = ["伊弗利特", "迦樓羅", "利維坦", "鳳凰", "奧汀", "巴哈姆特", "拉姆", "泰坦"]
UA = "retainer-radar-tw/1.0 (GitHub Pages market analysis)"


def fetch_json(url: str, attempts: int = 5):
    for attempt in range(attempts):
        try:
            request = Request(url, headers={"Accept": "application/json", "User-Agent": UA})
            with urlopen(request, timeout=120) as response:
                return json.load(response)
        except Exception:
            if attempt + 1 == attempts:
                raise
            time.sleep(2 ** attempt)


def chunks(values, size):
    for index in range(0, len(values), size):
        yield values[index:index + size]


def unpack(payload):
    if "items" in payload:
        return {int(key): value for key, value in payload["items"].items()}
    if payload.get("itemID"):
        return {int(payload["itemID"]): payload}
    return {}


def fetch_batch(world_id: int, item_ids: list[int]):
    joined = ",".join(map(str, item_ids))
    history_query = urlencode({"entriesWithin": WINDOW_SECONDS, "entriesToReturn": 99999, "minSalePrice": 1})
    for attempt in range(4):
        try:
            history = fetch_json(f"{API}/history/{world_id}/{joined}?{history_query}")
            current = fetch_json(f"{API}/{world_id}/{joined}?entries=0")
            return unpack(history), unpack(current)
        except Exception:
            if attempt == 3:
                raise
            time.sleep(3 * (attempt + 1))


def build_world(world_id: int, world_name: str, catalog: list[dict]):
    now = int(time.time())
    since = now - WINDOW_SECONDS
    histories, currents = {}, {}
    # Smaller item groups keep high-volume history responses below proxy limits.
    batches = list(chunks(sorted({row["id"] for row in catalog}), 40))
    # Stay comfortably below Universalis' burst and concurrent-connection limits.
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(fetch_batch, world_id, batch) for batch in batches]
        for number, future in enumerate(as_completed(futures), 1):
            history, current = future.result()
            histories.update(history)
            currents.update(current)
            print(f"  {world_name}: batch {number}/{len(batches)}", flush=True)

    rows = []
    for task in catalog:
        entries = [entry for entry in histories.get(task["id"], {}).get("entries", [])
                   if not entry.get("hq", False) and since <= int(entry.get("timestamp", 0)) <= now]
        listings = [listing for listing in currents.get(task["id"], {}).get("listings", [])
                    if not listing.get("hq", False)]
        prices = sorted(int(entry["pricePerUnit"]) for entry in entries if int(entry.get("pricePerUnit", 0)) > 0)
        units = sum(int(entry.get("quantity", 0)) for entry in entries)
        sale_days = len({datetime.fromtimestamp(int(entry["timestamp"]), timezone.utc).date() for entry in entries})
        median = statistics.median(prices) if prices else 0
        mean = statistics.fmean(prices) if prices else 0
        cv = statistics.pstdev(prices) / mean if len(prices) > 1 and mean else 0
        lowest = min((int(x["pricePerUnit"]) for x in listings if int(x.get("pricePerUnit", 0)) > 0), default=0)
        stock = sum(int(x.get("quantity", 0)) for x in listings)
        daily = units / WINDOW_DAYS
        sale_ratio = min(1, sale_days / WINDOW_DAYS)
        if units < 20 or sale_ratio < .30 or not (median or lowest):
            continue
        rows.append({**task, "median": median, "daily": round(daily, 4), "units": units,
                     "saleDays": sale_days, "saleRatio": round(sale_ratio, 6), "cv": round(cv, 6),
                     "lowest": lowest, "stock": stock, "stockDays": round(stock / daily, 4) if daily else None})

    payload = {"world": world_id, "worldName": world_name, "generatedAt": datetime.now(timezone.utc).isoformat(),
               "windowDays": WINDOW_DAYS, "quality": "NQ", "dataAvailable": bool(rows), "rows": rows}
    output = ROOT / "public" / "data" / f"world-{world_id}.json"
    output.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"  {world_name}: wrote {len(rows)} qualified rows", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--world", choices=WORLD_NAMES, help="Analyze one world for local validation")
    args = parser.parse_args()
    catalog = json.loads((ROOT / "scripts" / "catalog.json").read_text(encoding="utf-8"))
    worlds = fetch_json(f"{API}/worlds")
    world_map = {world["name"]: int(world["id"]) for world in worlds if world.get("name") in WORLD_NAMES}
    missing = [name for name in WORLD_NAMES if name not in world_map]
    if missing:
        raise RuntimeError(f"Universalis world lookup failed: {missing}")
    (ROOT / "public" / "data").mkdir(parents=True, exist_ok=True)
    (ROOT / "public" / "data" / "worlds.json").write_text(
        json.dumps([{"id": world_map[name], "name": name} for name in WORLD_NAMES], ensure_ascii=False), encoding="utf-8")
    print("Universalis TW worlds: " + ", ".join(f"{name}={world_map[name]}" for name in WORLD_NAMES))
    for name in ([args.world] if args.world else WORLD_NAMES):
        build_world(world_map[name], name, catalog)


if __name__ == "__main__":
    main()
