#!/usr/bin/env python3
"""Build Lv80-100 crafting profitability snapshots for TW worlds."""
from __future__ import annotations

import json
import argparse
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
MIN_RECIPE_LEVEL = 1
WORLD_NAMES = ["伊弗利特", "迦樓羅", "利維坦", "鳳凰", "奧汀", "巴哈姆特", "拉姆", "泰坦"]
UA = "retainer-radar-tw/1.1 (GitHub Pages crafting analysis)"


def fetch_json(url: str, attempts: int = 6):
    for attempt in range(attempts):
        try:
            request = Request(url, headers={"Accept": "application/json", "User-Agent": UA})
            with urlopen(request, timeout=120) as response:
                return json.load(response)
        except Exception:
            if attempt + 1 == attempts:
                raise
            time.sleep(min(30, 2 ** attempt))


def chunks(values, size):
    for index in range(0, len(values), size):
        yield values[index:index + size]


def unpack(payload):
    if "items" in payload:
        return {int(key): value for key, value in payload["items"].items()}
    return {int(payload["itemID"]): payload} if payload.get("itemID") else {}


def fetch_current(world_id, ids):
    joined = ",".join(map(str, ids))
    return unpack(fetch_json(f"{API}/{world_id}/{joined}?entries=0"))


def fetch_history(world_id, ids):
    joined = ",".join(map(str, ids))
    query = urlencode({"entriesWithin": WINDOW_SECONDS, "entriesToReturn": 99999, "minSalePrice": 1})
    return unpack(fetch_json(f"{API}/history/{world_id}/{joined}?{query}"))


def parallel_batches(groups, function, workers=5):
    merged = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(function, group) for group in groups]
        for future in as_completed(futures):
            merged.update(future.result())
    return merged


def product_metrics(item, current, history, now):
    since = now - WINDOW_SECONDS
    output = []
    for quality, is_hq in (("NQ", False), ("HQ", True)):
        entries = [entry for entry in history.get("entries", [])
                   if bool(entry.get("hq", False)) == is_hq and since <= int(entry.get("timestamp", 0)) <= now]
        listings = [listing for listing in current.get("listings", []) if bool(listing.get("hq", False)) == is_hq]
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
        output.append({"quality": quality, "median": median, "lowest": lowest, "units": units,
                       "daily": round(daily, 4), "saleDays": sale_days, "saleRatio": round(sale_ratio, 6),
                       "cv": round(cv, 6), "stock": stock, "stockDays": round(stock / daily, 4) if daily else None})
    return output


def build_world(world_id, world_name, recipes, marketable):
    now = int(time.time())
    valid = [recipe for recipe in recipes if recipe["result"] in marketable and
             all(item["id"] in marketable for item in recipe["ingredients"])]
    result_ids = sorted({recipe["result"] for recipe in valid})
    ingredient_ids = sorted({item["id"] for recipe in valid for item in recipe["ingredients"]})
    all_current_ids = sorted(set(result_ids) | set(ingredient_ids))
    current = parallel_batches(list(chunks(all_current_ids, 90)),
                               lambda group: fetch_current(world_id, group), workers=5)
    history = parallel_batches(list(chunks(result_ids, 45)),
                               lambda group: fetch_history(world_id, group), workers=5)

    ingredient_prices = {}
    for item_id in ingredient_ids:
        listings = [x for x in current.get(item_id, {}).get("listings", []) if not x.get("hq", False)]
        ingredient_prices[item_id] = min((int(x["pricePerUnit"]) for x in listings
                                          if int(x.get("pricePerUnit", 0)) > 0), default=0)

    metric_cache = {item_id: product_metrics(item_id, current.get(item_id, {}), history.get(item_id, {}), now)
                    for item_id in result_ids}
    best = {}
    for recipe in valid:
        components = []
        unavailable = False
        material_cost = 0
        for ingredient in recipe["ingredients"]:
            unit_price = ingredient_prices.get(ingredient["id"], 0)
            if unit_price <= 0:
                unavailable = True
                break
            subtotal = unit_price * ingredient["amount"]
            material_cost += subtotal
            components.append({"id": ingredient["id"], "name": ingredient["name"], "amount": ingredient["amount"],
                               "unitPrice": unit_price, "subtotal": subtotal})
        if unavailable:
            continue
        for metric in metric_cache[recipe["result"]]:
            if metric["quality"] == "HQ" and not recipe["canHq"]:
                continue
            reference = min(metric["median"], metric["lowest"]) if metric["median"] and metric["lowest"] else (metric["median"] or metric["lowest"])
            if reference <= 0:
                continue
            revenue = reference * recipe["yields"] * .95
            profit = revenue - material_cost
            if profit <= 0:
                continue
            inventory_factor = 1 / (1 + metric["stockDays"] / 30) if metric["stockDays"] is not None else 0
            risk_profit = profit * metric["saleRatio"] * (1 / (1 + metric["cv"])) * inventory_factor
            row = {**{key: recipe[key] for key in ("recipeId", "job", "level", "result", "name", "yields", "craftsmanshipReq", "craftsmanshipHardReq", "craftsmanshipSuggested", "controlReq", "masterbook")},
                   **metric, "referencePrice": reference, "materialCost": material_cost,
                   "netRevenue": round(revenue, 2), "netProfit": round(profit, 2),
                   "profitMargin": round(profit / revenue, 6), "riskProfit": round(risk_profit, 2),
                   "stableEligible": metric["units"] >= 20 and metric["saleRatio"] >= .30,
                   "grossEligible": metric["units"] >= 5 and metric["saleRatio"] >= .10,
                   "ingredients": sorted(components, key=lambda x: x["subtotal"], reverse=True)}
            key = (recipe["job"], recipe["result"], metric["quality"])
            if key not in best or row["netProfit"] > best[key]["netProfit"]:
                best[key] = row

    rows = list(best.values())
    payload = {"world": world_id, "worldName": world_name, "generatedAt": datetime.now(timezone.utc).isoformat(),
               "windowDays": WINDOW_DAYS, "minRecipeLevel": MIN_RECIPE_LEVEL,
               "dataAvailable": bool(current), "costBasis": "NQ current lowest", "marketFeeRate": .05, "rows": rows}
    output = ROOT / "public" / "data" / f"craft-world-{world_id}.json"
    output.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"  {world_name}: {len(result_ids)} products, {len(ingredient_ids)} ingredients, {len(rows)} profitable rows", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--world", choices=WORLD_NAMES, help="Analyze one world for local validation")
    args = parser.parse_args()
    recipes = [row for row in json.loads((ROOT / "scripts" / "recipe_catalog.json").read_text(encoding="utf-8"))
               if row["level"] >= MIN_RECIPE_LEVEL]
    marketable = set(map(int, fetch_json(f"{API}/marketable")))
    worlds = fetch_json(f"{API}/worlds")
    world_map = {world["name"]: int(world["id"]) for world in worlds if world.get("name") in WORLD_NAMES}
    missing = [name for name in WORLD_NAMES if name not in world_map]
    if missing:
        raise RuntimeError(f"Universalis world lookup failed: {missing}")
    print(f"Analyzing {len(recipes)} Lv{MIN_RECIPE_LEVEL}-100 recipes")
    for name in ([args.world] if args.world else WORLD_NAMES):
        build_world(world_map[name], name, recipes, marketable)


if __name__ == "__main__":
    main()
