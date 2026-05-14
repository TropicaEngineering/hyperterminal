# coin_loader.py
import requests, json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
RUNTIME_DIR = BASE_DIR / "runtime"
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

CONFIG_DIR = BASE_DIR / "config"
FILTERED_COINS_PATH = CONFIG_DIR / "filtered_coins.json"
ASSET_MAP_PATH = RUNTIME_DIR / "asset_map.json"

MIN_24H_VOLUME = 10_000_000
MIN_OPEN_INTEREST = 1_000
INFO_URL = "https://api.hyperliquid.xyz/info"

def get_filtered_coins(min_vol: float = MIN_24H_VOLUME,
                       min_oi: float = MIN_OPEN_INTEREST) -> list[str]:
    """
    Returns symbols passing the public liquidity filter.
    Also writes asset metadata to runtime/asset_map.json.
    """
    resp = requests.post(INFO_URL, json={"type": "metaAndAssetCtxs"}, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    # sanity
    if not (isinstance(data, list) and len(data) >= 1 and isinstance(data[0], dict)):
        return []

    universe = data[0].get("universe", [])
    # universe[i] might look like:
    # {
    #   "name": "HYPE",
    #   "szDecimals": ...,
    #   "maxLeverage": 50,
    #   "onlyIsolated": false,
    #   "isDelisted": false
    # }

    # assetCtxs aligned by index
    asset_ctxs = None
    if len(data) > 1:
        if isinstance(data[1], dict) and "assetCtxs" in data[1]:
            asset_ctxs = data[1]["assetCtxs"]
        elif isinstance(data[1], list):
            asset_ctxs = data[1]
    if not isinstance(asset_ctxs, list):
        asset_ctxs = []

    n = min(len(universe), len(asset_ctxs))

    symbols_for_runner = []
    asset_map = {}

    for i in range(n):
        u = universe[i]
        ctx = asset_ctxs[i]

        sym = (u.get("name") or "").upper()
        if not sym:
            continue

        is_delisted = bool(u.get("isDelisted", False))

        # grab liquidity stats for filtering
        try:
            oi = float(ctx.get("openInterest", 0) or 0)
        except Exception:
            oi = 0.0
        try:
            vol_ntl = float(ctx.get("dayNtlVlm", 0) or 0)
        except Exception:
            vol_ntl = 0.0

        # does this coin pass our liquidity gates?
        if (not is_delisted) and (vol_ntl >= min_vol) and (oi >= min_oi):
            symbols_for_runner.append(sym)

        # keep public asset metadata for downstream scripts
        asset_map[sym] = {
            "asset": i,
            "maxLeverage": u.get("maxLeverage"),
            "onlyIsolated": u.get("onlyIsolated", False),
            "isDelisted": is_delisted,
            "openInterest": oi,
            "dayNtlVlm": vol_ntl,
        }

    # dedupe + sort runner list (just cosmetic)
    symbols_for_runner = sorted(set(symbols_for_runner))

    # Do not write config files at runtime.

    # write our execution map
    try:
        with open(ASSET_MAP_PATH, "w") as f:
            json.dump(asset_map, f, indent=2)
    except Exception:
        pass

    return symbols_for_runner


if __name__ == "__main__":
    syms = get_filtered_coins()
    print("symbols_for_runner:", syms[:10])
    with open(ASSET_MAP_PATH) as f:
        amap = json.load(f)
        # show first 3 keys just to sanity check
        first_keys = list(amap.keys())[:3]
        print("asset_map sample:", {k: amap[k] for k in first_keys})