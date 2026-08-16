"""Chain metadata + ETA policy.

For each supported chain we know:
  - approx block time (seconds)
  - CG-canonical platform slug (already in coingecko.CG_TO_DS_CHAIN)
  - normalized aliases so different exchanges' labels ("ERC20" vs "ETH")
    map to a single canonical chain.

ETA_minutes for a transfer = max(min_confirmations_from_capital_feed) *
block_time / 60. If a chain isn't in the table we treat it as "unknown"
and reject with `chain_unknown`.
"""
BLOCK_SEC = {
    "ethereum": 12.0,
    "bsc": 3.0,
    "polygon": 2.0,
    "arbitrum": 0.25,
    "optimism": 2.0,
    "base": 2.0,
    "avalanche": 2.0,
    "fantom": 1.5,
    "solana": 0.4,
    "tron": 3.0,
    "sui": 3.0,
    "ton": 5.0,
    "aptos": 4.0,
    "linea": 12.0,
    "scroll": 3.0,
    "mantle": 2.0,
    "zksync": 1.0,
    "blast": 2.0,
    "ronin": 3.0,
    "cronos": 5.5,
    "celo": 5.0,
    "gnosis": 5.2,
    "pulsechain": 10.0,
    "berachain": 2.0,
    "bitcoin": 600.0,
}

# Free-form network labels (from exchange capital feeds) → canonical chain
_ALIASES = {
    # ethereum
    "eth": "ethereum", "erc20": "ethereum", "ethereum": "ethereum",
    "ethereum(erc20)": "ethereum", "eth (erc20)": "ethereum",
    # bsc
    "bsc": "bsc", "bep20": "bsc", "bnb": "bsc", "bnbsmartchain": "bsc",
    "binance-smart-chain": "bsc",
    # polygon
    "matic": "polygon", "polygon": "polygon", "pol": "polygon",
    # arbitrum
    "arb": "arbitrum", "arbone": "arbitrum", "arbitrum": "arbitrum",
    "arbevm": "arbitrum", "arbeth": "arbitrum",
    # optimism
    "op": "optimism", "optimism": "optimism",
    "opeth": "optimism",
    # base
    "base": "base", "baseeth": "base",
    # avalanche
    "avax": "avalanche", "avaxc": "avalanche", "avalanche": "avalanche",
    "cchain": "avalanche",
    # fantom
    "ftm": "fantom", "fantom": "fantom", "sonic": "fantom",
    # solana
    "sol": "solana", "solana": "solana", "spl": "solana",
    # tron
    "trx": "tron", "trc20": "tron", "tron": "tron",
    # sui / ton / aptos
    "sui": "sui", "ton": "ton", "toncoin": "ton",
    "apt": "aptos", "aptos": "aptos",
    # L2s
    "linea": "linea", "scroll": "scroll", "mantle": "mantle",
    "zksync": "zksync", "zksyncera": "zksync", "era": "zksync",
    "blast": "blast",
    "ronin": "ronin", "ron": "ronin",
    "cronos": "cronos", "cro": "cronos",
    "celo": "celo",
    "gnosis": "gnosis", "xdai": "gnosis",
    "pulsechain": "pulsechain", "pls": "pulsechain",
    "berachain": "berachain", "bera": "berachain",
    # btc
    "btc": "bitcoin", "bitcoin": "bitcoin",
}


def canonical(label: str) -> str | None:
    if not label:
        return None
    key = label.lower().replace("-", "").replace(" ", "").replace("_", "")
    return _ALIASES.get(key) or _ALIASES.get(label.lower()) or None


def eta_minutes(chain: str, min_confirms: int = 1) -> float | None:
    c = canonical(chain) or chain.lower()
    bt = BLOCK_SEC.get(c)
    if bt is None:
        return None
    return max(1, min_confirms) * bt / 60.0


def pick_transfer_chain(from_nets: list[dict], to_nets: list[dict]) -> dict | None:
    """Choose the fastest chain both sides support (withdraw on source,
    deposit on destination).
    Returns {"chain", "eta_min", "fee",
             "src_network", "dst_network"} — the raw per-exchange labels
    so create_withdraw(params={"network": ...}) uses each exchange's
    own vocabulary (Binance='ETH', Bitget='ERC20', Bitvavo='ERC20', ...)."""
    src_by_chain: dict[str, dict] = {}
    for n in (from_nets or []):
        if not n.get("withdraw"):
            continue
        c = canonical(n["network"])
        if not c:
            continue
        src_by_chain[c] = n
    dst_by_chain: dict[str, dict] = {}
    for n in (to_nets or []):
        if not n.get("deposit"):
            continue
        c = canonical(n["network"])
        if not c:
            continue
        dst_by_chain[c] = n

    common = set(src_by_chain) & set(dst_by_chain)
    if not common:
        return None
    scored = []
    for c in common:
        bt = BLOCK_SEC.get(c)
        if bt is None:
            continue
        src_n = src_by_chain[c]
        dst_n = dst_by_chain[c]
        confirms = 12
        try:
            confirms = int((src_n.get("raw") or {}).get("minConfirm") or 12)
        except (TypeError, ValueError):
            pass
        eta = eta_minutes(c, confirms)
        scored.append((eta, c, src_n.get("fee"), src_n["network"], dst_n["network"]))
    if not scored:
        return None
    scored.sort()
    eta, chain, fee, src_net, dst_net = scored[0]
    return {"chain": chain, "eta_min": eta, "fee": fee,
            "src_network": src_net, "dst_network": dst_net}
