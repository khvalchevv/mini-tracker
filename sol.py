"""Solana hot wallet: address derivation, balance reads, SPL sends.
All RPC through Alchemy (needs ALCHEMY_KEY in env)."""
import logging
import os

import aiohttp

log = logging.getLogger(__name__)


def _alchemy_url() -> str | None:
    key = os.getenv("ALCHEMY_KEY", "").strip()
    if not key:
        return None
    return f"https://solana-mainnet.g.alchemy.com/v2/{key}"


def hot_wallet_address() -> str | None:
    pk = os.getenv("SOLANA_PRIVATE_KEY", "").strip()
    if not pk:
        return None
    try:
        from solders.keypair import Keypair
        kp = Keypair.from_base58_string(pk)
        return str(kp.pubkey())
    except Exception as e:
        log.warning("solana key derive err: %s", e)
        return None


HOT_WALLET_ADDRESS = hot_wallet_address()


# Common SPL mints
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
_MINT_TO_SYMBOL = {USDC_MINT: "USDC", USDT_MINT: "USDT"}


async def wallet_snapshot(price_map: dict[str, float] | None = None,
                          min_usd: float = 1.0) -> list[tuple[str, float, float]]:
    """[(symbol, amount, usd), ...] on Solana for the hot wallet.
    Filters out entries worth < min_usd. `price_map` supplies USD prices
    for SOL and any recognized SPL symbols."""
    url = _alchemy_url()
    addr = HOT_WALLET_ADDRESS
    if not url or not addr:
        return []
    pm = {k.upper(): v for k, v in (price_map or {}).items()}
    out: list[tuple[str, float, float]] = []
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as sess:
        # native SOL
        try:
            r = await sess.post(url, json={"jsonrpc": "2.0", "method": "getBalance",
                                           "params": [addr], "id": 1})
            data = await r.json()
            lamports = int((data.get("result") or {}).get("value") or 0)
            sol = lamports / 1e9
            usd = sol * (pm.get("SOL") or 0.0)
            if usd >= min_usd:
                out.append(("SOL", sol, usd))
        except Exception as e:
            log.debug("sol getBalance err: %s", e)
        # SPL tokens
        try:
            r = await sess.post(url, json={
                "jsonrpc": "2.0", "id": 2,
                "method": "getTokenAccountsByOwner",
                "params": [addr, {"programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"},
                           {"encoding": "jsonParsed"}],
            })
            data = await r.json()
            for acc in ((data.get("result") or {}).get("value") or []):
                info = ((acc.get("account") or {}).get("data") or {}).get("parsed", {}).get("info", {})
                mint = info.get("mint")
                ta = info.get("tokenAmount") or {}
                amt = float(ta.get("uiAmount") or 0)
                if amt <= 0:
                    continue
                sym = _MINT_TO_SYMBOL.get(mint) or mint[:6]
                px = 1.0 if sym in ("USDC", "USDT") else (pm.get(sym.upper()) or 0.0)
                usd = amt * px
                if usd >= min_usd:
                    out.append((sym, amt, usd))
        except Exception as e:
            log.debug("sol tokens err: %s", e)
    out.sort(key=lambda e: -e[2])
    return out


async def spl_balance(mint: str) -> int | None:
    """Return raw SPL amount held by the hot wallet for a given mint,
    or None on any error. Uses jsonParsed to get the amount as int."""
    url = _alchemy_url()
    if not url or not HOT_WALLET_ADDRESS:
        return None
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as sess:
            r = await sess.post(url, json={
                "jsonrpc": "2.0", "id": 1,
                "method": "getTokenAccountsByOwner",
                "params": [HOT_WALLET_ADDRESS, {"mint": mint},
                           {"encoding": "jsonParsed"}],
            })
            data = await r.json()
            for acc in ((data.get("result") or {}).get("value") or []):
                info = (((acc.get("account") or {}).get("data") or {})
                        .get("parsed", {}).get("info", {}))
                ta = info.get("tokenAmount") or {}
                return int(ta.get("amount") or 0)
    except Exception as e:
        log.debug("spl_balance err: %s", e)
    return 0


async def native_balance() -> int:
    """Return native SOL balance in lamports."""
    url = _alchemy_url()
    if not url or not HOT_WALLET_ADDRESS:
        return 0
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as sess:
            r = await sess.post(url, json={"jsonrpc": "2.0", "method": "getBalance",
                                           "params": [HOT_WALLET_ADDRESS], "id": 1})
            data = await r.json()
            return int((data.get("result") or {}).get("value") or 0)
    except Exception as e:
        log.debug("native_balance err: %s", e)
    return 0


async def send_token(mint: str | None, amount_raw: int, to_address: str) -> dict:
    """Sign & broadcast a Solana transfer.
    - mint=None → native SOL transfer (amount in lamports)
    - mint=<address> → SPL transfer (amount in token base units)
    Returns {ok, signature?, error?}."""
    pk_b58 = os.getenv("SOLANA_PRIVATE_KEY", "").strip()
    url = _alchemy_url()
    if not pk_b58 or not url:
        return {"ok": False, "error": "SOLANA_PRIVATE_KEY or ALCHEMY_KEY missing"}
    try:
        from solders.keypair import Keypair
        from solders.pubkey import Pubkey
        from solders.transaction import VersionedTransaction
        from solders.system_program import TransferParams, transfer
        from solders.message import MessageV0
        from solana.rpc.async_api import AsyncClient
        # solana-py refactor: TxOpts moved from .rpc.types to .rpc.models
        try:
            from solana.rpc.models import TxOpts
        except ImportError:
            from solana.rpc.types import TxOpts
    except ImportError as e:
        return {"ok": False, "error": f"missing solana deps: {e}"}
    kp = Keypair.from_base58_string(pk_b58)
    client = AsyncClient(url)
    try:
        if mint is None:
            # native SOL transfer
            recent = (await client.get_latest_blockhash()).value.blockhash
            ix = transfer(TransferParams(from_pubkey=kp.pubkey(),
                                          to_pubkey=Pubkey.from_string(to_address),
                                          lamports=int(amount_raw)))
            msg = MessageV0.try_compile(payer=kp.pubkey(), instructions=[ix],
                                         address_lookup_table_accounts=[],
                                         recent_blockhash=recent)
            tx = VersionedTransaction(msg, [kp])
            sig = (await client.send_transaction(
                tx, opts=TxOpts(skip_preflight=False))).value
            return {"ok": True, "signature": str(sig)}
        else:
            # SPL transfer via SPL token program
            from spl.token.constants import TOKEN_PROGRAM_ID
            from spl.token.instructions import (
                get_associated_token_address,
                create_associated_token_account,
                transfer_checked,
            )
            # spl-token refactor: TransferCheckedParams moved from
            # instructions → models
            try:
                from spl.token.models import TransferCheckedParams
            except ImportError:
                from spl.token.instructions import TransferCheckedParams
            mint_pub = Pubkey.from_string(mint)
            to_pub = Pubkey.from_string(to_address)
            # Sender ATA — the canonical account holding our balance
            src = get_associated_token_address(kp.pubkey(), mint_pub)
            dst_ata = get_associated_token_address(to_pub, mint_pub)
            # Fetch decimals from mint account (parsed)
            dec = 6
            try:
                mi = await client.get_account_info_json_parsed(mint_pub)
                dec = int(mi.value.data.parsed["info"]["decimals"])
            except Exception:
                pass
            recent = (await client.get_latest_blockhash()).value.blockhash
            # Build tx: create dst ATA if needed + transfer_checked
            ixs = []
            dst_acc = await client.get_account_info(dst_ata)
            if not dst_acc.value:
                ixs.append(create_associated_token_account(
                    payer=kp.pubkey(), owner=to_pub, mint=mint_pub))
            ixs.append(transfer_checked(TransferCheckedParams(
                program_id=TOKEN_PROGRAM_ID, source=src, dest=dst_ata,
                mint=mint_pub, owner=kp.pubkey(),
                amount=int(amount_raw), decimals=dec, signers=[])))
            msg = MessageV0.try_compile(payer=kp.pubkey(), instructions=ixs,
                                         address_lookup_table_accounts=[],
                                         recent_blockhash=recent)
            tx = VersionedTransaction(msg, [kp])
            sig = (await client.send_transaction(
                tx, opts=TxOpts(skip_preflight=False))).value
            return {"ok": True, "signature": str(sig)}
    except Exception as e:
        log.warning("sol send err: %s", e)
        return {"ok": False, "error": str(e)[:200]}
    finally:
        await client.close()
