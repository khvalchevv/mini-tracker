"""Entry point: build TG application + start tracker in one asyncio loop."""
import asyncio
import io
import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv(override=True)

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("main")

import storage
from bot import build_application, make_alert_sender
from tracker import Tracker


def _parse_allowed(raw: str) -> set[int]:
    out = set()
    for x in (raw or "").split(","):
        x = x.strip()
        if x.isdigit():
            out.add(int(x))
    return out


async def _run():
    token = os.getenv("TG_BOT_TOKEN")
    if not token:
        raise SystemExit("TG_BOT_TOKEN not set in .env")

    allowed = _parse_allowed(os.getenv("TG_ALLOWED_USERS", ""))
    log.info("allowed users: %s", allowed or "(anyone)")

    storage.init()
    log.info("storage: %d pairs loaded", len(storage.all_pairs()))

    app = build_application(token, allowed)
    tracker = Tracker(
        send_alert_cb=make_alert_sender(app),
        poll_interval=float(os.getenv("POLL_INTERVAL_SEC", "3")),
        cooldown=float(os.getenv("ALERT_COOLDOWN_SEC", "60")),
        timeout=float(os.getenv("DS_TIMEOUT", "8")),
    )

    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    log.info("bot polling started")

    tracker_task = asyncio.create_task(tracker.run())
    try:
        await tracker_task
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        tracker.stop()
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass
