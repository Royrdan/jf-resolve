"""Live seeder counts via the BitTorrent UDP tracker scrape convention (BEP 48).

WHY THIS EXISTS
---------------
Zilean is sourced from DebridMediaManager hashlists — dumps of what other people
already hold in their debrid accounts. No tracker is ever contacted while
building that index, so a Zilean row carries no seeder count and never can.
(Its Torznab output reports a hardcoded ``seeders=999`` for every single item,
so that field is worse than useless — do not read it.)

The uncached-load path in ``api/stream.py`` is the one place seeder counts
decide anything: it queues a not-yet-cached magnet onto TorBox and waits. A
0-seed magnet can never finish, and TorBox keeps reporting plain ``downloading``
for minutes before admitting ``stalled (no seeds)`` — far longer than the ~20s
wait budget — so every retry re-queued the same dead hash and burned the budget
again. A scrape answers the same question in under a second, up front.

Cached candidates do NOT come through here: they stream from the debrid CDN, so
their seeder count is irrelevant (a 0-seed 2160p release that TorBox already has
plays perfectly). Only the uncached tail is ranked on this.

DESIGN NOTES
------------
- Best-effort by construction. Anything unknown — tracker down, UDP blocked,
  hash not tracked (common for private-tracker-sourced DMM rows) — is simply
  absent from the returned mapping, and the caller treats absent as "unknown →
  allow", which is exactly the pre-existing behaviour. A total scrape failure
  can therefore never be worse than not scraping at all.
- One UDP round trip covers up to ~74 hashes, so a full 200-candidate list costs
  about a second regardless of list size.
- Every socket call runs in a worker thread under a hard ``asyncio.wait_for``
  ceiling, so a hung tracker cannot stall the event loop or the play request.
"""

import asyncio
import binascii
import random
import socket
import struct
from typing import Dict, Iterable, List, Optional

from .log_service import log_service

# Protocol constant that opens every UDP tracker handshake (BEP 15).
_PROTOCOL_ID = 0x41727101980

_ACTION_CONNECT = 0
_ACTION_SCRAPE = 2
_ACTION_ERROR = 3

# A scrape request is one UDP datagram; 74 hashes is the conventional ceiling
# that keeps it inside a safe MTU.
_MAX_HASHES_PER_PACKET = 74

# Public trackers carrying broad swathes of the public swarm. Ordered by
# observed reliability — measured 2026-09-21, opentrackr and stealth answered in
# ~0.6s while torrent.eu.org and exodus.desync timed out, hence more than one.
DEFAULT_TRACKERS = [
    "udp://tracker.opentrackr.org:1337",
    "udp://open.stealth.si:80",
    "udp://tracker.torrent.eu.org:451",
    "udp://exodus.desync.com:6969",
]

DEFAULT_TIMEOUT_SECONDS = 3.0


def _parse_tracker(entry: str):
    """``udp://host:port`` → ``(host, port)``. Returns None if unparseable."""
    raw = (entry or "").strip()
    if not raw:
        return None
    if "://" in raw:
        scheme, _, raw = raw.partition("://")
        if scheme.lower() != "udp":
            # HTTP(S) scrape is a different wire format; not supported here.
            return None
    raw = raw.split("/", 1)[0]
    host, _, port = raw.rpartition(":")
    if not host or not port.isdigit():
        return None
    return host, int(port)


def _normalise_hashes(infohashes: Iterable[str]) -> List[str]:
    """Lowercase 40-char hex hashes, de-duplicated, order preserved."""
    out, seen = [], set()
    for h in infohashes or []:
        h = (h or "").strip().lower()
        if len(h) != 40 or h in seen:
            continue
        try:
            binascii.unhexlify(h)
        except (binascii.Error, ValueError):
            continue
        seen.add(h)
        out.append(h)
    return out


def _scrape_one_blocking(
    host: str, port: int, hashes: List[str], timeout: float
) -> Dict[str, int]:
    """Connect + scrape a single tracker. Runs in a worker thread.

    Returns ``{infohash: seeders}`` for the hashes this tracker knew about.
    Raises on any protocol/socket failure; the caller downgrades that to "no
    data from this tracker".
    """
    results: Dict[str, int] = {}
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
    try:
        addr = (socket.gethostbyname(host), port)

        # --- handshake -------------------------------------------------
        txn = random.randint(0, 2**31 - 1)
        sock.sendto(struct.pack(">QII", _PROTOCOL_ID, _ACTION_CONNECT, txn), addr)
        data, _ = sock.recvfrom(512)
        if len(data) < 16:
            raise ValueError("short connect response")
        action, resp_txn, connection_id = struct.unpack(">IIQ", data[:16])
        if action != _ACTION_CONNECT or resp_txn != txn:
            raise ValueError(f"bad connect response (action={action})")

        # --- scrape, in packet-sized batches ---------------------------
        for i in range(0, len(hashes), _MAX_HASHES_PER_PACKET):
            batch = hashes[i : i + _MAX_HASHES_PER_PACKET]
            txn = random.randint(0, 2**31 - 1)
            payload = struct.pack(">QII", connection_id, _ACTION_SCRAPE, txn)
            payload += b"".join(binascii.unhexlify(h) for h in batch)
            sock.sendto(payload, addr)

            data, _ = sock.recvfrom(8 + 12 * len(batch) + 64)
            if len(data) < 8:
                raise ValueError("short scrape response")
            action, resp_txn = struct.unpack(">II", data[:8])
            if action == _ACTION_ERROR:
                raise ValueError(
                    f"tracker error: {data[8:].decode('utf-8', 'replace')[:80]}"
                )
            if action != _ACTION_SCRAPE or resp_txn != txn:
                raise ValueError(f"bad scrape response (action={action})")

            # 12 bytes per hash: seeders, completed, leechers. A tracker may
            # return fewer entries than asked for — take what is there.
            for idx, infohash in enumerate(batch):
                start = 8 + 12 * idx
                if start + 12 > len(data):
                    break
                seeders, _completed, _leechers = struct.unpack(
                    ">III", data[start : start + 12]
                )
                results[infohash] = seeders
        return results
    finally:
        sock.close()


async def scrape_seeders(
    infohashes: Iterable[str],
    trackers: Optional[List[str]] = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> Dict[str, int]:
    """Live seeder counts for ``infohashes``, best-effort.

    Queries every tracker concurrently and keeps the HIGHEST count seen for each
    hash — trackers see overlapping slices of a swarm, so the max is the closest
    available estimate of reachable seeders.

    A hash absent from the result means "unknown", NOT "zero": either no tracker
    answered, or none of them track it. Callers must not treat absence as dead.
    """
    hashes = _normalise_hashes(infohashes)
    if not hashes:
        return {}

    parsed = [t for t in (_parse_tracker(e) for e in (trackers or DEFAULT_TRACKERS)) if t]
    if not parsed:
        log_service.info("Tracker scrape: no usable tracker URLs configured — skipping.")
        return {}

    async def _one(host: str, port: int):
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(_scrape_one_blocking, host, port, hashes, timeout),
                timeout=timeout + 1.0,
            )
        except (asyncio.TimeoutError, OSError, ValueError, struct.error) as exc:
            log_service.info(
                f"Tracker scrape: {host}:{port} unavailable "
                f"({type(exc).__name__}) — ignoring."
            )
            return {}
        except Exception as exc:  # never let a scrape break a play request
            log_service.warning(
                f"Tracker scrape: {host}:{port} unexpected error "
                f"{type(exc).__name__}: {exc} — ignoring."
            )
            return {}

    merged: Dict[str, int] = {}
    for result in await asyncio.gather(*(_one(h, p) for h, p in parsed)):
        for infohash, seeders in result.items():
            if seeders > merged.get(infohash, -1):
                merged[infohash] = seeders

    if merged:
        alive = sum(1 for v in merged.values() if v > 0)
        log_service.info(
            f"Tracker scrape: {len(merged)}/{len(hashes)} hash(es) answered "
            f"across {len(parsed)} tracker(s) — {alive} with seeders, "
            f"{len(merged) - alive} dead."
        )
    else:
        log_service.info(
            f"Tracker scrape: no tracker answered for {len(hashes)} hash(es) — "
            f"treating all as unknown."
        )
    return merged
