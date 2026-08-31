"""Regression tests for candidate ordering (stremio_service).

Anchored to the real 2026-08-31 incident: "KPop Demon Hunters" resolved fine but
the walk probed a run of untagged Russian/Japanese "4K AI" upscales FIRST, taking
~35s before it reached a clean English copy. These tests lock in that the sorter,
using Zilean's structured fields (quality / codec / audio / dubbed / size), now
puts the clean English source at the front and the AI-upscales at the back —
without DROPPING any candidate (reorder-only contract).

Loaded in isolation (the services package __init__ pulls auth deps we don't need).
"""
import importlib.util
import sys
import types

import pytest


def _load():
    pkg = types.ModuleType("backend"); pkg.__path__ = []
    svc = types.ModuleType("backend.services"); svc.__path__ = []
    logm = types.ModuleType("backend.services.log_service")

    class _L:
        def __getattr__(self, n):
            return lambda *a, **k: None

    logm.log_service = _L()
    sys.modules.update({
        "backend": pkg, "backend.services": svc,
        "backend.services.log_service": logm,
        "httpx": types.ModuleType("httpx"),
    })

    def load(name, path):
        spec = importlib.util.spec_from_file_location(name, path)
        m = importlib.util.module_from_spec(spec)
        m.__package__ = "backend.services"
        sys.modules[name] = m
        spec.loader.exec_module(m)
        return m

    import os
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load("backend.services.zilean_service", os.path.join(here, "services/zilean_service.py"))
    S = load("backend.services.stremio_service", os.path.join(here, "services/stremio_service.py"))
    return S


S = _load()


def _cand(idx, title, quality="", codec="", audio=None, dubbed=False,
          langs=None, size=0):
    return {
        "title": title, "name": "Zilean 2160p",
        "quality": quality, "codec": codec, "audio": audio or [],
        "dubbed": dubbed, "languages": langs or [], "sizeBytes": size,
        "url": f"hash{idx}",
    }


# The actual KPop Demon Hunters candidate shapes (from live Zilean 2026-08-31).
KPOP = [
    _cand(1, "KPop Demon Hunters 2025 4K AI mkv", size=3_200_000_000),
    _cand(2, "KPop Demon Hunters 2025 4K AI mkv v2", size=3_100_000_000),
    _cand(3, "KPop Demon Hunters 2025 2160p HDR10 WEBRip 6CH x265",
          quality="WEBRip", codec="hevc", size=8_000_000_000),
    _cand(4, "Kpop Demon Hunters 2024 MULTi VFF 2160p WEB-RIP SDR x265 DDP5 1-MRD",
          quality="WEBRip", codec="hevc", audio=["Dolby Digital Plus"],
          dubbed=True, langs=["fr"], size=14_517_943_468),
    _cand(5, "KPop Demon Hunters 2025 2160p WEB-DL DDP5 1 Atmos HDR HEVC-XEBEC",
          quality="WEB-DL", codec="hevc", audio=["Atmos", "Dolby Digital Plus"],
          size=20_000_000_000),
    _cand(6, "KPop Demon Hunters 2025 MULTi 4K AI x265 atmos AC3 5 1 mkv",
          codec="hevc", audio=["Atmos", "Dolby Digital"], dubbed=True,
          size=23_012_898_287),
]


def _order():
    inst = S.StremioService.__new__(S.StremioService)
    urls = inst.ordered_candidates(KPOP, "4k", fallback_enabled=True)
    by = {c["url"]: c for c in KPOP}
    return [by[u]["title"] for u in urls]


def test_no_candidate_dropped():
    """Ordering is reorder-only: every candidate survives."""
    assert len(_order()) == len(KPOP)


def test_clean_english_web_dl_probed_first():
    """The English WEB-DL/Atmos copy leads — not the untagged foreign upscales."""
    assert _order()[0].endswith("HEVC-XEBEC")


def test_ai_upscales_sorted_last():
    """Every '4K AI' upscale lands in the tail of the walk order."""
    order = _order()
    ai = [i for i, t in enumerate(order) if "4K AI" in t]
    assert ai and min(ai) >= len(order) - 3


def test_dubbed_flag_deranks_below_clean_english():
    """A dubbed=True French copy ranks below a clean English one of same tier."""
    order = _order()
    assert order.index("KPop Demon Hunters 2025 2160p HDR10 WEBRip 6CH x265") < \
        order.index("Kpop Demon Hunters 2024 MULTi VFF 2160p WEB-RIP SDR x265 DDP5 1-MRD")


def test_upscale_pattern_matches_4k_ai_but_not_real_titles():
    assert S._UPSCALE_PATTERN.search("KPop Demon Hunters 2025 4K AI mkv")
    assert S._UPSCALE_PATTERN.search("Movie 2160p AI x265")
    # Must NOT flag a legit 'A.I.'-style title or an unrelated 'ai' token.
    assert not S._UPSCALE_PATTERN.search("A.I. Artificial Intelligence 2001 1080p BluRay")
    assert not S._UPSCALE_PATTERN.search("Cairo Station 1958 1080p BluRay")


def test_structured_size_breaks_ties_bigger_first():
    """Two otherwise-identical sources order by size (bitrate proxy) descending."""
    small = _cand(10, "Film 2160p WEB-DL x265", quality="WEB-DL",
                  codec="hevc", size=3_000_000_000)
    big = _cand(11, "Film 2160p WEB-DL x265", quality="WEB-DL",
                codec="hevc", size=25_000_000_000)
    inst = S.StremioService.__new__(S.StremioService)
    order = inst.ordered_candidates([small, big], "4k", fallback_enabled=True)
    assert order[0] == "hash11"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
