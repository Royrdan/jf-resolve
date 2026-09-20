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


def test_dts_audio_ranks_below_decodable():
    """This player can't decode DTS/TrueHD, so a DTS-named copy must probe AFTER
    an EAC3/AC3 one of otherwise-identical quality (they'd be hard-rejected)."""
    eac3 = _cand(20, "Film 2160p WEB-DL EAC3 5 1 x265", quality="WEB-DL",
                 codec="hevc", audio=["Dolby Digital Plus"], size=10_000_000_000)
    dts = _cand(21, "Film 2160p WEB-DL DTS-HD MA 5 1 x265", quality="WEB-DL",
                codec="hevc", audio=["DTS-HD MA"], size=10_000_000_000)
    inst = S.StremioService.__new__(S.StremioService)
    order = inst.ordered_candidates([dts, eac3], "4k", fallback_enabled=True)
    assert order[0] == "hash20"  # the EAC3 copy leads


def test_ddp_atmos_still_ranks_high():
    """A 'DDP ... Atmos' name is decodable (EAC3) and must NOT be dragged down by
    the bare-atmos bottom tier — _tier_score takes the highest matching tier."""
    assert S._tier_score("Film 2160p WEB-DL DDP5 1 Atmos", S._AUDIO_TIERS, 2) == 4


# ── 2026-09-20: the language scale was INVERTED ─────────────────────────────
# Measured against live Zilean data: an English-only language list barely exists
# (Encanto 0/127, Toy Story 4 0/138, Inside Out 2 0/200), so ranking multi/dual
# second put foreign-default-audio releases at the head of every walk while the
# 79-112 unmarked plain-English ones sat below the ~9-candidate probe budget.
# Ground truth over 20 titles (real ffprobe): 13 better, 6 unchanged, 0 worse.

def test_unmarked_outranks_dual_audio():
    """THE fix. An unmarked release probes BEFORE a dual-audio one — the latter's
    foreign track is often the default and gets hard-rejected at validation."""
    dual = _cand(30, "Film 2021 iTA-ENG Bluray 2160p HDR x265-CYBER mkv",
                 quality="bluray", codec="hevc", langs=["en", "it"],
                 size=14_000_000_000)
    plain = _cand(31, "Film 2021 1080p BluRay DD+7 1 x264-TayTO mkv",
                  quality="bluray", codec="avc", size=8_000_000_000)
    assert S._language_rank(plain) > S._language_rank(dual)
    inst = S.StremioService.__new__(S.StremioService)
    order = inst.ordered_candidates([dual, plain], "4k", fallback_enabled=True)
    assert order[0] == "hash31"


def test_eng_inside_dual_audio_tag_is_not_clean_english():
    """`\\beng\\b` matches inside 'iTA-ENG' / '[UKR_ENG]' / 'NORDiC ENG'. Testing
    the English marker before the foreign one scored those as clean English —
    the TOP rank, for exactly the candidates that fail validation."""
    for name in ("Film 2021 iTA-ENG Bluray 2160p x265-CYBER mkv",
                 "Film (2021) UHD-BDRip 1080p H 265 HDR [UKR_ENG] [Hurtom] mkv",
                 "Film 2021 NORDiC ENG 1080p REMUX BluRay AVC mkv"):
        c = _cand(40, name)  # no structured languages → name-only path
        assert S._language_rank(c) == 1, name
    # A genuinely English-only name still earns the top rank.
    assert S._language_rank(_cand(41, "Film 2021 1080p BluRay ENG x264")) == 3
    # Foreign-only still floors.
    assert S._language_rank(_cand(42, "Film 2021 TRUEFRENCH 1080p x264")) == 0
    # No language signal at all → unmarked, above dual-audio but below clean ENG.
    assert S._language_rank(_cand(43, "Film 2021 1080p WEB-DL DDP5 1-EVO")) == 2


def test_undecodable_audio_outranks_quality_tiers():
    """Audio playability beats source/size. A TrueHD-only remux is unplayable on
    this player, so a humble DDP web-dl must probe first despite a worse tier."""
    remux = _cand(50, "Film 2021 2160p UHD BluRay REMUX HEVC TrueHD 7 1 Atmos",
                  quality="remux", codec="hevc", audio=["TrueHD"],
                  size=60_000_000_000)
    webdl = _cand(51, "Film 2021 1080p WEB-DL DDP5 1 H 264-EVO mkv",
                  quality="web-dl", codec="avc",
                  audio=["Dolby Digital Plus"], size=5_000_000_000)
    inst = S.StremioService.__new__(S.StremioService)
    order = inst.ordered_candidates([remux, webdl], "4k", fallback_enabled=True)
    assert order[0] == "hash51"


def test_episode_specificity_still_outranks_audio():
    """TV-safety guarantee. Audio was deliberately NOT promoted above episode
    matching: doing so pulled season packs ahead of the real episode file on 14
    of 20 TV episodes for zero gain. The episode-specific file leads even when a
    season pack carries better audio."""
    pack = _cand(60, "Show S03 COMPLETE 1080p WEB-DL DDP5 1 Atmos H 264-NTb",
                 quality="web-dl", codec="avc", audio=["Atmos", "Dolby Digital Plus"],
                 size=40_000_000_000)
    ep = _cand(61, "Show S03E07 1080p WEB-DL AAC2 0 H 264-GRP",
               quality="web-dl", codec="avc", audio=["AAC"], size=2_000_000_000)
    inst = S.StremioService.__new__(S.StremioService)
    order = inst.ordered_candidates([pack, ep], "1080p", fallback_enabled=True,
                                    season=3, episode=7)
    assert order[0] == "hash61"


def test_reorder_only_contract_holds_for_new_key():
    """Nothing above may DROP a candidate — the validator is the only gate."""
    cands = [
        _cand(70, "Film 2021 iTA-ENG 2160p x265", langs=["en", "it"]),
        _cand(71, "Film 2021 TRUEFRENCH 1080p x264"),
        _cand(72, "Film 2021 1080p WEB-DL DDP5 1-EVO"),
        _cand(73, "Film 2021 2160p REMUX TrueHD Atmos", audio=["TrueHD"]),
    ]
    inst = S.StremioService.__new__(S.StremioService)
    order = inst.ordered_candidates(cands, "4k", fallback_enabled=True)
    assert sorted(order) == sorted(c["url"] for c in cands)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
