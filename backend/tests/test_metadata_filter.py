"""Regression tests for the metadata filter's TV soft-fallback (stremio_service).

Anchored to the real 2026-10-08/09 incident: Saturday Night Live S52E01-E03 each
played "Saturday Night Live S39E01 Tina Fey" (2013). StremThru answered the S52
query with 37 rows of S39-S50 — 1480 of its 1489 SNL torrents are mapped to
tt1372614, a *different* 2009 series of the same name, leaving only old dregs on
the real tt0072562. filter_streams_by_metadata correctly rejected 37/37, then hit
`if not matched: return streams` and handed the whole wrong-season list straight
back. The top pick validated perfectly — it IS a real episode of the right
series, just the wrong one — so nothing downstream could catch it.

These lock in that a season/episode mismatch now yields a real zero, while the
movie-year soft fallback (a filter limitation, not a wrong show) is preserved.

Loaded in isolation (the services package __init__ pulls auth deps we don't need).
"""
import importlib.util
import os
import sys
import types


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

    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load("backend.services.zilean_service",
         os.path.join(here, "services/zilean_service.py"))
    return load("backend.services.stremio_service",
                os.path.join(here, "services/stremio_service.py"))


S = _load()
SNL = "Saturday Night Live"


def _c(title):
    return {"title": title, "name": "StremThru 1080p", "url": title}


# Verbatim from the live StremThru response for
# imdbid=tt0072562&season=52&ep=1 (2026-10-09). Not one is season 52.
WRONG_SEASON = [
    _c("Saturday.Night.Live.S49E19.Maya.Rudolph-Vampire.Weekend.720p.WEBRip.x265-PSA.mkv"),
    _c("Saturday.Night.Live.S50E05.John.Mulaney.720p.HEVC.x265-MeGusta.mkv"),
    _c("Saturday Night Live S39E01 Tina Fey Arcade Fire 1080p PCOK WEB-DL DDP 5 1 H 264-FLUX.mkv"),
    _c("Saturday.Night.Live.S48E12.Pedro.Pescal.1080p.WEB.h264-KOGi"),
    _c("Saturday.Night.Live.S01.540p.x265.QAAC-CKlicious"),
]


def test_wrong_season_yields_empty_not_the_unfiltered_list():
    """The SNL bug: 37 wrong-season rows must not come back as "fine"."""
    out = S.StremioService.filter_streams_by_metadata(
        WRONG_SEASON, SNL, season=52, episode=1
    )
    assert out == [], (
        "a season/episode mismatch must return zero, never the unfiltered list"
    )


def test_the_2013_episode_is_not_served_for_a_2026_request():
    """Explicitly: S39E01 Tina Fey must never survive an S52E01 request."""
    out = S.StremioService.filter_streams_by_metadata(
        WRONG_SEASON, SNL, season=52, episode=1
    )
    assert not any("S39E01" in c["title"] for c in out)


def test_episode_match_count_sees_a_real_zero():
    """stream.py's StremThru guard relies on this predicate not soft-falling back."""
    assert S.StremioService.episode_match_count(
        WRONG_SEASON, SNL, 52, 1
    ) == 0


def test_the_right_episode_is_kept():
    cands = WRONG_SEASON + [
        _c("Saturday Night Live S52E01 Jalen Brunson 1080p WEB h264-GRACE")
    ]
    out = S.StremioService.filter_streams_by_metadata(
        cands, SNL, season=52, episode=1
    )
    assert [c["title"] for c in out] == [
        "Saturday Night Live S52E01 Jalen Brunson 1080p WEB h264-GRACE"
    ]


def test_a_season_pack_for_the_right_season_is_kept():
    """Packs are servable — the walk extracts the episode from them."""
    cands = WRONG_SEASON + [_c("Saturday Night Live S52 1080p WEB-DL x264")]
    out = S.StremioService.filter_streams_by_metadata(
        cands, SNL, season=52, episode=1
    )
    assert len(out) == 1 and "S52" in out[0]["title"]


def test_movie_year_mismatch_still_falls_back():
    """Unchanged on purpose: a year that won't parse is a filter limitation,
    not a wrong film, and a hard zero there would turn plays into 404s."""
    cands = [_c("Encanto 1999 1080p WEB-DL x264")]
    out = S.StremioService.filter_streams_by_metadata(
        cands, "Encanto", year=2021
    )
    assert out == cands


def test_non_tv_path_untouched_when_nothing_matches():
    cands = [_c("Some Entirely Different Film 2020 1080p")]
    out = S.StremioService.filter_streams_by_metadata(
        cands, "Encanto", year=2021
    )
    assert out == cands, "movie fallback must still return the original list"
