"""TorBox library lookup must skip foreign-dub packs by torrent name.

Regression (2026-11-18): Peppa Pig S05E08/E09 played Ukrainian from the library
pack "Свинка Пеппа  Peppa Pig [S05] (2018) SATRip | UKR". Its file paths carry no
language marker and its audio tracks carry NO language tag at all, so the ffprobe
gate saw them as neutral and passed them. The torrent name was the only signal.

Run: /tmp/jfrtest-venv/bin/pytest backend/tests/test_torbox_foreign_pack.py -q --asyncio-mode=auto
"""

import asyncio
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
    })

    def load(name, path):
        spec = importlib.util.spec_from_file_location(name, path)
        m = importlib.util.module_from_spec(spec)
        m.__package__ = "backend.services"
        sys.modules[name] = m
        spec.loader.exec_module(m)
        return m

    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for mod in ("zilean_service", "stream_validator", "rd_service", "stremio_service"):
        load(f"backend.services.{mod}", os.path.join(here, f"services/{mod}.py"))
    return load("backend.services.torbox_service", os.path.join(here, "services/torbox_service.py"))


T = _load()

UKR_PACK = {
    "id": 149108192,
    "name": "Свинка Пеппа  Peppa Pig [S05] (2018) SATRip | UKR",
    "files": [{"id": 9, "name": "Peppa Pig SATRip/Peppa Pig S05 SATRip/S05E09.Simple Science.SATRip.mkv", "size": 1}],
}
ENG_PACK = {
    "id": 1,
    "name": "Peppa Pig Season 05",
    "files": [{"id": 7, "name": "Peppa Pig Season 05/Peppa Pig - S05E09 - Simple Science.mkv", "size": 1}],
}
MULTI_PACK = {"id": 2, "name": "Peppa.Pig.S05.MULTi.1080p.WEB-DL.x264-jdox", "files": []}


def _svc(library):
    s = T.TorBoxService.__new__(T.TorBoxService)
    served = []

    async def get_library():
        return library

    async def request_dl(tid, fid):
        served.append(tid)
        return f"https://cdn/{tid}/{fid}"

    s.get_library = get_library
    s.request_dl = request_dl
    return s, served


def test_foreign_only_pack_skipped_english_served():
    s, served = _svc([UKR_PACK, ENG_PACK])
    url = asyncio.run(s.find_episode_stream("Peppa Pig", 5, 9, "auto"))
    assert url == "https://cdn/1/7"
    assert served == [1]


def test_foreign_only_pack_alone_defers_to_walk():
    s, served = _svc([UKR_PACK])
    assert asyncio.run(s.find_episode_stream("Peppa Pig", 5, 9, "auto")) is None
    assert served == []


def test_name_classifier():
    assert T.is_foreign_only_release(UKR_PACK["name"])
    assert not T.is_foreign_only_release(ENG_PACK["name"])
    assert not T.is_foreign_only_release(MULTI_PACK["name"])  # English present; ffprobe gate decides
