"""Audio language detection in the stream validator.

Regression: a "Peppa Pig S05 1080p WEBRip" pack carried its Ukrainian audio
language only as an uppercase Matroska `LANGUAGE` tag (track-header language
unset). The validator read only the lowercase `language` key, saw the track as
untagged (neutral) and served Ukrainian audio.

Run: /tmp/jfrtest-venv/bin/pytest backend/tests/test_stream_lang.py -q --asyncio-mode=auto
"""

import importlib.util
import sys
import types
from pathlib import Path

import pytest

_SERVICES = Path(__file__).resolve().parents[1] / "services"


def _stub_pkg(name, path):
    pkg = types.ModuleType(name)
    pkg.__path__ = [str(path)]
    sys.modules[name] = pkg


def _load(modname, filepath):
    spec = importlib.util.spec_from_file_location(modname, filepath)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[modname] = mod
    spec.loader.exec_module(mod)
    return mod


_stub_pkg("backend", _SERVICES.parent)
_stub_pkg("backend.services", _SERVICES)
_load("backend.services.log_service", _SERVICES / "log_service.py")
sv = _load("backend.services.stream_validator", _SERVICES / "stream_validator.py")


def _probe(audio_tags):
    """ffprobe JSON shaped like the real S05E04.Scooters.1080p.WEBRip.mkv."""
    return {
        "format": {"format_name": "matroska,webm", "duration": "301.08"},
        "streams": [
            {"codec_type": "video", "codec_name": "h264", "disposition": {"default": 1}},
            {"codec_type": "audio", "codec_name": "aac", "tags": audio_tags,
             "disposition": {"default": 1}},
        ],
    }


async def _validate(audio_tags):
    v = sv.StreamValidator(
        sv.ValidationPolicy(require_preferred_audio=True, preferred_audio_langs=["eng"])
    )

    async def fake_ffprobe(url):
        return _probe(audio_tags)

    v._run_ffprobe = fake_ffprobe
    return await v.validate("https://cdn.example/dld/S05E04.Scooters.1080p.WEBRip.mkv")


def test_stream_lang_reads_uppercase_tag():
    assert sv._stream_lang({"tags": {"LANGUAGE": "ukr"}}) == "ukr"
    assert sv._stream_lang({"tags": {"language": "en"}}) == "eng"
    assert sv._stream_lang({"tags": {}}) == ""
    assert sv._stream_lang({}) == ""


def test_stream_lang_prefers_specific_over_neutral():
    assert sv._stream_lang({"tags": {"language": "und", "LANGUAGE": "ukr"}}) == "ukr"


@pytest.mark.asyncio
async def test_uppercase_foreign_tag_is_rejected():
    r = await _validate({"LANGUAGE": "ukr"})
    assert not r.ok
    assert "foreign_default_audio" in r.reason


@pytest.mark.asyncio
async def test_english_and_untagged_still_pass():
    assert (await _validate({"language": "eng"})).ok
    assert (await _validate({"LANGUAGE": "eng"})).ok
    assert (await _validate({})).ok
