"""Stream playability validation via ffprobe.

Probes a resolved stream URL (after redirect resolution) by reading only the
container header over HTTP range requests — it does not download the file. Used
to reject dead/expired debrid links, non-media error pages, too-short files
(sample/featurette junk), and optionally codecs/containers a client can't play.
"""

import asyncio
import json
import re
import shutil
from dataclasses import dataclass, field
from typing import List, Optional
from urllib.parse import unquote, urlparse

from .log_service import log_service

# Known foreign-dub release groups / language tags whose audio is frequently
# LEFT UNTAGGED — so the ffprobe language gate below cannot catch them (an
# untagged Russian voiceover looks identical to an untagged English original).
# These groups (ColdFilm, Ultradox, LostFilm, … are Russian voiceover studios)
# must be rejected on the resolved FILENAME instead. Kept tight to avoid false
# positives on genuine English releases; the ru/rus token requires delimiters
# so titles like "Krusty" don't match.
FOREIGN_DUB_RELEASE = re.compile(
    r'(?i)(?:\b(?:coldfilm|ultradox|lostfilm|hdrezka|baibako|newstudio|'
    r'kerob|jaskier|kubik|amedia)\b|[._\s\-]rus?[._\s\-]|\brussian\b)'
)

# Codecs/containers known to fail playback. The NVIDIA Shield (and most
# clients) direct-play almost everything, so we only block specific types that
# are actually reported as unplayable. Add entries here as bad file types
# surface. Liveness + min-duration checks always apply.
#   av1 — Frozen (2013) froze on playback 2026-07-24: resolved to an AV1/Opus
#         encode the client couldn't hardware-decode. AV1 has no reliable decode
#         support across the household's players; an h264/h265 release is almost
#         always available to fail over to.
DEFAULT_VIDEO_DENYLIST: List[str] = ["av1"]
DEFAULT_AUDIO_DENYLIST: List[str] = []
DEFAULT_CONTAINER_DENYLIST: List[str] = []


# Audio language tags that are NOT a specific foreign language — treat as
# acceptable so we never reject a legit English/original release. English is
# frequently left untagged ("und") or marked "mul"/"zxx" on multi/no-language
# tracks. Normalised en/english → eng before comparison.
NEUTRAL_AUDIO_LANGS = {"und", "unknown", "mis", "mul", "zxx", "", "eng"}


def _norm_lang(lang: Optional[str]) -> str:
    """Normalise an ffprobe language tag to a lowercase ISO-639-2/B-ish token."""
    l = (lang or "").strip().lower()
    if l in ("en", "eng", "english"):
        return "eng"
    return l


@dataclass
class ProbeResult:
    """Outcome of validating a single stream URL."""

    ok: bool
    reason: str = ""
    format_name: Optional[str] = None
    duration: Optional[float] = None
    video_codec: Optional[str] = None
    audio_codec: Optional[str] = None
    audio_langs: List[str] = field(default_factory=list)
    sub_langs: List[str] = field(default_factory=list)
    # Dolby Vision profile of the video stream (None if not DV). 5 = single-layer
    # DV with no HDR10 fallback (the green/purple culprit on non-DV players).
    dv_profile: Optional[int] = None
    dv_bl_compat: Optional[int] = None


@dataclass
class ValidationPolicy:
    """Tunable rules applied to a probe."""

    min_duration_seconds: int = 180
    video_denylist: List[str] = field(default_factory=lambda: list(DEFAULT_VIDEO_DENYLIST))
    audio_denylist: List[str] = field(default_factory=lambda: list(DEFAULT_AUDIO_DENYLIST))
    container_denylist: List[str] = field(default_factory=lambda: list(DEFAULT_CONTAINER_DENYLIST))
    probe_timeout_seconds: int = 10
    # Language preference. When require_preferred_audio is on, a stream whose
    # audio tracks are ALL an explicit non-preferred foreign language (e.g. a
    # Polish/Russian dub) is rejected so the resolver falls through to an
    # English/original-audio source. Untagged/neutral audio always passes.
    preferred_audio_langs: List[str] = field(default_factory=lambda: ["eng"])
    require_preferred_audio: bool = False
    # Reject Dolby Vision sources that have NO backward-compatible base layer
    # (Profile 5, or any DV stream whose bl_signal_compatibility_id is 0). These
    # play as green/purple on any client that can't decode DV; a DV Profile 8/7
    # source (HDR10/SDR/HLG compatible) or a plain HDR10/SDR one is preferred
    # instead. Turn OFF only if every playback client is DV-capable.
    block_dv_no_fallback: bool = True


class StreamValidator:
    """Run ffprobe against a stream URL and apply a ValidationPolicy."""

    def __init__(self, policy: Optional[ValidationPolicy] = None):
        self.policy = policy or ValidationPolicy()

    @staticmethod
    def available() -> bool:
        """True if the ffprobe binary is on PATH."""
        return shutil.which("ffprobe") is not None

    async def _run_ffprobe(self, url: str) -> Optional[dict]:
        """Invoke ffprobe and return parsed JSON, or None on failure/timeout."""
        timeout = self.policy.probe_timeout_seconds
        cmd = [
            "ffprobe",
            "-v", "error",
            "-hide_banner",
            # Cap how much of the stream ffprobe pulls before giving up.
            "-analyzeduration", "5M",
            "-probesize", "5M",
            # HTTP/network read timeout in microseconds (protocol-level option).
            "-timeout", str(timeout * 1_000_000),
            "-user_agent", "Mozilla/5.0 (jf-resolve)",
            "-show_entries",
            "format=format_name,duration:stream=codec_name,codec_type:"
            "stream_tags=language,title:"
            # Dolby Vision config record (attached to the video stream when
            # present) — used to reject Profile 5 / no-HDR10-fallback sources
            # that render green+purple on non-DV players.
            "stream_side_data=dv_profile,dv_bl_signal_compatibility_id",
            "-of", "json",
            url,
        ]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError:
            log_service.warning("ffprobe binary not found; skipping stream validation")
            return None

        try:
            # Hard backstop on top of ffprobe's own network timeout.
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=timeout + 5
            )
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            log_service.warning(f"ffprobe timed out after {timeout}s for {url[:80]}...")
            return None

        if proc.returncode != 0:
            err = (stderr or b"").decode("utf-8", "replace").strip()
            log_service.info(f"ffprobe failed ({proc.returncode}) for {url[:80]}...: {err[:200]}")
            return None

        try:
            return json.loads(stdout.decode("utf-8", "replace"))
        except (json.JSONDecodeError, ValueError) as e:
            log_service.info(f"ffprobe returned unparsable output for {url[:80]}...: {e}")
            return None

    async def validate(self, url: str) -> ProbeResult:
        """Probe `url` and return whether it is playable under the policy."""
        # Filename-based foreign-dub gate (runs BEFORE ffprobe): some release
        # groups ship their non-English audio UNTAGGED, so the language gate
        # below would wave them through as "neutral". When English is required,
        # reject known foreign-dub releases on the resolved filename so we never
        # serve (or waste a probe on) e.g. a ColdFilm/Ultradox Russian voiceover.
        if self.policy.require_preferred_audio and "eng" in {
            _norm_lang(l) for l in self.policy.preferred_audio_langs
        }:
            fname = unquote(urlparse(url).path).rsplit("/", 1)[-1].lower()
            if FOREIGN_DUB_RELEASE.search(fname):
                return ProbeResult(
                    ok=False, reason=f"foreign_dub_release ({fname[:80]})"
                )

        data = await self._run_ffprobe(url)
        if data is None:
            # ffprobe couldn't open it: dead link, HTML error page, or corrupt.
            return ProbeResult(ok=False, reason="probe_failed")

        fmt = data.get("format", {}) or {}
        streams = data.get("streams", []) or []

        video = next((s for s in streams if s.get("codec_type") == "video"), None)
        audio = next((s for s in streams if s.get("codec_type") == "audio"), None)

        audio_streams = [s for s in streams if s.get("codec_type") == "audio"]
        sub_streams = [s for s in streams if s.get("codec_type") == "subtitle"]
        audio_langs = [_norm_lang((s.get("tags") or {}).get("language")) for s in audio_streams]
        sub_langs = [_norm_lang((s.get("tags") or {}).get("language")) for s in sub_streams]

        format_name = fmt.get("format_name")
        video_codec = (video or {}).get("codec_name")
        audio_codec = (audio or {}).get("codec_name")

        # Dolby Vision config (if any) lives in the video stream's side_data_list.
        dv_profile = None
        dv_bl_compat = None
        for sd in (video or {}).get("side_data_list", []) or []:
            if "dv_profile" in sd:
                dv_profile = sd.get("dv_profile")
                dv_bl_compat = sd.get("dv_bl_signal_compatibility_id")
                break

        duration = None
        try:
            if fmt.get("duration") is not None:
                duration = float(fmt["duration"])
        except (TypeError, ValueError):
            duration = None

        result = ProbeResult(
            ok=True,
            format_name=format_name,
            duration=duration,
            video_codec=video_codec,
            audio_codec=audio_codec,
            audio_langs=audio_langs,
            sub_langs=sub_langs,
            dv_profile=dv_profile,
            dv_bl_compat=dv_bl_compat,
        )

        # Liveness: must actually contain a video stream.
        if video is None:
            result.ok = False
            result.reason = "no_video_stream"
            return result

        # Dolby Vision gate: reject a DV source with no backward-compatible base
        # layer (Profile 5, or bl_signal_compatibility_id == 0). Without DV RPU
        # processing these decode in the wrong colour space → green/purple on
        # non-DV players. DV Profile 8/7 (compat 1/2/4) and plain HDR10/SDR pass,
        # so the resolver falls through to a source that displays correctly.
        if self.policy.block_dv_no_fallback and dv_profile is not None:
            if dv_profile == 5 or dv_bl_compat == 0:
                result.ok = False
                result.reason = (
                    f"dolby_vision_no_fallback (profile={dv_profile}, "
                    f"bl_compat={dv_bl_compat})"
                )
                return result

        # Duration gate: reject samples/featurettes/broken short files.
        min_dur = self.policy.min_duration_seconds
        if min_dur and duration is not None and duration < min_dur:
            result.ok = False
            result.reason = f"too_short ({duration:.0f}s < {min_dur}s)"
            return result

        # Codec/container denylist — empty by default; rejects only types that
        # have been reported as unplayable (the Shield handles almost everything).
        containers = set((format_name or "").split(","))
        if containers & set(self.policy.container_denylist):
            result.ok = False
            result.reason = f"container_denied ({format_name})"
            return result
        if video_codec and video_codec in self.policy.video_denylist:
            result.ok = False
            result.reason = f"video_codec_denied ({video_codec})"
            return result
        if audio_codec and audio_codec in self.policy.audio_denylist:
            result.ok = False
            result.reason = f"audio_codec_denied ({audio_codec})"
            return result

        # Language gate: reject foreign-only dubs so we hear English/original
        # audio. Passes when ANY audio track is preferred OR neutral/untagged
        # (English is often left untagged). Only rejects when every audio track
        # carries an explicit non-preferred foreign tag (e.g. a pol/rus dub).
        if self.policy.require_preferred_audio and audio_langs:
            preferred = {_norm_lang(l) for l in self.policy.preferred_audio_langs}
            acceptable = preferred | NEUTRAL_AUDIO_LANGS
            if not any(l in acceptable for l in audio_langs):
                result.ok = False
                result.reason = (
                    f"foreign_audio (audio={audio_langs} subs={sub_langs}; "
                    f"want one of {sorted(preferred)})"
                )
                return result

        return result
