"""Stream playability validation via ffprobe.

Probes a resolved stream URL (after redirect resolution) by reading only the
container header over HTTP range requests — it does not download the file. Used
to reject dead/expired debrid links, non-media error pages, too-short files
(sample/featurette junk), and optionally codecs/containers a client can't play.
"""

import asyncio
import json
import shutil
from dataclasses import dataclass, field
from typing import List, Optional

from .log_service import log_service

# Codecs/containers known to fail playback. Empty by default — the NVIDIA
# Shield (and most clients) direct-play almost everything, so we only block
# specific types that are actually reported as unplayable. Add entries here as
# bad file types surface. Liveness + min-duration checks always apply.
DEFAULT_VIDEO_DENYLIST: List[str] = []
DEFAULT_AUDIO_DENYLIST: List[str] = []
DEFAULT_CONTAINER_DENYLIST: List[str] = []


@dataclass
class ProbeResult:
    """Outcome of validating a single stream URL."""

    ok: bool
    reason: str = ""
    format_name: Optional[str] = None
    duration: Optional[float] = None
    video_codec: Optional[str] = None
    audio_codec: Optional[str] = None


@dataclass
class ValidationPolicy:
    """Tunable rules applied to a probe."""

    min_duration_seconds: int = 180
    video_denylist: List[str] = field(default_factory=lambda: list(DEFAULT_VIDEO_DENYLIST))
    audio_denylist: List[str] = field(default_factory=lambda: list(DEFAULT_AUDIO_DENYLIST))
    container_denylist: List[str] = field(default_factory=lambda: list(DEFAULT_CONTAINER_DENYLIST))
    probe_timeout_seconds: int = 10


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
            "-show_entries", "format=format_name,duration:stream=codec_name,codec_type",
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
        data = await self._run_ffprobe(url)
        if data is None:
            # ffprobe couldn't open it: dead link, HTML error page, or corrupt.
            return ProbeResult(ok=False, reason="probe_failed")

        fmt = data.get("format", {}) or {}
        streams = data.get("streams", []) or []

        video = next((s for s in streams if s.get("codec_type") == "video"), None)
        audio = next((s for s in streams if s.get("codec_type") == "audio"), None)

        format_name = fmt.get("format_name")
        video_codec = (video or {}).get("codec_name")
        audio_codec = (audio or {}).get("codec_name")

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
        )

        # Liveness: must actually contain a video stream.
        if video is None:
            result.ok = False
            result.reason = "no_video_stream"
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

        return result
