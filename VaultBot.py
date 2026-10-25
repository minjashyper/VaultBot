"""
VaultBot — a Discord music + community bot.

File layout
-----------
 1. Imports
 2. Configuration & constants
 3. Bot setup
 4. yt-dlp source configuration
 5. Diagnostics (temporary)
 6. Music state, data types & helpers
 7. Music engine (extract / queue / playback)
 8. Juice WRLD integration (API, tracker feed)
 9. Help system (multi-page command list)
10. Events & error handling
11. Prefix commands (music, Juice WRLD, help)
12. Slash commands
13. Startup
"""

from dotenv import load_dotenv

# Support both project-local .env files and hosts that use /home/container/.env.
# Existing host environment variables take precedence over values from these files.
load_dotenv()
load_dotenv("/home/container/.env")

# =============================================================================
# 1. IMPORTS
# =============================================================================
import asyncio
import faulthandler
import json
import os
import random
import re
import shutil
import signal
import sys
import tempfile
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, urlencode

import aiohttp
import discord
from discord.ext import commands, tasks
import yt_dlp


import html
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from datetime import timezone
from discord import app_commands
# =============================================================================
# 2. CONFIGURATION & CONSTANTS
# =============================================================================
TOKEN = os.getenv("DISCORD_TOKEN")

# Keep the existing prefix. Message Content Intent must also be enabled
# for this bot in the Discord Developer Portal.
PREFIX = "~"

# Embed colours.
BRAND_COLOR = discord.Color.from_rgb(28, 27, 32)
JUICE_COLOR = discord.Color.from_rgb(123, 44, 191)

# Music settings.
DEFAULT_VOLUME = 0.5
ALLOWED_UPLOAD_SUFFIXES = {".mp3", ".wav", ".opus", ".ogg", ".m4a", ".flac"}
MAX_UPLOAD_MB = 20

# Juice WRLD API settings.
# Public read endpoints are used by default; no API key is required for them.
# If your API account requires a key, set JUICEWRLD_API_KEY in the host's
# environment settings. Never commit tokens or API keys to GitHub.
JUICEWRLD_API_BASE = os.getenv(
    "JUICEWRLD_API_BASE",
    "https://juicewrldapi.com/juicewrld"
).rstrip("/")
JUICEWRLD_API_KEY = os.getenv("JUICEWRLD_API_KEY")
JUICEWRLD_CHANNEL_ID = os.getenv("JUICEWRLD_CHANNEL_ID")
def _positive_int_env(name: str, default: int) -> int:
    """Read a positive integer environment variable without crashing startup."""
    try:
        return max(1, int(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        print(f"WARNING: {name} must be a positive integer; using {default}.")
        return default


JUICEWRLD_POLL_MINUTES = _positive_int_env("JUICEWRLD_POLL_MINUTES", 5)
JUICEWRLD_STATE_FILE = Path(os.getenv(
    "JUICEWRLD_STATE_FILE",
    str(Path(tempfile.gettempdir()) / "juicewrld_seen.json")
))


# =============================================================================
# 3. BOT SETUP
# =============================================================================
intents = discord.Intents.default()
intents.message_content = True
intents.voice_states = True
intents.guilds = True
intents.messages = True

# help_command=None disables discord.py's built-in help so the custom
# multi-page help in section 9 can take over the ~help command.
bot = commands.Bot(command_prefix=PREFIX, intents=intents, help_command=None)


# =============================================================================
# 4. YT-DLP SOURCE CONFIGURATION (COOKIE-FREE)
# =============================================================================
# yt-dlp settings. This resolves public, playable media sources; it does not
# bypass paid services, DRM, private content, or service licensing restrictions.
YTDL_OPTIONS = {
    # Prefer an audio-only format; avoid fetching video when audio is available.
    "format": "bestaudio[acodec!=none]/bestaudio/best[acodec!=none]/best",
    "noplaylist": True,
    "playlist_items": "1",
    "quiet": True,
    "no_warnings": True,
    "default_search": "ytsearch",
    "source_address": "0.0.0.0",
    "extract_flat": False,
    "skip_download": False,
    # Keep yt-dlp's disk cache off and prevent concurrent fragment buffering.
    "cachedir": False,
    "concurrent_fragment_downloads": 1,
    "retries": 2,
    "fragment_retries": 2,
    "socket_timeout": 12,
}

# By default let yt-dlp pick its own YouTube clients (they are updated with every
# yt-dlp release, so hard-coding a list tends to go stale). If you want to force
# specific clients, set YT_PLAYER_CLIENTS on your host, e.g. "android_vr,web_embedded".
_yt_clients = [c.strip() for c in os.getenv("YT_PLAYER_CLIENTS", "").split(",") if c.strip()]
if _yt_clients:
    YTDL_OPTIONS["extractor_args"] = {"youtube": {"player_client": _yt_clients}}

FFMPEG_OPTIONS = {"options": "-vn -threads 1"}


# This build intentionally does not read or create YouTube cookie files.
# Public tracks may work without sign-in; YouTube can still restrict requests.

print(f"yt-dlp version: {yt_dlp.version.__version__}")
_js_runtime = shutil.which("deno") or shutil.which("node") or shutil.which("bun")
print(f"JS runtime for yt-dlp: {_js_runtime or 'NONE FOUND (YouTube extraction will be unreliable; install deno or node)'}")


class YTDLLogger:
    """Print yt-dlp's own messages to the console so we can see where it stops."""

    def debug(self, msg):
        if not msg.startswith("[download]") or "Destination" in msg or "100%" in msg:
            print(f"[yt-dlp] {msg}", flush=True)

    def info(self, msg):
        print(f"[yt-dlp] {msg}", flush=True)

    def warning(self, msg):
        print(f"[yt-dlp warning] {msg}", flush=True)

    def error(self, msg):
        print(f"[yt-dlp error] {msg}", flush=True)


# =============================================================================
# 5. DIAGNOSTICS (temporary)
# =============================================================================
# These lines help find out why the host restarts the bot during a song
# request. They only print to the console; they do not change how the bot works.
faulthandler.enable()  # prints a traceback if Python itself crashes hard


def _peak_ram_mb() -> str:
    try:
        import resource
        return f"{resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024:.0f}MB"
    except Exception:
        return "?"


def _read_first(paths) -> str:
    for p in paths:
        try:
            return Path(p).read_text().strip()
        except Exception:
            continue
    return "n/a"


def diag(message: str) -> None:
    print(f"[diag] {message} (peak RAM so far: {_peak_ram_mb()})", flush=True)


def _on_stop_signal(signum, frame):
    name = signal.Signals(signum).name
    print(f"[diag] Received {name}: the host is asking the bot to stop.", flush=True)
    signal.signal(signum, signal.SIG_DFL)
    os.kill(os.getpid(), signum)


for _sig in (getattr(signal, "SIGTERM", None), getattr(signal, "SIGHUP", None)):
    if _sig is not None:
        try:
            signal.signal(_sig, _on_stop_signal)
        except Exception:
            pass

# oom_kill > 0 here (it counts since the container started) means the host
# killed the bot for using too much memory.
diag(
    "memory limit: "
    + _read_first(["/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"])
    + " | memory events: "
    + _read_first(["/sys/fs/cgroup/memory.events"]).replace("\n", ", ")
)
diag(f"ffmpeg found: {shutil.which('ffmpeg')} | node: {shutil.which('node')} | deno: {shutil.which('deno')}")


# =============================================================================
# 6. MUSIC STATE, DATA TYPES & HELPERS
# =============================================================================
# Per-server music state.
queues: dict[int, deque] = {}
current_tracks: dict[int, "Track"] = {}
loop_modes: dict[int, str] = {}  # off, song, queue
volumes: dict[int, float] = {}
now_playing_messages: dict[int, discord.Message] = {}
queue_locks: dict[int, asyncio.Lock] = {}
TEMP_FILES: set[str] = set()


@dataclass
class Track:
    title: str
    webpage_url: str
    requester: str
    stream_url: Optional[str] = None
    local_path: Optional[str] = None
    duration: Optional[int] = None
    source_name: str = "audio"


def get_queue(guild_id: int) -> deque:
    return queues.setdefault(guild_id, deque())


def get_lock(guild_id: int) -> asyncio.Lock:
    return queue_locks.setdefault(guild_id, asyncio.Lock())


def format_duration(seconds: Optional[int]) -> str:
    if seconds is None:
        return "Unknown"
    minutes, seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}:{minutes:02d}:{seconds:02d}" if hours else f"{minutes}:{seconds:02d}"


def clean_temp_file(path: Optional[str]) -> None:
    """Remove a temporary audio file and its private download directory."""
    if not path or path not in TEMP_FILES:
        return
    try:
        file_path = Path(path)
        file_path.unlink(missing_ok=True)
        # yt-dlp downloads are stored in their own temp directory.
        if file_path.parent.name.startswith("vaultbot_audio_"):
            shutil.rmtree(file_path.parent, ignore_errors=True)
    except OSError:
        pass
    TEMP_FILES.discard(path)


def build_now_playing_embed(track: Track, loop_mode: Optional[str] = None) -> discord.Embed:
    """Embed shown by the 'Now Playing' announcement and ~nowplaying."""
    embed = discord.Embed(
        title="🎶 Now Playing",
        description=f"[{discord.utils.escape_markdown(track.title)}]({track.webpage_url})",
        color=BRAND_COLOR,
    )
    embed.add_field(name="Duration", value=format_duration(track.duration), inline=True)
    embed.add_field(name="Requested by", value=track.requester, inline=True)
    if loop_mode is not None:
        embed.set_footer(text=f"Loop: {loop_mode}")
    return embed


# =============================================================================
# 7. MUSIC ENGINE
# =============================================================================
# Only one yt-dlp extraction/download runs at a time to limit memory spikes
# on small hosting plans.
_YTDL_SEMAPHORE = asyncio.Semaphore(1)


def _is_signin_error(text: str) -> bool:
    lower = text.lower()
    return (
        ("confirm you" in lower and "not a bot" in lower)
        or "sign in to confirm" in lower
        or "login required" in lower
    )


def _norm_tokens(text: str) -> set:
    import re
    return {t for t in re.findall(r"[a-z0-9]+", text.lower()) if len(t) > 1}


def _soundcloud_pick(search_text: str) -> Optional[str]:
    """Search SoundCloud and return the URL of the best-matching result, or None."""
    wanted = _norm_tokens(search_text)
    if not wanted:
        return None
    opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": True,
        "skip_download": True,
        "cachedir": False,
        "socket_timeout": 12,
    }
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(f"scsearch8:{search_text}", download=False)
    except Exception as exc:
        print(f"[fallback] SoundCloud search error: {exc}", flush=True)
        return None
    best_url, best_score = None, 0.0
    for entry in (info or {}).get("entries") or []:
        if not entry:
            continue
        url = entry.get("url") or entry.get("webpage_url")
        got = _norm_tokens(f"{entry.get('title') or ''} {entry.get('uploader') or ''}")
        if not url or not got:
            continue
        score = len(wanted & got) / len(wanted)
        if score > best_score:
            best_url, best_score = url, score
    # Require most of the words to match so we never play a random wrong song.
    return best_url if best_score >= 0.6 else None


async def _fallback_search_variants(query: str) -> list:
    """Turn a blocked YouTube request into a few plain-text song searches."""
    import re
    if not query.lower().startswith(("http://", "https://")):
        return [query]  # it was already a title search
    if "youtube.com" not in query and "youtu.be" not in query:
        return []
    title = ""
    try:
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                "https://www.youtube.com/oembed",
                params={"url": query, "format": "json"},
            ) as resp:
                if resp.status == 200:
                    title = ((await resp.json()).get("title") or "").strip()
    except Exception as exc:
        print(f"[fallback] oEmbed lookup failed: {exc}", flush=True)
    if not title:
        return []

    # Strip tags like (UNRELEASED) / [Official Video] that hurt matching.
    cleaned = re.sub(r"[\(\[][^\)\]]*[\)\]]", " ", title)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -")
    variants = [cleaned]
    # "Song A/Song B" or "Artist - Song A / Song B": also try just the first song.
    if "/" in cleaned:
        first = cleaned.split("/")[0].strip(" -")
        if first:
            variants.append(first)
    # Last resort: the raw title.
    variants.append(re.sub(r"\s+", " ", title))
    seen, out = set(), []
    for v in variants:
        if v and v.lower() not in seen:
            seen.add(v.lower())
            out.append(v)
    return out


async def extract_track(query: str, requester: str) -> Track:
    """Find and download one playable audio track with one yt-dlp extraction pass."""
    def _extract(query: str):
        folder = tempfile.mkdtemp(prefix="vaultbot_audio_")
        options = dict(YTDL_OPTIONS)
        options.update({
            "outtmpl": str(Path(folder) / "%(id)s.%(ext)s"),
            "logger": YTDLLogger(),
            "noplaylist": True,
            "playlist_items": "1",
            "skip_download": False,
        })

        try:
            diag("yt-dlp: starting one-pass search/extraction/download")
            # Passing download=True here lets yt-dlp resolve the search and
            # download the selected result without a second extract_info call.
            with yt_dlp.YoutubeDL(options) as ydl:
                info = ydl.extract_info(query, download=True)

            if not info:
                raise ValueError("No playable result was found.")

            # Be defensive if a site returns a playlist-shaped result anyway.
            if isinstance(info, dict) and info.get("entries"):
                entries = info.get("entries") or []
                info = next((entry for entry in entries if entry), None)
            if not isinstance(info, dict):
                raise ValueError("yt-dlp did not return a playable track.")

            webpage_url = (
                info.get("webpage_url")
                or info.get("original_url")
                or query
            )

            # Prefer the actual filepath yt-dlp reports. Some extractors report
            # requested_downloads; others report _filename or prepare_filename.
            requested = info.get("requested_downloads") or []
            path = requested[0].get("filepath") if requested and isinstance(requested[0], dict) else None
            if not path:
                path = info.get("_filename")
            if not path:
                try:
                    path = ydl.prepare_filename(info)
                except Exception:
                    path = None

            if not path or not Path(path).is_file():
                candidates = [
                    candidate for candidate in Path(folder).iterdir()
                    if candidate.is_file() and not candidate.name.endswith((".part", ".ytdl"))
                ]
                if not candidates:
                    raise ValueError("yt-dlp did not produce an audio file.")
                path = str(candidates[0])

            diag("yt-dlp: download finished")
            return {
                "title": info.get("title") or "Unknown title",
                "webpage_url": webpage_url,
                "path": str(path),
                "duration": info.get("duration"),
                "extractor": info.get("extractor_key") or info.get("extractor") or "audio",
            }
        except Exception:
            shutil.rmtree(folder, ignore_errors=True)
            raise

    async with _YTDL_SEMAPHORE:
        try:
            data = await asyncio.to_thread(_extract, query)
        except Exception as exc:
            if not _is_signin_error(str(exc)):
                raise
            # YouTube blocked this host. Look up the video's title through
            # YouTube's public oEmbed endpoint (no sign-in needed) and try to
            # find the same song on SoundCloud instead.
            diag("yt-dlp: YouTube sign-in wall hit, trying SoundCloud fallback")
            variants = await _fallback_search_variants(query)
            print(f"[fallback] search variants: {variants}", flush=True)
            data = None
            for text in variants:
                sc_url = await asyncio.to_thread(_soundcloud_pick, text)
                print(f"[fallback] {text!r} -> {sc_url}", flush=True)
                if not sc_url:
                    continue
                try:
                    data = await asyncio.to_thread(_extract, sc_url)
                    break
                except Exception as sc_exc:
                    print(f"[fallback] SoundCloud download failed: {sc_exc}", flush=True)
            if data is None:
                raise exc

    TEMP_FILES.add(data["path"])
    return Track(
        title=data["title"],
        webpage_url=data["webpage_url"],
        requester=requester,
        local_path=data["path"],
        duration=data["duration"],
        source_name=data["extractor"],
    )


async def update_now_playing(ctx: commands.Context, track: Track) -> None:
    embed = build_now_playing_embed(track, loop_modes.get(ctx.guild.id, "off"))
    try:
        msg = await ctx.send(embed=embed)
        now_playing_messages[ctx.guild.id] = msg
    except discord.HTTPException:
        pass


async def play_next(ctx: commands.Context) -> None:
    """Start the next item. Called after a track ends."""
    if not ctx.guild:
        return
    guild_id = ctx.guild.id
    async with get_lock(guild_id):
        voice = ctx.guild.voice_client
        if not voice or not voice.is_connected():
            current_tracks.pop(guild_id, None)
            return

        previous = current_tracks.get(guild_id)
        mode = loop_modes.get(guild_id, "off")
        if previous and mode == "song":
            get_queue(guild_id).appendleft(previous)
        elif previous and mode == "queue":
            get_queue(guild_id).append(previous)
        elif previous:
            clean_temp_file(previous.local_path)

        if not get_queue(guild_id):
            current_tracks.pop(guild_id, None)
            return

        track = get_queue(guild_id).popleft()
        current_tracks[guild_id] = track
        volume = volumes.get(guild_id, DEFAULT_VOLUME)

        try:
            if not track.local_path or not Path(track.local_path).is_file():
                raise ValueError("The downloaded audio file is missing. Try the song again.")
            # Both YouTube downloads and Discord attachments play from local files.
            audio = discord.FFmpegPCMAudio(track.local_path, **FFMPEG_OPTIONS)

            source = discord.PCMVolumeTransformer(audio, volume=volume)

            def after_playback(error):
                if error:
                    print(f"Playback error: {error}")
                # Cleanup is handled by play_next after checking loop mode.
                try:
                    bot.loop.call_soon_threadsafe(
                        lambda: asyncio.create_task(play_next(ctx))
                    )
                except RuntimeError as exc:
                    print(f"Could not schedule next track: {exc}")

            voice.play(source, after=after_playback)
            # Keep the voice player's audio source alive and announce the track.
            await update_now_playing(ctx, track)
        except Exception as error:
            clean_temp_file(track.local_path)
            await ctx.send(
                f"⚠️ Couldn't play **{discord.utils.escape_markdown(track.title)}**: "
                f"{discord.utils.escape_markdown(str(error))[:300]}"
            )
            current_tracks.pop(guild_id, None)
            # Try the next queued track.
            bot.loop.call_soon(lambda: asyncio.create_task(play_next(ctx)))


async def ensure_voice(ctx: commands.Context) -> discord.VoiceClient:
    if not ctx.guild:
        raise commands.CommandError("Use music commands inside a server.")
    if not ctx.author.voice or not ctx.author.voice.channel:
        raise commands.CommandError("Join a voice channel first.")
    voice = ctx.guild.voice_client
    if voice and voice.is_connected():
        if voice.channel != ctx.author.voice.channel:
            await voice.move_to(ctx.author.voice.channel)
        return voice
    return await ctx.author.voice.channel.connect()


async def queue_track(ctx: commands.Context, track: Track) -> None:
    guild_id = ctx.guild.id
    get_queue(guild_id).append(track)
    await ctx.send(
        f"➕ Added **{discord.utils.escape_markdown(track.title)}** "
        f"(`{format_duration(track.duration)}`) to the queue. Position: "
        f"{len(get_queue(guild_id))}"
    )
    voice = ctx.guild.voice_client
    if voice and not voice.is_playing() and not voice.is_paused():
        await play_next(ctx)


# =============================================================================
# 8. JUICE WRLD INTEGRATION
# =============================================================================
_juicewrld_seen: set[str] = set()
_juicewrld_state_loaded = False
_juicewrld_baseline_initialized = False


def _load_juicewrld_state() -> None:
    """Load persisted event IDs once, so restarts do not repost old events."""
    global _juicewrld_state_loaded, _juicewrld_baseline_initialized

    if _juicewrld_state_loaded:
        return

    _juicewrld_state_loaded = True
    try:
        data = json.loads(JUICEWRLD_STATE_FILE.read_text(encoding="utf-8"))
        saved_ids = data.get("seen", [])
        if isinstance(saved_ids, list):
            _juicewrld_seen.update(str(value) for value in saved_ids)
            _juicewrld_baseline_initialized = True
        print(f"[JuiceWRLD] Loaded {len(_juicewrld_seen)} saved tracker event(s).")
    except FileNotFoundError:
        # A new install establishes a baseline and skips historical events.
        _juicewrld_baseline_initialized = False
    except (OSError, json.JSONDecodeError, AttributeError) as exc:
        print(f"[JuiceWRLD] Could not load state file: {exc}")
        _juicewrld_baseline_initialized = False


def _save_juicewrld_state() -> None:
    """Write tracker state atomically to reduce corruption on a restart."""
    JUICEWRLD_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = JUICEWRLD_STATE_FILE.with_suffix(
        JUICEWRLD_STATE_FILE.suffix + ".tmp"
    )
    temporary_path.write_text(
        json.dumps({"seen": sorted(_juicewrld_seen)}, ensure_ascii=False),
        encoding="utf-8"
    )
    temporary_path.replace(JUICEWRLD_STATE_FILE)


async def _juicewrld_api_get(path: str, params: Optional[dict] = None):
    timeout = aiohttp.ClientTimeout(total=20)
    headers = {"Accept": "application/json"}
    if JUICEWRLD_API_KEY:
        headers["X-API-Key"] = JUICEWRLD_API_KEY

    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        async with session.get(
            f"{JUICEWRLD_API_BASE}/{path.lstrip('/')}",
            params=params
        ) as response:
            response.raise_for_status()
            return await response.json(content_type=None)


def _juicewrld_items(payload):
    """Support common paginated and list-shaped API responses."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("results", "items", "data", "events", "files", "entries", "children"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
    return []


def _juicewrld_safe(value, fallback="Unknown", limit=250):
    if value is None:
        return fallback
    text = str(value).strip()
    if not text:
        return fallback
    # Prevent accidental mass mentions in third-party API text.
    text = text.replace("@", "@\u200b")
    return text[:limit]


@tasks.loop(minutes=JUICEWRLD_POLL_MINUTES)
async def juicewrld_tracker_task():
    """Poll the API tracker feed and post unseen events to the configured channel."""
    global _juicewrld_baseline_initialized

    if not JUICEWRLD_CHANNEL_ID:
        return

    try:
        channel_id = int(JUICEWRLD_CHANNEL_ID)
        channel = bot.get_channel(channel_id)
        if channel is None:
            channel = await bot.fetch_channel(channel_id)

        if not hasattr(channel, "send"):
            print("[JuiceWRLD] Configured channel cannot receive messages.")
            return

        payload = await _juicewrld_api_get(
            "/feeds/tracker.json",
            params={"limit": 50}
        )
        updates = _juicewrld_items(payload)

        if not _juicewrld_baseline_initialized:
            for item in updates:
                if not isinstance(item, dict):
                    continue
                event_id = item.get("id", item.get("proposal_id"))
                if event_id is not None:
                    _juicewrld_seen.add(str(event_id))
            _save_juicewrld_state()
            _juicewrld_baseline_initialized = True
            print(
                f"[JuiceWRLD] Baseline initialized with "
                f"{len(_juicewrld_seen)} existing event(s); old events skipped."
            )
            return

        fresh = []
        for item in updates:
            if not isinstance(item, dict):
                continue
            event_id = item.get("id", item.get("proposal_id"))
            if event_id is not None and str(event_id) not in _juicewrld_seen:
                fresh.append((str(event_id), item))

        # Feeds are commonly newest-first; send oldest unseen event first.
        for event_id, item in reversed(fresh):
            title = _juicewrld_safe(item.get("name"), "Song tracker update", 200)
            action = _juicewrld_safe(item.get("action"), "Updated", 100)
            editor = _juicewrld_safe(item.get("user"), "Unknown", 100)

            embed = discord.Embed(
                title=f"🎵 Juice WRLD Tracker Update: {title}",
                description=f"**Action:** {action}\n**Updated by:** {editor}",
                color=JUICE_COLOR
            )

            changed_fields = item.get("fields")
            if isinstance(changed_fields, list) and changed_fields:
                embed.add_field(
                    name="Changed fields",
                    value=", ".join(
                        _juicewrld_safe(field, limit=100)
                        for field in changed_fields
                    )[:1000],
                    inline=False
                )

            notes = item.get("notes")
            if notes:
                embed.add_field(
                    name="Notes",
                    value=_juicewrld_safe(notes, limit=1000),
                    inline=False
                )

            timestamp = item.get("timestamp")
            if timestamp:
                try:
                    parsed = datetime.fromisoformat(
                        str(timestamp).replace("Z", "+00:00")
                    )
                    embed.timestamp = parsed
                except (ValueError, TypeError):
                    pass

            link = item.get("link")
            if (
                isinstance(link, str)
                and link.startswith("https://juicewrldapi.com/")
            ):
                embed.url = link

            embed.set_footer(
                text="Juice WRLD API • Tracker event (not necessarily a new release)"
            )

            await channel.send(
                embed=embed,
                allowed_mentions=discord.AllowedMentions.none()
            )
            _juicewrld_seen.add(event_id)
            _save_juicewrld_state()
            print(f"[JuiceWRLD] Posted tracker event {event_id}: {title}")

    except asyncio.CancelledError:
        raise
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        print(f"[JuiceWRLD] API request failed: {exc}")
    except (ValueError, discord.HTTPException, OSError, TypeError) as exc:
        print(f"[JuiceWRLD] Tracker task failed: {exc}")


@juicewrld_tracker_task.before_loop
async def _before_juicewrld_tracker_task():
    await bot.wait_until_ready()


# =============================================================================
# 9. HELP SYSTEM (multi-page command list)
# =============================================================================
# To add or edit a page, change an entry in HELP_PAGES. Everything else
# (dropdown, buttons, page counter, `~help <topic>`, `/help`) updates itself.
#
#   key      - short name used by `~help <key>` and the /help page choice
#   aliases  - extra words that also jump to this page (e.g. `~help volume`)
#   emoji    - icon shown in the title and dropdown
#   title    - page name
#   summary  - one-line blurb for the dropdown (max 100 characters)
#   color    - embed colour
#   description / fields - the page content (field values max 1024 characters)
HELP_PAGES: list[dict] = [
    {
        "key": "home",
        "aliases": ("overview", "start", "main"),
        "emoji": "🏠",
        "title": "Overview",
        "summary": "What VaultBot does and where to find everything",
        "color": BRAND_COLOR,
        "description": (
            "VaultBot plays music in your voice channels, looks up Juice WRLD "
            "catalog info, and gives staff tools for posting releases and "
            "announcements.\n\n"
            f"**Prefix commands** start with `{PREFIX}` and **slash commands** "
            "start with `/`.\n"
            "Use the ◀ ▶ buttons or the dropdown below to browse every feature."
        ),
        "fields": [
            (
                "🎵 Music Player — pages 2 & 3",
                "Play songs by name or link, or upload your own audio file. "
                "Comes with a queue, loop modes, volume control, and more.",
            ),
            (
                "🧃 Juice WRLD Tools — page 4",
                "Search the Juice WRLD catalog and get automatic tracker "
                "updates posted to a channel.",
            ),
            (
                "📣 Server Tools — page 5",
                "Slash commands for posting music releases, sending "
                "announcements, and checking the bot's latency.",
            ),
            (
                "💡 Tips & Troubleshooting — page 6",
                "Requirements, supported sources, and fixes for common problems.",
            ),
        ],
    },
    {
        "key": "music",
        "aliases": ("play", "queue", "skip", "stop", "playback"),
        "emoji": "🎵",
        "title": "Music: Play & Queue",
        "summary": "Play songs, upload files, view the queue, skip, stop",
        "color": BRAND_COLOR,
        "description": (
            "Join a voice channel, then use these commands. The bot joins "
            "you automatically and starts playing as soon as a song is ready."
        ),
        "fields": [
            (
                f"`{PREFIX}play <song or URL>`  •  `{PREFIX}p`",
                "Searches for the song (or opens the link you give it), "
                "downloads the audio, and adds it to the queue. If nothing is "
                "playing, it starts right away.",
            ),
            (
                f"`{PREFIX}play` + an attached file",
                "Plays an audio file you upload straight from Discord. "
                f"Supports MP3, WAV, Opus, OGG, M4A, and FLAC up to {MAX_UPLOAD_MB} MB.",
            ),
            (
                f"`{PREFIX}queue`  •  `{PREFIX}q`",
                "Shows what's playing now, the next 10 songs in line, how many "
                "more are queued, and the current loop mode.",
            ),
            (
                f"`{PREFIX}skip`  •  `{PREFIX}s`",
                "Skips the current song and moves on to the next one in the queue.",
            ),
            (
                f"`{PREFIX}stop`",
                "Stops playback, clears the whole queue, and disconnects the bot "
                "from voice.",
            ),
        ],
    },
    {
        "key": "controls",
        "aliases": ("volume", "loop", "pause", "resume", "nowplaying", "leave"),
        "emoji": "🎛️",
        "title": "Music: Controls & Loop",
        "summary": "Pause, resume, volume, now playing, loop modes, leave",
        "color": BRAND_COLOR,
        "description": "Fine-tune playback while a song is going.",
        "fields": [
            (
                f"`{PREFIX}pause`  /  `{PREFIX}resume`  •  `{PREFIX}unpause`",
                "Pause the current song and pick it back up right where you left off.",
            ),
            (
                f"`{PREFIX}volume <0-100>`  •  `{PREFIX}vol`",
                "Sets the playback volume for this server. "
                f"The default is {int(DEFAULT_VOLUME * 100)}%. "
                "The change applies instantly, even mid-song.",
            ),
            (
                f"`{PREFIX}nowplaying`  •  `{PREFIX}np`  •  `{PREFIX}current`",
                "Shows the current song with a link, its duration, and who requested it.",
            ),
            (
                f"`{PREFIX}loop <song|queue|off>`",
                "**song** — repeats the current track\n"
                "**queue** — cycles through the whole queue forever\n"
                "**off** — plays normally (default)\n"
                "*Also accepts `track`, `all`, and `none`.* "
                f"Using `{PREFIX}skip` while on **song** loop turns the loop off.",
            ),
            (
                f"`{PREFIX}leave`  •  `{PREFIX}disconnect`",
                "Clears the queue and disconnects the bot from the voice channel.",
            ),
        ],
    },
    {
        "key": "juice",
        "aliases": ("jw", "jwsong", "tracker", "juicewrld"),
        "emoji": "🧃",
        "title": "Juice WRLD Tools",
        "summary": "Catalog search and automatic tracker updates",
        "color": JUICE_COLOR,
        "description": (
            "Catalog features powered by the Juice WRLD API. Information only — "
            "these tools don't play audio."
        ),
        "fields": [
            (
                f"`{PREFIX}jwsong <search>`",
                "Searches the Juice WRLD catalog, opens a detailed song embed, "
                "lets you choose alternate or related matches from a dropdown, and "
                "shows detailed metadata and lets you search the API file browser for matching audio files.\n"
                f"Example: `{PREFIX}jwsong lucid dreams`",
            ),
            (
                "📡 Automatic Tracker Feed",
                "When the bot owner picks a channel for it, VaultBot checks the "
                "tracker on a timer and posts every new update: the song name, "
                "what happened, who updated it, which fields changed, and any "
                "notes.\n"
                "*A tracker update isn't necessarily a new release.*",
            ),
        ],
    },
    {
        "key": "slash",
        "aliases": ("server", "admin", "staff", "announce", "musicdrop", "ping"),
        "emoji": "📣",
        "title": "Server Tools (Slash Commands)",
        "summary": "/musicdrop, /announce, /ping and /help",
        "color": BRAND_COLOR,
        "description": "Type `/` in the message box to see these. Some are staff-only.",
        "fields": [
            (
                "`/musicdrop`  🔒 Manage Messages",
                "Posts a polished music release in a channel of your choice: "
                "title, description, square artwork, an artist footer, and "
                "**mp3** / **wav** download buttons.\n"
                "Options: `channel`, `title`, `description`, `artist`, "
                "`artwork_url`, `mp3_url`, `wav_url`.\n"
                "Links must be HTTPS, and the bot needs Send Messages + Embed "
                "Links in the target channel.",
            ),
            (
                "`/announce`  🔒 Administrator",
                "Sends an announcement to any channel. Set `ping_everyone` to "
                "True to include an @everyone ping (off by default).",
            ),
            (
                "`/ping`",
                "Checks that the bot is online and shows its latency. "
                "Only you can see the reply.",
            ),
            (
                "`/help`",
                "Opens this command list. You can jump straight to a section "
                f"too, or use `{PREFIX}help` for the same menu.",
            ),
        ],
    },
    {
        "key": "tips",
        "aliases": ("faq", "troubleshooting", "problems", "info"),
        "emoji": "💡",
        "title": "Tips & Troubleshooting",
        "summary": "Requirements, supported sources, and common fixes",
        "color": BRAND_COLOR,
        "description": "Quick answers if something isn't working the way you expect.",
        "fields": [
            (
                "🔊 Before you play",
                "Join a voice channel first — the bot follows you in. It needs "
                "**Connect** and **Speak** permissions, plus **Send Messages** "
                "and **Embed Links** in the text channel.",
            ),
            (
                "🔗 What you can play",
                "Song names (searched automatically), public links that yt-dlp "
                "supports such as YouTube and SoundCloud, and uploaded audio "
                "files. Each command adds a single track, not a full playlist.\n"
                "Spotify and Apple Music links can't be streamed directly — use "
                "the song title or a YouTube/SoundCloud link instead.",
            ),
            (
                "⚠️ If something goes wrong",
                "**\"YouTube is blocking this bot\"** — some videos or hosting IPs "
                "require sign-in. This build tries alternate public clients, but "
                "there is no guaranteed cookie-free fix. Try another public link.\n"
                "**\"Couldn't play\"** — try the song again or use a different link.\n"
                "**\"Missing argument\"** — check the usage shown for that "
                "command on the earlier pages.",
            ),
            (
                "🧭 Using this menu",
                "Only the person who opened the menu can use its buttons. It "
                f"stops responding after 2 minutes, so just run `{PREFIX}help` "
                "or `/help` again. Jump to a page directly, e.g. "
                f"`{PREFIX}help music` or `{PREFIX}help juice`.",
            ),
        ],
    },
]


def find_help_page(topic: Optional[str]) -> int:
    """Turn a topic like 'music', 'volume' or '3' into a page index (default: 0)."""
    topic = (topic or "").strip().lower()
    if not topic:
        return 0
    if topic.isdigit():
        number = int(topic)
        return number - 1 if 1 <= number <= len(HELP_PAGES) else 0
    for index, page in enumerate(HELP_PAGES):
        if topic == page["key"] or topic in page["aliases"]:
            return index
    return 0


def build_help_embed(index: int) -> discord.Embed:
    page = HELP_PAGES[index]
    embed = discord.Embed(
        title=f"{page['emoji']} VaultBot — {page['title']}",
        description=page["description"],
        color=page["color"],
    )
    for name, value in page["fields"]:
        embed.add_field(name=name, value=value, inline=False)

    # Live status line on the Juice WRLD page.
    if page["key"] == "juice":
        if JUICEWRLD_CHANNEL_ID:
            status = f"✅ Enabled — checks every {JUICEWRLD_POLL_MINUTES} minute(s)."
        else:
            status = "❌ Not enabled on this bot (the owner hasn't set a channel)."
        embed.add_field(name="Tracker status", value=status, inline=False)

    embed.set_footer(
        text=f"Page {index + 1} of {len(HELP_PAGES)}  •  Prefix: {PREFIX}  •  Slash: /"
    )
    return embed


_HELP_SELECT_OPTIONS = [
    discord.SelectOption(
        label=f"{i + 1}. {page['title']}"[:100],
        value=str(i),
        description=page["summary"][:100],
        emoji=page["emoji"],
    )
    for i, page in enumerate(HELP_PAGES)
]


class HelpView(discord.ui.View):
    """Interactive paginator: Previous / Next buttons plus a jump-to-page dropdown."""

    def __init__(self, author_id: int, page: int = 0):
        super().__init__(timeout=120)
        self.author_id = author_id
        self.page = page
        self.message: Optional[discord.Message] = None
        self._sync()

    def _sync(self) -> None:
        """Update button states and the page counter to match the current page."""
        last = len(HELP_PAGES) - 1
        self.previous_button.disabled = self.page <= 0
        self.next_button.disabled = self.page >= last
        self.indicator.label = f"{self.page + 1}/{len(HELP_PAGES)}"

    async def _show(self, interaction: discord.Interaction) -> None:
        self._sync()
        await interaction.response.edit_message(
            embed=build_help_embed(self.page), view=self
        )

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id == self.author_id:
            return True
        await interaction.response.send_message(
            f"Run `{PREFIX}help` or `/help` to open your own copy of the command list.",
            ephemeral=True,
        )
        return False

    async def on_timeout(self) -> None:
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass

    @discord.ui.button(label="Previous", emoji="◀️", style=discord.ButtonStyle.secondary)
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = max(0, self.page - 1)
        await self._show(interaction)

    @discord.ui.button(label="1/1", style=discord.ButtonStyle.secondary, disabled=True)
    async def indicator(self, interaction: discord.Interaction, button: discord.ui.Button):
        pass  # Display-only page counter.

    @discord.ui.button(label="Next", emoji="▶️", style=discord.ButtonStyle.primary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = min(len(HELP_PAGES) - 1, self.page + 1)
        await self._show(interaction)

    @discord.ui.select(
        placeholder="Jump to a page…",
        options=_HELP_SELECT_OPTIONS,
        min_values=1,
        max_values=1,
        row=1,
    )
    async def page_select(self, interaction: discord.Interaction, select: discord.ui.Select):
        self.page = int(select.values[0])
        await self._show(interaction)


async def send_help(ctx: commands.Context, topic: str = "") -> None:
    view = HelpView(ctx.author.id, find_help_page(topic))
    view.message = await ctx.send(embed=build_help_embed(view.page), view=view)


# =============================================================================
# 10. EVENTS & ERROR HANDLING
# =============================================================================
@bot.event
async def on_ready():
    print(f"Bot is online as {bot.user}")
    try:
        synced = await bot.tree.sync()
        print(f"Synced {len(synced)} slash command(s)")
    except Exception as error:
        print(f"Command sync failed: {error}")

    _load_juicewrld_state()
    if JUICEWRLD_CHANNEL_ID:
        if not juicewrld_tracker_task.is_running():
            juicewrld_tracker_task.start()
            print(
                f"[JuiceWRLD] Tracker notifications enabled; "
                f"channel ID={JUICEWRLD_CHANNEL_ID}, "
                f"poll interval={JUICEWRLD_POLL_MINUTES} minute(s)."
            )
    else:
        print("[JuiceWRLD] JUICEWRLD_CHANNEL_ID is not set; notifications disabled.")


@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError):
    if isinstance(error, commands.CommandNotFound):
        return
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(f"Missing argument: `{error.param.name}`. Use `{PREFIX}help` to see examples.")
        return
    if isinstance(error, commands.CommandOnCooldown):
        await ctx.send("Please wait a moment before using that command again.")
        return
    if isinstance(error, commands.CommandError):
        await ctx.send(str(error))
        return
    print(f"Command error: {error}")
    await ctx.send("Something went wrong while running that command. Check the bot console.")


# ---- Random auto-replies (no command needed) ----
# Occasionally replies to normal chat messages or image uploads. It only fires
# when a keyword/image matches, a dice roll passes, and the channel cooldown
# has expired. Edit the dictionaries below to change what it says.
AUTO_REPLY_ENABLED = True
AUTO_REPLY_CHANCE = 0.4          # chance to reply WHEN something matches (0.0 - 1.0)
AUTO_REPLY_COOLDOWN = 60         # minimum seconds between auto-replies per channel

AUTO_REPLY_KEYWORDS = {
    ("hello", "hi", "hey", "yo"): ["Hey there!", "Yo!", "Well hello 👋"],
    ("gm", "good morning"): ["Morning! ☀️", "gm gm"],
    ("gn", "good night"): ["Night! 🌙", "Sleep well!"],
    ("lol", "lmao", "haha"): ["😂", "lmaooo", "Okay that was funny"],
    ("game", "play", "fortnite"): ["I'm down 🎮", "Count me in", "Run it"],
}
AUTO_REPLY_IMAGES = ["Nice pic!", "Ooh, what's this? 👀", "Saving this one 📸"]

_auto_reply_last_sent: dict[int, float] = {}


def _pick_auto_reply(message: discord.Message) -> Optional[str]:
    """Return a reply that fits the message, or None if nothing matches."""
    if any(
        attachment.content_type and attachment.content_type.startswith("image/")
        for attachment in message.attachments
    ):
        return random.choice(AUTO_REPLY_IMAGES)

    text = message.content.lower()
    for keywords, replies in AUTO_REPLY_KEYWORDS.items():
        if any(re.search(rf"\b{re.escape(word)}\b", text) for word in keywords):
            return random.choice(replies)
    return None


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    # Commands are handled normally and never get an auto-reply.
    if message.content.startswith(PREFIX):
        await bot.process_commands(message)
        return

    if AUTO_REPLY_ENABLED and message.guild:
        reply = _pick_auto_reply(message)
        now = time.time()
        last_sent = _auto_reply_last_sent.get(message.channel.id, 0.0)
        if (
            reply
            and random.random() < AUTO_REPLY_CHANCE
            and now - last_sent > AUTO_REPLY_COOLDOWN
        ):
            try:
                await message.channel.send(reply)
                _auto_reply_last_sent[message.channel.id] = now
            except discord.HTTPException as error:
                print(f"[AutoReply] Could not send reply: {error}")

    await bot.process_commands(message)


# =============================================================================
# 11. PREFIX COMMANDS
# =============================================================================

# ---- Help ----

@bot.command(name="help", aliases=["commands", "cmds", "h"])
async def help_command(ctx: commands.Context, *, topic: str = ""):
    """Open the multi-page command list. Optional topic, e.g. ~help music"""
    await send_help(ctx, topic)


@bot.command(name="musichelp", aliases=["mhelp"])
async def musichelp_command(ctx: commands.Context):
    """Open the command list on the music page."""
    await send_help(ctx, "music")


# ---- Music ----

@bot.command(name="play", aliases=["p"])
async def play_command(ctx: commands.Context, *, query: str = ""):
    """Play a song name, supported URL, or attached MP3/WAV/Opus file."""
    if not ctx.guild:
        return await ctx.send("Use this command inside a server.")
    diag(f"~play received: {query[:80]!r}")
    try:
        await ensure_voice(ctx)
    except Exception as exc:
        return await ctx.send(str(exc))
    diag("joined voice, continuing")

    attachment = ctx.message.attachments[0] if ctx.message.attachments else None
    if attachment:
        suffix = Path(urlparse(attachment.filename).path).suffix.lower()
        if suffix not in ALLOWED_UPLOAD_SUFFIXES:
            return await ctx.send("Attach an MP3, WAV, Opus, OGG, M4A, or FLAC audio file.")
        if attachment.size > MAX_UPLOAD_MB * 1024 * 1024:
            return await ctx.send(f"That file is over the bot's {MAX_UPLOAD_MB} MB safety limit.")
        if not query:
            query = attachment.filename
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp:
                path = temp.name
            await attachment.save(path)
            TEMP_FILES.add(path)
            track = Track(
                title=attachment.filename,
                webpage_url=attachment.url,
                requester=ctx.author.display_name,
                local_path=path,
                source_name="uploaded file",
            )
            await queue_track(ctx, track)
        except Exception as exc:
            clean_temp_file(locals().get("path"))
            await ctx.send(f"Couldn't save that upload: {exc}")
        return

    if not query.strip():
        return await ctx.send(
            f"Usage: `{PREFIX}play song name or URL` — or attach an audio file to `{PREFIX}play`."
        )

    # Spotify and Apple Music links may provide metadata or a playable match,
    # but direct catalog audio streaming is not guaranteed or bypassed here.
    await ctx.typing()
    try:
        track = await extract_track(query.strip(), ctx.author.display_name)
        diag(f"track ready: {track.title!r}, queueing")
        await queue_track(ctx, track)
    except Exception as exc:
        error_text = str(exc)
        print(f"yt-dlp error: {error_text}", flush=True)
        lower_error = error_text.lower()
        if (
            ("confirm you" in lower_error and "not a bot" in lower_error)
            or "sign in to confirm" in lower_error
            or "login required" in lower_error
        ):
            await ctx.send(
                "⚠️ YouTube requires sign-in for this request. VaultBot is configured "
                "to try alternate public YouTube clients without cookies, but YouTube "
                "can still block particular tracks or this host. Try a different "
                "public YouTube URL or a SoundCloud link."
            )
        else:
            await ctx.send(
                "I couldn't find a playable stream for that request. "
                "For Spotify/Apple Music, direct playback may not be available; "
                "try a public YouTube/SoundCloud URL or a song title.\n"
                f"Details: {error_text[:250]}"
            )


@bot.command(name="queue", aliases=["q"])
async def queue_command(ctx: commands.Context):
    if not ctx.guild:
        return
    guild_id = ctx.guild.id
    tracks = list(get_queue(guild_id))
    current = current_tracks.get(guild_id)
    embed = discord.Embed(title="🎵 Music Queue", color=BRAND_COLOR)
    if current:
        embed.add_field(
            name="Now playing",
            value=f"[{discord.utils.escape_markdown(current.title)}]({current.webpage_url})",
            inline=False,
        )
    if tracks:
        lines = [
            f"`{i}.` [{discord.utils.escape_markdown(t.title)}]({t.webpage_url}) "
            f"— `{format_duration(t.duration)}`"
            for i, t in enumerate(tracks[:10], start=1)
        ]
        embed.add_field(name=f"Up next ({len(tracks)} queued)", value="\n".join(lines), inline=False)
        if len(tracks) > 10:
            embed.set_footer(text=f"And {len(tracks) - 10} more track(s)")
    elif not current:
        embed.description = "The queue is empty."
    embed.add_field(name="Loop mode", value=loop_modes.get(guild_id, "off"), inline=True)
    await ctx.send(embed=embed)


@bot.command(name="skip", aliases=["s"])
async def skip_command(ctx: commands.Context):
    voice = ctx.guild.voice_client if ctx.guild else None
    if not voice or not (voice.is_playing() or voice.is_paused()):
        return await ctx.send("Nothing is playing right now.")
    loop_modes[ctx.guild.id] = "off" if loop_modes.get(ctx.guild.id) == "song" else loop_modes.get(ctx.guild.id, "off")
    voice.stop()
    await ctx.send("⏭️ Skipped the current song.")


@bot.command(name="stop")
async def stop_command(ctx: commands.Context):
    if not ctx.guild:
        return
    voice = ctx.guild.voice_client
    get_queue(ctx.guild.id).clear()
    loop_modes[ctx.guild.id] = "off"
    current = current_tracks.pop(ctx.guild.id, None)
    if current:
        clean_temp_file(current.local_path)
    if voice:
        if voice.is_playing() or voice.is_paused():
            voice.stop()
        await voice.disconnect()
    await ctx.send("⏹️ Playback stopped and the queue was cleared.")


@bot.command(name="pause")
async def pause_command(ctx: commands.Context):
    voice = ctx.guild.voice_client if ctx.guild else None
    if voice and voice.is_playing():
        voice.pause()
        await ctx.send("⏸️ Paused.")
    else:
        await ctx.send("Nothing is currently playing.")


@bot.command(name="resume", aliases=["unpause"])
async def resume_command(ctx: commands.Context):
    voice = ctx.guild.voice_client if ctx.guild else None
    if voice and voice.is_paused():
        voice.resume()
        await ctx.send("▶️ Resumed.")
    else:
        await ctx.send("Playback isn't paused.")


@bot.command(name="volume", aliases=["vol"])
async def volume_command(ctx: commands.Context, percent: int):
    if not ctx.guild:
        return
    if not 0 <= percent <= 100:
        return await ctx.send("Choose a volume from 0 to 100.")
    volumes[ctx.guild.id] = percent / 100
    voice = ctx.guild.voice_client
    if voice and voice.source and isinstance(voice.source, discord.PCMVolumeTransformer):
        voice.source.volume = percent / 100
    await ctx.send(f"🔊 Volume set to **{percent}%**.")


@bot.command(name="nowplaying", aliases=["np", "current"])
async def nowplaying_command(ctx: commands.Context):
    track = current_tracks.get(ctx.guild.id) if ctx.guild else None
    if not track:
        return await ctx.send("Nothing is playing right now.")
    await ctx.send(embed=build_now_playing_embed(track))


@bot.command(name="loop")
async def loop_command(ctx: commands.Context, mode: str = "off"):
    if not ctx.guild:
        return
    mode = mode.lower()
    aliases = {"song": "song", "track": "song", "queue": "queue", "all": "queue", "off": "off", "none": "off"}
    if mode not in aliases:
        return await ctx.send(f"Usage: `{PREFIX}loop song`, `{PREFIX}loop queue`, or `{PREFIX}loop off`.")
    loop_modes[ctx.guild.id] = aliases[mode]
    await ctx.send(f"🔁 Loop mode set to **{aliases[mode]}**.")


@bot.command(name="leave", aliases=["disconnect"])
async def leave_command(ctx: commands.Context):
    if not ctx.guild or not ctx.guild.voice_client:
        return await ctx.send("I'm not connected to a voice channel.")
    guild_id = ctx.guild.id
    voice = ctx.guild.voice_client
    get_queue(guild_id).clear()
    loop_modes[guild_id] = "off"
    current = current_tracks.pop(guild_id, None)
    if current:
        clean_temp_file(current.local_path)
    if voice.is_playing() or voice.is_paused():
        voice.stop()
    await voice.disconnect()
    await ctx.send("👋 Disconnected from voice.")


# ---- Juice WRLD ----

def _jw_plain(value, fallback="", limit=900):
    """Convert API metadata to safe, readable text for Discord embeds."""
    if value is None:
        return fallback
    if isinstance(value, (list, tuple, set)):
        parts = [_jw_plain(item, "", 250) for item in value]
        text = ", ".join(part for part in parts if part)
    elif isinstance(value, dict):
        parts = [f"{key}: {_jw_plain(item, '', 200)}" for key, item in value.items()
                 if item not in (None, "", [], {})]
        text = "\n".join(parts)
    else:
        text = str(value).strip()
    text = text.replace("@", "@\u200b").strip()
    return text[:limit]


def _jw_has_value(value):
    return value not in (None, "", [], {}, False)


def _jw_song_score(song, query):
    """Rank exact title and alternate-title matches above broad search matches."""
    query_norm = "".join(ch.casefold() for ch in query if ch.isalnum())
    names = [song.get("name", "")]
    titles = song.get("track_titles") or []
    if isinstance(titles, str):
        titles = [titles]
    names.extend(titles if isinstance(titles, list) else [])
    normalized = ["".join(ch.casefold() for ch in str(name) if ch.isalnum())
                  for name in names if name]
    if query_norm and query_norm in normalized:
        return 100
    if query_norm and any(query_norm in name for name in normalized):
        return 80
    if query_norm and any(name in query_norm for name in normalized if name):
        return 60
    return 10


def _build_juicewrld_song_embed(song, query=""):
    """Build a detailed song page from whichever metadata the API supplies."""
    name = _juicewrld_safe(song.get("name"), "Unknown title", 250)
    embed = discord.Embed(title=f"🎵 {name}", color=JUICE_COLOR)

    category = _jw_plain(song.get("category"), "Unknown")
    artist = _jw_plain(song.get("credited_artists"), "Juice WRLD")
    summary = [f"**Artist:** {artist}", f"**Category:** {category}"]
    era = song.get("era")
    if isinstance(era, dict):
        era_name = _jw_plain(era.get("name"), "")
        era_period = _jw_plain(era.get("time_frame"), "")
        if era_name:
            summary.append(f"**Era:** {era_name}" + (f" — {era_period}" if era_period else ""))
    elif _jw_has_value(era):
        summary.append(f"**Era:** {_jw_plain(era)}")
    album = _jw_plain(song.get("album"), "")
    if album:
        summary.append(f"**Album:** {album}")
    if _jw_has_value(song.get("version_title")):
        summary.append(f"**Version:** {_jw_plain(song.get('version_title'), limit=150)}")
    embed.description = "\n".join(summary)[:4000]

    # The API returns a large catalogue object, but Discord embeds have strict
    # field and total-character limits. Include as much non-empty metadata as fits.
    fields = [
        ("Alternate titles", song.get("track_titles")),
        ("Producers", song.get("producers")),
        ("Engineers", song.get("engineers")),
        ("Recording locations", song.get("recording_locations")),
        ("Recording dates", song.get("record_dates")),
        ("Track length", song.get("length")),
        ("Release date", song.get("release_date")),
        ("Preview date", song.get("preview_date")),
        ("Other dates", song.get("dates")),
        ("Leak information", "\n".join(
            f"{label}: {_jw_plain(song.get(key), '', 250)}"
            for label, key in (("Date leaked", "date_leaked"), ("Leak type", "leak_type"))
            if _jw_has_value(song.get(key))
        )),
        ("File names", song.get("file_names")),
        ("Instrumentals", song.get("instrumentals") or song.get("instrumental_names")),
        ("Session titles", song.get("session_titles")),
        ("Session tracking", song.get("session_tracking")),
        ("Snippets", song.get("snippets")),
        ("Additional information", song.get("additional_information")),
        ("Notes", song.get("notes")),
        ("Group-buy information", song.get("groupbuy_info")),
        ("Version records", song.get("versions")),
    ]

    used_chars = len(embed.title or "") + len(embed.description or "")
    for field_name, raw_value in fields:
        if not _jw_has_value(raw_value):
            continue
        value = _jw_plain(raw_value, "", 900)
        if not value:
            continue
        if used_chars + len(field_name) + len(value) > 5700 or len(embed.fields) >= 24:
            break
        embed.add_field(name=field_name, value=value, inline=False)
        used_chars += len(field_name) + len(value)

    if song.get("lyrics"):
        lyrics = _jw_plain(song.get("lyrics"), "", 650)
        if lyrics and used_chars + len("Lyrics preview") + len(lyrics) < 5900 and len(embed.fields) < 25:
            embed.add_field(name="Lyrics preview", value=lyrics + ("…" if len(str(song.get("lyrics", ""))) > 650 else ""), inline=False)
            used_chars += len(lyrics)

    image_url = song.get("image_url")
    if isinstance(image_url, str) and image_url.strip():
        if image_url.startswith("/"):
            image_url = "https://juicewrldapi.com" + image_url
        if image_url.startswith("https://") or image_url.startswith("http://"):
            embed.set_thumbnail(url=image_url)

    catalog_id = song.get("public_id") or song.get("id")
    footer_parts = []
    if catalog_id is not None:
        footer_parts.append(f"Catalog ID: {catalog_id}")
    if song.get("id") is not None:
        footer_parts.append(f"Database ID: {song.get('id')}")
    footer_parts.append("Metadata: Juice WRLD Community API")
    embed.set_footer(text=" • ".join(footer_parts)[:2048])
    return embed


def _jw_file_path(item):
    """Extract a path relative to the API's compilation root."""
    if isinstance(item, str):
        # A bare name is only useful if the API itself returned a relative path.
        return item.strip() if "/" in item.strip() else None
    if not isinstance(item, dict):
        return None
    for key in ("path", "file_path", "relative_path", "filepath", "key"):
        value = item.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    # Some listings return `id` as the relative path.
    value = item.get("id")
    if isinstance(value, str) and "/" in value:
        return value.strip()
    return None


def _jw_file_is_directory(item):
    if not isinstance(item, dict):
        return False
    if item.get("is_directory") is True or item.get("is_dir") is True or item.get("directory") is True:
        return True
    return str(item.get("type", "")).casefold() in {"directory", "dir", "folder"}


def _jw_audio_candidates(items, query, limit=25):
    """Filter API file-list entries to likely matching audio files."""
    suffixes = {".mp3", ".wav", ".m4a", ".flac", ".ogg", ".opus", ".aac"}
    q = "".join(ch.casefold() for ch in query if ch.isalnum())
    matches = []
    seen = set()
    for item in items:
        if _jw_file_is_directory(item):
            continue
        path = _jw_file_path(item)
        if not path or Path(path).suffix.casefold() not in suffixes:
            continue
        normalized = "".join(ch.casefold() for ch in Path(path).stem if ch.isalnum())
        if q and q not in normalized and normalized not in q:
            # Also allow one meaningful word to match if title variants differ.
            words = ["".join(ch.casefold() for ch in w if ch.isalnum())
                     for w in query.split()]
            words = [w for w in words if len(w) >= 4]
            if not words or not any(w in normalized for w in words):
                continue
        if path in seen:
            continue
        seen.add(path)
        # Exact/close filename matches first.
        score = 0 if q == normalized else (1 if q in normalized else 2)
        matches.append((score, path, item))
    matches.sort(key=lambda row: (row[0], row[1].casefold()))
    return [(path, item) for _, path, item in matches[:limit]]


def _jw_download_url(path, compressed=True):
    """Build a URL to the API's documented file endpoint for an API-listed path."""
    from urllib.parse import quote
    endpoint = "files/download-compressed/" if compressed else "files/download/"
    params = {"path": path}
    if compressed:
        params["bitrate"] = "160k"
    return f"{JUICEWRLD_API_BASE}/{endpoint}?{urlencode(params)}"


class JuiceWRLDFileSelect(discord.ui.Select):
    def __init__(self, files, owner_id, song_title):
        self.files = files
        self.owner_id = owner_id
        self.song_title = song_title
        options = []
        for index, (path, item) in enumerate(files[:25]):
            filename = Path(path).name
            size = item.get("size") if isinstance(item, dict) else None
            description = str(size) + " bytes" if isinstance(size, (int, float)) else str(Path(path).parent)
            options.append(discord.SelectOption(
                label=filename[:100] or f"Audio file {index + 1}",
                value=str(index),
                description=description[:100] or "Audio file"
            ))
        super().__init__(
            placeholder="Choose the matching audio file…",
            min_values=1,
            max_values=1,
            options=options
        )

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "Only the person who requested the search can choose a file.",
                ephemeral=True
            )
            return
        try:
            index = int(self.values[0])
            path, metadata = self.files[index]
            size = metadata.get("size") if isinstance(metadata, dict) else None
            embed = discord.Embed(
                title="⬇️ Audio download",
                description=(f"**Song:** {discord.utils.escape_markdown(self.song_title[:200])}\n"
                            f"**File:** `{Path(path).name[:200]}`\n"
                            f"**Path:** `{path[:800]}`\n\n"
                            "Use the button below to download the selected file from the API."),
                color=JUICE_COLOR
            )
            if isinstance(size, (int, float)):
                embed.add_field(name="File size", value=f"{size / (1024 * 1024):.2f} MB", inline=True)
            view = discord.ui.View(timeout=180)
            view.add_item(discord.ui.Button(
                label="Download MP3",
                style=discord.ButtonStyle.link,
                url=_jw_download_url(path, compressed=True),
                emoji="⬇️"
            ))
            view.add_item(discord.ui.Button(
                label="Original audio file",
                style=discord.ButtonStyle.link,
                url=_jw_download_url(path, compressed=False),
                emoji="🎧"
            ))
            await interaction.response.edit_message(embed=embed, view=view)
        except (IndexError, ValueError, TypeError) as exc:
            print(f"[JuiceWRLD] Invalid file selection: {exc}")
            await interaction.response.send_message(
                "That file selection was invalid. Run the song search again.",
                ephemeral=True
            )


class JuiceWRLDFindFilesButton(discord.ui.Button):
    def __init__(self, owner_id, song):
        super().__init__(
            label="Find downloadable files",
            style=discord.ButtonStyle.primary,
            emoji="🔎"
        )
        self.owner_id = owner_id
        self.song = song

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "Only the person who ran the search can find files for this song.",
                ephemeral=True
            )
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        title = _juicewrld_safe(self.song.get("name"), "Juice WRLD song", 200)
        try:
            # Search the API's actual file browser; song metadata alone doesn't
            # guarantee that a file path exists for the catalog entry.
            payload = await _juicewrld_api_get("/files/browse/", params={"search": title})
            entries = _juicewrld_items(payload)
            matches = _jw_audio_candidates(entries, title)

            # Some API deployments provide search only on browse, while others
            # return a directory summary. Fall back to the flat file list.
            if not matches:
                payload = await _juicewrld_api_get("/files/list-all/")
                entries = _juicewrld_items(payload)
                matches = _jw_audio_candidates(entries, title)

            if not matches:
                await interaction.followup.send(
                    f"I couldn't find an audio file matching **{title}** in the API file listing. "
                    "The catalog entry may not have an associated file, or the file browser may use a different path format.",
                    ephemeral=True
                )
                return

            view = discord.ui.View(timeout=180)
            view.add_item(JuiceWRLDFileSelect(matches, self.owner_id, title))
            await interaction.followup.send(
                content=f"Found **{len(matches)}** matching audio file(s) for **{title}**. Choose the exact file:",
                view=view,
                ephemeral=True
            )
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError) as exc:
            print(f"[JuiceWRLD] File search failed: {exc}")
            await interaction.followup.send(
                "The Juice WRLD file browser couldn't be reached. Try again later.",
                ephemeral=True
            )


class JuiceWRLDSongSelect(discord.ui.Select):
    def __init__(self, songs, owner_id):
        self.owner_id = owner_id
        options = []
        for song in songs[:25]:
            song_id = song.get("id")
            if song_id is None:
                continue
            title = _juicewrld_safe(song.get("name"), "Unknown title", 75)
            category = _juicewrld_safe(song.get("category"), "Unknown category", 35)
            version = song.get("version_title")
            if version:
                title = f"{title} — {_juicewrld_safe(version, limit=20)}"[:100]
            era = song.get("era")
            if isinstance(era, dict):
                era = era.get("name")
            description_parts = [category]
            if era:
                description_parts.append(_juicewrld_safe(era, limit=30))
            if song.get("album"):
                description_parts.append(_juicewrld_safe(song.get("album"), limit=30))
            options.append(discord.SelectOption(
                label=title[:100],
                value=str(song_id),
                description=" • ".join(description_parts)[:100]
            ))
        super().__init__(
            placeholder="Choose another matching song or version…",
            min_values=1,
            max_values=1,
            options=options
        )

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "Only the person who ran the search can change this song page.",
                ephemeral=True
            )
            return

        try:
            song_id = int(self.values[0])
            payload = await _juicewrld_api_get(f"/songs/{song_id}/")
            if not isinstance(payload, dict) or not payload.get("id"):
                raise ValueError("API returned no song detail object")
            selected = next((item for item in self.view.songs
                             if str(item.get("id")) == str(song_id)), {})
            merged = dict(selected)
            merged.update(payload)
            embed = _build_juicewrld_song_embed(merged)
            # Rebuild the view so the download button corresponds to the song
            # currently displayed, not the song that was selected previously.
            new_view = JuiceWRLDSongView(self.view.songs, self.owner_id, merged)
            await interaction.response.edit_message(embed=embed, view=new_view)
            new_view.message = interaction.message
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError) as exc:
            print(f"[JuiceWRLD] Detail lookup failed: {exc}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "I couldn't load that song's full details. Please try selecting it again.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    "I couldn't load that song's full details. Please try selecting it again.",
                    ephemeral=True
                )


class JuiceWRLDSongView(discord.ui.View):
    def __init__(self, songs, owner_id, selected_song=None):
        super().__init__(timeout=180)
        self.songs = songs
        self.owner_id = owner_id
        self.message = None
        select = JuiceWRLDSongSelect(songs, owner_id)
        if select.options:
            self.add_item(select)

        if selected_song:
            self.add_item(JuiceWRLDFindFilesButton(owner_id, selected_song))

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if getattr(self, "message", None):
            try:
                await self.message.edit(view=self)
            except (discord.HTTPException, discord.NotFound):
                pass


@bot.command(name="jwsong", aliases=["jwsearch", "jwmusicsearch"])
async def juicewrld_song_search(ctx: commands.Context, *, query: str):
    """Search the Juice WRLD catalog and browse full song details."""
    try:
        payload = await _juicewrld_api_get(
            "/songs/",
            params={"search": query, "page_size": 25, "versions": "true"}
        )
        songs = [song for song in _juicewrld_items(payload)
                 if isinstance(song, dict) and song.get("id") is not None]
        if not songs:
            await ctx.send(
                f"No Juice WRLD catalog results found for **{query[:100]}**."
            )
            return

        # Put the closest title/alternate-title match first, then keep the API's
        # order for equally relevant results. The dropdown can show up to 25.
        songs = sorted(songs, key=lambda song: _jw_song_score(song, query), reverse=True)
        first = songs[0]
        try:
            detail = await _juicewrld_api_get(f"/songs/{int(first['id'])}/")
            if isinstance(detail, dict) and detail.get("id") is not None:
                merged = dict(first)
                merged.update(detail)
                first = merged
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError) as exc:
            print(f"[JuiceWRLD] Initial detail lookup failed: {exc}")

        view = JuiceWRLDSongView(songs, ctx.author.id, first)
        embed = _build_juicewrld_song_embed(first, query)
        if len(songs) > 1:
            embed.add_field(
                name="Related results",
                value=(f"Found **{len(songs)}** matching or related catalog entries. "
                       "Use the dropdown below to open another song or version."),
                inline=False
            )
        message = await ctx.send(embed=embed, view=view)
        view.message = message

    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError) as exc:
        print(f"[JuiceWRLD] Song search failed: {exc}")
        await ctx.send("The Juice WRLD API is unavailable right now. Try again later.")


# =============================================================================
# 12. SLASH COMMANDS
# =============================================================================

@bot.tree.command(name="help", description="Browse everything VaultBot can do")
@discord.app_commands.describe(page="Optional: jump straight to a section")
@discord.app_commands.choices(
    page=[
        discord.app_commands.Choice(name=p["title"], value=p["key"])
        for p in HELP_PAGES
    ]
)
async def slash_help(interaction: discord.Interaction, page: Optional[str] = None):
    view = HelpView(interaction.user.id, find_help_page(page))
    await interaction.response.send_message(embed=build_help_embed(view.page), view=view)
    view.message = await interaction.original_response()


@bot.tree.command(name="ping", description="Check if the bot is online")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message(
        f"Pong! Bot latency: {round(bot.latency * 1000)} ms",
        ephemeral=True
    )


@bot.tree.command(
    name="musicdrop",
    description="Post a music release with artwork and download buttons"
)
@discord.app_commands.describe(
    channel="Channel where the release will be posted",
    title="Release title",
    description="Message displayed under the title",
    artist="Artist or label displayed at the bottom",
    artwork_url="Direct URL to the square artwork image",
    mp3_url="MP3 download URL",
    wav_url="WAV download URL"
)
async def musicdrop(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
    title: str,
    description: str,
    artist: str,
    artwork_url: str,
    mp3_url: str,
    wav_url: str
):
    if not interaction.guild:
        await interaction.response.send_message(
            "Use this command inside your server.",
            ephemeral=True
        )
        return

    if not interaction.user.guild_permissions.manage_messages:
        await interaction.response.send_message(
            "You need Manage Messages permission to post releases.",
            ephemeral=True
        )
        return

    if not channel.permissions_for(interaction.guild.me).send_messages:
        await interaction.response.send_message(
            "I cannot send messages in that channel.",
            ephemeral=True
        )
        return

    if not channel.permissions_for(interaction.guild.me).embed_links:
        await interaction.response.send_message(
            "I need Embed Links permission in that channel.",
            ephemeral=True
        )
        return

    urls = (artwork_url, mp3_url, wav_url)
    if any(not url.lower().startswith("https://") for url in urls):
        await interaction.response.send_message(
            "Artwork and download links must use HTTPS URLs.",
            ephemeral=True
        )
        return

    if any(len(url) > 500 for url in urls):
        await interaction.response.send_message(
            "Each URL must be 500 characters or fewer.",
            ephemeral=True
        )
        return

    if len(title) > 256 or len(description) > 4000:
        await interaction.response.send_message(
            "The title must be at most 256 characters and "
            "the description at most 4000 characters.",
            ephemeral=True
        )
        return

    embed = discord.Embed(
        title=title,
        description=description,
        color=BRAND_COLOR
    )
    embed.set_thumbnail(url=artwork_url)
    embed.set_footer(text=artist)
    view = discord.ui.View(timeout=None)
    view.add_item(discord.ui.Button(label="mp3", emoji="💿", style=discord.ButtonStyle.link, url=mp3_url))
    view.add_item(discord.ui.Button(label="wav", emoji="💿", style=discord.ButtonStyle.link, url=wav_url))

    await interaction.response.defer(ephemeral=True)
    try:
        await channel.send(embed=embed, view=view)
        await interaction.followup.send(
            f"Music release posted in {channel.mention}!",
            ephemeral=True
        )
    except discord.Forbidden:
        await interaction.followup.send(
            "Discord denied permission to post the release.",
            ephemeral=True
        )
    except discord.HTTPException:
        await interaction.followup.send(
            "Discord could not post the release. Check the links and try again.",
            ephemeral=True
        )


@bot.tree.command(name="announce", description="Send an announcement")
@discord.app_commands.describe(
    channel="The channel to post in",
    message="What you want to announce",
    ping_everyone="Ping everyone in the server"
)
@discord.app_commands.checks.has_permissions(administrator=True)
async def announce(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
    message: str,
    ping_everyone: bool = False
):
    await interaction.response.defer(ephemeral=True)
    try:
        await channel.send(
            content=f"@everyone\n{message}" if ping_everyone else message,
            allowed_mentions=discord.AllowedMentions(everyone=ping_everyone)
        )
        await interaction.followup.send(
            f"Announcement sent to {channel.mention}!",
            ephemeral=True
        )
    except discord.Forbidden:
        await interaction.followup.send(
            "I don't have permission to send messages in that channel.",
            ephemeral=True
        )




# =============================================================================
# AI CHAT (Groq free tier; no OpenAI package required)
# =============================================================================
# Free API access is rate-limited, not unlimited. Set GROQ_API_KEY in the
# host's environment variables or in /home/container/.env.
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "openai/gpt-oss-20b")
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
AI_SYSTEM_PROMPT = os.getenv(
    "AI_SYSTEM_PROMPT",
    "You are VaultBot, a helpful, friendly AI assistant in a Discord server. "
    "Be clear and concise. Follow safety rules and do not claim to have abilities "
    "you do not have."
)
AI_HISTORY_LIMIT = 12  # Maximum saved messages per conversation (6 exchanges)
_ai_history: dict[tuple[int, int], list[dict[str, str]]] = {}
_ai_http_semaphore = asyncio.Semaphore(3)


async def _groq_chat_reply(user_id: int, conversation_id: int, prompt: str) -> str:
    """Ask Groq's OpenAI-compatible chat-completions API."""
    if not GROQ_API_KEY:
        raise RuntimeError(
            "AI isn't configured yet. Add GROQ_API_KEY to your host environment "
            "variables (or /home/container/.env), then restart VaultBot."
        )

    key = (conversation_id, user_id)
    history = _ai_history.setdefault(key, [])
    history.append({"role": "user", "content": prompt})
    # Keep the prompt bounded and only send a small recent conversation.
    messages = [{"role": "system", "content": AI_SYSTEM_PROMPT}]
    messages.extend(history[-AI_HISTORY_LIMIT:])

    payload = {
        "model": GROQ_MODEL,
        "messages": messages,
        "temperature": 0.7,
        "max_tokens": 900,
    }
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }
    timeout = aiohttp.ClientTimeout(total=45)

    try:
        async with _ai_http_semaphore:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(GROQ_API_URL, headers=headers, json=payload) as response:
                    body = await response.json(content_type=None)
                    if response.status == 401:
                        history.pop()
                        raise RuntimeError("Groq rejected the API key. Check GROQ_API_KEY.")
                    if response.status == 429:
                        # Remove the unanswered prompt so history remains consistent.
                        history.pop()
                        raise RuntimeError(
                            "The free AI limit has been reached for now. Please wait and try again."
                        )
                    if response.status >= 400:
                        history.pop()
                        message = body.get("error", {}).get("message", "Unknown API error") if isinstance(body, dict) else "Unknown API error"
                        print(f"[VaultBot AI] Groq HTTP {response.status}: {message}")
                        raise RuntimeError("The AI provider returned an error. Check the bot console.")
    except asyncio.TimeoutError:
        history.pop()
        raise RuntimeError("The AI request timed out. Please try again.") from None
    except aiohttp.ClientError as exc:
        history.pop()
        print(f"[VaultBot AI] Network error: {exc}")
        raise RuntimeError("Couldn't connect to the AI provider. Try again shortly.") from None
    except RuntimeError:
        # RuntimeErrors raised above already have user-friendly messages.
        raise

    try:
        answer = body["choices"][0]["message"]["content"].strip()
    except (KeyError, IndexError, TypeError, AttributeError):
        history.pop()
        raise RuntimeError("The AI provider returned an unreadable response.") from None

    if not answer:
        history.pop()
        raise RuntimeError("The AI returned an empty response. Please try again.")

    history.append({"role": "assistant", "content": answer})
    # Bound history storage across many users/conversations.
    if len(_ai_history) > 500:
        oldest_key = next(iter(_ai_history))
        if oldest_key != key:
            _ai_history.pop(oldest_key, None)
    return answer


async def _send_ai_chunks(send_func, answer: str) -> None:
    """Send long AI replies as multiple Discord messages."""
    remaining = answer
    while remaining:
        chunk = remaining[:1900]
        if len(remaining) > 1900:
            split_at = chunk.rfind("\n")
            if split_at < 900:
                split_at = chunk.rfind(" ")
            if split_at > 900:
                chunk = remaining[:split_at]
        await send_func(chunk)
        remaining = remaining[len(chunk):].lstrip()


@bot.command(name="ai", aliases=["askai"])
@commands.cooldown(1, 8, commands.BucketType.user)
async def ai_command(ctx: commands.Context, *, question: str):
    """Ask VaultBot's AI. Usage: ~ai your question"""
    async with ctx.typing():
        try:
            answer = await _groq_chat_reply(
                ctx.author.id,
                ctx.guild.id if ctx.guild else ctx.channel.id,
                question[:4000],
            )
            await _send_ai_chunks(ctx.send, answer)
        except RuntimeError as exc:
            await ctx.send(str(exc))


@bot.tree.command(name="ask", description="Ask VaultBot's AI assistant")
@discord.app_commands.describe(question="What would you like to ask VaultBot?")
async def ask_ai_slash(interaction: discord.Interaction, question: str):
    """Slash-command version of the AI chat."""
    if not GROQ_API_KEY:
        await interaction.response.send_message(
            "AI isn't configured. The bot owner needs to add GROQ_API_KEY to the host environment.",
            ephemeral=True,
        )
        return
    await interaction.response.defer(thinking=True)
    try:
        answer = await _groq_chat_reply(
            interaction.user.id,
            interaction.guild_id or interaction.channel_id or 0,
            question[:4000],
        )
        await _send_ai_chunks(interaction.followup.send, answer)
    except RuntimeError as exc:
        await interaction.followup.send(str(exc), ephemeral=True)


@bot.command(name="clear")
@commands.guild_only()
@commands.has_permissions(manage_messages=True)
async def clear_messages(ctx: commands.Context, amount: int = 100):
    """Clear 1–100 recent messages. Defaults to 100, plus the command message."""

    if not 1 <= amount <= 100:
        await ctx.send(
            "❌ Please choose a number between 1 and 100.",
            delete_after=5,
        )
        return

    # Check the bot's effective permissions in this exact channel. Server role
    # permissions can be overridden by channel-specific permission settings.
    bot_member = ctx.guild.me
    if bot_member is None:
        print("[CLEAR] Could not resolve the bot's guild member.")
        await ctx.send(
            "❌ I couldn't check my server permissions. Please try again.",
            delete_after=5,
        )
        return

    permissions = ctx.channel.permissions_for(bot_member)
    missing = []
    if not permissions.manage_messages:
        missing.append("Manage Messages")
    if not permissions.read_message_history:
        missing.append("Read Message History")

    if missing:
        missing_text = " and ".join(missing)
        print(
            f"[CLEAR] Missing effective permission(s) in channel "
            f"{ctx.channel.id}: {missing_text}"
        )
        await ctx.send(
            f"❌ I need **{missing_text}** permission(s) in this channel "
            "to clear messages. Check the channel's permission overrides.",
            delete_after=8,
        )
        return

    try:
        # Fetch enough messages to include the requested number plus ~clear.
        deleted = await ctx.channel.purge(limit=amount + 1)
        count = sum(message.id != ctx.message.id for message in deleted)

        print(
            f"[CLEAR] User={ctx.author.id}, channel={ctx.channel.id}, "
            f"requested={amount}, deleted_other_messages={count}"
        )
        await ctx.send(
            f"✅ Cleared **{count}** message(s)!",
            delete_after=5,
        )

    except discord.Forbidden as exc:
        # This should be uncommon after the permission check; log the details
        # because Discord can still reject an operation for other reasons.
        print(
            f"[CLEAR] Discord returned Forbidden in channel {ctx.channel.id}. "
            f"manage_messages={permissions.manage_messages}, "
            f"read_message_history={permissions.read_message_history}, "
            f"error={exc}"
        )
        try:
            await ctx.send(
                "❌ Discord denied the deletion. Recheck this channel's "
                "permissions for the bot and its role.",
                delete_after=8,
            )
        except discord.HTTPException:
            pass

    except discord.HTTPException as exc:
        print(
            f"[CLEAR] Discord HTTP error in channel {ctx.channel.id}: "
            f"status={getattr(exc, 'status', 'unknown')}, error={exc}"
        )
        try:
            await ctx.send(
                "❌ Discord couldn't complete the cleanup. Check the bot "
                "console for the `[CLEAR]` error details.",
                delete_after=8,
            )
        except discord.HTTPException:
            pass


@clear_messages.error
async def clear_messages_error(ctx: commands.Context, error: commands.CommandError):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send(
            "❌ You need the **Manage Messages** permission to use this command.",
            delete_after=5,
        )
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(
            "Usage: `~clear` or `~clear 1-100`",
            delete_after=5,
        )
    elif isinstance(error, commands.BadArgument):
        await ctx.send(
            "❌ Please enter a whole number from 1 to 100.",
            delete_after=5,
        )
    elif isinstance(error, commands.NoPrivateMessage):
        await ctx.send("❌ Use this command inside a server.", delete_after=5)
    else:
        print(f"[CLEAR] Command setup error: {type(error).__name__}: {error}")
        await ctx.send(
            "❌ The clear command couldn't run. Check the bot console for details.",
            delete_after=5,
        )


# =============================================================================
# 12b. FREESTUFF FREE-GAME ALERTS (webhook receiver + settings panel)
# =============================================================================
# FreeStuff sends signed webhooks to a small HTTP server that runs next to the
# bot. Every server picks its own alert channel, ping, stores/launchers and
# free-game types with the /freestuff panel. If FREESTUFF_PUBLIC_KEY is missing
# or pynacl isn't installed, the webhook stays off, but the panel still works
# and the rest of VaultBot keeps running.
import base64
import binascii
from collections import OrderedDict

from aiohttp import web

try:
    from nacl.exceptions import BadSignatureError
    from nacl.signing import VerifyKey
    _NACL_AVAILABLE = True
except ImportError:
    _NACL_AVAILABLE = False

FREESTUFF_PUBLIC_KEY = os.getenv("FREESTUFF_PUBLIC_KEY", "").strip()
# A separate variable from PORT, which many hosts set for their own use.
FREESTUFF_PORT = _positive_int_env("FREESTUFF_PORT", 3000)
FREESTUFF_WEBHOOK_PATH = (
    os.getenv("FREESTUFF_WEBHOOK_PATH", "/freestuff/webhook").strip() or "/freestuff/webhook"
)
# Saved per-server panel choices. Stored next to VaultBot.py unless overridden.
FREESTUFF_SETTINGS_FILE = Path(os.getenv(
    "FREESTUFF_SETTINGS_FILE",
    str(Path(__file__).resolve().parent / "freestuff_settings.json"),
))

# Stores/launchers a server can pick. FreeStuff may add stores later; unknown
# ones are treated as "other".
FREESTUFF_STORE_CHOICES = {
    "steam": "Steam",
    "epic": "Epic Games Store",
    "gog": "GOG",
    "humble": "Humble Bundle",
    "origin": "EA App / Origin",
    "ubi": "Ubisoft Connect",
    "itch": "itch.io",
    "prime": "Prime Gaming",
    "other": "Other stores",
}
# Channel values from FreeStuff's product types.
FREESTUFF_TYPE_CHOICES = {
    "keep": "Free to keep (100% off)",
    "timed": "Free weekends & trials",
    "prime": "Prime Gaming games",
    "gamepass": "Xbox Game Pass",
    "other": "DLC & extra content",
}
FREESTUFF_DEFAULTS = {
    "channel_id": None,
    "ping": "none",       # none | everyone | role
    "role_id": None,
    "stores": list(FREESTUFF_STORE_CHOICES),
    "types": ["keep", "timed"],
}

FREESTUFF_EPOCH_MS = 1735689600000  # FreeStuff timestamps count seconds from 2025-01-01
FREESTUFF_MAX_AGE_SECONDS = 300
_FS_FLAG_TRASH = 1 << 0
_FS_IMG_WIDE = 1 << 1
_FS_COLOR = discord.Color.from_rgb(46, 204, 113)

_freestuff_guilds: dict[str, dict] = {}  # server ID (as text) -> saved settings
_freestuff_seen: OrderedDict = OrderedDict()  # message IDs already handled (replay protection)
_freestuff_tasks: set = set()
_freestuff_key = None
_freestuff_started = False
_freestuff_status = "still starting up. Try /freestuff again in a few seconds."
_freestuff_last_ok = None       # unix time of the last genuine message from FreeStuff
_freestuff_rejected = 0         # how many messages failed the signature check
_freestuff_last_reject = ""     # why the most recent one failed


# ---------- saved settings ----------

def _load_freestuff_settings() -> None:
    try:
        data = json.loads(FREESTUFF_SETTINGS_FILE.read_text(encoding="utf-8"))
        guilds = data.get("guilds") if isinstance(data, dict) else None
        if isinstance(guilds, dict):
            _freestuff_guilds.update(
                {str(k): v for k, v in guilds.items() if isinstance(v, dict)}
            )
        print(f"[FreeStuff] Loaded alert settings for {len(_freestuff_guilds)} server(s).")
    except FileNotFoundError:
        print("[FreeStuff] No saved alert settings yet. Use /freestuff to set them up.")
    except (OSError, ValueError) as exc:
        print(f"[FreeStuff] Could not read {FREESTUFF_SETTINGS_FILE.name}: {exc}")


def _save_freestuff_settings() -> None:
    """Write atomically so a crash mid-save can't corrupt the file."""
    FREESTUFF_SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    temp = FREESTUFF_SETTINGS_FILE.with_name(FREESTUFF_SETTINGS_FILE.name + ".tmp")
    temp.write_text(json.dumps({"guilds": _freestuff_guilds}, indent=2), encoding="utf-8")
    temp.replace(FREESTUFF_SETTINGS_FILE)


def _fs_get(guild_id: int) -> dict:
    """Settings for one server, with defaults filled in."""
    settings = {**FREESTUFF_DEFAULTS, **_freestuff_guilds.get(str(guild_id), {})}
    settings["stores"] = [k for k in FREESTUFF_STORE_CHOICES if k in settings["stores"]]
    settings["types"] = [k for k in FREESTUFF_TYPE_CHOICES if k in settings["types"]]
    return settings


def _fs_update(guild_id: int, **changes) -> None:
    settings = _fs_get(guild_id)
    settings.update(changes)
    _freestuff_guilds[str(guild_id)] = settings
    try:
        _save_freestuff_settings()
    except OSError as exc:
        print(f"[FreeStuff] Could not save settings: {exc}")


# ---------- webhook verification ----------

_ED25519_SPKI_PREFIX = bytes.fromhex("302a300506032b6570032100")


def _decode_freestuff_key(raw: str) -> bytes:
    """Return the raw 32-byte Ed25519 public key from hex, base64, or PEM/DER text."""
    text = raw.strip().strip('"').strip("'")
    if "BEGIN" in text:  # PEM: drop the ----- lines and keep the base64 body
        text = "".join(line for line in text.splitlines() if not line.startswith("-----"))
    text = "".join(text.removeprefix("whpk_").split())
    try:
        data = bytes.fromhex(text)
    except ValueError:
        padded = text + "=" * (-len(text) % 4)
        try:
            data = base64.b64decode(padded, validate=True)
        except (binascii.Error, ValueError):
            data = base64.urlsafe_b64decode(padded)
    if len(data) == 44 and data.startswith(_ED25519_SPKI_PREFIX):
        data = data[len(_ED25519_SPKI_PREFIX):]  # standard wrapper around the 32-byte key
    if len(data) != 32:
        raise ValueError(f"expected a 32-byte key but it decoded to {len(data)} bytes")
    return data


def _load_freestuff_key(raw: str):
    return VerifyKey(_decode_freestuff_key(raw))


def _verify_freestuff_request(msg_id, timestamp, signature_header, body, verify_key):
    """Check the Standard Webhooks signature. Returns None if valid, else a short reason."""
    try:
        sent_ms = int(timestamp) * 1000 + FREESTUFF_EPOCH_MS
    except (TypeError, ValueError):
        return "the Webhook-Timestamp header was missing or not a number"
    if abs(time.time() * 1000 - sent_ms) > FREESTUFF_MAX_AGE_SECONDS * 1000:
        return "the message timestamp is too far from this host's clock"
    signed = f"{msg_id}.{timestamp}.".encode() + body
    parts = signature_header.split()
    if not parts:
        return "the Webhook-Signature header was missing"
    for part in parts:
        sig_b64 = part.split(",", 1)[-1]  # "v1a,<base64>" or just "<base64>"
        try:
            verify_key.verify(signed, base64.b64decode(sig_b64))
            return None
        except (BadSignatureError, ValueError):
            continue
    return "the signature didn't match this public key (wrong key, or the data was changed)"


# ---------- building and sending alerts ----------

def _fs_store_key(product: dict) -> str:
    store = product.get("store")
    return store if store in FREESTUFF_STORE_CHOICES else "other"


def _fs_ping(settings: dict):
    """Return (text to prepend, AllowedMentions) for the chosen ping."""
    if settings["ping"] == "everyone":
        return "@everyone ", discord.AllowedMentions(everyone=True)
    if settings["ping"] == "role" and settings.get("role_id"):
        role_id = int(settings["role_id"])
        return f"<@&{role_id}> ", discord.AllowedMentions(roles=[discord.Object(id=role_id)])
    return "", discord.AllowedMentions.none()


def _build_freestuff_embed(product: dict) -> discord.Embed:
    urls = sorted(product.get("urls") or [], key=lambda u: u.get("priority", 0), reverse=True)
    images = sorted(
        product.get("images") or [],
        key=lambda i: (bool(i.get("flags", 0) & _FS_IMG_WIDE), i.get("priority", 0)),
        reverse=True,
    )

    embed = discord.Embed(
        title=str(product.get("title") or "Free game")[:256],
        description=(product.get("description") or "")[:300] or None,
        color=_FS_COLOR,
    )
    if urls and urls[0].get("url"):
        embed.url = urls[0]["url"]
    if images and images[0].get("url"):
        embed.set_image(url=images[0]["url"])

    embed.add_field(name="Store", value=FREESTUFF_STORE_CHOICES[_fs_store_key(product)], inline=True)
    usd = next((p for p in product.get("prices") or [] if p.get("currency") == "USD"), None)
    if usd and usd.get("oldValue"):
        embed.add_field(name="Normally", value=f"~~${usd['oldValue'] / 100:.2f}~~ → **FREE**", inline=True)
    if product.get("until"):
        embed.add_field(name="Ends", value=f"<t:{int(product['until'] // 1000)}:R>", inline=True)
    if product.get("notice"):
        embed.add_field(name="Note", value=str(product["notice"])[:200], inline=False)
    return embed


def _freestuff_test_product() -> dict:
    """Sample product for the panel's Send test button."""
    return {
        "title": "Test alert: this is how FreeStuff games will look",
        "description": "This is a test message from the /freestuff panel. No real game is being offered.",
        "store": "steam",
        "type": "keep",
        "prices": [],
        "urls": [],
        "images": [],
        "flags": 0,
    }


async def _post_freestuff_alert(channel_id: int, products: list, settings: dict) -> None:
    channel = bot.get_channel(channel_id) or await bot.fetch_channel(channel_id)
    prefix, allowed = _fs_ping(settings)
    for start in range(0, len(products), 10):  # Discord allows 10 embeds per message
        await channel.send(
            content=f"{prefix}🎮 **Free game alert!**",
            embeds=[_build_freestuff_embed(p) for p in products[start:start + 10]],
            allowed_mentions=allowed,
        )


async def _process_freestuff_event(event: dict) -> None:
    kind = event.get("type")
    if kind == "fsb:event:ping":
        print("[FreeStuff] Ping received. The webhook is connected.")
        return
    if kind != "fsb:event:announcement_created":
        return

    products = [
        p for p in ((event.get("data") or {}).get("resolvedProducts") or [])
        if isinstance(p, dict) and not (p.get("flags", 0) & _FS_FLAG_TRASH)
    ]
    if not products:
        return

    # FreeStuff sends one webhook for everyone, so filter for each server here.
    for guild_key in list(_freestuff_guilds):
        settings = _fs_get(int(guild_key))
        if not settings["channel_id"]:
            continue
        matches = [
            p for p in products
            if p.get("type") in settings["types"] and _fs_store_key(p) in settings["stores"]
        ]
        if not matches:
            continue
        try:
            await _post_freestuff_alert(int(settings["channel_id"]), matches, settings)
            print(f"[FreeStuff] Posted {len(matches)} game(s) in server {guild_key}.")
        except discord.HTTPException as exc:
            print(f"[FreeStuff] Couldn't post in server {guild_key}: {exc}")


async def _run_freestuff_event(event: dict) -> None:
    """Log failures so one bad event never takes down the webhook server."""
    try:
        await _process_freestuff_event(event)
    except Exception as exc:  # noqa: BLE001
        print(f"[FreeStuff] Could not process event: {exc}")


async def _handle_freestuff_webhook(request: web.Request) -> web.Response:
    body = await request.read()  # raw bytes are required for the signature check
    msg_id = request.headers.get("Webhook-Id", "")
    timestamp = request.headers.get("Webhook-Timestamp", "")
    signature = request.headers.get("Webhook-Signature", "")

    global _freestuff_last_ok, _freestuff_rejected, _freestuff_last_reject
    reason = _verify_freestuff_request(msg_id, timestamp, signature, body, _freestuff_key)
    if reason:
        _freestuff_rejected += 1
        _freestuff_last_reject = reason
        print(f"[FreeStuff] Rejected a webhook: {reason}.")
        return web.Response(status=401)
    _freestuff_last_ok = int(time.time())

    # Duplicate delivery (FreeStuff retries): acknowledge without posting again.
    if msg_id in _freestuff_seen:
        return web.Response(status=204)
    _freestuff_seen[msg_id] = time.time()
    while len(_freestuff_seen) > 500:
        _freestuff_seen.popitem(last=False)

    try:
        event = json.loads(body)
    except ValueError:
        return web.Response(status=204)

    # Reply right away so FreeStuff doesn't count the delivery as failed.
    task = asyncio.create_task(_run_freestuff_event(event))
    _freestuff_tasks.add(task)
    task.add_done_callback(_freestuff_tasks.discard)
    return web.Response(status=204)


# ---------- the /freestuff panel ----------

def _build_freestuff_panel_embed(settings: dict) -> discord.Embed:
    if settings["channel_id"]:
        status, channel_text = "✅ On", f"<#{settings['channel_id']}>"
    else:
        status, channel_text = "⛔ Off", "Not set. Pick a channel below to turn alerts on."

    if settings["ping"] == "everyone":
        ping_text = "@everyone"
    elif settings["ping"] == "role":
        ping_text = f"<@&{settings['role_id']}>" if settings.get("role_id") else "Role not picked yet"
    else:
        ping_text = "No ping"

    stores = ", ".join(FREESTUFF_STORE_CHOICES[k] for k in settings["stores"]) or "None"
    types = ", ".join(FREESTUFF_TYPE_CHOICES[k] for k in settings["types"]) or "None"

    embed = discord.Embed(
        title="🎮 FreeStuff Alerts",
        description=f"**Status:** {status}\nChanges save right away.",
        color=BRAND_COLOR,
    )
    embed.add_field(name="Channel", value=channel_text, inline=False)
    embed.add_field(name="Ping", value=ping_text, inline=True)
    embed.add_field(name="Stores / launchers", value=stores, inline=False)
    embed.add_field(name="Game types", value=types, inline=False)
    if _freestuff_key is None:
        embed.add_field(
            name="⚠️ Webhook is off",
            value=f"No alerts will arrive until this is fixed: {_freestuff_status}"[:1000],
            inline=False,
        )
    else:
        if _freestuff_last_ok:
            contact = f"Last message from FreeStuff: <t:{_freestuff_last_ok}:R>"
        else:
            contact = "No message from FreeStuff yet. Press the ping button on the FreeStuff dashboard to test."
        lines = [f"✅ Receiver is running on port {FREESTUFF_PORT}.", contact]
        if _freestuff_rejected:
            lines.append(
                f"⚠️ {_freestuff_rejected} message(s) rejected. Latest reason: {_freestuff_last_reject}."
            )
        embed.add_field(name="Webhook", value="\n".join(lines)[:1000], inline=False)
    embed.set_footer(text="Only you can see this panel. Requires the Manage Server permission.")
    return embed


class FreeStuffStoreSelect(discord.ui.Select):
    def __init__(self, selected):
        super().__init__(
            placeholder="Stores / launchers to alert for…",
            min_values=1,
            max_values=len(FREESTUFF_STORE_CHOICES),
            options=[
                discord.SelectOption(label=label, value=key, default=key in selected)
                for key, label in FREESTUFF_STORE_CHOICES.items()
            ],
            row=1,
        )

    async def callback(self, interaction: discord.Interaction):
        await self.view.update_settings(interaction, stores=list(self.values))


class FreeStuffTypeSelect(discord.ui.Select):
    def __init__(self, selected):
        super().__init__(
            placeholder="Types of free games to post…",
            min_values=1,
            max_values=len(FREESTUFF_TYPE_CHOICES),
            options=[
                discord.SelectOption(label=label, value=key, default=key in selected)
                for key, label in FREESTUFF_TYPE_CHOICES.items()
            ],
            row=2,
        )

    async def callback(self, interaction: discord.Interaction):
        await self.view.update_settings(interaction, types=list(self.values))


class FreeStuffChannelSelect(discord.ui.ChannelSelect):
    def __init__(self):
        super().__init__(
            placeholder="Pick the channel for alerts…",
            channel_types=[discord.ChannelType.text],
            min_values=1,
            max_values=1,
            row=0,
        )

    async def callback(self, interaction: discord.Interaction):
        await self.view.update_settings(interaction, channel_id=self.values[0].id)


class FreeStuffRoleSelect(discord.ui.RoleSelect):
    def __init__(self):
        super().__init__(
            placeholder="Or ping a role instead (optional)…",
            min_values=1,
            max_values=1,
            row=4,
        )

    async def callback(self, interaction: discord.Interaction):
        await self.view.update_settings(interaction, ping="role", role_id=self.values[0].id)


class FreeStuffPanelView(discord.ui.View):
    def __init__(self, owner_id: int, guild_id: int):
        super().__init__(timeout=300)
        self.owner_id = owner_id
        self.guild_id = guild_id
        self.message: Optional[discord.Message] = None
        settings = _fs_get(guild_id)
        self.add_item(FreeStuffChannelSelect())
        self.add_item(FreeStuffStoreSelect(settings["stores"]))
        self.add_item(FreeStuffTypeSelect(settings["types"]))
        self.add_item(FreeStuffRoleSelect())
        self._sync_ping_buttons(settings)

    def _sync_ping_buttons(self, settings: dict) -> None:
        """Highlight the ping option that's currently active."""
        self.ping_none.style = (
            discord.ButtonStyle.success if settings["ping"] == "none" else discord.ButtonStyle.secondary
        )
        self.ping_everyone.style = (
            discord.ButtonStyle.success if settings["ping"] == "everyone" else discord.ButtonStyle.secondary
        )

    async def update_settings(self, interaction: discord.Interaction, **changes) -> None:
        _fs_update(self.guild_id, **changes)
        settings = _fs_get(self.guild_id)
        self._sync_ping_buttons(settings)
        await interaction.response.edit_message(
            embed=_build_freestuff_panel_embed(settings), view=self
        )

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "Only the person who opened this panel can change it. Run `/freestuff` to open your own.",
                ephemeral=True,
            )
            return False
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message(
                "You need the **Manage Server** permission to change FreeStuff alerts.",
                ephemeral=True,
            )
            return False
        return True

    async def on_timeout(self) -> None:
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass

    @discord.ui.button(label="No ping", style=discord.ButtonStyle.secondary, row=3)
    async def ping_none(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.update_settings(interaction, ping="none")

    @discord.ui.button(label="@everyone", emoji="📢", style=discord.ButtonStyle.secondary, row=3)
    async def ping_everyone(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.update_settings(interaction, ping="everyone")

    @discord.ui.button(label="Send test", emoji="🧪", style=discord.ButtonStyle.primary, row=3)
    async def test_alert(self, interaction: discord.Interaction, button: discord.ui.Button):
        settings = _fs_get(self.guild_id)
        if not settings["channel_id"]:
            await interaction.response.send_message("Pick a channel first.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            await _post_freestuff_alert(int(settings["channel_id"]), [_freestuff_test_product()], settings)
            await interaction.followup.send("✅ Test alert sent to the channel.", ephemeral=True)
        except discord.HTTPException as exc:
            await interaction.followup.send(
                f"❌ Couldn't post there: {exc}. Check the bot's permissions in that channel.",
                ephemeral=True,
            )

    @discord.ui.button(label="Turn off", emoji="⛔", style=discord.ButtonStyle.danger, row=3)
    async def turn_off(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.update_settings(interaction, channel_id=None)


@bot.tree.command(
    name="freestuff",
    description="Choose where FreeStuff alerts go and which stores they cover",
)
@discord.app_commands.guild_only()
@discord.app_commands.default_permissions(manage_guild=True)
@discord.app_commands.checks.has_permissions(manage_guild=True)
async def freestuff_panel(interaction: discord.Interaction):
    view = FreeStuffPanelView(interaction.user.id, interaction.guild_id)
    await interaction.response.send_message(
        embed=_build_freestuff_panel_embed(_fs_get(interaction.guild_id)),
        view=view,
        ephemeral=True,
    )
    view.message = await interaction.original_response()


@bot.listen("on_ready")
async def _start_freestuff_alerts():
    """Load settings and start the webhook server once. Existing on_ready is unchanged."""
    global _freestuff_started, _freestuff_key, _freestuff_status
    if _freestuff_started:
        return
    _freestuff_started = True
    _load_freestuff_settings()

    if not FREESTUFF_PUBLIC_KEY:
        _freestuff_status = "FREESTUFF_PUBLIC_KEY isn't set on this host."
        print(f"[FreeStuff] {_freestuff_status} The /freestuff panel still works.")
        return
    if not _NACL_AVAILABLE:
        _freestuff_status = "the pynacl package isn't installed (add it to requirements.txt)."
        print(f"[FreeStuff] {_freestuff_status}")
        return
    try:
        key = _load_freestuff_key(FREESTUFF_PUBLIC_KEY)
    except (ValueError, TypeError) as exc:
        _freestuff_status = (
            f"the public key couldn't be read ({exc}). Make sure FREESTUFF_PUBLIC_KEY "
            "is the PUBLIC key from the FreeStuff dashboard, not the REST API key."
        )
        print(f"[FreeStuff] {_freestuff_status}")
        return

    app = web.Application()
    app.router.add_post(FREESTUFF_WEBHOOK_PATH, _handle_freestuff_webhook)
    runner = web.AppRunner(app)
    await runner.setup()
    try:
        await web.TCPSite(runner, "0.0.0.0", FREESTUFF_PORT).start()
    except OSError as exc:
        _freestuff_status = f"port {FREESTUFF_PORT} couldn't be opened on this host ({exc})."
        print(f"[FreeStuff] {_freestuff_status}")
        await runner.cleanup()
        return
    _freestuff_key = key  # set last, so the webhook only counts as "on" once the server is up
    _freestuff_status = "running"
    print(f"[FreeStuff] Webhook server listening on port {FREESTUFF_PORT} at {FREESTUFF_WEBHOOK_PATH}")


# =============================================================================
# 12c. PATCH NOTES (merged from patchnotes.py)
# =============================================================================
"""
patchnotes.py - game & driver update alerts for VaultBot.

How it works
------------
* /patchnotes add      follows a Steam game (Steam's public news feed, no key).
* /patchnotes addfeed  follows Minecraft (Java/Bedrock), NVIDIA GeForce drivers
                       or Fortnite updates.
* Every few minutes the bot checks each followed source and posts anything it
  hasn't posted before to the subscribed channel as an embed.

This file is self-contained. VaultBot.py loads it inside a try/except, so if
anything in here ever fails, the rest of the bot keeps running.

Optional environment variables (all have sensible defaults):
  PATCHNOTES_POLL_MINUTES   how often to check Steam (default 10)
  PATCHNOTES_DATA_FILE      where subscriptions are saved
                            (default: patchnotes_data.json next to this file)
  NVIDIA_PFID               NVIDIA product ID used for the driver lookup
                            (default 877; all GeForce desktop cards share one driver)
"""



# -----------------------------------------------------------------------------
# Settings
# -----------------------------------------------------------------------------
STEAM_NEWS_URL = "https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/"
STEAM_SEARCH_URL = "https://store.steampowered.com/api/storesearch/"
STEAM_DETAILS_URL = "https://store.steampowered.com/api/appdetails"
STEAM_HEADER_URL = "https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/header.jpg"

MINECRAFT_URL = "https://launchercontent.mojang.com/{kind}PatchNotes.json"
MINECRAFT_HOST = "https://launchercontent.mojang.com"
NVIDIA_URL = "https://gfwsl.geforce.com/services_toolkit/services/com/nvidia/services/AjaxDriverService.php"
NVIDIA_PFID = os.getenv("NVIDIA_PFID", "877").strip() or "877"
FORTNITE_BUILD_URL = "https://fortnite-api.com/v2/aes"
FORTNITE_NEWS_PAGE = "https://www.fortnite.com/news"

DATA_FILE = Path(os.getenv(
    "PATCHNOTES_DATA_FILE",
    str(Path(__file__).resolve().parent / "patchnotes_data.json"),
))

MAX_GAMES_PER_SERVER = 25     # keeps one server from creating endless lookups
MAX_POSTS_PER_POLL = 3        # if a game posts a burst of news, only send the newest few
MAX_AGE_DAYS = 14             # never announce posts older than this (prevents old-news floods)
SEEN_HISTORY = 100            # how many post IDs to remember per game
EMBED_COLOR = discord.Color.from_rgb(0, 225, 255)  # VaultBot neon-cyan sci-fi accent


def _poll_minutes() -> int:
    try:
        return max(2, int(os.getenv("PATCHNOTES_POLL_MINUTES", "10")))
    except (TypeError, ValueError):
        print("[PatchNotes] PATCHNOTES_POLL_MINUTES must be a number; using 10.")
        return 10


POLL_MINUTES = _poll_minutes()

# -----------------------------------------------------------------------------
# Saved data
#   _subs: server ID (text) -> game ID (text) -> {"name", "channel_id", "role_id"}
#   _seen: game ID (text)   -> list of post IDs already handled
# -----------------------------------------------------------------------------
_bot: Optional[discord.Client] = None
_subs: dict = {}
_seen: dict = {}
_started = False


def _load() -> None:
    global _subs, _seen
    try:
        data = json.loads(DATA_FILE.read_text(encoding="utf-8"))
        subs = data.get("subs", {})
        seen = data.get("seen", {})
        if isinstance(subs, dict) and isinstance(seen, dict):
            _subs, _seen = subs, seen
        total = sum(len(g) for g in _subs.values())
        print(f"[PatchNotes] Loaded {total} subscription(s) across {len(_subs)} server(s).")
    except FileNotFoundError:
        print("[PatchNotes] No saved subscriptions yet. Use /patchnotes add to create one.")
    except (OSError, json.JSONDecodeError, AttributeError) as exc:
        # Keep the unreadable file instead of overwriting it with an empty one.
        print(f"[PatchNotes] Could not read {DATA_FILE.name}: {exc}")
        try:
            DATA_FILE.replace(DATA_FILE.with_name(DATA_FILE.name + ".unreadable"))
        except OSError:
            pass


def _save() -> None:
    try:
        DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
        temp = DATA_FILE.with_name(DATA_FILE.name + ".tmp")
        temp.write_text(json.dumps({"subs": _subs, "seen": _seen}, indent=2), encoding="utf-8")
        temp.replace(DATA_FILE)
    except OSError as exc:
        print(f"[PatchNotes] Could not save subscriptions: {exc}")


# -----------------------------------------------------------------------------
# Steam lookups
# -----------------------------------------------------------------------------
async def _get_json(url: str, params: dict):
    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url, params=params, headers={"Accept": "application/json"}) as resp:
            resp.raise_for_status()
            return await resp.json(content_type=None)


async def _fetch_news(appid: str, count: int = 6) -> list:
    """Newest developer announcements for a Steam game, newest first."""
    payload = await _get_json(STEAM_NEWS_URL, {
        "appid": appid,
        "count": count,
        "maxlength": 8000,
        "feeds": "steam_community_announcements",
        "format": "json",
    })
    items = (payload or {}).get("appnews", {}).get("newsitems", []) or []
    items = [i for i in items if isinstance(i, dict) and i.get("gid")]
    items.sort(key=lambda i: int(i.get("date") or 0), reverse=True)
    return items


async def _search_game(query: str) -> Optional[tuple]:
    """Find a game by name. Returns (appid, name) or None."""
    payload = await _get_json(STEAM_SEARCH_URL, {"term": query, "l": "english", "cc": "US"})
    items = (payload or {}).get("items", []) or []
    apps = [i for i in items if i.get("type", "app") == "app" and i.get("id")]
    if not apps:
        return None
    return str(apps[0]["id"]), str(apps[0].get("name") or f"App {apps[0]['id']}")


async def _game_name(appid: str) -> str:
    try:
        payload = await _get_json(STEAM_DETAILS_URL, {"appids": appid, "filters": "basic"})
        entry = (payload or {}).get(appid, {})
        if entry.get("success"):
            return str(entry["data"].get("name") or f"App {appid}")
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, KeyError, TypeError):
        pass
    return f"App {appid}"


# -----------------------------------------------------------------------------
# Turning a Steam post into a Discord embed
# -----------------------------------------------------------------------------
def _tidy(t: str, limit: int) -> str:
    """Final clean-up shared by every source: entities, blank lines, length."""
    t = html.unescape(t)
    t = re.sub(r"[ \t]+\n", "\n", t)
    t = re.sub(r"\n{3,}", "\n\n", t).strip()
    if len(t) > limit:
        t = t[:limit].rsplit(" ", 1)[0].rstrip() + "…"
        if t.count("**") % 2:      # don't leave bold switched on after cutting
            t += "**"
    return t


def html_to_text(raw: str, limit: int = 3500) -> str:
    """Minecraft's notes are HTML. Keep headings and bullets, drop the rest."""
    t = raw or ""
    t = re.sub(r"(?is)<(script|style).*?</\1>", "", t)
    t = re.sub(r"(?is)<h[1-6][^>]*>(.*?)</h[1-6]>", r"\n**\1**\n", t)
    t = re.sub(r"(?i)<li[^>]*>", "\n• ", t)
    t = re.sub(r"(?i)<(?:br|/p|/div|/ul|/ol)[^>]*>", "\n", t)
    t = re.sub(r"<[^>]+>", "", t)
    return _tidy(t, limit)


def _parse_date(value) -> int:
    """Best-effort timestamp from a date string; falls back to 'now'."""
    text = str(value or "").strip()
    if text:
        try:
            return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
        except ValueError:
            pass
        for fmt in ("%a %b %d, %Y", "%b %d, %Y"):
            try:
                return int(datetime.strptime(text, fmt).replace(tzinfo=timezone.utc).timestamp())
            except ValueError:
                continue
    return int(time.time())


#  ── Steam BBCode → Discord markdown ─────────────────────────────────────────
#  Steam posts are written in BBCode ([p], [h2], [list], [*], [url=..], [img]..).
#  The old converter deleted the tags WITHOUT leaving line breaks behind, which
#  is why paragraphs, headings and bullets were glued into one wall of text.
#  This one turns every tag into the right Discord equivalent instead.
_HEAD_OPEN, _HEAD_CLOSE = "\x01", "\x02"          # private markers around heading text
STEAM_CLAN_IMAGE = "https://clan.fastly.steamstatic.com/images/"
_BBCODE_TAGS = (r"b|i|u|strike|s|spoiler|noparse|url|img|code|quote|list|olist|hr|br|p|"
                r"table|tr|td|th|h[1-6]|previewyoutube|video")


def _md_link(text: str, url: str) -> str:
    url = (url or "").strip().strip('"\'')
    text = (text or "").strip()
    if not url.startswith(("https://", "http://")):
        return text
    if not text or text == url:
        text = re.sub(r"^https?://(?:www\.)?", "", url).split("/")[0] + "/…"
    text = text.replace("[", "(").replace("]", ")")
    return f"[{text}]({url.replace(')', '%29')})"


def _wrap(mark: str, inner: str) -> str:
    """Wrap in markdown marks, keeping edge spaces outside (`** x**` doesn't render)."""
    core = inner.strip()
    if not core:
        return inner
    lead = inner[:len(inner) - len(inner.lstrip())]
    trail = inner[len(inner.rstrip()):]
    return f"{lead}{mark}{core}{mark}{trail}"


def steam_markup_to_md(raw: str) -> tuple:
    """Convert a Steam post to Discord markdown.

    Returns (markdown, [image urls found in the post], youtube video id or None).
    Headings come out wrapped in _HEAD_OPEN/_HEAD_CLOSE so they can be split off.
    """
    t = (raw or "").replace("\r\n", "\n").replace("\r", "\n")
    t = html.unescape(t)
    t = re.sub(rf"\\+(?=\[/?(?:{_BBCODE_TAGS}|\*)(?:[= ][^\]]*)?\])", "", t)   # "\[list]" is still a tag
    t = t.replace("\\[", "\x03").replace("\\]", "\x04")        # other escaped brackets are literal text
    t = re.sub(r"\[[^\]\n]*\Z", "", t)                         # tag cut in half by Steam's length limit

    # [noparse] keeps its brackets as plain text
    t = re.sub(r"\[noparse\](.*?)\[/noparse\]",
               lambda m: m.group(1).replace("[", "\x03").replace("]", "\x04"),
               t, flags=re.I | re.S)

    # Images: remember them (first one becomes the banner), remove from the text.
    images: list = []

    def grab(src: str) -> str:
        src = (src or "").strip().strip('"\'')
        src = re.sub(r"\{STEAM_CLAN(?:_LOC)?_IMAGE\}/?", STEAM_CLAN_IMAGE, src)
        if src.startswith("//"):
            src = "https:" + src
        if src.startswith(("https://", "http://")):
            images.append(src)
        return ""

    t = re.sub(r"\[img\](.*?)\[/img\]", lambda m: grab(m.group(1)), t, flags=re.I | re.S)
    t = re.sub(r"\[img\s[^\]]*?src=\"([^\"]+)\"[^\]]*\](?:\s*\[/img\])?",
               lambda m: grab(m.group(1)), t, flags=re.I)

    # YouTube previews: keep the id (used as a banner fallback), drop the tag.
    video = re.search(r"\[previewyoutube=([\w-]{6,20})", t, flags=re.I)
    video_id = video.group(1) if video else None
    t = re.sub(r"\[previewyoutube=[^\]]*\](?:\s*\[/previewyoutube\])?", "", t, flags=re.I)

    # Code
    def code(m):
        body = m.group(1).strip("\n")
        if "\n" in body:
            return "\n```\n" + body.replace("```", "'''") + "\n```\n"
        return "`" + body.replace("`", "'") + "`"
    t = re.sub(r"\[code\](.*?)\[/code\]", code, t, flags=re.I | re.S)

    # Headings → marker lines (bold/italic tags inside them are dropped)
    def heading(m):
        text = re.sub(r"\[/?[a-z]+\]", "", m.group(2), flags=re.I).replace("**", "")
        text = " ".join(text.split())
        return f"\n\n{_HEAD_OPEN}{text}{_HEAD_CLOSE}\n" if text else "\n"
    t = re.sub(r"\[h([1-6])\](.*?)\[/h\1\]", heading, t, flags=re.I | re.S)

    # Lists (nested lists get indented)
    depth = 0

    def list_tag(m):
        nonlocal depth
        closing, tag = m.group(1), m.group(2).lower()
        if tag == "*":
            if closing:
                return ""
            return "\n" + "\u2003" * max(depth - 1, 0) + ("▸ " if depth <= 1 else "◦ ")
        if closing:
            depth = max(depth - 1, 0)
        else:
            depth += 1
        return "\n"
    t = re.sub(r"\[(/?)(list|olist|\*)\]", list_tag, t, flags=re.I)

    # Paragraphs, breaks, rules, quotes, tables
    t = re.sub(r"\[/p\]", "\n\n", t, flags=re.I)
    t = re.sub(r"\[p\]", "", t, flags=re.I)
    t = re.sub(r"\[(?:br|hr)\s*/?\]", "\n\n", t, flags=re.I)

    def quote(m):
        author = (m.group(1) or "").strip()
        lines = [l.strip() for l in m.group(2).strip().split("\n") if l.strip()]
        if author:
            lines.insert(0, f"**{author}**")
        return "\n\n" + "\n".join("> " + l for l in lines) + "\n\n"
    t = re.sub(r"\[quote(?:=([^\]]*))?\](.*?)\[/quote\]", quote, t, flags=re.I | re.S)

    t = re.sub(r"\[/t[dh]\]", "  ·  ", t, flags=re.I)
    t = re.sub(r"\[/?(?:tr|table)[^\]]*\]", "\n", t, flags=re.I)
    t = re.sub(r"[ ]*·[ ]*(?=\n|\Z)", "", t)

    # Inline styles
    t = re.sub(r"\[b\](.*?)\[/b\]", lambda m: _wrap("**", m.group(1)), t, flags=re.I | re.S)
    t = re.sub(r"\[i\](.*?)\[/i\]", lambda m: _wrap("*", m.group(1)), t, flags=re.I | re.S)
    t = re.sub(r"\[u\](.*?)\[/u\]", lambda m: _wrap("__", m.group(1)), t, flags=re.I | re.S)
    t = re.sub(r"\[(strike|s)\](.*?)\[/\1\]", lambda m: _wrap("~~", m.group(2)), t, flags=re.I | re.S)
    t = re.sub(r"\[spoiler\](.*?)\[/spoiler\]", lambda m: _wrap("||", m.group(1)), t, flags=re.I | re.S)

    # Links become clickable masked links
    t = re.sub(r"\[url=\"?([^\]\"\s]+)\"?\](.*?)\[/url\]",
               lambda m: _md_link(m.group(2), m.group(1)), t, flags=re.I | re.S)
    t = re.sub(r"\[url\](.*?)\[/url\]", lambda m: _md_link(m.group(1), m.group(1)), t, flags=re.I | re.S)

    # Anything BBCode left over (but NOT things like "[Fixed]" typed by the devs)
    t = re.sub(rf"\[/?(?:{_BBCODE_TAGS})(?:[= ][^\]]*)?\]", "", t, flags=re.I)
    t = re.sub(r"\{STEAM_CLAN(?:_LOC)?_IMAGE\}\S*", "", t)
    t = t.replace("\x03", "[").replace("\x04", "]")

    # Tidy up
    t = re.sub(r"(?m)^[ \t]*\\+(?=\S)", "", t)           # stray leading backslashes
    t = re.sub(r"[ \t]+\n", "\n", t)
    t = re.sub(r"\n{3,}", "\n\n", t).strip()
    return t, images, video_id


def clean_contents(text: str, limit: int = 3500) -> str:
    """Kept for compatibility: Steam BBCode -> readable markdown text."""
    md, _, _ = steam_markup_to_md(text)
    md = md.replace(_HEAD_OPEN, "**").replace(_HEAD_CLOSE, "**")
    return _tidy(md, limit)


def _clip(text: str, limit: int) -> str:
    """Shorten at a line/word boundary and close any markdown left open."""
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    cut = text[:max(limit - 2, 1)]
    nl = cut.rfind("\n")
    if nl >= len(cut) * 0.6:
        cut = cut[:nl]
    else:
        sp = cut.rfind(" ")
        if sp >= len(cut) * 0.6:
            cut = cut[:sp]
    broken_link = re.search(r"\[[^\]]*\Z|\[[^\]]*\]\([^)]*\Z", cut)
    if broken_link:
        cut = cut[:broken_link.start()]
    cut = cut.rstrip(" ,;:-\n\u2003")
    if cut.count("```") % 2:
        cut += "\n```"
    for mark in ("**", "__", "~~", "||"):
        if cut.count(mark) % 2:
            cut += mark
    return cut + " …"


def _split_sections(md: str) -> tuple:
    """Split markdown into (intro, [(heading, body), ...]) at heading lines."""
    intro: list = []
    sections: list = []
    current = intro
    for line in md.split("\n"):
        s = line.strip()
        head = None
        if s.startswith(_HEAD_OPEN) and s.endswith(_HEAD_CLOSE):
            head = s[1:-1].strip()
        else:
            m = re.fullmatch(r"\*\*([^*]{2,80}?)\*\*:?", s)      # a line that is only bold = a heading
            if m:
                head = m.group(1).strip()
        if head:
            current = []
            sections.append((head, current))
        else:
            current.append(line)
    merged: list = []
    pending = ""
    for h, b in sections:
        body = "\n".join(b).strip()
        if not body:
            pending = f"{pending} › {h}" if pending else h        # "Coming Up › Contests"
            continue
        merged.append((f"{pending} › {h}" if pending else h, body))
        pending = ""
    return "\n".join(intro).strip(), merged


# ── Update type → label, glyph and neon accent colour ────────────────────────
_KINDS = (
    ("HOTFIX",      "▲", (255, 77, 77),
     r"hot\s?-?fix|emergency|roll\s?back|maintenance|downtime|outage|server (?:issue|problem)|incident"),
    ("PATCH NOTES", "⬢", (0, 225, 255),
     r"patch|update|changelog|change log|release notes|bug ?fix|fixes|build \d"),
    ("SALE",        "◆", (255, 64, 200),
     r"\bsale\b|% off|discount|free weekend|bundle|\bdeals?\b|steam (?:summer|winter|autumn|spring|next)"),
    ("RELEASE",     "◉", (57, 255, 140),
     r"\blaunch|\breleas(?:e|ed|ing)\b|out now|available now|now live|early access|\b1\.0\b"),
    ("EVENT",       "✦", (176, 96, 255),
     r"event|season|festival|weekly|community|contest|showcase|stream|dev ?log|interview|spotlight|"
     r"tournament|giveaway|anniversary|roadmap"),
)
_KIND_DEFAULT = ("NEWS", "◈", (60, 150, 255))


def _classify(title: str) -> tuple:
    low = (title or "").lower()
    for label, glyph, rgb, pattern in _KINDS:
        if re.search(pattern, low):
            return label, glyph, discord.Color.from_rgb(*rgb)
    label, glyph, rgb = _KIND_DEFAULT
    return label, glyph, discord.Color.from_rgb(*rgb)


# ── Game artwork (newer Steam games use hashed header URLs, so ask Steam) ───
_art_cache: dict = {}


async def _steam_art(appid: str) -> Optional[str]:
    """Banner image for a Steam app, or None for non-Steam feeds."""
    if not str(appid).isdigit():
        return None
    if appid in _art_cache:
        return _art_cache[appid]
    url = STEAM_HEADER_URL.format(appid=appid)
    try:
        payload = await _get_json(STEAM_DETAILS_URL, {"appids": appid, "filters": "basic"})
        data = ((payload or {}).get(appid) or {}).get("data") or {}
        header = data.get("header_image")
        if isinstance(header, str) and header.startswith("http"):
            url = header
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, AttributeError, TypeError):
        return url                      # don't cache a failed lookup
    if len(_art_cache) > 200:
        _art_cache.clear()
    _art_cache[appid] = url
    return url


def _post_url(appid: str, item: dict) -> Optional[str]:
    """Canonical Steam news link for the post (falls back to whatever the feed gave)."""
    gid = str(item.get("gid") or "")
    if str(appid).isdigit() and gid.isdigit():
        return f"https://store.steampowered.com/news/app/{appid}/view/{gid}"
    url = item.get("url")
    return url if url and str(url).startswith(("https://", "http://")) else None


EMBED_TOTAL_BUDGET = 5600       # Discord's hard limit is 6000 characters per embed
MAX_SECTION_FIELDS = 10


def build_embed(appid: str, game: str, item: dict, art: Optional[str] = None) -> discord.Embed:
    """Cyber-style update card: type chips, summary quote, one field per section, banner."""
    raw_title = (item.get("title") or "UPDATE DETECTED").strip()
    kind, glyph, color = _classify(raw_title)
    if isinstance(item.get("color"), int):
        color = discord.Color(item["color"])

    # 1) Get clean markdown (+ banner candidates) out of the source text.
    contents = (item.get("contents") or "").strip()
    images: list = []
    video_id = None
    if item.get("plain"):
        md = _tidy(contents, 20000)
    else:
        md, images, video_id = steam_markup_to_md(contents)
    intro, sections = _split_sections(md)

    is_steam = str(appid).isdigit()
    source = (item.get("source") or game or "").strip()
    url = _post_url(appid, item)
    try:
        stamp = int(item["date"])
        posted = datetime.fromtimestamp(stamp, tz=timezone.utc)
    except (KeyError, TypeError, ValueError, OSError):
        stamp, posted = int(time.time()), datetime.now(timezone.utc)

    # 2) Header strip: type chip, source chip, live "x minutes ago".
    origin = f"STEAM  //  APP {appid}" if is_steam else (game or source).upper()[:40]
    strip = f"`{glyph} {kind}`  `⬡ {origin}`  ◷ <t:{stamp}:R>\n▰▰▰▰▰▰▰▰▰▰▰▰▱▱▱▱▱▱▱▱"

    footer_name = item.get("footer") or (
        "STEAM NEWS  •  VAULTBOT SYSTEMS" if is_steam else "PATCH INTELLIGENCE  •  VAULTBOT SYSTEMS")
    footer_text = f"⬡ {footer_name}  //  LIVE UPDATE FEED"[:2048]
    title = f"{glyph}  {raw_title}"[:256]
    author = f"VAULTBOT  //  {source.upper()}"[:256]

    embed_fields: list = []
    truncated = False

    # Optional metadata from non-Steam feeds, shown as compact dashboard fields.
    for label, key, wrap in (("▣  VERSION", "version", "`{}`"),
                             ("◉  STATUS", "status", "{}"),
                             ("⌘  PLATFORM", "platform", "{}")):
        if item.get(key):
            embed_fields.append((label, wrap.format(str(item[key])[:100]), True))

    # 3) Description + section fields, kept inside Discord's 6000-character cap.
    used = len(title) + len(author) + len(footer_text) + sum(len(n) + len(v) for n, v, _ in embed_fields)
    reserve = 160                                           # room for the "more" field
    if sections:
        quote = ""
        if intro:
            paragraphs = intro.split("\n\n")
            if len(intro) <= 650 and not intro.startswith(("▸", "◦")):
                quote = intro                                    # short prose intro: show it all
            elif not paragraphs[0].startswith(("▸", "◦")):
                quote = _clip(paragraphs[0], 650)                # long intro: lead paragraph here...
                rest = "\n\n".join(paragraphs[1:]).strip()
                if rest:
                    sections = [("OVERVIEW", rest)] + list(sections)
                elif quote != paragraphs[0]:
                    sections = [("OVERVIEW", paragraphs[0])] + list(sections)
            else:
                sections = [("OVERVIEW", intro)] + list(sections)  # intro that is a bullet list: its own field
        description = strip + (
            "\n\n" + "\n".join("> " + l if l.strip() else ">" for l in quote.split("\n")) if quote else "")
        used += len(description)
        for i, (name, body) in enumerate(sections):
            field_name = f"▸  {name.upper()}"[:256]
            room = EMBED_TOTAL_BUDGET - used - reserve - len(field_name)
            if len([f for f in embed_fields if not f[2]]) >= MAX_SECTION_FIELDS or room < 120:
                truncated = True
                break
            value = _clip(body, min(1000, room))
            if value != body:
                truncated = True
            embed_fields.append((field_name, value, False))
            used += len(field_name) + len(value)
    else:
        room = min(3900, EMBED_TOTAL_BUDGET - used - reserve - len(strip) - 4)
        body = _clip(md, room) if md else ""
        if body != md:
            truncated = True
        description = strip + "\n\n" + (
            body or "**Update detected.** Open the full report for the details.")

    if truncated and url:
        embed_fields.append(("⋯  MORE IN THE FULL REPORT",
                             "Some of this post was trimmed to fit. Press **OPEN FULL REPORT** below.", False))

    embed = discord.Embed(title=title, url=url, description=description[:4096], color=color)
    embed.set_author(name=author, url=f"https://store.steampowered.com/app/{appid}" if is_steam else None)
    for name, value, inline in embed_fields:
        embed.add_field(name=name, value=value, inline=inline)

    # 4) Banner: the feed's own image > the post's first image > video thumbnail > game art.
    #    (A big image, not a thumbnail: a thumbnail squeezes all the text into a narrow column.)
    banner = item.get("image")
    if not (banner and str(banner).startswith(("https://", "http://"))):
        banner = images[0] if images else None
    if not banner and video_id:
        banner = f"https://img.youtube.com/vi/{video_id}/hqdefault.jpg"
    if not banner and is_steam:
        banner = art or STEAM_HEADER_URL.format(appid=appid)
    if not banner and item.get("thumbnail") and str(item["thumbnail"]).startswith(("https://", "http://")):
        banner = item["thumbnail"]
    if banner:
        embed.set_image(url=banner)

    embed.set_footer(text=footer_text)
    embed.timestamp = posted
    return embed


def build_view(appid: str, item: dict) -> Optional[discord.ui.View]:
    """Link buttons under the embed (they need no handlers, so no persistence worries)."""
    view = discord.ui.View(timeout=None)
    url = _post_url(appid, item)
    if url:
        view.add_item(discord.ui.Button(style=discord.ButtonStyle.link, label="▶  OPEN FULL REPORT", url=url))
    if str(appid).isdigit():
        view.add_item(discord.ui.Button(style=discord.ButtonStyle.link, label="◈  STORE PAGE",
                                        url=f"https://store.steampowered.com/app/{appid}"))
    return view if view.children else None


async def _send(channel_id: int, embed: discord.Embed,
                role_id: Optional[int] = None, note: Optional[str] = None,
                view: Optional[discord.ui.View] = None) -> None:
    channel = _bot.get_channel(channel_id) or await _bot.fetch_channel(channel_id)
    content = note
    mentions = discord.AllowedMentions.none()
    if role_id:
        content = f"<@&{role_id}>" + (f" {note}" if note else "")
        mentions = discord.AllowedMentions(roles=[discord.Object(id=int(role_id))])
    kwargs = {"content": content, "embed": embed, "allowed_mentions": mentions}
    if view is not None:
        kwargs["view"] = view
    await channel.send(**kwargs)


# -----------------------------------------------------------------------------
# Non-Steam sources. Each returns items shaped like the Steam ones:
#   {"gid", "title", "url", "date", "contents", "plain": True, "image", "footer"}
# They read services that are NOT officially documented for bots, so every one
# turns an unexpected reply into a ValueError (reported, never a crash).
# -----------------------------------------------------------------------------
async def _fetch_minecraft(kind: str) -> list:
    payload = await _get_json(MINECRAFT_URL.format(kind=kind), {})
    try:
        items = []
        for e in payload["entries"]:
            if not isinstance(e, dict):
                continue
            version = str(e.get("version") or "").strip()
            if not version and not e.get("title"):
                continue
            image = (e.get("image") or {}).get("url") or ""
            if image.startswith("/"):
                image = MINECRAFT_HOST + image
            items.append({
                "gid": str(e.get("id") or f"{e.get('type', '')}:{version}"),
                "title": str(e.get("title") or f"Minecraft {version}"),
                "url": None,
                "date": _parse_date(e.get("date")),
                "contents": html_to_text(e.get("body", "")),
                "plain": True,
                "image": image if image.startswith("http") else None,
                "footer": "Minecraft • patch notes",
            })
    except (KeyError, TypeError, AttributeError) as exc:
        raise ValueError(f"unexpected Minecraft feed format ({exc})")
    items.sort(key=lambda i: i["date"], reverse=True)    # stable: keeps feed order on ties
    return items[:6]


async def _fetch_nvidia() -> list:
    payload = await _get_json(NVIDIA_URL, {
        "func": "DriverManualLookup", "pfid": NVIDIA_PFID, "osID": "57", "dch": "1",
    })
    try:
        info = payload["IDS"][0]["downloadInfo"]
        version = str(info["Version"]).strip()
        name = str(info.get("Name") or "GeForce Game Ready Driver").strip()
        released = str(info.get("ReleaseDateTime") or "").strip()
        details = info.get("DetailsURL") or None
    except (KeyError, IndexError, TypeError, AttributeError) as exc:
        raise ValueError(f"unexpected NVIDIA reply ({exc})")
    if not version:
        raise ValueError("NVIDIA reply had no driver version")
    lines = [
        "**▰ DRIVER PACKAGE DETECTED**",
        "A new GeForce driver package is available for your system.",
        "",
        f"**Driver build**  `{version}`",
    ]
    if released:
        lines.append(f"**Release date**  {released}")
    lines += [
        "",
        "**WHAT'S INSIDE**",
        "NVIDIA's lookup response provides version and release metadata, but not the full release-note/change list. I won't invent fixes or game optimizations.",
        "",
        f"**NEXT STEP**  [Open NVIDIA's driver details]({details})" if details else "**NEXT STEP**  Open the NVIDIA App or [NVIDIA Driver Downloads](https://www.nvidia.com/Download/index.aspx).",
    ]
    return [{
        "gid": version,
        "title": f"GEFORCE DRIVER // {version}",
        "url": details or "https://www.nvidia.com/Download/index.aspx",
        "date": _parse_date(released),
        "contents": "\n".join(lines),
        "plain": True,
        "image": None,
        "thumbnail": "https://www.nvidia.com/content/dam/en-zz/Solutions/geforce/geforce-rtx-40-series/geforce-rtx-40-series-og-1200x630.jpg",
        "version": version,
        "status": "NEW DRIVER",
        "platform": "GEFORCE / WINDOWS",
        "footer": "NVIDIA GEFORCE • DRIVER INTEL",
        "source": "NVIDIA DRIVER INTELLIGENCE",
    }]


async def _fetch_fortnite() -> list:
    payload = await _get_json(FORTNITE_BUILD_URL, {})
    try:
        build = str(payload["data"]["build"]).strip()
    except (KeyError, TypeError, AttributeError) as exc:
        raise ValueError(f"unexpected Fortnite reply ({exc})")
    found = re.search(r"Release-(\d+(?:\.\d+)*)", build)
    version = found.group(1) if found else build
    if not version:
        raise ValueError("Fortnite reply had no build")

    # Fortnite's public news endpoint provides actual Battle Royale news items;
    # if it is unavailable, keep the build alert honest and link to Epic's site.
    lines = [
        "**◈ LIVE BUILD SIGNAL ACQUIRED**",
        f"Fortnite build `{version}` has been detected by the update monitor.",
    ]
    image = None
    news_url = FORTNITE_NEWS_PAGE
    try:
        news_payload = await _get_json("https://fortnite-api.com/v2/news", {"language": "en"})
        data = news_payload.get("data") or {}
        br = data.get("br") or {}
        motds = br.get("motds") or []
        if motds:
            lines += ["", "**▰ BATTLE ROYALE INTEL**"]
            for entry in motds[:4]:
                if not isinstance(entry, dict):
                    continue
                headline = str(entry.get("title") or entry.get("tabTitle") or "Update")
                body = re.sub(r"<[^>]+>", "", str(entry.get("body") or ""))
                body = html.unescape(re.sub(r"\s+", " ", body)).strip()
                lines.append(f"**{headline[:150]}**")
                if body:
                    lines.append(body[:650])
                lines.append("")
                image = image or entry.get("image") or entry.get("tileImage")
            lines.append("*News items are supplied by the public Fortnite news feed; availability can vary by mode/region.*")
        else:
            lines += ["", "The public news feed did not return current Battle Royale headlines. Open Epic's official news page for the published notes."]
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError, KeyError):
        lines += ["", "**PATCH INTEL UNAVAILABLE**  The public news feed could not be read right now. Open Epic's official news page for the published notes."]

    if image and not str(image).startswith(("https://", "http://")):
        image = None
    items = [{
        "gid": version,
        "title": f"FORTNITE // BUILD {version}",
        "url": news_url,
        "date": int(time.time()),
        "contents": "\n".join(lines)[:3800],
        "plain": True,
        "image": image,
        "version": version,
        "status": "BUILD DETECTED",
        "platform": "FORTNITE / LIVE SERVICE",
        "footer": "FORTNITE • LIVE PATCH INTELLIGENCE",
        "source": "FORTNITE LIVE OPS",
    }]

    # HYPEX posts are added to the same Fortnite subscription/channel. The feed
    # bridge is public; if it is temporarily unavailable, official Fortnite
    # monitoring continues normally.
    try:
        items.extend(await _fetch_hypex())
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError, ET.ParseError) as exc:
        print(f"[PatchNotes] HYPEX feed unavailable; Fortnite updates still active: {exc}")
    return items


async def _fetch_hypex() -> list:
    """Read recent HYPEX posts through public FxEmbed RSS/Atom feed endpoints.

    Try several documented endpoint variants so a single feed URL returning
    404 does not disable HYPEX alerts. If every bridge is unavailable, raise
    the final error; the caller keeps official Fortnite monitoring running.
    """
    urls = (
        "https://fxtwitter.com/hypex/feed.xml",
        "https://fxtwitter.com/hypex/feed.atom.xml",
        "https://fixupx.com/hypex/feed.xml",
        "https://fixupx.com/hypex/feed.atom.xml",
    )
    timeout = aiohttp.ClientTimeout(total=20)
    headers = {
        "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml",
        "User-Agent": "VaultBot/1.0 (+Discord Fortnite update feed)",
    }
    raw = None
    errors = []
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        for url in urls:
            try:
                async with session.get(url, allow_redirects=True) as resp:
                    if resp.status == 404:
                        errors.append(f"{url}: HTTP 404")
                        continue
                    resp.raise_for_status()
                    candidate = await resp.text()
                    # Some bridges may return an HTML error page with HTTP 200.
                    if "<rss" not in candidate[:1000].lower() and "<feed" not in candidate[:1000].lower():
                        errors.append(f"{url}: response was not RSS/Atom XML")
                        continue
                    raw = candidate
                    break
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                errors.append(f"{url}: {exc}")
    if raw is None:
        raise aiohttp.ClientError("all HYPEX feed endpoints failed: " + "; ".join(errors))

    root = ET.fromstring(raw)
    entries = root.findall(".//item")
    if not entries:
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        entries = root.findall(".//atom:entry", ns)

    items = []
    for entry in entries[:10]:
        def child_text(names):
            for name in names:
                found = entry.find(name)
                if found is not None and found.text:
                    return found.text.strip()
            return ""

        title = child_text(["title", "{http://www.w3.org/2005/Atom}title"]) or "HYPEX POST"
        link = child_text(["link", "{http://www.w3.org/2005/Atom}link"])
        if not link:
            atom_link = entry.find("{http://www.w3.org/2005/Atom}link")
            if atom_link is not None:
                link = atom_link.attrib.get("href", "")
        guid = child_text(["guid", "id", "{http://www.w3.org/2005/Atom}id"]) or link or title
        description = child_text(["description", "{http://www.w3.org/2005/Atom}content", "{http://www.w3.org/2005/Atom}summary"])
        description = html.unescape(description)
        description = html_to_text(description, 3000)
        date_text = child_text(["pubDate", "published", "updated", "{http://www.w3.org/2005/Atom}published", "{http://www.w3.org/2005/Atom}updated"])
        try:
            date_value = int(parsedate_to_datetime(date_text).timestamp()) if date_text else _parse_date("")
        except (TypeError, ValueError, OverflowError):
            date_value = _parse_date(date_text)

        image = None
        for node in entry.iter():
            if node.tag.endswith("}content") or node.tag.endswith("}thumbnail") or node.tag == "enclosure":
                candidate = node.attrib.get("url")
                if candidate and candidate.startswith("https://") and any(ext in candidate.lower() for ext in (".jpg", ".jpeg", ".png", ".webp")):
                    image = candidate
                    break
        if not link.startswith(("https://", "http://")):
            link = "https://x.com/HYPEX"
        if not description:
            description = title
        items.append({
            "gid": f"hypex:{guid}",
            "title": f"HYPEX // {title}"[:250],
            "url": link,
            "date": date_value,
            "contents": f"**◈ HYPEX / LEAKS INTEL**\n{description}\n\n[OPEN ORIGINAL POST]({link})"[:3800],
            "plain": True,
            "image": image,
            "status": "COMMUNITY INTEL",
            "platform": "FORTNITE / X",
            "footer": "HYPEX • COMMUNITY INTEL (UNOFFICIAL)",
            "source": "HYPEX // UNOFFICIAL SOURCE",
        })
    items.sort(key=lambda item: int(item.get("date") or 0), reverse=True)
    return items[:8]


FEEDS = {
    "feed:minecraft_java": ("Minecraft: Java Edition", lambda: _fetch_minecraft("java")),
    "feed:minecraft_bedrock": ("Minecraft: Bedrock Edition", lambda: _fetch_minecraft("bedrock")),
    "feed:nvidia": ("NVIDIA GeForce drivers", _fetch_nvidia),
    "feed:fortnite": ("Fortnite updates", _fetch_fortnite),
}


async def _fetch_items(key: str) -> list:
    """One entry point for every source: a Steam app ID or a feed key."""
    if key in FEEDS:
        return await FEEDS[key][1]()
    if key.startswith("feed:"):
        raise ValueError(f"unknown source {key}")
    return await _fetch_news(key)


# -----------------------------------------------------------------------------
# Background checker
# -----------------------------------------------------------------------------
def _remember(appid: str, gids: list) -> None:
    history = _seen.setdefault(appid, [])
    for gid in gids:
        if gid not in history:
            history.append(gid)
    del history[:-SEEN_HISTORY]


async def poll_once() -> None:
    targets: dict = {}   # game ID -> [(server ID, subscription), ...]
    for guild_id, games in _subs.items():
        for appid, sub in games.items():
            targets.setdefault(appid, []).append((guild_id, sub))

    changed = False
    cutoff = time.time() - MAX_AGE_DAYS * 86400
    for appid, subscribers in targets.items():
        try:
            items = await _fetch_items(appid)
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
            print(f"[PatchNotes] Lookup failed for {appid}: {exc}")
            continue

        if appid not in _seen:       # first time we see this game: just remember what's there
            _remember(appid, [i["gid"] for i in items])
            changed = True
            continue

        known = set(_seen[appid])
        new = [i for i in items if i["gid"] not in known]
        if not new:
            continue
        _remember(appid, [i["gid"] for i in new])
        changed = True

        fresh = [i for i in new if int(i.get("date") or 0) >= cutoff]
        fresh.sort(key=lambda i: int(i.get("date") or 0))      # oldest first
        fresh = fresh[-MAX_POSTS_PER_POLL:]

        art = await _steam_art(appid)            # game banner (None for non-Steam feeds)
        for guild_id, sub in subscribers:
            for item in fresh:
                try:
                    await _send(int(sub["channel_id"]), build_embed(appid, sub["name"], item, art),
                                role_id=sub.get("role_id"), view=build_view(appid, item))
                    print(f"[PatchNotes] Posted '{item.get('title', '')[:60]}' ({sub['name']}) in server {guild_id}.")
                except (discord.HTTPException, ValueError, TypeError, AttributeError) as exc:
                    print(f"[PatchNotes] Couldn't post for {sub['name']} in server {guild_id}: {exc}")
        await asyncio.sleep(1)       # be gentle with Steam

    if changed:
        _save()


def _make_loop():
    @tasks.loop(minutes=POLL_MINUTES)
    async def patchnotes_loop():
        try:
            await poll_once()
        except Exception as exc:     # never let one bad round stop future checks
            print(f"[PatchNotes] Check failed: {type(exc).__name__}: {exc}")

    @patchnotes_loop.before_loop
    async def _wait():
        await _bot.wait_until_ready()

    return patchnotes_loop


# -----------------------------------------------------------------------------
# Slash commands:  /patchnotes add | remove | list
# -----------------------------------------------------------------------------
group = app_commands.Group(
    name="patchnotes",
    description="Get Steam game patch notes posted in a channel",
    guild_only=True,
    default_permissions=discord.Permissions(manage_guild=True),
)


async def _require_manager(interaction: discord.Interaction) -> bool:
    perms = getattr(interaction.user, "guild_permissions", None)
    if perms is not None and perms.manage_guild:
        return True
    await interaction.response.send_message(
        "You need the **Manage Server** permission to change patch note alerts.", ephemeral=True
    )
    return False


async def _prepare_target(interaction: discord.Interaction, channel):
    """Pick the channel and make sure the bot can post there. None = already replied."""
    target = channel or interaction.channel
    if not isinstance(target, discord.TextChannel):
        await interaction.response.send_message("Pick a regular text channel.", ephemeral=True)
        return None
    perms = target.permissions_for(interaction.guild.me)
    if not (perms.view_channel and perms.send_messages and perms.embed_links):
        await interaction.response.send_message(
            f"I need **View Channel**, **Send Messages** and **Embed Links** in {target.mention}.",
            ephemeral=True,
        )
        return None
    return target


async def _subscribe(interaction: discord.Interaction, key: str, name: str, items: list,
                     target: discord.TextChannel, role: Optional[discord.Role], where: str) -> None:
    """Save a subscription, remember what already exists, and post a one-time preview."""
    games = _subs.setdefault(str(interaction.guild_id), {})
    if key not in games and len(games) >= MAX_GAMES_PER_SERVER:
        await interaction.followup.send(
            f"This server already follows {MAX_GAMES_PER_SERVER} sources. Remove one first.",
            ephemeral=True)
        return

    games[key] = {"name": name, "channel_id": target.id, "role_id": role.id if role else None}
    if key not in _seen:             # start from "now" so old posts aren't dumped into the channel
        _remember(key, [i["gid"] for i in items])
    _save()

    try:   # show the newest post once, so you can see exactly how alerts will look
        art = await _steam_art(key)
        await _send(target.id, build_embed(key, name, items[0], art),
                    note=f"✅ Updates for **{name}** will post here. Latest one, as a preview:",
                    view=build_view(key, items[0]))
    except discord.HTTPException as exc:
        await interaction.followup.send(
            f"Saved, but I couldn't post in {target.mention}: {exc}", ephemeral=True)
        return

    ping = f" and ping {role.mention}" if role else ""
    await interaction.followup.send(
        f"✅ Following **{name}**{where} in {target.mention}{ping}.", ephemeral=True)


@group.command(name="add", description="Post a Steam game's patch notes in a channel")
@app_commands.describe(
    game="Game name (or its Steam app ID, the number in its store page link)",
    channel="Where to post the patch notes (default: this channel)",
    role="Optional role to ping for each new update",
)
async def patchnotes_add(
    interaction: discord.Interaction,
    game: str,
    channel: Optional[discord.TextChannel] = None,
    role: Optional[discord.Role] = None,
):
    if not await _require_manager(interaction):
        return
    target = await _prepare_target(interaction, channel)
    if target is None:
        return

    await interaction.response.defer(ephemeral=True, thinking=True)
    query = game.strip()
    try:
        if query.isdigit():
            appid, name = query, await _game_name(query)
        else:
            found = await _search_game(query)
            if not found:
                await interaction.followup.send(
                    f"I couldn't find a Steam game called **{query[:80]}**. "
                    "Try the exact name, or use its Steam app ID.", ephemeral=True)
                return
            appid, name = found
        items = await _fetch_items(appid)
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
        await interaction.followup.send(
            "Steam didn't answer just now. Please try again in a minute.", ephemeral=True)
        return

    if not items:
        await interaction.followup.send(
            f"**{name}** has no developer announcements on Steam, so there's nothing to follow.",
            ephemeral=True)
        return

    await _subscribe(
        interaction, appid, name, items, target, role,
        where=f" (Steam app {appid}). Not the right game? Use `/patchnotes remove`, "
              "then add it again with its Steam app ID")


@group.command(name="addfeed", description="Follow Minecraft, NVIDIA drivers or Fortnite updates")
@app_commands.describe(
    source="What to follow",
    channel="Where to post the updates (default: this channel)",
    role="Optional role to ping for each new update",
)
@app_commands.choices(source=[
    app_commands.Choice(name=label, value=key) for key, (label, _fn) in FEEDS.items()
])
async def patchnotes_addfeed(
    interaction: discord.Interaction,
    source: str,
    channel: Optional[discord.TextChannel] = None,
    role: Optional[discord.Role] = None,
):
    if not await _require_manager(interaction):
        return
    if source not in FEEDS:
        await interaction.response.send_message("Unknown source.", ephemeral=True)
        return
    target = await _prepare_target(interaction, channel)
    if target is None:
        return

    label = FEEDS[source][0]
    await interaction.response.defer(ephemeral=True, thinking=True)
    try:
        items = await _fetch_items(source)
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
        await interaction.followup.send(
            f"I couldn't read the **{label}** source right now ({str(exc)[:150]}). "
            "It may be down or may have changed. Try again later.", ephemeral=True)
        return
    if not items:
        await interaction.followup.send(
            f"The **{label}** source returned nothing, so there's nothing to follow yet.",
            ephemeral=True)
        return
    await _subscribe(interaction, source, label, items, target, role, where="")


async def _remove_autocomplete(interaction: discord.Interaction, current: str):
    games = _subs.get(str(interaction.guild_id), {})
    current = current.lower()
    return [
        app_commands.Choice(name=sub["name"][:100], value=appid)
        for appid, sub in games.items()
        if current in sub["name"].lower() or current in appid
    ][:25]


@group.command(name="remove", description="Stop following a game or feed")
@app_commands.describe(game="The game or feed to stop following")
@app_commands.autocomplete(game=_remove_autocomplete)
async def patchnotes_remove(interaction: discord.Interaction, game: str):
    if not await _require_manager(interaction):
        return
    games = _subs.get(str(interaction.guild_id), {})
    key = game.strip()
    if key not in games:
        key = next((a for a, s in games.items() if s["name"].lower() == key.lower()), "")
    if not key:
        await interaction.response.send_message(
            "That game isn't on this server's list. Use `/patchnotes list` to see it.", ephemeral=True)
        return
    name = games.pop(key)["name"]
    if not games:
        _subs.pop(str(interaction.guild_id), None)
    if not any(key in g for g in _subs.values()):
        _seen.pop(key, None)         # nobody follows it any more
    _save()
    await interaction.response.send_message(f"🗑️ Stopped following **{name}**.", ephemeral=True)


@group.command(name="list", description="Show what this server follows")
async def patchnotes_list(interaction: discord.Interaction):
    if not await _require_manager(interaction):
        return
    games = _subs.get(str(interaction.guild_id), {})
    if not games:
        await interaction.response.send_message(
            "No games yet. Add one with `/patchnotes add`.", ephemeral=True)
        return
    lines = []
    for appid, sub in games.items():
        ping = f" (pings <@&{sub['role_id']}>)" if sub.get("role_id") else ""
        kind = f"Steam app {appid}" if appid.isdigit() else "feed"
        lines.append(f"• **{sub['name']}** ({kind}) → <#{sub['channel_id']}>{ping}")
    embed = discord.Embed(title="📰 Patch note alerts", description="\n".join(lines)[:4000],
                          color=EMBED_COLOR)
    await interaction.response.send_message(
        embed=embed, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())


# -----------------------------------------------------------------------------
# Hook-up (called once from VaultBot.py)
# -----------------------------------------------------------------------------
def install(bot: discord.Client) -> None:
    global _bot
    _bot = bot
    bot.tree.add_command(group)
    loop = _make_loop()

    async def _on_ready():
        global _started
        if _started:                 # on_ready can fire again after a reconnect
            return
        _started = True
        _load()
        if not loop.is_running():
            loop.start()
        print(f"[PatchNotes] Checking Steam every {POLL_MINUTES} minute(s).")

    bot.add_listener(_on_ready, "on_ready")


# Install PatchNotes commands and its background polling loop.
install(bot)

# =============================================================================
# 13. STARTUP
# =============================================================================
if not TOKEN:
    print("DISCORD_TOKEN detected:", bool(os.getenv("DISCORD_TOKEN")))
    raise RuntimeError(
        "DISCORD_TOKEN is not set. Set your bot token before starting."
    )

if not shutil.which("ffmpeg"):
    print("WARNING: ffmpeg executable not found. Voice playback will fail until FFmpeg is available.")

bot.run(TOKEN)

