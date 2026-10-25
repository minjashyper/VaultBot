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

