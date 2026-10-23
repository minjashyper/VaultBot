"""
VaultBot — a Discord music + community bot.

File layout
-----------
 1. Imports
 2. Configuration & constants
 3. Bot setup
 4. yt-dlp & YouTube cookies
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

load_dotenv("/home/container/.env")

# =============================================================================
# 1. IMPORTS
# =============================================================================
import asyncio
import base64
import faulthandler
import json
import os
import shutil
import signal
import sys
import tempfile
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
JUICEWRLD_POLL_MINUTES = max(
    1, int(os.getenv("JUICEWRLD_POLL_MINUTES", "5"))
)
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
# 4. YT-DLP & YOUTUBE COOKIES
# =============================================================================
# yt-dlp settings. This resolves public, playable media sources; it does not
# bypass paid services, DRM, private content, or service licensing restrictions.
YTDL_OPTIONS = {
    "format": "bestaudio/best",
    "noplaylist": True,
    "quiet": True,
    "no_warnings": True,
    "default_search": "ytsearch",
    "source_address": "0.0.0.0",
    "extract_flat": False,
    "skip_download": True,
}
FFMPEG_OPTIONS = {"options": "-vn"}


# Put your cookies in the host's environment variables as base64 of cookies.txt.
# The host limits each variable to 2000 characters, so the base64 text can be
# split across COOKIES_B64_1, COOKIES_B64_2, COOKIES_B64_3, ... (in order).
# A single COOKIES_B64 variable still works too. The bot rebuilds the cookies
# file from these on every startup, so the cookies never have to be committed
# to GitHub.
def _read_cookies_b64() -> str:
    parts = []
    for i in range(1, 51):
        part = os.getenv(f"COOKIES_B64_{i}")
        if part is None:
            break
        parts.append("".join(part.split()))
    if parts:
        return "".join(parts)
    return "".join((os.getenv("COOKIES_B64") or "").split())


COOKIE_PATH = os.path.join(tempfile.gettempdir(), "vaultbot_cookies.txt")
_cookies_b64 = _read_cookies_b64()
if _cookies_b64:
    try:
        Path(COOKIE_PATH).write_bytes(base64.b64decode(_cookies_b64))
        YTDL_OPTIONS["cookiefile"] = COOKIE_PATH
        print(f"yt-dlp will use cookies from {COOKIE_PATH}")
    except Exception as exc:
        print(f"Could not write cookies file from the COOKIES_B64 variables: {exc}")
else:
    print("WARNING: no COOKIES_B64_1 / COOKIES_B64 variable is set. YouTube may block this host's IP.")

print(f"yt-dlp version: {yt_dlp.version.__version__}")


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
async def extract_track(query: str, requester: str) -> Track:
    """Resolve and download a playable track so live YouTube URLs cannot expire mid-song."""
    def _extract():
        # Download each track to its own directory. Playing a local file avoids
        # the short-lived googlevideo stream URL being reset during playback.
        folder = tempfile.mkdtemp(prefix="vaultbot_audio_")
        options = dict(YTDL_OPTIONS)
        options.update({
            "outtmpl": str(Path(folder) / "%(id)s.%(ext)s"),
            "noplaylist": True,
            "skip_download": False,
            "quiet": True,
            "no_warnings": False,
            "logger": YTDLLogger(),
        })
        try:
            diag("extract: looking up the video")
            with yt_dlp.YoutubeDL(options) as ydl:
                info = ydl.extract_info(query, download=False)
                diag("extract: lookup finished, starting download")
                if "entries" in info:
                    info = next((entry for entry in info["entries"] if entry), None)
                if not info:
                    raise ValueError("No playable result was found.")
                webpage_url = info.get("webpage_url") or info.get("original_url") or query
                # Download the selected item rather than retaining an expiring stream URL.
                downloaded = ydl.extract_info(webpage_url, download=True)
                diag("extract: download finished")
                requested = (downloaded or {}).get("requested_downloads") or []
                path = requested[0].get("filepath") if requested else None
                if not path or not Path(path).is_file():
                    path = ydl.prepare_filename(downloaded or info)
                if not Path(path).is_file():
                    candidates = [p for p in Path(folder).iterdir() if p.is_file()]
                    if not candidates:
                        raise ValueError("yt-dlp did not produce an audio file.")
                    path = str(candidates[0])
                return {
                    "title": (downloaded or info).get("title") or info.get("title") or "Unknown title",
                    "webpage_url": webpage_url,
                    "path": str(path),
                    "duration": (downloaded or info).get("duration") or info.get("duration"),
                    "extractor": info.get("extractor_key") or info.get("extractor") or "audio",
                }
        except Exception:
            shutil.rmtree(folder, ignore_errors=True)
            raise

    data = await asyncio.to_thread(_extract)
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
        for key in ("results", "items", "data", "events"):
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
                "Searches the Juice WRLD catalog and shows up to 5 matches with "
                "the artist, category, album, and catalog ID.\n"
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
                "**\"YouTube is blocking this bot\"** — the bot owner needs to "
                "refresh the YouTube cookies and restart the bot.\n"
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
        if "confirm you" in error_text and "not a bot" in error_text:
            await ctx.send(
                "⚠️ YouTube is blocking this bot's server. The YouTube cookies are "
                "missing, expired, or not being accepted. The bot owner needs to "
                "export fresh cookies (see the setup notes) and restart the bot."
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
    current_tracks.pop(ctx.guild.id, None)
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
    current_tracks.pop(guild_id, None)
    if voice.is_playing() or voice.is_paused():
        voice.stop()
    await voice.disconnect()
    await ctx.send("👋 Disconnected from voice.")


# ---- Juice WRLD ----

@bot.command(name="jwsong")
async def juicewrld_song_search(ctx: commands.Context, *, query: str):
    """Search Juice WRLD catalog metadata. Usage: ~jwsong lucid dreams"""
    try:
        payload = await _juicewrld_api_get(
            "/songs/",
            params={"search": query, "page_size": 5}
        )
        songs = _juicewrld_items(payload)

        if not songs:
            await ctx.send(
                f"No Juice WRLD catalog results found for **{query[:100]}**."
            )
            return

        embed = discord.Embed(
            title=f"🔎 Juice WRLD Search: {query[:200]}",
            color=JUICE_COLOR
        )

        for song in songs[:5]:
            if not isinstance(song, dict):
                continue
            name = _juicewrld_safe(song.get("name"), "Unknown title", 250)
            category = _juicewrld_safe(
                song.get("category"), "Uncategorized", 100
            )
            artists = _juicewrld_safe(
                song.get("credited_artists"), "Juice WRLD", 200
            )
            details = f"Artist: {artists}\nCategory: {category}"
            if song.get("id") is not None:
                details += f"\nCatalog ID: `{song['id']}`"
            if song.get("album"):
                details += f"\nAlbum: {_juicewrld_safe(song['album'], limit=150)}"
            embed.add_field(name=name, value=details[:1024], inline=False)

        embed.set_footer(text="Catalog information from Juice WRLD API")
        await ctx.send(embed=embed)

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
