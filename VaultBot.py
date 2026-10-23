from dotenv import load_dotenv

load_dotenv("/home/container/.env")

import asyncio
import base64
import os
import shutil
import tempfile
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import discord
from discord.ext import commands
import yt_dlp

TOKEN = os.getenv("DISCORD_TOKEN")

# Keep the existing prefix. Message Content Intent must also be enabled
# for this bot in the Discord Developer Portal.
intents = discord.Intents.default()
intents.message_content = True
intents.voice_states = True
intents.guilds = True
intents.messages = True

bot = commands.Bot(command_prefix="~", intents=intents)

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

# -------------------- YouTube cookies --------------------
# Put your cookies in the host's environment variables as COOKIES_B64 (base64
# of cookies.txt). The bot rebuilds the cookies file from it on every startup,
# so the cookies never have to be committed to GitHub.
COOKIE_PATH = os.path.join(tempfile.gettempdir(), "vaultbot_cookies.txt")
_cookies_b64 = os.getenv("COOKIES_B64")
if _cookies_b64:
    try:
        Path(COOKIE_PATH).write_bytes(base64.b64decode(_cookies_b64.strip()))
        YTDL_OPTIONS["cookiefile"] = COOKIE_PATH
        print(f"yt-dlp will use cookies from {COOKIE_PATH}")
    except Exception as exc:
        print(f"Could not write cookies file from COOKIES_B64: {exc}")
else:
    print("WARNING: COOKIES_B64 is not set. YouTube may block this host's IP.")

print(f"yt-dlp version: {yt_dlp.version.__version__}")


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
        })
        try:
            with yt_dlp.YoutubeDL(options) as ydl:
                info = ydl.extract_info(query, download=False)
                if "entries" in info:
                    info = next((entry for entry in info["entries"] if entry), None)
                if not info:
                    raise ValueError("No playable result was found.")
                webpage_url = info.get("webpage_url") or info.get("original_url") or query
                # Download the selected item rather than retaining an expiring stream URL.
                downloaded = ydl.extract_info(webpage_url, download=True)
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
    embed = discord.Embed(
        title="🎶 Now Playing",
        description=f"[{discord.utils.escape_markdown(track.title)}]({track.webpage_url})",
        color=discord.Color.from_rgb(28, 27, 32),
    )
    embed.add_field(name="Duration", value=format_duration(track.duration), inline=True)
    embed.add_field(name="Requested by", value=track.requester, inline=True)
    embed.set_footer(text=f"Loop: {loop_modes.get(ctx.guild.id, 'off')}")
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
        volume = volumes.get(guild_id, 0.5)

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


@bot.event
async def on_ready():
    print(f"Bot is online as {bot.user}")
    try:
        synced = await bot.tree.sync()
        print(f"Synced {len(synced)} slash command(s)")
    except Exception as error:
        print(f"Command sync failed: {error}")


@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError):
    if isinstance(error, commands.CommandNotFound):
        return
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(f"Missing argument: `{error.param.name}`. Use `~help` to see examples.")
        return
    if isinstance(error, commands.CommandOnCooldown):
        await ctx.send("Please wait a moment before using that command again.")
        return
    if isinstance(error, commands.CommandError):
        await ctx.send(str(error))
        return
    print(f"Command error: {error}")
    await ctx.send("Something went wrong while running that command. Check the bot console.")


# -------------------- Prefix music commands --------------------

@bot.command(name="play", aliases=["p"])
async def play_command(ctx: commands.Context, *, query: str = ""):
    """Play a song name, supported URL, or attached MP3/WAV/Opus file."""
    if not ctx.guild:
        return await ctx.send("Use this command inside a server.")
    try:
        await ensure_voice(ctx)
    except Exception as exc:
        return await ctx.send(str(exc))

    attachment = ctx.message.attachments[0] if ctx.message.attachments else None
    if attachment:
        suffix = Path(urlparse(attachment.filename).path).suffix.lower()
        if suffix not in {".mp3", ".wav", ".opus", ".ogg", ".m4a", ".flac"}:
            return await ctx.send("Attach an MP3, WAV, Opus, OGG, M4A, or FLAC audio file.")
        if attachment.size > 20 * 1024 * 1024:
            return await ctx.send("That file is over the bot's 20 MB safety limit.")
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
            "Usage: `~play song name or URL` — or attach an audio file to `~play`."
        )

    # Spotify and Apple Music links may provide metadata or a playable match,
    # but direct catalog audio streaming is not guaranteed or bypassed here.
    await ctx.typing()
    try:
        track = await extract_track(query.strip(), ctx.author.display_name)
        await queue_track(ctx, track)
    except Exception as exc:
        error_text = str(exc)
        print(f"yt-dlp error: {error_text}")
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
    embed = discord.Embed(title="🎵 Music Queue", color=discord.Color.from_rgb(28, 27, 32))
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
    embed = discord.Embed(
        title="🎶 Now Playing",
        description=f"[{discord.utils.escape_markdown(track.title)}]({track.webpage_url})",
        color=discord.Color.from_rgb(28, 27, 32),
    )
    embed.add_field(name="Duration", value=format_duration(track.duration), inline=True)
    embed.add_field(name="Requested by", value=track.requester, inline=True)
    await ctx.send(embed=embed)


@bot.command(name="loop")
async def loop_command(ctx: commands.Context, mode: str = "off"):
    if not ctx.guild:
        return
    mode = mode.lower()
    aliases = {"song": "song", "track": "song", "queue": "queue", "all": "queue", "off": "off", "none": "off"}
    if mode not in aliases:
        return await ctx.send("Usage: `~loop song`, `~loop queue`, or `~loop off`.")
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


@bot.command(name="musichelp", aliases=["mhelp"])
async def musichelp_command(ctx: commands.Context):
    embed = discord.Embed(title="VaultBot Music Commands", color=discord.Color.from_rgb(28, 27, 32))
    embed.description = (
        "`~play <song or URL>` — play/search and queue a song\n"
        "`~play` with an audio attachment — play your uploaded file\n"
        "`~queue` — show the queue\n"
        "`~skip` — skip the current song\n"
        "`~stop` — stop and clear the queue\n"
        "`~pause` / `~resume` — pause or resume\n"
        "`~volume 0-100` — set volume\n"
        "`~nowplaying` — show current track\n"
        "`~loop song|queue|off` — set loop mode\n"
        "`~leave` — disconnect from voice\n"
        "`~help` — show all bot commands"
    )
    await ctx.send(embed=embed)


# -------------------- Existing slash commands: preserved --------------------

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
        color=discord.Color.from_rgb(28, 27, 32)
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


if not TOKEN:
    print("DISCORD_TOKEN detected:", bool(os.getenv("DISCORD_TOKEN")))
    raise RuntimeError(
        "DISCORD_TOKEN is not set. Set your bot token before starting."
    )

if not shutil.which("ffmpeg"):
    print("WARNING: ffmpeg executable not found. Voice playback will fail until FFmpeg is available.")

bot.run(TOKEN)
