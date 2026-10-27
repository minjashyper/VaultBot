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
