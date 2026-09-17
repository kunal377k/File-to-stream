import asyncio
import math
import os
import re
import secrets
import traceback
from typing import Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from pyrogram import Client, enums, filters, raw, utils
from pyrogram.errors import FloodWait, UserNotParticipant
from pyrogram.file_id import FileId
from pyrogram.session import Auth, Session
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message

from config import Config
from database import db

# Some Pyrogram 2.x builds have an old lower-bound check for supergroup/channel IDs.
# This keeps valid Telegram -100... channel IDs accepted.
utils.MIN_CHANNEL_ID = -1007852516352
utils.MIN_CHAT_ID = -999999999999

app = FastAPI(title="File-to-Stream Bot")

bot = Client(
    "FileToStreamBot",
    api_id=Config.API_ID,
    api_hash=Config.API_HASH,
    bot_token=Config.BOT_TOKEN,
    in_memory=True,
)

BOT_USERNAME = ""
WORK_LOAD = 0
MEDIA_SESSIONS = {}


def readable_size(size: int) -> str:
    if size <= 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.2f} {unit}"
        value /= 1024
    return f"{size} B"


def safe_filename(name: Optional[str]) -> str:
    name = name or "file"
    name = re.sub(r"[^A-Za-z0-9._ -]", "_", name).strip()
    return name[:180] or "file"


def masked_filename(name: str) -> str:
    base, ext = os.path.splitext(name or "file")
    pattern = re.compile(
        r"((19|20)\d{2}|4k|2160p|1080p|720p|480p|360p|HEVC|x265|BluRay|WEB-DL|HDRip)",
        re.IGNORECASE,
    )
    match = pattern.search(base)
    if match:
        title = base[: match.start()].strip(" .-_")
        metadata = base[match.start() :]
    else:
        title, metadata = base, ""
    masked = "".join(
        ch if (i % 3 == 0 and ch.isalnum()) else ("*" if ch.isalnum() else ch)
        for i, ch in enumerate(title)
    )
    return f"{masked} {metadata}{ext}".strip()


def get_media(message: Message):
    return message.document or message.video or message.audio


@bot.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, message: Message):
    print(f"📩 /start received | user_id={message.from_user.id}")
    if len(message.command) > 1 and message.command[1].startswith("file_"):
        unique_id = message.command[1][5:]
        message_id = await db.get_message_id(unique_id)
        if not message_id:
            await message.reply_text("❌ This link is invalid or expired.")
            return
        link = f"{Config.BASE_URL}/show/{unique_id}"
        await message.reply_text(
            "✅ Link generated successfully.\n\n"
            f"Open: {link}",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🌐 Open File", url=link)]]
            ),
            disable_web_page_preview=True,
        )
        return

    await message.reply_text(
        "👋 **Welcome to File-to-Stream Bot!**\n\n"
        "📤 Send me a video, document or audio file.\n"
        "🔗 I will store it and generate a shareable streaming link.\n\n"
        "You can then open the link in a browser or media player."
    )


@bot.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def file_handler(client: Client, message: Message):
    global WORK_LOAD
    print(f"📁 File received | user_id={message.from_user.id}")
    WORK_LOAD += 1
    try:
        stored = await message.copy(chat_id=Config.STORAGE_CHANNEL)
        unique_id = secrets.token_urlsafe(10).replace("-", "_")
        await db.save_link(unique_id, stored.id)

        link = f"{Config.BASE_URL}/show/{unique_id}"
        await message.reply_text(
            "✅ **File uploaded successfully!**\n\n"
            f"🔗 `{link}`",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🌐 Open Link", url=link)]]
            ),
            disable_web_page_preview=True,
        )
        print(f"✅ File stored | storage_message_id={stored.id} | id={unique_id}")
    except Exception:
        print("❌ File upload error:\n" + traceback.format_exc())
        await message.reply_text(
            "❌ File upload failed. Please make sure the bot is an admin in the storage channel."
        )
    finally:
        WORK_LOAD -= 1


class ByteStreamer:
    def __init__(self, client: Client):
        self.client = client

    @staticmethod
    def location(file_id: FileId):
        return raw.types.InputDocumentFileLocation(
            id=file_id.media_id,
            access_hash=file_id.access_hash,
            file_reference=file_id.file_reference,
            thumb_size=file_id.thumbnail_size,
        )

    async def iter_range(
        self,
        file_id: FileId,
        start: int,
        end: int,
        chunk_size: int = 1024 * 1024,
    ):
        global MEDIA_SESSIONS
        client = self.client
        dc_id = file_id.dc_id
        media_session = MEDIA_SESSIONS.get(dc_id)

        if media_session is None:
            if dc_id != await client.storage.dc_id():
                auth_key = await Auth(
                    client, dc_id, await client.storage.test_mode()
                ).create()
                media_session = Session(
                    client,
                    dc_id,
                    auth_key,
                    await client.storage.test_mode(),
                    is_media=True,
                )
                await media_session.start()
                exported = await client.invoke(
                    raw.functions.auth.ExportAuthorization(dc_id=dc_id)
                )
                await media_session.invoke(
                    raw.functions.auth.ImportAuthorization(
                        id=exported.id, bytes=exported.bytes
                    )
                )
            else:
                media_session = client.session
            MEDIA_SESSIONS[dc_id] = media_session

        location = self.location(file_id)
        offset = (start // chunk_size) * chunk_size
        first_cut = start - offset
        remaining = end - start + 1

        while remaining > 0:
            request_size = min(chunk_size, remaining + first_cut)
            request_size = max(4096, request_size)
            request_size = (request_size // 4096) * 4096
            if request_size <= 0:
                request_size = 4096

            result = await media_session.invoke(
                raw.functions.upload.GetFile(
                    location=location, offset=offset, limit=request_size
                ),
                retries=2,
            )
            if not isinstance(result, raw.types.upload.File) or not result.bytes:
                break

            data = result.bytes
            data = data[first_cut:]
            first_cut = 0
            if len(data) > remaining:
                data = data[:remaining]

            if not data:
                break
            yield data
            remaining -= len(data)
            offset += request_size


@app.api_route("/", methods=["GET", "HEAD"])
async def health():
    return JSONResponse({"status": "ok", "bot": BOT_USERNAME or "starting"})


@app.get("/show/{unique_id}", response_class=HTMLResponse)
async def show_page(unique_id: str):
    message_id = await db.get_message_id(unique_id)
    if not message_id:
        raise HTTPException(404, "File link is invalid or expired.")

    try:
        message = await bot.get_messages(Config.STORAGE_CHANNEL, message_id)
    except Exception:
        raise HTTPException(404, "File is not available on Telegram.")

    media = get_media(message)
    if not media:
        raise HTTPException(404, "Media not found.")

    filename = safe_filename(getattr(media, "file_name", None))
    mime = getattr(media, "mime_type", None) or "application/octet-stream"
    size = readable_size(getattr(media, "file_size", 0) or 0)
    dl = f"{Config.BASE_URL}/dl/{message_id}/{filename}"
    escaped_name = masked_filename(filename).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    player = mime.startswith("video/") or mime.startswith("audio/")

    media_player = ""
    if player:
        tag = "video" if mime.startswith("video/") else "audio"
        controls = "controls autoplay" if tag == "video" else "controls"
        media_player = f'<{tag} {controls} preload="metadata" src="{dl}"></{tag}>'

    html = f"""<!doctype html>
<html><head><meta name="viewport" content="width=device-width,initial-scale=1">
<title>File to Stream</title>
<style>
body{{font-family:Arial,sans-serif;background:#f4f4f4;margin:0;padding:24px}}
.card{{max-width:720px;margin:auto;background:white;border-radius:16px;padding:24px;box-shadow:0 4px 20px #0001}}
h1{{font-size:22px}}.name{{font-weight:600;word-break:break-word}}.muted{{color:#666}}
.player{{width:100%;margin:20px 0}}video,audio{{width:100%;max-height:70vh}}
.btn{{display:inline-block;padding:12px 18px;background:#111;color:#fff;text-decoration:none;border-radius:10px;margin-top:12px}}
</style></head><body><div class="card">
<h1>📁 File to Stream</h1><p class="name">{escaped_name}</p><p class="muted">Size: {size}</p>
<div class="player">{media_player}</div>
<a class="btn" href="{dl}">⬇️ Download / Open</a>
</div></body></html>"""
    return HTMLResponse(html)


@app.get("/dl/{message_id}/{filename}")
async def download_file(request: Request, message_id: int, filename: str):
    try:
        message = await bot.get_messages(Config.STORAGE_CHANNEL, message_id)
    except Exception:
        raise HTTPException(404, "File not found.")

    media = get_media(message)
    if not media or message.empty:
        raise HTTPException(404, "File not found.")

    file_id = FileId.decode(media.file_id)
    file_size = int(media.file_size or 0)
    if file_size <= 0:
        raise HTTPException(404, "Empty file.")

    range_header = request.headers.get("range")
    start, end = 0, file_size - 1

    if range_header:
        match = re.fullmatch(r"bytes=(\d*)-(\d*)", range_header.strip())
        if not match:
            raise HTTPException(416, "Invalid Range header.")
        a, b = match.groups()
        if a == "" and b == "":
            raise HTTPException(416, "Invalid Range header.")
        if a == "":
            length = int(b)
            if length <= 0:
                raise HTTPException(416, "Invalid range.")
            start = max(file_size - length, 0)
        else:
            start = int(a)
            end = int(b) if b else file_size - 1
        if start >= file_size or start < 0 or end < start:
            raise HTTPException(416, "Requested range not satisfiable.")
        end = min(end, file_size - 1)

    length = end - start + 1
    streamer = ByteStreamer(bot)
    body = streamer.iter_range(file_id, start, end)
    status = 206 if range_header else 200
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Length": str(length),
        "Content-Type": getattr(media, "mime_type", None) or "application/octet-stream",
        "Content-Disposition": f'inline; filename="{safe_filename(getattr(media, "file_name", None))}"',
        "Cache-Control": "public, max-age=3600",
    }
    if range_header:
        headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"

    return StreamingResponse(body, status_code=status, headers=headers)


async def validate_and_start_bot():
    global BOT_USERNAME
    print("--- Starting File-to-Stream ---")
    print(f"Storage channel: {Config.STORAGE_CHANNEL}")
    print(f"Base URL: {Config.BASE_URL}")

    await db.connect()
    print("Starting Telegram bot...")
    await bot.start()
    me = await bot.get_me()
    BOT_USERNAME = me.username or ""
    print(f"✅ Bot started: @{BOT_USERNAME}")

    chat = await bot.get_chat(Config.STORAGE_CHANNEL)
    print(f"✅ Storage channel accessible: {chat.title!r} | id={chat.id}")
    print("📡 Telegram update receiver is active.")


async def main():
    await validate_and_start_bot()
    port = int(os.getenv("PORT", "10000"))
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host="0.0.0.0",
            port=port,
            log_level="info",
            loop="asyncio",
        )
    )
    try:
        await server.serve()
    finally:
        print("--- Shutting down ---")
        try:
            await bot.stop()
        finally:
            await db.close()
        print("--- Shutdown complete ---")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
