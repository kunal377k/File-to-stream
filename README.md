# File-to-Stream Bot

A clean Telegram file-to-stream bot designed for Render + MongoDB.

## Required Render environment variables

- `API_ID`
- `API_HASH`
- `BOT_TOKEN`
- `DATABASE_URL`
- `OWNER_ID`
- `STORAGE_CHANNEL`
- `BASE_URL`

`PORT` is provided by Render automatically; do not add it manually.

The storage channel must be accessible by the bot. The bot needs permission to post/copy messages there.
