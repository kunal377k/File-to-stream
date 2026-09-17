import os


def required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


class Config:
    API_ID = int(required("API_ID"))
    API_HASH = required("API_HASH")
    BOT_TOKEN = required("BOT_TOKEN")
    DATABASE_URL = required("DATABASE_URL")
    OWNER_ID = int(required("OWNER_ID"))
    STORAGE_CHANNEL = int(required("STORAGE_CHANNEL"))
    BASE_URL = required("BASE_URL").rstrip("/")
