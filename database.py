from motor.motor_asyncio import AsyncIOMotorClient
from config import Config


class Database:
    def __init__(self):
        self.client = AsyncIOMotorClient(
            Config.DATABASE_URL,
            serverSelectionTimeoutMS=10000,
            connectTimeoutMS=10000,
        )
        self.collection = self.client["FileToStreamDB"]["links"]

    async def connect(self):
        await self.client.admin.command("ping")
        print("✅ MongoDB connection established.")

    async def close(self):
        self.client.close()
        print("MongoDB connection closed.")

    async def save_link(self, unique_id: str, message_id: int):
        await self.collection.update_one(
            {"_id": unique_id},
            {"$set": {"message_id": message_id}},
            upsert=True,
        )

    async def get_message_id(self, unique_id: str):
        doc = await self.collection.find_one({"_id": unique_id})
        return doc["message_id"] if doc else None


db = Database()
