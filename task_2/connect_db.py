import os
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
from pathlib import Path


ENV_PATH = Path(__file__).parent.parent / ".env"


def create_connect() -> MongoClient:
    load_dotenv(ENV_PATH)

    client = MongoClient(
        os.getenv("MONGO_DB_HOST"),
        server_api=ServerApi("1"),
    )

    return client
