import os
import pymongo
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

def get_mongo_collection():
    """Establishes a connection to MongoDB and returns the users collection."""
    mongo_client = pymongo.MongoClient(
        host=os.getenv("MONGO_HOST", "localhost"),
        port=int(os.getenv("MONGO_PORT", 27017))
    )
    db_name = os.getenv("MONGO_DB", "users_db")
    db = mongo_client[db_name]
    return db.users

def seed_mongo_data():
    """Inserts sample data into the MongoDB users collection."""
    collection = get_mongo_collection()

    # Clear existing data to ensure a clean seed
    collection.delete_many({})

    users = [
        {
            "_id": "6735f0123b1a2c4e876a9bde",
            "Uid": "user1",
            "firstName": "John",
            "lastName": "Doe",
            "occupation": "Engineer",
            "state": "CA",
            "last_updated_at": datetime.now(timezone.utc)
        },
        {
            "_id": "6735f0123b1a2c4e876a9bdf",
            "Uid": "user2",
            "firstName": "Jane",
            "lastName": "Smith",
            "occupation": "Doctor",
            "state": "NY",
            "last_updated_at": datetime.now(timezone.utc)
        }
    ]

    collection.insert_many(users)
    print(f"Successfully seeded {len(users)} users into MongoDB.")

if __name__ == "__main__":
    seed_mongo_data()
