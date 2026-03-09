import os
import random
import time
from datetime import UTC, datetime, timedelta

from pymongo import MongoClient


random.seed(42)


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Environment variable '{name}' is required")
    return value


def connect_with_retry(uri: str, retries: int = 30, delay: int = 5) -> MongoClient:
    for attempt in range(1, retries + 1):
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=3000)
            client.admin.command("ping")
            return client
        except Exception:
            if attempt == retries:
                raise
            time.sleep(delay)
    raise RuntimeError("Could not connect to MongoDB")


def iso(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def random_choice(values):
    return random.choice(values)


def generate_user_sessions(base_time: datetime, users: list[str], products: list[str]):
    pages = ["/home", "/catalog", "/cart", "/checkout", "/support", "/profile"]
    actions = ["login", "view_product", "search", "add_to_cart", "checkout", "logout", "open_ticket"]
    devices = [
        {"type": "desktop", "os": "Windows", "browser": "Chrome"},
        {"type": "mobile", "os": "Android", "browser": "Chrome"},
        {"type": "mobile", "os": "iOS", "browser": "Safari"},
        {"type": "tablet", "os": "iPadOS", "browser": "Safari"},
    ]

    sessions = []
    for index in range(1, 401):
        user_id = random_choice(users)
        start_time = base_time + timedelta(hours=index * 3)
        duration_minutes = random.randint(5, 95)
        product_page = f"/products/{random_choice(products)}"
        visited_pages = ["/home", "/catalog", product_page]
        if random.random() > 0.4:
            visited_pages.append("/cart")
        if random.random() > 0.6:
            visited_pages.append("/checkout")
        user_actions = ["login", "view_product"]
        if "/cart" in visited_pages:
            user_actions.append("add_to_cart")
        if "/checkout" in visited_pages:
            user_actions.append("checkout")
        if random.random() > 0.8:
            user_actions.append("open_ticket")
        user_actions.append("logout")
        sessions.append(
            {
                "session_id": f"sess_{index:04d}",
                "user_id": user_id,
                "start_time": iso(start_time),
                "end_time": iso(start_time + timedelta(minutes=duration_minutes)),
                "pages_visited": visited_pages,
                "device": random_choice(devices),
                "actions": user_actions,
            }
        )

    for duplicate_index in range(10):
        source = dict(sessions[duplicate_index])
        source["end_time"] = iso(
            datetime.fromisoformat(source["end_time"].replace("Z", "+00:00")) + timedelta(minutes=5)
        )
        source["actions"] = list(dict.fromkeys(source["actions"] + ["search"]))
        sessions.append(source)

    return sessions


def main():
    client = connect_with_retry(env("MONGO_URI"))
    database = client[env("MONGO_DB", "source_db")]

    users = [f"user_{index:03d}" for index in range(1, 121)]
    products = [f"prod_{index:03d}" for index in range(1, 81)]
    base_time = datetime.now(UTC) - timedelta(days=45)

    collection = database["user_sessions"]
    sessions = generate_user_sessions(base_time, users, products)
    collection.delete_many({})
    collection.insert_many(sessions)
    print(f"Inserted {len(sessions)} records into user_sessions")


if __name__ == "__main__":
    main()
