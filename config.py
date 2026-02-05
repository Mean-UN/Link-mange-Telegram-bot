import os

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional dependency
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv()


def _read_env_list(name: str) -> set[int]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return set()
    items = [x.strip() for x in raw.split(",") if x.strip()]
    ids: set[int] = set()
    for item in items:
        try:
            ids.add(int(item))
        except ValueError:
            continue
    return ids


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS = _read_env_list("ADMIN_IDS")
DB_PATH = os.getenv("DB_PATH", "linkbot.db").strip() or "linkbot.db"
