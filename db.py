import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Iterable


class Database:
    def __init__(self, path: str) -> None:
        self.path = path
        self._init_db()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.path)
        try:
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.row_factory = sqlite3.Row
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS titles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    created_by INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS episodes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    url TEXT NOT NULL,
                    created_by INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(title_id) REFERENCES titles(id) ON DELETE CASCADE
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS admins (
                    user_id INTEGER PRIMARY KEY,
                    created_at TEXT NOT NULL
                )
                """
            )
            # Lightweight migration for older DBs missing created_by columns
            self._ensure_column(conn, "titles", "created_by", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "episodes", "created_by", "INTEGER NOT NULL DEFAULT 0")

    def _ensure_column(self, conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
        cur = conn.execute(f"PRAGMA table_info({table})")
        existing = {row["name"] for row in cur.fetchall()}
        if column in existing:
            return
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def add_title(self, name: str, created_by: int) -> int | None:
        now = datetime.utcnow().isoformat(timespec="seconds")
        with self._conn() as conn:
            try:
                cur = conn.execute(
                    "INSERT INTO titles (name, created_by, created_at) VALUES (?, ?, ?)",
                    (name, created_by, now),
                )
                return int(cur.lastrowid)
            except sqlite3.IntegrityError:
                return None

    def get_titles(self) -> list[sqlite3.Row]:
        with self._conn() as conn:
            cur = conn.execute("SELECT id, name, created_by FROM titles ORDER BY name ASC")
            return list(cur.fetchall())

    def get_title(self, title_id: int) -> sqlite3.Row | None:
        with self._conn() as conn:
            cur = conn.execute(
                "SELECT id, name, created_by FROM titles WHERE id = ?",
                (title_id,),
            )
            return cur.fetchone()

    def get_title_by_name(self, name: str) -> sqlite3.Row | None:
        with self._conn() as conn:
            cur = conn.execute(
                "SELECT id, name, created_by FROM titles WHERE name = ?",
                (name,),
            )
            return cur.fetchone()

    def add_episode(self, title_id: int, name: str, url: str, created_by: int) -> int:
        now = datetime.utcnow().isoformat(timespec="seconds")
        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO episodes (title_id, name, url, created_by, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (title_id, name, url, created_by, now),
            )
            return int(cur.lastrowid)

    def update_title(self, title_id: int, name: str) -> bool:
        with self._conn() as conn:
            cur = conn.execute(
                "UPDATE titles SET name = ? WHERE id = ?",
                (name, title_id),
            )
            return cur.rowcount > 0

    def delete_title(self, title_id: int) -> bool:
        with self._conn() as conn:
            cur = conn.execute("DELETE FROM titles WHERE id = ?", (title_id,))
            return cur.rowcount > 0

    def get_episodes(self, title_id: int) -> list[sqlite3.Row]:
        with self._conn() as conn:
            cur = conn.execute(
                "SELECT id, name, url, created_by FROM episodes WHERE title_id = ? ORDER BY id ASC",
                (title_id,),
            )
            return list(cur.fetchall())

    def get_episode(self, episode_id: int) -> sqlite3.Row | None:
        with self._conn() as conn:
            cur = conn.execute(
                "SELECT id, title_id, name, url, created_by FROM episodes WHERE id = ?",
                (episode_id,),
            )
            return cur.fetchone()

    def get_prev_episode_id(self, title_id: int, episode_id: int) -> int | None:
        with self._conn() as conn:
            cur = conn.execute(
                "SELECT id FROM episodes WHERE title_id = ? AND id < ? ORDER BY id DESC LIMIT 1",
                (title_id, episode_id),
            )
            row = cur.fetchone()
            return int(row["id"]) if row else None

    def get_next_episode_id(self, title_id: int, episode_id: int) -> int | None:
        with self._conn() as conn:
            cur = conn.execute(
                "SELECT id FROM episodes WHERE title_id = ? AND id > ? ORDER BY id ASC LIMIT 1",
                (title_id, episode_id),
            )
            row = cur.fetchone()
            return int(row["id"]) if row else None

    def update_episode(self, episode_id: int, name: str, url: str) -> bool:
        with self._conn() as conn:
            cur = conn.execute(
                "UPDATE episodes SET name = ?, url = ? WHERE id = ?",
                (name, url, episode_id),
            )
            return cur.rowcount > 0

    def delete_episode(self, episode_id: int) -> bool:
        with self._conn() as conn:
            cur = conn.execute("DELETE FROM episodes WHERE id = ?", (episode_id,))
            return cur.rowcount > 0

    def count_titles(self) -> int:
        with self._conn() as conn:
            cur = conn.execute("SELECT COUNT(*) AS c FROM titles")
            row = cur.fetchone()
            return int(row["c"]) if row else 0

    def count_episodes(self) -> int:
        with self._conn() as conn:
            cur = conn.execute("SELECT COUNT(*) AS c FROM episodes")
            row = cur.fetchone()
            return int(row["c"]) if row else 0

    def add_admin(self, user_id: int) -> bool:
        now = datetime.utcnow().isoformat(timespec="seconds")
        with self._conn() as conn:
            try:
                conn.execute(
                    "INSERT INTO admins (user_id, created_at) VALUES (?, ?)",
                    (user_id, now),
                )
                return True
            except sqlite3.IntegrityError:
                return False

    def remove_admin(self, user_id: int) -> bool:
        with self._conn() as conn:
            cur = conn.execute("DELETE FROM admins WHERE user_id = ?", (user_id,))
            return cur.rowcount > 0

    def get_admin_ids(self) -> list[int]:
        with self._conn() as conn:
            cur = conn.execute("SELECT user_id FROM admins ORDER BY user_id ASC")
            return [int(row["user_id"]) for row in cur.fetchall()]
