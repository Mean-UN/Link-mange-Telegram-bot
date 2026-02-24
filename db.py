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
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS manga_admins (
                    title_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (title_id, user_id),
                    FOREIGN KEY(title_id) REFERENCES titles(id) ON DELETE CASCADE
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

    def search_titles_by_keyword(self, keyword: str) -> list[sqlite3.Row]:
        like = f"%{keyword.strip()}%"
        with self._conn() as conn:
            cur = conn.execute(
                """
                SELECT id, name, created_by
                FROM titles
                WHERE name LIKE ? COLLATE NOCASE
                ORDER BY name ASC
                """,
                (like,),
            )
            return list(cur.fetchall())

    def get_manga_update_counts_since(self, start_iso: str) -> list[sqlite3.Row]:
        with self._conn() as conn:
            cur = conn.execute(
                """
                SELECT
                    t.id AS title_id,
                    t.name AS title_name,
                    COUNT(e.id) AS added_episodes,
                    (
                        SELECT COUNT(*)
                        FROM episodes e_all
                        WHERE e_all.title_id = t.id
                    ) AS total_episodes
                FROM episodes e
                JOIN titles t ON t.id = e.title_id
                WHERE e.created_at >= ?
                GROUP BY t.id, t.name
                ORDER BY added_episodes DESC, t.name ASC
                """,
                (start_iso,),
            )
            return list(cur.fetchall())

    def get_last_update_for_title(self, title_id: int) -> sqlite3.Row | None:
        with self._conn() as conn:
            cur = conn.execute(
                """
                SELECT
                    t.id AS title_id,
                    t.name AS title_name,
                    MAX(e.created_at) AS last_update_at,
                    COUNT(e.id) AS total_links
                FROM titles t
                LEFT JOIN episodes e ON e.title_id = t.id
                WHERE t.id = ?
                GROUP BY t.id, t.name
                """,
                (title_id,),
            )
            return cur.fetchone()

    def get_duplicate_link_usages(self) -> list[sqlite3.Row]:
        with self._conn() as conn:
            cur = conn.execute(
                """
                SELECT
                    e.url AS url,
                    e.id AS episode_id,
                    e.name AS episode_name,
                    t.id AS title_id,
                    t.name AS title_name,
                    dup.cnt AS duplicate_count
                FROM episodes e
                JOIN titles t ON t.id = e.title_id
                JOIN (
                    SELECT url, COUNT(*) AS cnt
                    FROM episodes
                    GROUP BY url
                    HAVING COUNT(*) > 1
                ) dup ON dup.url = e.url
                ORDER BY dup.cnt DESC, e.url ASC, t.name ASC, e.id ASC
                """
            )
            return list(cur.fetchall())

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

    def add_manga_admin(self, title_id: int, user_id: int) -> bool:
        now = datetime.utcnow().isoformat(timespec="seconds")
        with self._conn() as conn:
            try:
                conn.execute(
                    "INSERT INTO manga_admins (title_id, user_id, created_at) VALUES (?, ?, ?)",
                    (title_id, user_id, now),
                )
                return True
            except sqlite3.IntegrityError:
                return False

    def remove_manga_admin(self, title_id: int, user_id: int) -> bool:
        with self._conn() as conn:
            cur = conn.execute(
                "DELETE FROM manga_admins WHERE title_id = ? AND user_id = ?",
                (title_id, user_id),
            )
            return cur.rowcount > 0

    def has_manga_admin(self, title_id: int, user_id: int) -> bool:
        with self._conn() as conn:
            cur = conn.execute(
                "SELECT 1 FROM manga_admins WHERE title_id = ? AND user_id = ? LIMIT 1",
                (title_id, user_id),
            )
            return cur.fetchone() is not None
