"""
SIRDASProAES — Database Layer (PostgreSQL)
"""

import json
import os

import psycopg2
import psycopg2.extras

DATABASE_URL = os.getenv("DATABASE_URL", "")


def get_connection():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    return conn


def execute(conn, sql: str, params=()):
    """
    Drop-in replacement for sqlite3's conn.execute().
    Converts ? placeholders to %s and returns the cursor.
    """
    sql = sql.replace("?", "%s")
    cur = conn.cursor()
    cur.execute(sql, params or ())
    return cur


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS products (
    id                  SERIAL PRIMARY KEY,
    code                TEXT    UNIQUE NOT NULL,
    name                TEXT    NOT NULL,
    cat                 TEXT    DEFAULT '',
    brand               TEXT    DEFAULT '',
    size                TEXT    DEFAULT '',
    ink                 TEXT    DEFAULT '',
    price               REAL    DEFAULT 0,
    cost                REAL    DEFAULT 0,
    stock               INTEGER DEFAULT 0,
    notes               TEXT    DEFAULT '',
    current_stock_price REAL    DEFAULT 0,
    avg_stock_price     REAL    DEFAULT 0
);

CREATE TABLE IF NOT EXISTS sales (
    id      SERIAL PRIMARY KEY,
    num     INTEGER NOT NULL,
    date    TEXT    NOT NULL,
    client  TEXT    DEFAULT '',
    payment TEXT    DEFAULT 'Espèces',
    items   TEXT    NOT NULL DEFAULT '[]',
    total   REAL    NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS expenses (
    id     SERIAL PRIMARY KEY,
    date   TEXT NOT NULL,
    cat    TEXT NOT NULL,
    desc   TEXT DEFAULT '',
    amount REAL NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS settings (
    id         INTEGER PRIMARY KEY DEFAULT 1,
    shop_name  TEXT    DEFAULT 'SIRDASProAES',
    currency   TEXT    DEFAULT 'CFA',
    low_stock  INTEGER DEFAULT 5,
    lang       TEXT    DEFAULT 'fr'
);
"""


def init_db() -> None:
    conn = get_connection()
    with conn:
        cur = conn.cursor()
        for statement in _SCHEMA.strip().split(";"):
            s = statement.strip()
            if s:
                cur.execute(s)
        cur.execute("INSERT INTO settings (id) VALUES (1) ON CONFLICT DO NOTHING")
    conn.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def row_to_dict(row) -> dict:
    return dict(row) if row else {}


def rows_to_list(rows) -> list[dict]:
    return [dict(r) for r in rows]


def parse_items(row: dict) -> dict:
    if isinstance(row.get("items"), str):
        try:
            row["items"] = json.loads(row["items"])
        except (json.JSONDecodeError, TypeError):
            row["items"] = []
    return row


def reset_sequences(conn) -> None:
    """After bulk import with explicit IDs, sync the SERIAL sequences."""
    with conn:
        cur = conn.cursor()
        for table in ("products", "sales", "expenses"):
            cur.execute(
                f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), "
                f"COALESCE((SELECT MAX(id) FROM {table}), 1))"
            )
