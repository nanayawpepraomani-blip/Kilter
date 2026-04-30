"""
db_mysql.py
===========

MySQL connection adapter. When DATABASE_URL is set to a mysql:// URI,
get_conn() returns a MySQLConnection wrapper instead of sqlite3.

The wrapper translates:
  - ? placeholders → %s (MySQL uses %s)
  - sqlite3.Row dict-style access → RealDictCursor rows
  - .executescript() → split on ';' and run each statement
  - PRAGMA statements → no-ops (MySQL doesn't use them)
  - AUTOINCREMENT → handled at schema level (db_mysql_schema.sql)

Usage:
  Set DATABASE_URL=mysql://user:pass@host:3306/kilter in environment.
  The app calls get_conn() from db.py — routing is transparent.
"""

from __future__ import annotations

import os
import re


def is_mysql() -> bool:
    url = os.environ.get('DATABASE_URL', '')
    return url.startswith('mysql://') or url.startswith('mysql+')


def get_mysql_conn():
    """Return a MySQLConnectionWrapper ready for use."""
    try:
        import mysql.connector
    except ImportError:
        raise RuntimeError(
            "mysql-connector-python is required for MySQL support. "
            "Install it: pip install mysql-connector-python"
        )
    url = os.environ.get('DATABASE_URL', '')
    # Parse mysql://user:pass@host:port/dbname
    m = re.match(
        r'mysql(?:\+\w+)?://([^:@]+)(?::([^@]*))?@([^:/]+)(?::(\d+))?/(.+)',
        url
    )
    if not m:
        raise ValueError(f"Cannot parse DATABASE_URL as MySQL URI: {url!r}")
    user, password, host, port, database = m.groups()
    raw = mysql.connector.connect(
        host=host,
        port=int(port or 3306),
        user=user,
        password=password or '',
        database=database,
        autocommit=False,
        charset='utf8mb4',
        use_unicode=True,
    )
    return MySQLConnectionWrapper(raw)


class MySQLConnectionWrapper:
    """Wraps a mysql.connector connection to mimic the sqlite3 interface."""

    def __init__(self, conn):
        self._conn = conn

    # ---- transaction control ----

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_):
        if exc_type:
            self.rollback()
        else:
            self.commit()
        self.close()

    # ---- query execution ----

    def execute(self, sql: str, params=()) -> MySQLCursorWrapper:
        sql = _translate_sql(sql)
        cur = self._conn.cursor(dictionary=True)
        cur.execute(sql, params)
        return MySQLCursorWrapper(cur)

    def executemany(self, sql: str, params_seq) -> MySQLCursorWrapper:
        sql = _translate_sql(sql)
        cur = self._conn.cursor(dictionary=True)
        cur.executemany(sql, list(params_seq))
        return MySQLCursorWrapper(cur)

    def executescript(self, script: str) -> None:
        """Run a multi-statement SQL script. Skips PRAGMA statements."""
        cur = self._conn.cursor()
        for stmt in _split_script(script):
            if stmt.strip():
                cur.execute(stmt)
        self._conn.commit()
        cur.close()

    def row_factory(self, *_):
        pass  # not needed — we use dictionary=True cursor


class MySQLCursorWrapper:
    """Wraps mysql.connector cursor to mimic sqlite3 cursor."""

    def __init__(self, cur):
        self._cur = cur

    @property
    def lastrowid(self):
        return self._cur.lastrowid

    @property
    def rowcount(self):
        return self._cur.rowcount

    def fetchone(self):
        row = self._cur.fetchone()
        if row is None:
            return None
        return _DictRow(row)

    def fetchall(self):
        return [_DictRow(r) for r in self._cur.fetchall()]

    def __iter__(self):
        for row in self._cur:
            yield _DictRow(row)


class _DictRow:
    """Mimics sqlite3.Row: dict-style and index access."""

    def __init__(self, d: dict):
        self._d = d

    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self._d.values())[key]
        return self._d[key]

    def get(self, key, default=None):
        return self._d.get(key, default)

    def keys(self):
        return self._d.keys()

    def __contains__(self, key):
        return key in self._d

    def __iter__(self):
        return iter(self._d)

    def items(self):
        return self._d.items()

    def __repr__(self):
        return f'_DictRow({self._d!r})'


# ---- SQL translation helpers ----

def _translate_sql(sql: str) -> str:
    """Translate SQLite SQL to MySQL SQL."""
    # ? → %s (parameter placeholder)
    sql = re.sub(r'\?', '%s', sql)
    # AUTOINCREMENT → AUTO_INCREMENT (handled in schema DDL separately)
    sql = sql.replace('AUTOINCREMENT', 'AUTO_INCREMENT')
    # Strip PRAGMA statements entirely
    if re.match(r'\s*PRAGMA\s', sql, re.IGNORECASE):
        return 'SELECT 1'
    # datetime('now') → NOW()
    sql = re.sub(r"datetime\('now'\)", 'NOW()', sql, flags=re.IGNORECASE)
    # julianday(...) approximation (used in SLA aging)
    sql = re.sub(
        r'julianday\(([^)]+)\)',
        r'TO_DAYS(\1)',
        sql, flags=re.IGNORECASE
    )
    # COALESCE is the same in MySQL — no change needed
    # json_extract / json functions differ — left for future work
    return sql


def _split_script(script: str) -> list[str]:
    """Split a multi-statement SQL script on semicolons, skipping PRAGMA."""
    statements = []
    for stmt in script.split(';'):
        stmt = stmt.strip()
        if not stmt:
            continue
        if re.match(r'PRAGMA\s', stmt, re.IGNORECASE):
            continue
        # SQLite CREATE TRIGGER syntax uses BEGIN...END — MySQL uses DELIMITER
        # For now pass through; init_db() should use a MySQL-specific schema
        statements.append(stmt)
    return statements
