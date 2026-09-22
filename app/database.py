"""SQLite 连接管理与表结构。

- 启用 WAL，允许读取与单个写入并发；
- busy_timeout + 进程内写锁共同保证多采集端并发提交/封存的可串行化；
- 无任何外部依赖，数据库即本地文件。
"""

import os
import sqlite3
import threading

from .config import settings

# 进程内互斥：SQLite 同一时刻只允许一个写事务。
write_lock = threading.RLock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    source_id    TEXT    NOT NULL,
    batch_no     INTEGER NOT NULL,
    prev_hash    TEXT    NOT NULL,
    payload_hash TEXT    NOT NULL,
    batch_hash   TEXT    NOT NULL,
    payload_json TEXT    NOT NULL,
    received_at  TEXT    NOT NULL,
    is_sealed    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (source_id, batch_no)
);

CREATE INDEX IF NOT EXISTS idx_batches_source_unsealed
    ON batches (source_id, batch_no) WHERE is_sealed = 0;

CREATE TABLE IF NOT EXISTS seals (
    seal_id        TEXT    NOT NULL,
    source_id      TEXT    NOT NULL,
    sealed_from    INTEGER NOT NULL,
    sealed_through INTEGER NOT NULL,
    merkle_root    TEXT    NOT NULL,
    leaf_count     INTEGER NOT NULL,
    created_at     TEXT    NOT NULL,
    PRIMARY KEY (source_id, seal_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_seals_source_through
    ON seals (source_id, sealed_through);
"""


def get_conn() -> sqlite3.Connection:
    """每个请求一个连接。"""
    db_dir = os.path.dirname(os.path.abspath(settings.db_path))
    os.makedirs(db_dir, exist_ok=True)

    # check_same_thread=False：连接为每请求新建，写操作由 write_lock 串行化，
    # 允许 TestClient 等跨线程场景使用连接。
    conn = sqlite3.connect(
        settings.db_path, timeout=30, isolation_level=None, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
