"""测试夹具：每个用例使用独立的临时 SQLite 数据库。"""

import os
import tempfile

import pytest


@pytest.fixture()
def client(monkeypatch):
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test_evidence.db")
    monkeypatch.setenv("EVIDENCE_DB_PATH", db_path)

    # 必须在设置环境变量后再导入，确保配置生效。
    from importlib import reload

    import app.config
    import app.database

    reload(app.config)
    reload(app.database)
    from fastapi.testclient import TestClient

    import app.main

    reload(app.main)
    with TestClient(app.main.app) as c:
        yield c
