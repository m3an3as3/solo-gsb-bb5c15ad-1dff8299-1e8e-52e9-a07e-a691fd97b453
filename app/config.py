"""全局配置：所有参数均可通过环境变量覆盖，无需任何外部账号。"""

import os


class Settings:
    # SQLite 数据库文件路径（Docker 中默认 /data/evidence.db）
    db_path: str = os.getenv("EVIDENCE_DB_PATH", "data/evidence.db")

    # 服务监听配置（容器内由 uvicorn 命令行控制，这里仅作文档默认值）
    host: str = os.getenv("EVIDENCE_HOST", "0.0.0.0")
    port: int = int(os.getenv("EVIDENCE_PORT", "8000"))

    api_title: str = "证据批次封存 API"
    api_version: str = "1.0.0"


settings = Settings()
