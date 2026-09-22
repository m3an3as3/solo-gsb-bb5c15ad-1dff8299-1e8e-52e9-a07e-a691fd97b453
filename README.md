# 证据批次封存 API（FastAPI + SQLite）

多个采集端（probe / agent）向同一服务上传带 `source_id`、`batch_no`、`prev_hash` 的 JSON 事件批次。服务支持：

- **乱序接收**：批次可任意顺序到达，未闭合前一律暂存；
- **重试幂等**：相同 `(source_id, batch_no)` 且内容一致的重复提交返回 `duplicate=true`，不产生副作用；
- **哈希链连续性判定**：仅当从封存水位起 `prev_hash → batch_hash` 链完整无缺口时才允许封存；
- **确定性 Merkle 封存**：封存时按 `source_id` 与 `batch_no` 顺序计算 Merkle 根，返回每批次的包含证明；
- **明确冲突报错**：同序号不同内容（409 `BATCH_CONFLICT`）、链断裂（422 `CHAIN_BROKEN`）、未闭合封存（409 `GAP_NOT_CLOSED`）等均有结构化错误码；
- **零外部依赖**：SQLite 本地文件，无需注册任何账号。

## 1. 快速启动

### 方式 A：Docker Compose（推荐）

```bash
docker compose up --build
# 服务在 http://localhost:8000，数据持久化在 evidence-data 卷
```

### 方式 B：本地 Python（3.11+）

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

启动后：

- 交互文档（Swagger UI）：http://localhost:8000/docs
- OpenAPI：http://localhost:8000/openapi.json
- 健康检查：http://localhost:8000/health

### 端到端演示

另开一个终端（服务已启动）：

```bash
python examples/demo.py
```

脚本使用标准库，自动走完「提交 → 乱序 → 缺口 → 冲突 → 封存 → 取证明 → 校验」全流程，
每次运行使用随机 `source_id`，可重复执行。也可在 `examples/requests.http` 中用
REST Client 手工发请求，或直接参考 README 第 5 节的 curl 示例。

### 运行测试

```bash
pip install -r requirements-dev.txt
pytest -q
```

## 2. 哈希链与封存规则

### 2.1 批次哈希

所有摘要为 SHA-256 的 64 位小写十六进制字符串。

- 载荷摘要（规范化 JSON：键按字典序、无空白、不转义 Unicode）：

  ```
  payload_hash = SHA256(canonical_json(payload))
  ```

- 批次哈希（绑定来源、序号、前序链接、载荷，带版本域分隔防止拼接歧义）：

  ```
  batch_hash = SHA256(canonical_json(
      ["BATCH_V1", source_id, batch_no, prev_hash, payload_hash]))
  ```

- **创世纪链接**：每个来源第 `0` 批的 `prev_hash` 必须固定为

  ```
  0000000000000000000000000000000000000000000000000000000000000000
  ```

- 第 `n` 批（`n ≥ 1`）的 `prev_hash` 必须等于第 `n-1` 批返回的 `batch_hash`。
- 等价 JSON（键序/空白不同）规范化后摘要相同，天然支持采集端重试。

### 2.2 接收语义（乱序 / 重试 / 冲突）

| 情况 | HTTP | 错误码 / 标记 | 数据处理 |
|---|---|---|---|
| 新批次，链接正确（或前驱暂缺） | 201 | — | 入库暂存 |
| 完全相同的重复提交 | 200 | `duplicate=true` | 不变（幂等） |
| 同 `(source_id, batch_no)` 但 `prev_hash`/载荷不同 | 409 | `BATCH_CONFLICT` | 保留原件，返回双方摘要 |
| 第 0 批 `prev_hash` 非法 | 422 | `INVALID_GENESIS_LINK` | 不入库 |
| 前驱已在库但 `prev_hash` 不匹配 | 422 | `CHAIN_BROKEN` | 入库暂存并标记断链 |
| 后继已在库但其 `prev_hash` ≠ 本批 `batch_hash` | 422 | `CHAIN_BROKEN` | 入库暂存并标记断链 |
| 提交已封存区间内的批次 | 409 | `SEALED_BATCH` | 拒绝 |

「前驱暂缺」的批次（例如先收到 2 再收到 1）正常暂存，不报错；待缺口补齐后自然成链。
断链批次仍保留在暂存区，便于采集端排查，但不会进入封存。

### 2.3 封存与 Merkle 树

- 每个来源维护一个**封存水位** `sealed_through`（初始为 -1）。
- `POST /seals` 从水位 +1 开始沿链行走，封存到**连续链头**；也可用
  `sealed_through` 指定更早的前缀。区间内有任何缺口或断链返回 409。
- 封存后该区间不可更改（再提交返回 `SEALED_BATCH`）；重复封存同一区间幂等返回
  同一 `seal_id`（`already_sealed=true`）。
- Merkle 树（确定性，双方约定一致）：
  1. 叶子严格按 `batch_no` 升序；
  2. `leaf = SHA256("MERKLE_LEAF_V1" ‖ batch_hash_bytes)`（域分隔防叶子/内节点伪造）；
  3. `node = SHA256("MERKLE_INTERNAL_V1" ‖ left ‖ right)`；
  4. 奇数节点复制最后一个节点与自身配对（Bitcoin 风格）；单叶子树根即叶子，证明路径为空。
- `seal_id = SHA256(["SEAL_V1", source_id, from, through, root])` 取前 32 位，
  同样的区间与根永远得到同一标识。

封存响应里直接携带区间内**每个批次**的包含证明；之后也可通过证明接口单独获取，
并用 `/verify` 在**不依赖本服务**的情况下凭 `batch_hash + proof_path + merkle_root`
独立复核（`app/merkle.py` 是纯函数，移植到任意语言都可验证）。

## 3. 接口一览

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查 |
| POST | `/batches` | 提交一个批次（201 新批次 / 200 幂等重试 / 409 冲突 / 422 断链） |
| GET | `/sources/{source_id}/gaps?max_batch=` | 查询封存水位、缺失批次、断链点、连续链头、可封存水位 |
| POST | `/seals` | 封存连续链（body：`source_id`，可选 `sealed_through`） |
| GET | `/sources/{source_id}/batches/{batch_no}/proof?sealed_through=` | 获取某已封存批次的 Merkle 包含证明 |
| POST | `/verify` | 校验：外部证明模式（传 `proof_path`+`merkle_root`）或查库模式（传 `source_id`+`batch_no`） |

### 缺口查询字段

```jsonc
{
  "sealed_through": 0,          // 已封存水位
  "next_expected_batch": 1,     // 下一个应到达的批次
  "highest_received": 3,        // 暂存/已存的最大批次号
  "missing": [1],               // 缺失批次号
  "chain_broken": [],           // prev_hash 链接不符的批次号
  "contiguous_head": 0,         // 自水位起连续链可达的最大批次
  "ready_to_seal_through": 0,   // 当前可立即封存至的批次
  "has_gap": true
}
```

### 错误响应格式

所有业务错误返回统一结构，便于采集端按 `code` 分支处理：

```json
{
  "code": "BATCH_CONFLICT",
  "message": "批次 0 已存在但内容不一致",
  "source_id": "probe-demo",
  "batch_no": 0,
  "existing": { "prev_hash": "...", "payload_hash": "...", "batch_hash": "..." },
  "submitted": { "prev_hash": "...", "payload_hash": "...", "batch_hash": "..." }
}
```

错误码：`SEALED_BATCH`、`BATCH_CONFLICT`、`INVALID_GENESIS_LINK`、`CHAIN_BROKEN`、
`NOTHING_TO_SEAL`、`CHAIN_NOT_CLOSED`、`GAP_NOT_CLOSED`、`INVALID_SEAL_RANGE`、
`SEAL_NOT_FOUND`、`BATCH_NOT_IN_SEAL`、`MISSING_LOOKUP_KEYS`、`MISSING_BATCH_HASH`。

## 4. 并发与持久化

- SQLite 启用 WAL；写入通过 `BEGIN IMMEDIATE` + 进程内互斥锁串行化，
  `busy_timeout=30s`，多采集端并发提交/封存不会产生交叉封存。
- 数据库即文件（默认 `./data/evidence.db`，容器内 `/data/evidence.db`，
  可用环境变量 `EVIDENCE_DB_PATH` 覆盖），首次请求自动建表。

## 5. curl 示例

```bash
BASE=http://localhost:8000
SRC=probe-curl
GEN=0000000000000000000000000000000000000000000000000000000000000000

# 批次 0
H0=$(curl -s -X POST $BASE/batches -H 'Content-Type: application/json' -d "{
  \"source_id\": \"$SRC\", \"batch_no\": 0, \"prev_hash\": \"$GEN\",
  \"payload\": {\"event\": \"a\"} }" | tee /dev/stderr | python3 -c 'import sys,json;print(json.load(sys.stdin)["batch_hash"])')

# 批次 1（prev_hash 链向上一批）
curl -s -X POST $BASE/batches -H 'Content-Type: application/json' -d "{
  \"source_id\": \"$SRC\", \"batch_no\": 1, \"prev_hash\": \"$H0\",
  \"payload\": {\"event\": \"b\"} }"

# 缺口
curl -s $BASE/sources/$SRC/gaps

# 封存（返回 merkle_root 与全部叶子证明）
curl -s -X POST $BASE/seals -H 'Content-Type: application/json' \
  -d "{\"source_id\": \"$SRC\"}"

# 取批次 0 的证明
curl -s $BASE/sources/$SRC/batches/0/proof

# 查库校验
curl -s -X POST $BASE/verify -H 'Content-Type: application/json' \
  -d "{\"source_id\": \"$SRC\", \"batch_no\": 0}"
```

## 6. 目录结构

```
app/
  config.py      # 环境变量配置
  database.py    # SQLite 连接、WAL、建表、写锁
  hashing.py     # 规范化 JSON 与 batch_hash / payload_hash
  merkle.py     # 确定性 Merkle 根、包含证明生成与离线复算（纯函数）
  models.py      # Pydantic 请求/响应模型
  services.py    # 接收/去重/链检测/封存/校验业务逻辑
  main.py        # FastAPI 路由
examples/
  demo.py        # 标准库端到端演示
  requests.http  # REST Client 手工请求
tests/           # pytest 端到端用例
Dockerfile
docker-compose.yml
```
