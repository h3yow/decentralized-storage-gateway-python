# 本地去中心化存储网关

在 FastAPI 服务之上实现的本地去中心化存储网关：用一组本地存储节点模拟
**内容寻址、副本管理与可检索性判定**，在内容重复、节点异常与并发修复下
给出一致且可解释的结论。

## 架构

```
main.py                FastAPI HTTP 入口（错误原因 → 可区分的 HTTP 状态码）
gateway/
  errors.py            FailureReason 枚举 + GatewayError（携带可区分原因）
  models.py            NodeInfo / ContentRecord / ReplicaRecord / 状态枚举
  node.py              LocalStorageNode：容量、授权、上下线、字节存取（节点级锁）
  gateway.py           StorageGateway：寻址、放置、判定、修复（网关级可重入锁）
tests/
  test_gateway.py      核心逻辑单测（含并发与守恒断言）
  test_api.py          HTTP 入口集成测试
```

## 内容寻址模型

- 内容标识 `cid = hashlib.new(checksum_algo, data).hexdigest()`（默认 SHA-256）。
- **去重归一**：相同内容重复 `submit` 归一到同一 `cid`，不产生多余副本、
  不新增内容记录（日志打印 `submit deduped ...`）。
- **完整性**：`submit(data, expected_checksum)` 校验失败抛出
  `CHECKSUM_MISMATCH`，不静默接受；已存字节在 `get` / 修复选源时重新计算
  摘要，不一致抛出 `CONTENT_CORRUPTED`。
- 网关持有每份内容的权威样本（canonical copy），作为修复与读取的最终来源，
  保证并发去重与修复时唯一样本不丢失。

## 副本与可检索性规则

- **放置**：`place_replica(cid, node_id)` 在网关锁内完成"检查并写入"，
  拒绝原因可区分：
  `UNAUTHORIZED_NODE`（未授权）、`NODE_OFFLINE`（离线）、
  `CAPACITY_EXCEEDED`（容量不足）、`DUPLICATE_PLACEMENT`（重复放置）。
- **计数**：逻辑副本数（`replica_count(cid, online_only=False)`）与
  在线副本数分别统计；节点上下线只影响在线计数，逻辑副本不丢。
- **可检索性**：`在线副本数 >= min_replicas` → `retrievable`，否则
  `unretrievable`；节点上下线后立即重算并记录判定依据
  （日志：`refresh_status cid=... online_replicas=N min=M -> ...`）。
- **修复**：`repair(cid)` 先找在线且字节完好的样本源（损坏源跳过并记日志），
  再向在线、已授权、未持有该内容的节点复制，只补到阈值、只增不删；
  已达标则幂等返回。失败时保留原状态与原副本，并抛出可区分原因：
  `NO_REPAIR_SOURCE`（无完好样本）或 `INSUFFICIENT_REPLICAS`
  （detail 中含 `last_blocker`，如 `capacity_exceeded`）。
- **并发**：所有复合操作在网关级 `RLock` 内串行化，节点内部另有锁保护
  用量计数；并发修复不会重复复制，修复与去重并发不会丢失唯一样本。

## 可配置项

`GatewayConfig`：

| 字段 | 默认 | 说明 |
|---|---|---|
| `min_replicas` | 2 | 可检索性阈值（>=1） |
| `checksum_algo` | `sha256` | 内容寻址摘要算法（hashlib 支持的名称） |

## HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/nodes` | 注册节点 `{node_id, capacity, authorized}` |
| GET | `/nodes` | 节点列表（容量/已用/状态） |
| POST | `/nodes/{id}/status` | 上下线 `{online}` |
| POST | `/contents` | 提交 `{data_hex, expected_checksum?}` → `{cid, status}` |
| GET | `/contents/{cid}` | 读取内容 |
| POST | `/contents/{cid}/replicas` | 放置副本 `{node_id}` |
| GET | `/contents/{cid}/retrievability` | 可检索性判定（含在线副本数与阈值） |
| POST | `/contents/{cid}/repair` | 触发修复 |
| GET | `/stats` | 副本总量（在线/逻辑） |

错误响应体形如 `{"detail": {"reason": "capacity_exceeded", "detail": "..."}}`，
`reason` 与 `FailureReason` 一一对应，HTTP 状态码按原因区分
（403 未授权 / 404 不存在 / 409 冲突类 / 422 校验失败 / 507 容量不足）。

## 验证方法

```bash
uv sync
uv run pytest tests/ -v                 # 全部测试
uv run pytest tests/ -o log_cli=true -o log_cli_level=INFO   # 查看输入与判定依据日志
uv run fastapi dev main.py              # 启动服务后访问 /docs
```

测试覆盖：内容损坏（提交校验失败、存储字节被篡改）、容量不足、未授权节点、
重复放置、节点上下线、修复成功/失败（保留原状态并给出原因）、
并发修复不重复不丢失、并发去重、放置计数守恒。
每个用例通过 `assert_conservation` 断言副本总量与节点实际存储一致、
节点 `used` 与真实字节一致且不超容量。
