# 本地去中心化存储网关（Python 实现）

在本地用多个**存储节点**模拟去中心化存储网络中的三个核心机制：

1. **内容寻址与元数据管理**——相同内容归一到同一标识（CID），损坏内容按原因拒绝；
2. **副本放置与节点状态管理**——多节点复制时对容量、重复、授权、离线做可区分拒绝，上下线联动可检索性；
3. **可检索性判定与修复流程**——在线健康副本数低于阈值即判定不可检索并触发修复，并发修复不重复复制、不丢唯一样本。

所有判定都返回/抛出**可解释的结论**（原因字符串或具名错误类型），并通过一把全局注册表锁保证并发下计数守恒。

---

## 1. 内容寻址模型

- 内容标识 `CID = sha256_hex(content)`，为 64 位十六进制字符串。
- 提交时对原始字节计算 SHA-256：
  - 空内容 → `EmptyContentError`（拒绝入库，不产生元数据）；
  - 若调用方提供 `declared_checksum` 且与实际摘要不一致 → `ChecksumMismatchError`（内容损坏/被篡改，拒绝入库）；
  - 校验通过（或未声明摘要）后按 CID 归一：**相同内容重复提交返回同一 `ContentMetadata`，不新增任何副本**。
- 元数据 `ContentMetadata` 是不可变快照：`cid / size / checksum / replica_nodes`，其中 `replica_nodes` 为当前持有该内容的节点集合快照。
- 注册表保留一份原始提交字节作为**可信样本**，供修复时读取；修复只做"增加副本"，从不删除或移动任何现有副本。

## 2. 副本与可检索性规则

### 2.1 副本放置（`place_replica` / `PUT /contents/{cid}/replicas/{node_id}`）

| 失败情形 | 错误类型 | HTTP 状态码 |
|---|---|---|
| 内容 CID 不存在 | `ContentNotFoundError` | 404 |
| 节点不存在 | `NodeNotFoundError` | 404 |
| 节点未授权 | `UnauthorizedNodeError` | 403 |
| 节点已离线 | `NodeOfflineError` | 409 |
| 剩余容量 < 内容大小 | `CapacityExceededError` | 507 |
| 该节点已有同一副本 | `DuplicatePlacementError` | 409 |

所有拒绝都是**显式**的：不会静默接受，也不会产生"半个副本"或容量记账残留
（容量校验先于记账，失败时节点 `used_bytes` 不变）。

批量复制接口（`POST /contents/{cid}/replicas`）对每个节点单独给结论：
`"ok"` 或 `"rejected:<错误类型>:<说明>"`，单节点失败不影响其余节点。

### 2.2 节点上下线

- 下线只改变节点状态，**不删除副本数据**，因此：
  - 总副本数（跨在线/离线节点）保持不变；
  - **在线健康副本数**实时随状态变化。
- 可检索性判定：`在线副本数 >= retrieval_threshold` 才可检索。

### 2.3 可检索性结论（`RetrievabilityResult`）

| 情形 | retrievable | 原因示例 |
|---|---|---|
| 无任何副本 | false | `内容尚无任何副本` |
| 副本都在离线节点 | false | `全部 N 个副本所在节点均离线` |
| 在线副本不足 | false | `在线副本数 1 低于检索阈值 2（总副本数 2）` |
| 达标 | true | `在线副本数 2 >= 检索阈值 2` |

### 2.4 修复（`RepairManager.repair`）

- 仅当不可检索时动作，把在线副本补齐到阈值；候选节点按节点 id 排序后确定性选择。
- 候选必须同时满足：**已注册、已授权、在线、尚无该副本、剩余容量充足**。
- **全部副本离线时不修复**：没有在线健康样本可复制，返回失败报告且保留原状态
  （唯一样本原样保留在原节点）。
- 找不到合规节点时**保留原状态**，报告原因"没有合规且容量足够的在线节点"。
- 只能补齐部分（中途候选耗尽）时返回"部分修复"，并给出修复前后计数。
- 已达标时修复为幂等空操作。

## 3. 并发与守恒

- 注册表持一把可重入锁 `RLock`；节点各自持锁。复合操作遵循固定加锁顺序
  **先注册表锁、后节点锁**，杜绝死锁。
- "候选筛选 + 实际放置"在注册表锁内原子完成（`place_on_first_eligible_node`），
  因此并发修复/放置时：同一节点对同一内容至多一份副本，不会重复复制。
- 可断言的守恒性质（测试已覆盖）：
  - 64 线程并发提交同一内容 → 恰有 1 个 CID、0 个多余副本；
  - 64 线程并发向同一节点放置 → 恰 1 次成功、其余全部 `DuplicatePlacementError`；
  - 20 线程并发修复阈值为 2 的内容 → 在线/总副本数恰好为 2，每节点至多 1 份；
  - 节点上下线与判定并发执行 → 总副本数恒等，在线数始终在 `[0, N]`；
  - 修复与外部放置并发 → 无重复副本，计数在确定区间内。

## 4. 可配置项（`GatewayConfig`）

| 配置 | 默认值 | 含义 |
|---|---|---|
| `retrieval_threshold` | `2` | 判定可检索所需的最少**在线**健康副本数（必须 ≥ 1） |
| `default_node_capacity` | `1 GiB` | HTTP 建节点未指定容量时的默认容量（字节） |

```python
from storage_gateway.config import GatewayConfig
from storage_gateway.gateway import StorageGateway

gw = StorageGateway(config=GatewayConfig(retrieval_threshold=3))
```

## 5. HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/nodes` | 建节点 `{node_id, capacity?, authorized?}` |
| POST | `/nodes/{id}/status` | 上下线 `{status: "online"|"offline"}` |
| POST | `/contents` | 提交内容 `{content_b64}` 或 `{content_text}`，可带 `declared_checksum` |
| POST | `/contents/{cid}/replicas` | 批量复制 `{node_ids: [...]}`，返回逐节点结论 |
| PUT | `/contents/{cid}/replicas/{node_id}` | 单节点放置，失败映射为可区分状态码 |
| GET | `/contents/{cid}/status` | 可检索性判定与原因 |
| POST | `/contents/{cid}/repair` | 触发修复，返回修复报告 |

启动：`uv run fastapi dev main.py`（交互式文档 `/docs`）。

## 6. 验证方法

```bash
# 全部单测（23 个用例，默认打印 INFO 级输入与判定依据日志）
uv run pytest

# 安静模式（日志仍按 pytest.ini 配置输出）
uv run pytest -q
```

测试覆盖：内容损坏/空内容、归一去重、容量不足、未授权节点、重复放置、
节点上下线计数守恒、修复成功/失败保留/全离线唯一样本保留，以及 4 组高并发场景。

## 7. 代码结构

```
storage_gateway/
├── config.py     # GatewayConfig 可配置项
├── errors.py     # 分层具名错误（内容层/放置层/修复层）
├── hashing.py    # SHA-256 摘要与校验
├── models.py     # ContentMetadata / RetrievabilityResult / RepairReport
├── node.py       # StorageNode：容量、授权、上下线、本地副本
├── registry.py   # ContentRegistry：内容归一、放置记账、可检索性（锁核心）
├── repair.py     # RepairManager：阈值修复与失败说明
├── gateway.py    # StorageGateway：服务门面
└── api.py        # FastAPI 入口与错误码映射
tests/            # 内容寻址/放置/上下线/修复/并发/HTTP 集成测试
```
