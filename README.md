# 本地去中心化存储网关（Python 实现）

在本地用多个**存储节点**模拟去中心化存储网络中的四个核心机制：

1. **内容寻址与元数据管理**——相同内容归一到同一标识（CID），损坏内容按原因拒绝；
2. **副本放置与节点状态管理**——多节点复制时对容量、重复、授权、离线做可区分拒绝，上下线联动可检索性；
3. **可检索性判定与修复流程**——在线健康副本数低于阈值即判定不可检索并触发修复，并发修复不重复复制、不丢唯一样本；
4. **内容读取与坏副本治理**——按 CID 取回原始字节，逐副本校验完整性，损坏副本被识别、隔离并由修复流程替换。

所有判定都返回/抛出**可解释的结论**（原因字符串或具名错误类型），并通过一把全局注册表锁保证并发下计数守恒。

---

## 1. 内容寻址模型

- 内容标识 `CID = sha256_hex(content)`，为 64 位十六进制字符串。
- 提交时对原始字节计算 SHA-256：
  - 空内容 → `EmptyContentError`（拒绝入库，不产生元数据）；
  - 若调用方提供 `declared_checksum` 且与实际摘要不一致 → `ChecksumMismatchError`（内容损坏/被篡改，拒绝入库）；
  - 校验通过（或未声明摘要）后按 CID 归一：**相同内容重复提交返回同一 `ContentMetadata`，不新增任何副本**。
- 元数据 `ContentMetadata` 是不可变快照：`cid / size / checksum / replica_nodes`，其中 `replica_nodes` 为当前持有该内容的节点集合快照。
- 注册表保留一份原始提交字节作为**可信样本**，供修复时读取；修复的数据来源始终是该样本或健康副本，从不读取任何已损坏副本。

## 1.1 内容读取（`retrieve` / `GET /contents/{cid}`）

- 按 CID 从**在线健康副本**取回原始字节；节点真实保存副本字节，读取时逐份校验
  `sha256(字节) == CID`，校验通过才返回，因此取回的字节必然与当初提交的完全一致。
- 候选副本按节点 id 排序依次尝试：某个副本字节与 CID 对不上（落盘数据被篡改）时，
  **该副本被登记为损坏并自动换下一份**，坏数据绝不作为结果返回，也不会被静默忽略。
- 失败原因严格区分：
  - CID 从未提交过 → `ContentNotFoundError`（HTTP 404）；
  - 有记录但当前没有健康在线副本（全部离线或全部损坏）→ `ContentUnavailableError`（HTTP 503）。
- 同一内容重复读取结果稳定（候选顺序确定，且读取与修复/上下线在注册表锁内串行化）。

### 坏副本的判定与处理规则

- **判定**：只有在读取时实际校验失败才会把副本登记为损坏（注册表维护
  `cid -> 损坏节点集合`），不做事先猜测。
- **隔离**：一旦登记，该副本不再计入"在线健康副本数"，`RetrievabilityResult`
  通过 `corrupt_replica_count` 单独说明；修复也不会拿它当来源。
- **替换**：修复时先移除损坏副本（释放其容量记账、清除损坏登记），再从可信样本
  补齐健康副本——原节点若在线且容量足够会被重新选中，即"原地替换"。
- **守恒**：替换是"先移除再放置"，容量不会重复计费；修复只把健康在线副本补到
  阈值，有效副本数不超出修复目标；原内容再次提交仍归一到同一 CID，不新增副本。

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
- 可检索性判定：`在线健康副本数 >= retrieval_threshold` 才可检索；
  已判定损坏的副本不计入健康副本数（见 1.1）。

### 2.3 可检索性结论（`RetrievabilityResult`）

| 情形 | retrievable | 原因示例 |
|---|---|---|
| 无任何副本 | false | `内容尚无任何副本` |
| 副本都在离线节点或已损坏 | false | `全部 N 个副本所在节点均离线或副本已损坏` |
| 在线副本不足 | false | `在线副本数 1 低于检索阈值 2（总副本数 2）` |
| 存在损坏副本 | （附加说明） | `另有 1 个副本已被判定损坏，不计入健康副本` |
| 达标 | true | `在线副本数 2 >= 检索阈值 2` |

### 2.4 修复（`RepairManager.repair`）

- 仅当不可检索**或存在已判定损坏的副本**时动作；候选节点按节点 id 排序后确定性选择。
- 候选必须同时满足：**已注册、已授权、在线、尚无该副本、剩余容量充足**。
- **先替换坏副本**：已判定损坏的副本先被移除（释放容量、清除登记），再按阈值缺口
  补齐健康副本；被移除坏副本的在线节点可作为普通候选被重新选中（原地替换）。
- **全部健康副本离线时不修复**：没有在线健康样本可复制，返回失败报告且保留原状态
  （唯一样本原样保留在原节点）；若副本全部损坏被移除，则从注册表可信样本恢复。
- 找不到合规节点时**保留健康副本原状**，报告原因"没有合规且容量足够的在线节点"。
- 只能补齐部分（中途候选耗尽）时返回"部分修复"，并给出修复前后计数。
- 已达标且无损坏副本时修复为幂等空操作。
- `RepairReport` 结论字段：`target_nodes`（新增副本节点）、
  `replaced_corrupt_nodes`（被替换的坏副本节点）、`valid_nodes`（修复后持有
  有效副本的全部节点），配合前后计数可完整解释修复结果。

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
  - 修复与外部放置并发 → 无重复副本，计数在确定区间内；
  - 64 线程并发读取同一内容 → 全部取回完全相同的字节；
  - 读取与修复并发交叉（含坏副本）→ 坏副本绝不作为结果返回，修复后健康副本
    恰好达到阈值、无残留损坏登记、容量记账与实际副本数一致；
  - 节点上下线与读取/修复交叉 → 读取要么得到正确字节要么显式失败，
    最终状态可修复回阈值且记账守恒。

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
| GET | `/contents/{cid}` | 取回原始字节（404 从未提交 / 503 无健康在线副本） |
| POST | `/contents/{cid}/replicas` | 批量复制 `{node_ids: [...]}`，返回逐节点结论 |
| PUT | `/contents/{cid}/replicas/{node_id}` | 单节点放置，失败映射为可区分状态码 |
| GET | `/contents/{cid}/status` | 可检索性判定与原因（含 `corrupt_replica_count`） |
| POST | `/contents/{cid}/repair` | 触发修复，返回修复报告（含坏副本替换说明） |

启动：`uv run fastapi dev main.py`（交互式文档 `/docs`）。

### 兼容范围

- 既有接口（提交、复制、状态、修复、节点管理）的路径、请求/响应字段与错误码
  全部保持不变；`/contents/{cid}/status` 与修复报告仅**新增**字段，旧客户端可忽略。
- 库层 `StorageGateway` 新增 `retrieve(cid)`；`StorageNode.place` 签名变为
  `place(cid, size, data)`（节点现在真实保存字节），直接调用节点的内部代码需适配。
- 唯一的行为变化：已判定损坏的副本不再计入"在线健康副本数"，修复会主动移除并
  替换坏副本（此前系统没有损坏概念）。

## 6. 验证方法

```bash
# 全部单测（37 个用例，默认打印 INFO 级输入与判定依据日志）
uv run pytest

# 安静模式（日志仍按 pytest.ini 配置输出）
uv run pytest -q

# 只看读取链路与坏副本治理
uv run pytest tests/test_retrieval.py -v
```

测试覆盖：内容损坏/空内容、归一去重、容量不足、未授权节点、重复放置、
节点上下线计数守恒、修复成功/失败保留/全离线唯一样本保留、按 CID 读取与
字节一致性、坏副本识别/隔离/替换、修复后容量记账守恒，以及多组高并发场景
（含读取与修复并发交叉、上下线抖动交叉）。

本地手动验证读取链路（模拟落盘数据被篡改）：

```python
from storage_gateway.gateway import StorageGateway
from storage_gateway.node import StorageNode

gw = StorageGateway(retrieval_threshold=2)
gw.registry.register_node(StorageNode("n1", 10_000))
gw.registry.register_node(StorageNode("n2", 10_000))
cid = gw.submit(b"hello").cid
gw.replicate(cid, ["n1", "n2"])

gw.registry.get_node("n1").overwrite_data(cid, b"tampered")  # 模拟 n1 落盘损坏
gw.retrieve(cid)            # 仍返回 b"hello"：坏副本被跳过并登记
gw.status(cid).corrupt_replica_count   # 1
gw.repair(cid)              # 替换 n1 上的坏副本，恢复 2 份健康副本
```

## 7. 代码结构

```
storage_gateway/
├── config.py     # GatewayConfig 可配置项
├── errors.py     # 分层具名错误（内容层/放置层/修复层，含 ContentUnavailableError）
├── hashing.py    # SHA-256 摘要与校验
├── models.py     # ContentMetadata / RetrievabilityResult / RepairReport
├── node.py       # StorageNode：容量、授权、上下线、本地副本字节
├── registry.py   # ContentRegistry：内容归一、放置记账、读取校验、损坏登记（锁核心）
├── repair.py     # RepairManager：阈值修复、坏副本替换与失败说明
├── gateway.py    # StorageGateway：服务门面
└── api.py        # FastAPI 入口与错误码映射
tests/            # 内容寻址/放置/上下线/修复/读取与坏副本/并发/HTTP 集成测试
```
