# 本地去中心化存储网关（Python 实现）

在本地用多个**存储节点**模拟去中心化存储网络中的四个核心机制：

1. **内容寻址与元数据管理**——相同内容归一到同一标识（CID），损坏内容按原因拒绝；
2. **副本放置与节点状态管理**——多节点复制时对容量、重复、授权、离线做可区分拒绝，上下线联动可检索性；
3. **可检索性判定与修复流程**——在线健康副本数低于阈值即判定不可检索并触发修复，并发修复不重复复制、不丢唯一样本；
4. **按内容标识读取与坏副本治理**——按 CID 取回与提交完全一致的字节；落盘字节被改坏的副本会被自动隔离：读取绝不返回坏数据、判定/修复不再把它当健康副本，修复可用健康样本就地替换坏副本，且全程不超标、容量不重复记账。

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
  - 总副本数（跨在线/离线节点，含已隔离坏副本占位）保持不变；
  - **在线健康副本数**实时随状态变化。
- 可检索性判定：`在线健康副本数 >= retrieval_threshold` 才可检索。

### 2.3 读取链路（`retrieve` / `GET /contents/{cid}/bytes`）

- 按 CID 取回内容，**取回的字节与当初提交的完全一致**（必然满足
  `sha256(取回字节) == CID`）；候选节点按节点 id 排序选取，同一份内容
  重复读取结果稳定、可复现。
- 取回前会对候选在线副本**逐个做字节校验**：
  - 某在线副本字节与其 CID 对不上（如落盘数据被人改过）→ 立即把该节点
    记入该 CID 的**损坏隔离集**，**改从下一个在线健康副本取回**，
    绝不把坏数据当结果返回，也不会静默忽略；
  - 已隔离节点在被正确字节替换/移除前，不再参与读取、计数与修复来源。
- 两类"取不到"被明确区分成不同原因（库内是不同异常，HTTP 是不同状态码）：

| 情形 | 异常 | HTTP |
|---|---|---|
| 该标识**从未提交过**（注册表里没有记录） | `ContentNotFoundError` | 404 |
| 有提交记录，但当前**没有任何在线健康副本**可取（无副本/在线副本全损坏/健康副本都在离线节点） | `ReplicaUnavailableError` | 503 |

- 故障注入：`registry.tamper_replica(cid, node_id, bad_bytes)` 可把某节点
  上的落盘字节改成与 CID 不符的内容（只改字节、不动容量记账），用于
  演练"副本字节损坏"场景；损坏由下一次读取/判定自动发现并隔离。

### 2.4 坏副本的判定与隔离规则

- **判定依据唯一**：以 `sha256(节点上实际保存的字节) == CID` 为准，
  与节点是否上报"有副本"无关。
- 判定时机：`check_retrievability`、`online_replica_count`、`retrieve_content`
  在扫描在线副本时实时复核字节；也可用 `verify_replica(cid, node_id)`
  显式校验单个副本（不区分在线/离线，因为磁盘内容不随上下线改变）。
- 隔离标记**始终与真实字节一致**：若坏字节后来被恢复成正确内容，下一次
  校验会自动解除隔离，不会出现"已经好了还被当成坏的"的陈旧结论。
- 计数语义：
  - `total_replica_count`：副本**占位**总数（在线/离线、含坏副本）；
  - `online_replica_count`：在线且字节健康的副本数（坏副本不计入）；
  - `online_corrupted_count`：在线但已隔离的坏副本数；
  - `healthy_replica_count`：全部节点（含离线）上的健康有效副本数；
  - `corrupted_nodes(cid)`：当前被隔离的节点集合快照。

### 2.5 可检索性结论（`RetrievabilityResult`）

| 情形 | retrievable | 原因示例 |
|---|---|---|
| 无任何副本 | false | `内容尚无任何副本` |
| 副本都在离线节点 | false | `全部 N 个副本所在节点均离线` |
| 在线副本不足 | false | `在线健康副本数 1 低于检索阈值 2（总副本数 2）` |
| 在线副本全损坏 | false | `在线的 N 个副本字节均已损坏被隔离，无健康副本（总副本数 M）` |
| 达标 | true | `在线健康副本数 2 >= 检索阈值 2`（另有坏副本时附带隔离说明） |

结论新增 `online_corrupted_count` 字段；`online_replica_count` 的含义
从"在线副本数"收紧为"在线**健康**副本数"。

### 2.6 修复（`RepairManager.repair`）

修复在注册表锁内串行完成，包含两类动作：

1. **替换坏副本**：对每个"在线且被隔离"的副本，用注册表保存的可信样本
   **就地重写**（`replaced_nodes`）。替换不新增副本、不删除唯一健康样本、
   **不改变容量记账**（该副本承诺占用的空间与放置时一致，不会重复计费）；
   坏节点绝不可能被当作修复来源，坏数据不会扩散。
2. **补齐缺口**（`target_nodes`）：替换后在线健康数仍不足阈值时，向
   已注册、已授权、在线、尚无该副本、容量足够的节点新增副本。

补齐数量同时受两个上界约束并取较小值，**修复结束后有效副本数绝不超标**：

- 在线缺口 = `阈值 - 在线健康数`（恢复可检索性所需）；
- 阈值总余量 = `阈值 - 全部健康副本数`（**含离线健康副本**）。

因此节点上下线抖动时，不会为"将来会恢复的离线健康副本"先补占位副本，
避免它们一回来就超过修复目标。修复报告新增 `replaced_nodes`（哪些节点的
坏副本被换掉）与 `valid_replica_count`（修复后有效副本总数）字段，
配合 `reason` 可完整解释"哪些节点上是有效副本、坏副本有没有被换掉、
有效副本数有没有超标"。

其余既有规则不变：

- 候选节点按节点 id 排序后确定性选择；
- **全部健康副本所在节点离线**时不修复（无在线健康样本可复制），
  返回失败报告且保留原状态（唯一样本原样保留）；
- 找不到合规节点时**保留原状态**并说明原因；
- 已达标且无坏副本时修复为幂等空操作（`repaired=false`）。
- 原内容**再次提交仍然归一到同一 CID，不会凭空多出副本**。

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
  - **多读取并发命中同一内容 + 并发篡改/修复** → 任何一次取回要么是正确
    字节、要么明确 `ReplicaUnavailableError`，绝不返回坏数据；
  - **读取 / 修复 / 上下线三者并发交叉** → 有效健康副本数始终不超过阈值、
    每节点对同一内容至多一份、各节点 `used_bytes` 与其副本承诺大小之和
    始终一致（替换坏副本不重复计费）、收敛后坏副本全部被替换且可正常读取。

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
| GET | `/contents/{cid}/status` | 可检索性判定与原因（含 `online_corrupted_count`） |
| **GET** | **`/contents/{cid}/bytes`** | **取回原始字节（`application/octet-stream`，带 `X-Content-Cid`/`X-Content-Sha256` 头）；未提交 → 404，无在线健康副本 → 503** |
| POST | `/contents/{cid}/repair` | 触发修复，返回报告（含 `replaced_nodes`、`valid_replica_count`） |

### 兼容性说明

- 既有接口的路径、请求体与既有字段全部保留，旧调用方式不变；
- 新增字段（`online_corrupted_count`、`replaced_nodes`、`valid_replica_count`）
  均为**增量字段**，不影响旧的响应消费方；
- 语义收紧一处：`status` 中的 `online_replica_count` 现在只统计**在线健康**
  副本（与"可检索性只依赖健康副本"的既有意图一致），坏副本改由
  `online_corrupted_count` 单独给出；
- 节点 `set_status` 现在同时接受枚举与裸字符串 `"online"/"offline"`，
  内部统一归一化为枚举。

启动：`uv run fastapi dev main.py`（交互式文档 `/docs`）。

## 6. 验证方法

```bash
# 全部单测（41 个用例，默认打印 INFO 级输入与判定依据日志）
uv run pytest

# 安静模式（日志仍按 pytest.ini 配置输出）
uv run pytest -q

# 只看读取链路 / 并发交叉用例
uv run pytest tests/test_retrieval.py tests/test_concurrency.py -q
```

测试覆盖：内容损坏/空内容、归一去重、容量不足、未授权节点、重复放置、
节点上下线计数守恒、修复成功/失败保留/全离线唯一样本保留，以及读取链路
（精确取回、重复稳定、未提交 404 与无健康副本 503 可区分、篡改坏副本被
隔离并改取健康副本、修复就地替换坏副本不新增不重复计费、有效副本不超标、
修复后重提不增副本、HTTP 取回/错误码/修复报告）和 6 组高并发场景
（含读取×篡改×修复、读取×修复×上下线交叉）。

## 7. 代码结构

```
storage_gateway/
├── config.py     # GatewayConfig 可配置项
├── errors.py     # 分层具名错误（内容层含 ReplicaUnavailableError/放置层/修复层）
├── hashing.py    # SHA-256 摘要与校验
├── models.py     # ContentMetadata / RetrievabilityResult / RepairReport（含坏副本字段）
├── node.py       # StorageNode：容量记账、授权、上下线、副本字节保存/重写/故障注入
├── registry.py   # ContentRegistry：归一、放置记账、坏副本隔离、读取、可检索性（锁核心）
├── repair.py     # RepairManager：替换坏副本 + 只补到阈值的修复与失败说明
├── gateway.py    # StorageGateway：服务门面（submit/replicate/status/retrieve/repair）
└── api.py        # FastAPI 入口、错误码映射（404/503）与 /bytes 读取端点
tests/            # 内容寻址/放置/上下线/修复/读取/并发/HTTP 集成测试
```
