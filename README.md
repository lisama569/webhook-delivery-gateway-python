# Webhook Delivery Gateway（事件通知接入与投递网关）

面向第三方合作方的事件通知接入与投递服务（FastAPI 实现）。全部能力可在本地
启动与验证：**不依赖外部服务、真实凭据或付费资源**。出站目标默认使用进程内
`MockTransport`（模拟合作方接收端），并用 SQLite（WAL）持久化。

## 1. 它解决什么问题

| 能力 | 保证 |
| --- | --- |
| 入站幂等 | 同一 `(tenant_id, event_id)` 在重复请求或并发到达时只处理一次；首次 `201 ACCEPTED`，重复 `200 DUPLICATE`，结论稳定可区分 |
| 租户隔离 | 所有数据读写强制带 `tenant_id`；鉴权采用常量时间比较；额度（限流）按租户独立计数，A 方失败/超限不影响 B 方 |
| 重试与顺序 | 失败按确定性指数退避重试；同一订阅严格按 `seq` 顺序投递（队头阻塞，绝不乱序）；超过上限进入可查询的死信链路 |
| 凭据轮换 | HMAC-SHA256 签名；同一租户可同时存在多把有效期重叠的凭据，新旧都能验签；过期凭据被明确拒绝，原因可解释 |
| 契约演进 | 订阅声明版本区间与已知字段；版本/字段不一致按确定规则处理，未知版本/字段不会崩溃也不会被静默丢弃 |
| 可解释性 | 每次受理与投递都记录判定依据（`delivery_attempts` / `dead_letters`），INFO 日志打印事件标识与判定结论 |

## 2. 投递协议

### 2.1 入站事件（合作方 → 网关）

```
POST /v1/events
Header: X-API-Key: <租户 API Key>
```

```json
{
  "event_id": "evt-1",            // 租户内唯一，幂等键
  "event_type": "order.created",
  "event_version": 2,             // 正整数，契约版本
  "data": { "order_id": "o-1" },  // 事件负载
  "occurred_at": "2026-09-22T08:00:00Z"  // 可选
}
```

响应：

* 首次受理：`201 Created`，`status="ACCEPTED"`；
* 重复/并发落败：`200 OK`，`status="DUPLICATE"`（body 仍返回该事件当前的投递结论，多次重复结论一致）；
* 鉴权失败：`401`，`detail.error ∈ {MISSING_API_KEY, INVALID_API_KEY}`；
* 超出该租户额度：`429 RATE_LIMITED`。

### 2.2 出站投递（网关 → 合作方接收端）

网关对每个匹配的订阅生成一条投递（同订阅内单调递增 `seq`），POST JSON 信封：

```json
{
  "event_id": "evt-1",
  "tenant_id": "acme",
  "subscription_id": 1,
  "seq": 1,
  "payload": { "order_id": "o-1", "future_field": 9 },
  "unknown_fields": ["future_field"]
}
```

请求头：

| Header | 含义 |
| --- | --- |
| `X-Signature` | `t=<unix_ts>,k=<key_id>,v=<HMAC_SHA256(secret, "<ts>." + body)>` |
| `X-Signature-Key-Id` | 本次签名使用的凭据 id（轮换期用于区分新旧凭据） |
| `X-Event-Id` / `X-Tenant-Id` / `X-Delivery-Seq` / `X-Attempt` | 事件标识、租户、订阅内序号、第几次尝试 |

接收端返回 `2xx` 视为成功；其余状态码归类为 `NON_2XX`；连接异常/超时归类为
`TRANSPORT_ERROR`（`http_status=null`，两类原因始终可区分）。

### 2.3 管理面（`/admin/*`）

管理面用 `X-Admin-Key` 鉴权（默认 `local-admin-key`）；租户资源用
`X-API-Key` 鉴权并只能访问本租户数据。

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /admin/tenants` | 创建租户（api_key、每分钟额度） |
| `POST /admin/credentials` | 新增签名凭据（`valid_from` / `valid_to`，可重叠） |
| `GET  /admin/credentials` | 列出凭据及 `is_active` / `is_expired` |
| `POST /admin/subscriptions` | 创建订阅（事件类型、版本区间、兼容策略、已知字段） |
| `GET  /admin/subscriptions` | 列出本租户订阅 |
| `GET  /admin/events/{event_id}/deliveries` | 事件的全部投递与每次尝试明细 |
| `GET  /admin/deliveries/{id}` | 单条投递（含状态、退避到期时间、尝试历史） |
| `GET  /admin/dead-letters` | 死信链路（终因 + 最后一次直接原因 + 时间） |
| `POST /admin/signing-key` | 强制本租户出站使用的 key_id（模拟轮换期切新/用旧） |
| `POST /admin/worker/tick` | 同步驱动一轮投递（测试/本地验证用） |
| `POST /admin/mock/endpoints/{url}` | 配置内置 Mock 接收端（失败次数/状态/接受的 key） |
| `GET  /admin/mock/endpoints/{url}/received` | 查看 Mock 收到的请求 |

## 3. 幂等与重试规则

### 3.1 幂等受理

* 幂等键：`(tenant_id, event_id)`，数据库唯一约束。
* 受理在一个 `BEGIN IMMEDIATE` 事务内完成：插入事件 → 为每个匹配订阅分配
  `seq`（来自 `subscriptions.next_seq`）→ 生成投递/契约死信。
* 并发竞争时只有一个事务赢得插入；其余请求捕获唯一约束冲突，回读获胜方的
  既有结论并统一返回 `DUPLICATE`。**重复请求不会产生第二条事件或第二次副作用。**
* 重复请求即使 body 与首次不同，也不会覆盖首次受理的内容（以首次为准）。

### 3.2 顺序保证

* 每条投递带 `(subscription_id, seq)`，`seq` 在订阅内严格单调。
* 领取待投递记录时，若同订阅存在更早的 `IN_FLIGHT` 记录、或尚未到期的
  `PENDING` 记录，则后续记录本轮不领取（队头阻塞）。
* 每个订阅每轮最多放行队头一条，因此同订阅事件严格有序，跨订阅互不阻塞。

### 3.3 退避重试（确定性、可解释）

```
第 n 次重试等待 = min(backoff_base * backoff_factor^(n-1), backoff_cap)
```

默认 `base=1s, factor=2, cap=300s`，无随机抖动，完全可复现：
第 1~4 次重试分别等待 **1s、2s、4s、8s**（可用环境变量调整，测试中置 0）。
每次尝试都写入 `delivery_attempts`（HTTP 状态、原因、使用的 key_id、时间、
说明），并打印 `verdict=RETRY reason=... next_not_before=...` 日志。

### 3.4 终态与死信

* 成功：`SUCCEEDED`。
* 第 `max_attempts` 次（默认 5）仍失败：`DEAD_LETTER`，并向 `dead_letters`
  写入一条记录：
  * `fail_reason = ATTEMPTS_EXHAUSTED`（终因）；
  * `detail` 保留最后一次直接原因（如 `NON_2XX: simulated HTTP 500`）。
* 契约不兼容导致的不可投递（见下）也直接进入死信链路，原因枚举不同。
* **任何失败都不会静默丢失**：要么在退避后重试，要么在死信链路可查询。

### 3.5 凭据轮换

* 每个租户可登记多把凭据，`valid_from`/`valid_to` 允许时间重叠；
  默认出站选用"最新生效"的一把，轮换窗口内可通过 `/admin/signing-key`
  显式用旧 key 签名，双方都能通过。
* 凭据选择只在当前有效期内（容忍 `GATEWAY_CLOCK_SKEW` 秒时钟偏移，默认 300s）。
* 指定/默认凭据已过期时，不进行外发，记录原因
  `no active signing credential` 并进入重试；持续无有效凭据则耗尽进死信。
* 接收方也可以拒绝它已轮换掉的旧 key（Mock 用 401 模拟），投递方得到
  `NON_2XX` + 明确 detail，可据以完成切换。

## 4. 版本兼容范围（契约规则）

订阅声明 `min_version`、`max_version`（可空=不设上限）、`known_fields`
以及超版本策略 `newer_version_policy`。受理事件时按以下**确定顺序**判定：

1. **事件版本 `< min_version`（发送方更旧）**
   → 不投递，`CONTRACT_VERSION_BEHIND`，直接进死信，detail 说明
   "older than subscriber min_version"。
2. **声明了 `max_version` 且事件版本 `> max_version`（发送方更新）**
   * `DELIVER_ANYWAY`（默认，前向兼容）：**照常投递**，在投递记录上标注
     超范围版本，不报错、不丢弃；
   * `QUARANTINE`：不投递，`CONTRACT_VERSION_AHEAD`，进死信。
3. **未知字段**
   → 不属于 `known_fields` 的字段**原样保留并随事件投递**，仅在
     `unknown_fields` 中显式列出（信封与管理面查询都可见）。
4. **未知事件类型**（无任何订阅）
   → 事件正常受理（返回 `deliveries: []`），不报错、不产生死信；将来新增
     订阅后同类型事件自然路由。

失败原因枚举互不重叠：`NON_2XX` / `TRANSPORT_ERROR` / `ATTEMPTS_EXHAUSTED` /
`CONTRACT_VERSION_AHEAD` / `CONTRACT_VERSION_BEHIND`。

## 5. 本地验证方法

需要 Python 3.14+ 与 [uv](https://docs.astral.sh/uv/)。

### 5.1 一键演示脚本（真实 HTTP 服务）

```bash
bash scripts/demo.sh
```

脚本会：清理临时库 → `uv run uvicorn main:app` 启动服务 → 创建
租户/新旧凭据/订阅 → 发送重复事件（观察 `ACCEPTED`/`DUPLICATE`）→
配置接收端前 2 次 503（观察 1s/2s 退避后成功）→ 持续失败（观察 4 次后
`ATTEMPTS_EXHAUSTED` 死信）。运行日志在 `.data/demo-server.log`。

### 5.2 手动启动

```bash
uv run uvicorn main:app --reload
# http://127.0.0.1:8000/docs  交互式文档
```

### 5.3 运行单元/并发测试（22 个用例）

```bash
uv run pytest                 # 安静模式
uv run pytest -o log_cli=true # 同时打印含事件标识与判定依据的日志
```

覆盖场景：

* `tests/test_idempotency.py`：重复请求稳定结论、**16 线程并发只受理一次**、
  跨租户隔离与不可见、鉴权失败码可区分、租户间额度互不影响；
* `tests/test_retry_order.py`：退避时间表确定性、失败后重试成功、
  `TRANSPORT_ERROR` 与 `NON_2XX` 可区分、重试耗尽进死信且不再投递、
  同订阅顺序与队头阻塞；
* `tests/test_credentials.py`：轮换期新旧凭据都验签成功、过期凭据标记并拒绝、
  接收方轮换后旧 key 返回 401；
* `tests/test_contract.py`：未知字段保留并标注、超版本默认照投/严格隔离、
  旧版本明确拒绝、未知事件类型正常受理、失败原因可区分；
* `tests/test_observability.py`：日志必须包含事件 id 与 verdict/basis。

测试使用临时 SQLite、关闭后台线程，通过 `/admin/worker/tick` 显式驱动投递，
结果确定且可重复。

## 6. 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `GATEWAY_DB` | `.data/gateway.db` | SQLite 文件路径 |
| `GATEWAY_ADMIN_KEY` | `local-admin-key` | 管理面 Key |
| `GATEWAY_MAX_ATTEMPTS` | `5` | 最大尝试次数（含首次） |
| `GATEWAY_BACKOFF_BASE` | `1.0` | 退避基数秒（测试置 0） |
| `GATEWAY_BACKOFF_FACTOR` | `2.0` | 退避乘数 |
| `GATEWAY_BACKOFF_CAP` | `300.0` | 单次退避上限秒 |
| `GATEWAY_CLOCK_SKEW` | `300` | 凭据有效期时钟容差秒 |
| `GATEWAY_RUN_WORKER` | `true` | 是否启动后台投递线程 |
| `GATEWAY_WORKER_INTERVAL` | `0.1` | 后台轮询间隔秒 |

## 7. 代码结构

```
main.py                  FastAPI 入口与生命周期
app/config.py            环境变量配置
app/models.py            领域枚举与 API schema
app/db.py                SQLite 连接与 schema（WAL + 唯一约束）
app/repository.py        仓储：幂等事务、租户过滤、seq 分配、状态流转
app/security.py          HMAC-SHA256 签名、常量时间比较、有效期判定
app/contract.py          契约兼容规则（版本区间 + 未知字段）
app/delivery.py          确定性退避计算、单次尝试分类
app/transport.py         传输抽象 + 内置 MockTransport（+可选 HttpxTransport）
app/worker.py            投递循环：顺序、重试、死信、日志、后台线程
app/state.py             运行期装配（settings/repo/transport/worker 单例）
app/routers/             inbound（入站）、admin（管理面）、deps（鉴权/限流）
tests/                   22 个用例（并发、重试、轮换、契约、可观测性）
scripts/demo.sh          一键本地演示
```
