# 投递协议、幂等、重试与版本兼容说明

本服务是一个**面向第三方合作方的事件通知接入与投递网关**，全部能力可在本地
运行与验证（内存存储 + 可编排 Mock 传输层），不依赖外部服务、真实凭据或付费资源。

---

## 1. 总体模型

```
合作方系统 ──(签名的入站事件)──▶ [接入层]
                                   │  ① 租户认证/验签
                                   │  ② 事件级幂等（并发安全）
                                   │  ③ 租户独立配额
                                   │  ④ 契约版本/字段兼容校验
                                   ▼
                              事件存储（按租户隔离）
                                   │  按订阅扇出（fan-out）
                                   ▼
                        每订阅一条 FIFO 队列（顺序保证）
                                   │  HMAC 签名投递 + 指数退避重试
                                   ▼
                     接收方回调 ── 成功 / 重试 / 死信（可查询）
```

- **入站**：合作方把业务事件签名后 POST 到 `/v1/events/inbound`。
- **出站**：网关按订阅把事件签名后 POST 到合作方回调地址；失败重试，
  超过上限进入死信链路（DLQ），绝不静默丢失。

---

## 2. 签名与认证协议

### 2.1 请求头

| 头 | 含义 |
|---|---|
| `X-Tenant-Id` | 租户标识 |
| `X-Key-Id` | 使用的凭据 ID（支持同一租户多把凭据） |
| `X-Timestamp` | Unix 秒级时间戳（防重放，允许 ±300s 偏移） |
| `X-Signature` | 十六进制小写 HMAC-SHA256 |

### 2.2 签名算法

```
canonical = X-Timestamp + "." + 原始请求体字节
signature = HMAC_SHA256(secret, canonical) 的 hex 小写
```

签名覆盖的是**原始字节**，因此验签必须在解析 body 之前完成。

### 2.3 判定结论（机器可读，互不混淆）

| 判定依据 basis | HTTP | FailReason | 含义 |
|---|---|---|---|
| `OK` | 202/200 | `NONE` | 验签通过 |
| `KEY_NOT_FOUND` | 401 | `INVALID_SIGNATURE` | key_id 在该租户下不存在 |
| `CREDENTIAL_NOT_YET_VALID` | 401 | `INVALID_SIGNATURE` | 凭据 `not_before` 未到 |
| `SIGNATURE_MISMATCH` | 401 | `INVALID_SIGNATURE` | HMAC 不匹配（含报文被篡改） |
| `STALE_TIMESTAMP` / `BAD_TIMESTAMP` | 401 | `STALE_TIMESTAMP` | 时间戳超出 ±300s 或非法 |
| `EXPIRED_CREDENTIAL` | 401 | `EXPIRED_CREDENTIAL` | 凭据 `not_after` 已过，**明确拒绝** |

### 2.4 凭据轮换

- 每把凭据含 `not_before` / `not_after` 时间窗；轮换时让新旧凭据的时间窗
  **重叠**，重叠期内新旧凭据都可验签；
- `deprecated=true` 且仍在有效期内 → **允许**（弃用宽限），仅在日志中标注；
- 一旦 `now > not_after`，即使未标记 deprecated 也一律拒绝（`EXPIRED_CREDENTIAL`）；
- 认证按 `X-Tenant-Id` 在本租户凭据集合内匹配，**跨租户凭据不可互用**。

出站投递同理：网关注入 `X-Key-Id/X-Timestamp/X-Signature`（另有
`X-Delivery-Id: <event_id>:<attempt>`），接收方可在凭据轮换重叠期平滑切换。

**出站凭据在发送前还要再过一次同一套有效期/轮换窗口**（与入站验签共用
`credential_window_basis` 判定）：

| 出站凭据情形 | basis | 处理 |
|---|---|---|
| 凭据不存在 | `KEY_NOT_FOUND` | **请求不发出**，直接死信 |
| `now < not_before` 未生效 | `CREDENTIAL_NOT_YET_VALID` | **请求不发出**，直接死信 |
| `now > not_after` 已过期/已轮换下线 | `EXPIRED_CREDENTIAL` | **请求不发出**，直接死信 |
| 窗口内（含 deprecated 宽限） | `OK` | 正常签名并发出 |

出站凭据失窗统一记为死信原因 **`CREDENTIAL_UNAVAILABLE`**：它描述的是"网关
这边没有可用凭据、请求根本没发出"，因此与"请求已到达接收方、由对方返回的
`NETWORK_ERROR / HTTP_5XX / HTTP_4XX / ATTEMPTS_EXHAUSTED`"严格可区分，也
**不占用退避重试次数**（`attempts` 中不产生网络尝试）。处置方式是修好凭据
（轮换/更新订阅 `credential_key_id`）后对死信发起**重放**。

---

## 3. 幂等与租户隔离

### 3.1 幂等键与并发保证

- 幂等键为 **`(tenant_id, event_id)`**；
- 受理时先获取**事件级互斥锁**，在锁内执行「是否已存在 → 唯一插入」，
  因此同一事件的重复请求或并发到达**只被处理一次**；
- 仅认证、配额、契约全部通过后才落库并占用幂等键（失败请求不占用键，
  客户端修正后可安全重试）。

### 3.2 稳定且可区分的结论

| 场景 | HTTP | 响应 |
|---|---|---|
| 首次受理 | **202** | `accepted=true, duplicate=false` |
| 重复请求（含并发落败者） | **200** | `accepted=true, duplicate=true`，`detail` 回带首次结论 `original status=...` |

重复响应不重新执行任何投递，只返回首次的稳定结论。

### 3.3 租户隔离

- 事件、凭据、订阅、配额窗口均以 `tenant_id` 为边界物理隔离；
- **配额**：每租户独立的滑动 60 秒窗口（种子数据：tenant-a 60/min，
  tenant-b 5/min），超限返回 **429 `QUOTA_EXCEEDED`**；一个租户耗尽额度
  不影响任何其他租户；重复请求不重复占用额度。

---

## 4. 契约（Schema）版本兼容

契约按 **`事件类型 + 主版本`** 注册（语义化版本 `M.m.p`）。

### 4.1 兼容规则（确定性）

1. 事件主版本在注册表中不存在 → 拒绝受理：**422 `UNKNOWN_EVENT_VERSION`**；
2. 订阅声明版本与事件契约**主版本不一致** → 该订阅不扇出，记录
   `CONTRACT_INCOMPATIBLE`（主版本代表破坏性变更）；
3. 同主版本内**前向兼容**：事件次版本高于注册契约时，契约未声明的字段记为
   `unknown_fields` —— **不报错、不丢弃**，payload 原样投递，字段名落库、
   在受理响应与事件查询接口中可查；
4. 注册契约中 `required=true` 的字段必须存在且类型匹配
   （`string/int/float/bool`），否则 **422 `CONTRACT_INCOMPATIBLE`**，
   `detail` 明确指出字段名与期望类型。

> 设计取向：未知版本/未知字段属于「需要确定结论」的输入，既不能导致服务
> 异常，也不能静默吞掉——未知主版本显式拒绝，同主版本未知字段显式保留并登记。

### 4.2 示例

注册契约 `order.created` 主版本 1（注册版本 `1.2.0`，必填 `order_id:string`）：

| 入站事件版本 | payload | 结论 |
|---|---|---|
| `9.0.0` | 任意 | 422 `UNKNOWN_EVENT_VERSION` |
| `1.0.0`/`1.3.0` | `{"order_id":"o1","promo":"x"}` | 受理；`unknown_fields=["promo"]`，payload 保留 `promo` |
| `1.0.0` | `{"amount":1}`（缺 order_id） | 422 `CONTRACT_INCOMPATIBLE: missing required field 'order_id'` |
| `2.0.0` | 合法 | 受理成功，但 v2 订阅与 v1 订阅互不扇出（主版本不匹配） |

---

## 5. 出站投递：自走运转、重试、退避与顺序

### 5.0 正常运行时自己往前走（无需手工推动）

- 服务 startup 自动拉起一个后台投递循环（`dispatcher.start_worker()`，节拍
  `WORKER_TICK_SECONDS=0.02s`，shutdown 时停止）；
- 入站受理后事件进入订阅 FIFO，后台循环会自动：到期投递 → 失败按退避重试 →
  成功终结 / 超过上限落入死信链路。整条链路不需要任何手工 `pump`；
- `/admin/pump`、`/admin/drain` 与 `run_pending(now=...)` 仅保留给本地演示与
  确定性测试；它们与后台循环并发也是安全的（订阅锁 + 队首/状态复核 + 在途
  去重，保证同一条事件不会被重复发送）。

### 5.1 响应分类（可解释）

| 接收方响应 | 分类 | 处理 |
|---|---|---|
| 2xx | `SUCCESS` | 投递成功，任务终结 |
| 408 / 425 / 429 / 5xx | `RETRYABLE` | 退避后重试 |
| 其余 4xx | `PERMANENT` | **不重试**，立即死信（`HTTP_4XX`） |
| 连接错误 / 超时 | `RETRYABLE` | 退避后重试（`NETWORK_ERROR`） |

### 5.2 退避策略（可解释、可复现）

- 第 `n` 次尝试失败后的等待：

  ```
  backoff(n) = min(MAX_BACKOFF, BASE * 2^(n-1))
  ```

  默认 `BASE=0.05s`（本地验证用小基数）、`MAX_BACKOFF=1s`，
  **无随机抖动**，因此退避序列与测试结论完全确定；
- 每次尝试的状态码、分类依据（basis）与当次 backoff 都记录在
  `attempts[]` 中，可在投递查询接口逐条审计。

### 5.3 重试上限与死信（结果不静默丢失）

- 达到订阅 `max_attempts`（默认 4）仍失败 → 状态 `DEAD_LETTER`；
- `fail_reason` 区分终局原因：
  - 可重试故障耗尽 → `ATTEMPTS_EXHAUSTED`，并在 `fail_detail` 保留
    **最后一跳**的底层依据 `last_basis=NETWORK_ERROR / HTTP_5XX`；
  - 永久 4xx → `HTTP_4XX`（仅尝试 1 次）；
  - 出站凭据缺失/未生效/已过期 → `CREDENTIAL_UNAVAILABLE`（请求未发出，
    不占用尝试次数，与接收方返回的失败严格区分）；
- 死信可查询：`GET /admin/deliveries/dead-letter`（可按租户过滤），
  返回完整尝试历史与失败细节。

### 5.4 顺序保证

- 每个订阅一条 FIFO 队列，入队即分配单调递增 `seq`；
- **队首未终结（成功/死信）前，后续事件一律不得发出**；前序事件处于退避
  等待时会阻塞队头（head-of-line blocking），从机制上保证同一订阅严格有序；
- 不同订阅之间相互独立、可并发。

### 5.5 订阅暂停 / 恢复（运维开关，粒度 = 订阅）

- `POST .../subscriptions/{id}/pause`：暂停后**新的入站事件照常受理、照常
  进入该订阅待投递队列**（`seq` 与先后位置不变），但不会真正发出任何请求；
- `POST .../subscriptions/{id}/resume`：恢复后从队首起**按原本的先后顺序**
  补发暂停期间积攒的事件，顺序不会乱；
- 两者都可**重复调用且结论稳定**：重复暂停保留首次 `paused_at`，重复恢复
  保持 `paused=false` 并唤醒后台循环；
- 影响面严格限定在该订阅本身：**停一个订阅不拖累其他订阅的投递，也不影响
  入站受理与配额**。

### 5.6 失败链路重放（单条 / 批量）

- **可重放对象**：仅 `DEAD_LETTER` 事件。已成功投递的事件不能再发
  （返回 `NOT_DEAD_LETTER`，HTTP 409），订阅/记录不存在返回 404；
- **顺序**：重放把事件**追加到该订阅 FIFO 尾部**，绝不插到前面尚未终结的
  事件之前；因此它天然接着原订阅顺序走。批量重放严格按 `seq` 升序**逐条
  串行**处理——积压再多也是排队一步步行进，不会乱序、不会并发冲垮服务；
- **幂等 / 并发安全**：一条失败事件在同一时刻只允许一次"生效重放"。对同一
  事件并发或连续多次触发时：

  | 结论 outcome | HTTP | 含义 |
  |---|---|---|
  | `REPLAYED` | 202 | 本次请求生效，事件已重新入队（`replay_generation +1`） |
  | `ALREADY_IN_FLIGHT` | 200 | 已有一次生效重放在途/处理中（稳定、可区分的去重结论） |
  | `NOT_DEAD_LETTER` | 409 | 不在失败链路（含已成功），不予重发 |
  | `NOT_FOUND` | 404 | 订阅或投递记录不存在 |

- **重放代次（generation）可审计**：首次投递为代次 0；每生效一次重放
  `replay_generation +1`。当代次尝试从 1 重新计数写入 `attempts[]`，历史
  代次尝试归档到 `previous_attempts[]`（投递查询接口返回
  `replay_generation`、`replay_in_flight`、`archived_attempt_generations`）；
- 重放后的投递仍走完整的退避/顺序/凭据窗口规则：接收方恢复即成功；仍故障
  则再次耗尽回到死信（代次保留），此时可再次重放；
- 暂停期间也允许发起重放（事件照样排在队尾），恢复后随队列一起顺序补发。

---

## 6. HTTP 接口一览

| 方法 & 路径 | 说明 |
|---|---|
| `POST /v1/events/inbound` | 入站事件受理（签名头见 §2） |
| `PUT /admin/tenants/{id}` | 创建/更新租户（含每分钟额度） |
| `PUT /admin/tenants/{id}/credentials` | 登记凭据（轮换时间窗） |
| `GET /admin/tenants/{id}/credentials` | 查看凭据及有效期 |
| `PUT /admin/schemas` | 注册事件契约（类型+版本+字段） |
| `PUT /admin/tenants/{id}/subscriptions` | 创建订阅（回调/版本/重试上限） |
| `GET /admin/tenants/{id}/subscriptions/{sid}` | 查询订阅（含 `paused/paused_at`） |
| `POST /admin/tenants/{id}/subscriptions/{sid}/pause` | **暂停订阅投递**（事件照常入队，可重复调用） |
| `POST /admin/tenants/{id}/subscriptions/{sid}/resume` | **恢复订阅投递**（按原顺序补发，可重复调用） |
| `GET /admin/deliveries` | 投递总览（可按 tenant、status、subscription 过滤；含重放代次） |
| `GET /admin/deliveries/dead-letter` | **死信链路查询** |
| `POST /admin/tenants/{id}/subscriptions/{sid}/dead-letter/{eid}/replay` | **重放单条失败事件**（202/200/409/404 见 §5.6） |
| `POST /admin/tenants/{id}/subscriptions/{sid}/dead-letter/replay` | **批量重放**该订阅全部未处理好的失败事件（按 seq 顺序） |
| `GET /admin/tenants/{id}/events/{eid}` | 查询事件（状态/契约版本/未知字段） |
| `POST /admin/pump` | 手动推进一次到期投递（仅演示/测试；正常运行由后台循环自动完成） |
| `POST /admin/drain` | 等待全部重试完成（演示用） |
| `POST /admin/mock/script` | 编排回调响应序列（本地复现故障） |
| `POST /admin/reset` | 重置到种子状态 |

事件状态机：`ACCEPTED → SCHEDULED → DELIVERING → RETRYING → … → DELIVERED`
或 `… → DEAD_LETTER`；重放时 `DEAD_LETTER → SCHEDULED`（代次 +1、追加队尾），
成功后终结；重复入站事件以 `DUPLICATE` 结论返回（不改变状态）。

### 兼容范围

- 本轮新增的暂停/恢复、重放、后台自走与出站凭据窗口，均为**向后兼容的增量**：
  既有入站协议、签名头、退避序列与既有 HTTP 路由的请求/响应字段保持不变；
- 投递查询响应在原有字段上**新增** `replay_generation / replay_in_flight /
  archived_attempt_generations`，订阅查询新增 `paused / paused_at`，未知字段
  的旧客户端不受影响；
- 新增的失败原因枚举值 `CREDENTIAL_UNAVAILABLE` 与重放结论
  （`REPLAYED / ALREADY_IN_FLIGHT / NOT_DEAD_LETTER / NOT_FOUND`）为新增取值，
  不改动既有取值含义；
- 内存存储仍为单进程本地实现；后台循环、FIFO 与重放占用态基于 asyncio 锁，
  适用于本地运行/验证与单实例部署（多实例水平扩展不在本轮兼容范围内）。
