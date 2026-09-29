# Webhook Delivery Gateway（事件通知接入与投递网关）

在 FastAPI 模板上实现的、面向第三方合作方的事件通知接入与投递能力。
事件在**多租户、并发、失败重试、凭据轮换、契约演进**下都有确定且可解释的结果。
全部逻辑可在本地启动与验证：内存存储 + 可编排 Mock 传输层，
**不依赖外部服务、真实凭据或付费资源**。

## 能力一览

- **入站幂等与租户隔离**：幂等键 `(tenant_id, event_id)`，事件级互斥锁 +
  唯一插入，并发重复只处理一次；首次 `202`、重复 `200/duplicate=true`（回带
  首次结论）；租户间凭据、额度、数据完全隔离。
- **HMAC-SHA256 签名与凭据轮换**：重叠有效期内新旧凭据都可验签；过期凭据
  明确拒绝（`EXPIRED_CREDENTIAL`）；±300s 时间戳防重放；跨租户凭据不可互用。
  **出站投递使用同一套有效期/轮换窗口**：凭据不在窗口内绝不发出，给出网关侧
  结论（`CREDENTIAL_MISSING/CREDENTIAL_NOT_YET_VALID/CREDENTIAL_EXPIRED`），
  与接收方失败（NETWORK/4XX/5XX）一眼可分。
- **出站自动运转**：服务启动即由后台 worker 自动推进首投、退避重试、重试
  耗尽入死信，**全程无需手工 pump**；确定性指数退避
  `0.05·2^(n-1)`（无随机、可复现）；网络错误/408/429/5xx 重试，4xx 立即死信；
  每订阅 FIFO 队头阻塞严格有序；超过上限进入可查询的死信链路。
- **订阅级暂停/恢复**：暂停期间入站照常受理并保序排队但不发出，恢复后按原
  顺序续发；重复调用幂等、结论稳定；只影响单个订阅，不拖累其他订阅与入站。
- **失败事件重放**：支持单条与按订阅批量重放死信；重放走订阅 FIFO（不超前序
  事件、不重发已成功事件）；并发/连续触发只有一次真正生效，其余返回稳定可
  区分的 `ALREADY_REPLAYING`；批量按序逐条处理、天然反压，积压再多也不乱序、
  不拖垮服务；原死信结论归档保留、尝试计数重置。
- **契约兼容演进**：语义化版本；未知主版本 422 拒绝、同主版本未知字段保留并
  登记（不丢弃、不报错）；必填字段缺失/类型不符给出确定结论。
- **可观测**：关键判定日志均打印 `event_id` 与判定依据（basis/reason）；
  投递记录含逐次尝试的状态码、分类与退避秒数，以及重放归档字段。

## 目录结构

```
app/
  models.py       领域模型与状态/原因/重放结论枚举
  logging_utils.py 统一日志（事件标识 + 判定依据）
  security.py     HMAC 签名/验签、凭据轮换时间窗
  contract.py     语义化版本与字段兼容性判定
  transport.py    HttpTransport（可选真实HTTP）与 MockTransport（可编排）
  storage.py      内存存储：事件锁/订阅锁、滑动窗口配额、顺序号、订阅开关
  dispatcher.py   FIFO 队列、退避重试、死信归档、后台worker、暂停/恢复、重放
  service.py      入站受理主流程（认证→幂等→配额→契约→扇出）
  api.py          HTTP 路由（入站/管理/失败链路/暂停恢复/重放/本地驱动）
  bootstrap.py    组装、worker 生命周期与演示种子数据
tests/            48 个测试（自动运转/凭据/暂停恢复顺序/重放幂等并发/…）
demo.py           一条命令跑通全部场景
docs/PROTOCOL.md  投递协议、自动运转、暂停恢复、重放、兼容范围
```

## 环境准备

需要 Python 3.14+ 与 [uv](https://docs.astral.sh/uv/)：

```bash
uv sync                 # 安装 fastapi/httpx/pytest 等全部依赖
```

## 本地验证方式

### 方式一：自动化测试（推荐先跑）

```bash
uv run pytest -q -s     # -s 可在输出中看到事件标识与判定依据
```

覆盖场景（均带数据一致性断言）：

| 测试文件 | 覆盖内容 |
|---|---|
| `tests/test_security.py` | 轮换重叠期新旧凭据通过、过期/未生效拒绝、防重放、篡改、未知 key |
| `tests/test_idempotency.py` | **20 路并发重复只受理 1 次**、串行重复稳定结论、同 event_id 跨租户隔离 |
| `tests/test_inbound.py` | 过期凭据 401、配额超限 429 且租户独立、未知版本 422、未知字段保留 |
| `tests/test_delivery.py` | 失败后重试成功、**重试耗尽死信（NETWORK/5XX/4XX 原因可区分）**、**同订阅严格有序** |
| `tests/test_contract.py` | 主版本不兼容、次版本前向兼容、未知字段保留、字段缺失/类型错误 |
| `tests/test_runtime.py` | **后台 worker 自动投递/重试/死信（无 pump）**、**出站凭据过期/未生效/缺失不发出且与接收方失败可分** |
| `tests/test_pause_resume.py` | **暂停期受理不发出、恢复后保序**、暂停/恢复幂等、**订阅间隔离**、恢复后队头退避仍保序 |
| `tests/test_replay.py` | **重放保序不超前序**、单条/批量幂等、**并发重放只发一次**、连续重复稳定结论、大积压按序反压 |
| `tests/test_api.py` | HTTP 全链路：签名入站→重复→故障编排→drain→死信查询 |
| `tests/test_admin_api.py` | HTTP：暂停/恢复/状态、**单条重放 REPLAYED→ALREADY_REPLAYING→ALREADY_DELIVERED**、批量重放、凭据死信 |

### 方式二：一条命令的端到端演示（无需起服务）

```bash
uv run python demo.py
```

依次演示：凭据轮换三类结论 → 并发 10 次幂等 → 未知版本/未知字段 →
503×2 后重试成功（退避 0.05/0.1/0.2s）→ 持续 500 重试耗尽入死信并查询 →
同订阅严格有序（队头阻塞）→ 租户额度隔离。

### 方式三：启动服务后手工调用

```bash
uv run uvicorn main:app --reload
# Swagger UI: http://127.0.0.1:8000/docs
```

服务启动即带演示种子（租户/凭据/契约/订阅，见 `app/bootstrap.py`）。
用 Python 快速构造一个合法签名请求：

```python
import json, time, httpx
from app.security import sign

ts = str(int(time.time()))
body = {"event_id": "evt-manual-1", "event_type": "order.created",
        "version": "1.0.0", "payload": {"order_id": "ord-1"}}
raw = json.dumps(body, separators=(",", ":")).encode()
headers = {
    "X-Tenant-Id": "tenant-a",
    "X-Key-Id": "key-a-new",          # 或重叠期内的 key-a-old
    "X-Timestamp": ts,
    "X-Signature": sign("secret-new", ts, raw),
}
r = httpx.post("http://127.0.0.1:8000/v1/events/inbound", content=raw, headers=headers)
print(r.status_code, r.json())        # 202 首次；再发一次 -> 200 duplicate=true
```

模拟接收方故障并观察重试/死信：

```bash
# 让回调前 3 次返回 500、第 4 次成功（用于看重试恢复）
curl -X POST localhost:8000/admin/mock/script \
  -H 'content-type: application/json' \
  -d '{"url":"mock://partner-a/order-hook","responses":[[500,"e"],[500,"e"],[500,"e"],[200,"ok"]]}'
# ...投递事件后...（无需任何手工操作，后台 worker 会自动重试到成功）
curl "localhost:8000/admin/deliveries?tenant_id=tenant-a&status=DELIVERED"
```

### 验证三件运维操作（服务已在运行）

设 `BASE=http://localhost:8000/admin/tenants/tenant-a/subscriptions/sub-a-orders`。

1. **暂停 / 恢复（顺序不乱）**

```bash
curl -X POST $BASE/pause                 # -> {"state":"PAUSED"}
curl -X POST $BASE/pause                 # 重复暂停，结论稳定
# 暂停期间照常投事件（返回 202），但不会真的发出：
curl "$BASE/state"                        # -> {"state":"PAUSED","pending":N}
curl -X POST $BASE/resume                 # -> {"state":"ACTIVE"}，按原顺序续发
```

2. **出站凭据不可用 vs 接收方失败**

把订阅的出站凭据换成已过期的那把再投事件，事件立即进入死信且一次请求都
不会发出，`fail_reason=CREDENTIAL_EXPIRED`、`fail_detail` 带 `(no request sent)`，
与对方返回的 `ATTEMPTS_EXHAUSTED(last_basis=HTTP_5XX/NETWORK_ERROR)` 一眼可分：

```bash
curl -X PUT localhost:8000/admin/tenants/tenant-a/subscriptions -H 'content-type: application/json' -d '{
  "subscription_id":"sub-a-orders","event_type":"order.created","version":"1.0.0",
  "callback_url":"mock://partner-a/order-hook","credential_key_id":"key-a-expired","max_attempts":4}'
# ...投事件后...
curl localhost:8000/admin/deliveries/dead-letter
```

3. **失败重放（单条 / 批量，幂等保序）**

```bash
# 单条：首次 202 REPLAYED；紧接着再来一次 -> 200 ALREADY_REPLAYING；
#       成功后再来 -> 409 ALREADY_DELIVERED
curl -X POST $BASE/events/<event_id>/replay
# 批量：把该订阅所有死信按 seq 逐条重放；并发/连续再调一次只会得到
#       ALREADY_REPLAYING，不会重复发送
curl -X POST $BASE/replay-dead-letters
```

> 上面这些步骤等价地由自动化测试覆盖：`tests/test_runtime.py`、
> `tests/test_pause_resume.py`、`tests/test_replay.py`、
> `tests/test_admin_api.py`，可直接 `uv run pytest -q` 复现。

## 关键规则速查

- 首次受理 **202**；重复（含并发落败者）**200 + `duplicate=true`**；
  认证失败 401；配额超限 429；契约/版本问题 422。
- 退避：`min(1s, 0.05·2^(n-1))`，无抖动；可重试故障最多 `max_attempts` 次；
  死信原因 `ATTEMPTS_EXHAUSTED`（细节含 `last_basis`）或 `HTTP_4XX`。
- 服务启动后投递/重试由后台 worker **自动推进**，无需 pump（节拍 0.02s）。
- 出站凭据不在 `not_before~not_after` 窗口：**不发请求**直接死信，原因为
  `CREDENTIAL_MISSING / CREDENTIAL_NOT_YET_VALID / CREDENTIAL_EXPIRED`，
  与接收方侧失败区分；轮换凭据后重放即可。
- 暂停只影响"是否发出"，不影响入站与其他订阅；恢复后按 `seq` 保序续发。
- 只有 `DEAD_LETTER` 可重放；已成功绝不重发；重放保序、尝试计数重置、原结论
  归档；并发/连续重放只有一次 `REPLAYED`，其余 `ALREADY_REPLAYING`。
- 版本：主版本必须已注册且与订阅一致；同主版本向前兼容，未知字段保留登记。

详见 [`docs/PROTOCOL.md`](docs/PROTOCOL.md)。
