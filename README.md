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
- **出站重试与顺序**：确定性指数退避 `0.05·2^(n-1)`（无随机、可复现）；
  网络错误/408/429/5xx 重试，4xx 立即死信；每订阅 FIFO 队头阻塞严格有序；
  超过上限进入可查询的死信链路，失败原因彼此可区分。
- **自走投递**：服务启动即拉起后台循环，到期投递/失败重试/耗尽入死信全部
  自动推进，正常运行无需任何手工 `pump`（pump 仅留给演示与确定性测试）。
- **出站凭据窗口**：发送前与入站验签同一套 not_before/not_after/轮换规则；
  凭据缺失/未生效/已过期则请求不发出，直接死信 `CREDENTIAL_UNAVAILABLE`，
  与接收方返回的失败（网络/4xx/5xx/耗尽）严格区分。
- **订阅暂停/恢复**：按订阅临时停投；暂停期间事件照常受理并入队（位置不变），
  恢复后按原顺序补发；可重复调用、结论稳定；只影响该订阅，不影响其他订阅
  与入站受理。
- **死信重放**：支持单条重放与按订阅批量重放；重放追加队尾不插队、按 seq
  有序；并发/连续重复触发只有一次生效（`REPLAYED`），其余得到稳定可区分的
  `ALREADY_IN_FLIGHT`；已成功事件不可再发（`NOT_DEAD_LETTER`）。
- **契约兼容演进**：语义化版本；未知主版本 422 拒绝、同主版本未知字段保留并
  登记（不丢弃、不报错）；必填字段缺失/类型不符给出确定结论。
- **可观测**：关键判定日志均打印 `event_id` 与判定依据（basis/reason）；
  投递记录含逐次尝试的状态码、分类与退避秒数，以及重放代次与历史尝试归档。

## 目录结构

```
app/
  models.py       领域模型与状态/原因枚举
  logging_utils.py 统一日志（事件标识 + 判定依据）
  security.py     HMAC 签名/验签、凭据轮换时间窗
  contract.py     语义化版本与字段兼容性判定
  transport.py    HttpTransport（可选真实HTTP）与 MockTransport（可编排）
  storage.py      内存存储：事件锁/订阅锁、滑动窗口配额、顺序号
  dispatcher.py   自走后台循环、FIFO 队列、退避重试、暂停/恢复、死信归档/重放
  service.py      入站受理主流程（认证→幂等→配额→契约→扇出）
  api.py          HTTP 路由（入站/管理/失败链路/暂停恢复/重放/本地驱动）
  bootstrap.py    组装与演示种子数据
tests/            47 个测试（并发幂等/重试耗尽/凭据轮换/契约/顺序/自走/暂停/重放/E2E）
demo.py           一条命令跑通全部场景（含自走、暂停/恢复、并发与批量重放）
docs/PROTOCOL.md  投递协议、自走/暂停/重放规则、版本兼容范围
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
| `tests/test_runtime.py` | **自走投递/自动重试入死信、出站凭据失窗（不发请求/原因可区分）、暂停期间不发与恢复后顺序、暂停隔离、单条/批量重放顺序、并发重放仅一次生效、已成功不可再发** |
| `tests/test_runtime_api.py` | **HTTP 全链路：服务启动自走投递、暂停/恢复接口、单条/批量重放状态码与幂等、跨订阅不连坐** |
| `tests/test_contract.py` | 主版本不兼容、次版本前向兼容、未知字段保留、字段缺失/类型错误 |
| `tests/test_api.py` | HTTP 全链路：签名入站→重复→故障编排→drain→死信查询 |

### 方式二：一条命令的端到端演示（无需起服务）

```bash
uv run python demo.py
```

依次演示：凭据轮换三类结论 → 并发 10 次幂等 → 未知版本/未知字段 →
503×2 后重试成功（退避 0.05/0.1/0.2s）→ 持续 500 重试耗尽入死信并查询 →
同订阅严格有序（队头阻塞）→ 租户额度隔离 → **暂停/恢复顺序补发** →
**并发重放仅一次生效 + 批量重放 + 凭据失窗可区分** → **worker 自走投递**。

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
# ...投递事件后...
curl -X POST localhost:8000/admin/drain            # 等重试跑完
curl localhost:8000/admin/deliveries/dead-letter   # 查询死信链路
curl "localhost:8000/admin/deliveries?tenant_id=tenant-a&status=DELIVERED"
```

自走投递 / 暂停恢复 / 死信重放（服务启动后投递自己跑，下面只做运维操作）：

```bash
SID=sub-a-orders
# 暂停一个订阅：期间入站事件照常 202 受理、排队但不发出
curl -X POST localhost:8000/admin/tenants/tenant-a/subscriptions/$SID/pause
curl -X POST localhost:8000/admin/tenants/tenant-a/subscriptions/$SID/pause   # 重复稳定
# ...暂停期继续投递若干事件...
curl -X POST localhost:8000/admin/tenants/tenant-a/subscriptions/$SID/resume  # 按原顺序补发

# 单条死信重放：202 REPLAYED=生效；200 ALREADY_IN_FLIGHT=重复触发；409=已成功不可再发
curl -X POST localhost:8000/admin/tenants/tenant-a/subscriptions/$SID/dead-letter/evt-http-1/replay
# 批量重放该订阅全部未处理好的失败事件（严格按 seq 顺序逐条处理）
curl -X POST localhost:8000/admin/tenants/tenant-a/subscriptions/$SID/dead-letter/replay
# 观察重放代次与在途标记
curl "localhost:8000/admin/deliveries?tenant_id=tenant-a&subscription_id=$SID"
```

## 关键规则速查

- 首次受理 **202**；重复（含并发落败者）**200 + `duplicate=true`**；
  认证失败 401；配额超限 429；契约/版本问题 422。
- 退避：`min(1s, 0.05·2^(n-1))`，无抖动；可重试故障最多 `max_attempts` 次；
  死信原因 `ATTEMPTS_EXHAUSTED`（细节含 `last_basis`）或 `HTTP_4XX`。
- 出站凭据失窗（缺失/未生效/已过期）：**请求不发出**，直接死信
  `CREDENTIAL_UNAVAILABLE`，不占用尝试次数，与接收方失败严格区分。
- 服务 startup 自动起后台投递循环（shutdown 停止）；正常运行无需 pump。
- 暂停只影响目标订阅的投递（入站/其他订阅不受影响）；恢复后按原 seq 补发；
  暂停/恢复可重复调用，结论稳定。
- 重放只追加队尾不插队；批量按 seq 升序串行；并发/连续重复仅一次 `REPLAYED`
  （202），其余 `ALREADY_IN_FLIGHT`（200）；已成功事件 `NOT_DEAD_LETTER`（409）。
- 版本：主版本必须已注册且与订阅一致；同主版本向前兼容，未知字段保留登记。
- 新增能力均为向后兼容增量（详见 PROTOCOL「兼容范围」）；内存存储面向本地
  运行与单实例，多实例水平扩展不在本轮范围。

详见 [`docs/PROTOCOL.md`](docs/PROTOCOL.md)。
