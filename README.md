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
- **契约兼容演进**：语义化版本；未知主版本 422 拒绝、同主版本未知字段保留并
  登记（不丢弃、不报错）；必填字段缺失/类型不符给出确定结论。
- **可观测**：关键判定日志均打印 `event_id` 与判定依据（basis/reason）；
  投递记录含逐次尝试的状态码、分类与退避秒数。

## 目录结构

```
app/
  models.py       领域模型与状态/原因枚举
  logging_utils.py 统一日志（事件标识 + 判定依据）
  security.py     HMAC 签名/验签、凭据轮换时间窗
  contract.py     语义化版本与字段兼容性判定
  transport.py    HttpTransport（可选真实HTTP）与 MockTransport（可编排）
  storage.py      内存存储：事件锁/订阅锁、滑动窗口配额、顺序号
  dispatcher.py   FIFO 队列、退避重试、死信归档
  service.py      入站受理主流程（认证→幂等→配额→契约→扇出）
  api.py          HTTP 路由（入站/管理/失败链路/本地驱动）
  bootstrap.py    组装与演示种子数据
tests/            29 个测试（并发幂等/重试耗尽/凭据轮换/契约/顺序/E2E）
demo.py           一条命令跑通全部场景
docs/PROTOCOL.md  投递协议、幂等/重试规则、版本兼容范围
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
| `tests/test_api.py` | HTTP 全链路：签名入站→重复→故障编排→drain→死信查询 |

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
# ...投递事件后...
curl -X POST localhost:8000/admin/drain            # 等重试跑完
curl localhost:8000/admin/deliveries/dead-letter   # 查询死信链路
curl "localhost:8000/admin/deliveries?tenant_id=tenant-a&status=DELIVERED"
```

## 关键规则速查

- 首次受理 **202**；重复（含并发落败者）**200 + `duplicate=true`**；
  认证失败 401；配额超限 429；契约/版本问题 422。
- 退避：`min(1s, 0.05·2^(n-1))`，无抖动；可重试故障最多 `max_attempts` 次；
  死信原因 `ATTEMPTS_EXHAUSTED`（细节含 `last_basis`）或 `HTTP_4XX`。
- 版本：主版本必须已注册且与订阅一致；同主版本向前兼容，未知字段保留登记。

详见 [`docs/PROTOCOL.md`](docs/PROTOCOL.md)。
