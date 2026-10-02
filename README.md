# 跨区域水资源使用权分配与转让

一个仅使用 Python 标准库实现的水权账户、计量、转让审批、旱期限额令和干旱情景服务。SQLite 保存账户额度、取水记录、季节规则、上下游影响规则、旱期限额令及其执行台账、计量事件台账、对账差异和完整审计日志。

## 运行

```bash
python app.py --init
python app.py --port 8007
```

打开 <http://127.0.0.1:8007>。`--init` 会创建北区水库和河口灌区两个示例账户，并添加一条 7 月季节上限和一条最小留存规则。数据库默认是 `water_rights.db`，可用 `--db` 或 `WATER_DB` 修改。

## 角色

请求头 `X-User` 和 `X-Role` 用来模拟身份：`editor`（配额管理员）、`reviewer`（审核人）、`meter`（计量员）、`dispatcher`（旱情调度员）、`viewer`。调度员**不能直接修改账户额度**，只能下达限额令。

## API

- `POST /api/accounts`：建立账户（额度、优先级、有效期）。
- `POST /api/rules/season`：设置某地区某月份的用水比例上限。
- `POST /api/rules/impact`：设置上下游转让的最小留存比例。
- `POST /api/drought/orders`：**下达旱期限额令**（`scope=region|account`、`limit_amount`、`effective_date`、可选 `end_date`/`note`）。
- `POST /api/drought/orders/{id}`：更新限额（收紧或放松）或 `status=revoked` 撤销；放松后已退回的转让不会自动复活，需重新发起。
- `POST /api/drought/recover`：手动触发未完成处理的恢复。
- `GET /api/drought/orders`：限额令及其执行状态。
- `POST /api/transfers`：发起转让；待审批金额立即预占，避免同一额度被重复转卖。
- `POST /api/transfers/{id}/approve|reject`：审核；发起人不能审批自己的记录。
- `POST /api/usage`：按计量事件登记实际取水，可带 `transfer_id` 关联已批准转让；同一事件编号不会重复入账。
- `GET /api/accounts/{id}/available?as_of=YYYY-MM-DD`：查看有效额度（含旱限）、实际用量、待审批预占和可用量。
- `GET /api/reconciliation`：对账差异（晚到计量与已批准转让不符的记录）。
- `GET /api/meter-events`：计量事件处理台账。
- `GET /api/drought/simulate?supply=1000&reduction=0.3`：按高优先级先行分配，同级账户按剩余额度比例分配（按旱限后的有效额度计算）。
- `GET /api/audit`：完整操作审计。

## 关键语义

### 限额令接入账户、取水与转让

- 账户**有效额度** = `min(许可额度, 生效中的限额令最小限额)`，按日期生效/失效，地区令覆盖该地区全部账户。可用量在读取和每一次写入校验时实时重算：`可用 = 有效额度 - 已登记取水 - 待审批预占`。
- 限额令下达或更新后，受影响账户中**超出限额的待审批转让自动退回**（状态 `returned`，记录退回原因，预占随即释放）；转让按发起先后（FIFO）保留，装不下的整笔退回。
- **已登记取水不改、已批准转让不撤回、许可额度不被改写**——限额只影响后续可用量和待审批流转。
- 限额收紧后，新发起的转让、取水以及转让审批都会按新的有效额度校验。

### 并发：同一份水只能被一方占用

余额判断与写入全部在单连接 `BEGIN IMMEDIATE` 事务内完成（SQLite 写锁在事务开始即获取），多人并发提交取水或转让时被数据库写锁串行化，任何一方超占都会被拒绝。

### 崩溃恢复：恢复且不重复扣减

- 限额令级联分两步持久化：先在 `drought_order_effects` 台账中确定要退回的转让（`pending/applying/applied` 状态机），再逐条用独立事务执行；退回语句带 `status='pending'` 条件，重复执行不会二次退回。
- 计量事件先进 `meter_events` 台账（事件号全局唯一），再入账并回写 `usage_record_id`；`applied` 事件重复执行直接返回既有结果，不会二次增加 `used`。因额度不足搁置的事件可用同一事件号重试。
- 服务启动时先执行 `recover()`，把所有未完成的限额令和 `received` 事件补齐后再对外服务；也可调用 `POST /api/drought/recover` 手动触发。

### 晚到计量与已批准转让对账

取水登记带 `transfer_id` 时：关联转让必须已批准（否则拒绝入账）。当实到水量与批准量不符、或计量账户不是转让当事方时，**取水按实物量照常入账，转让记录保持不变**，差异（期望/实到/差值/原因）写入 `reconciliation_entries` 并记审计，供事后对账；这种情况下不再做额度与季节上限拦截，以免真实来水无法落账。

## 测试

```bash
python -m unittest discover -s tests -v
```

测试覆盖：限额令可用量重算、超限待审批转让 FIFO 退回且已登记取水不变、放松限额不复活退回、取水/转让并发互斥（含取水与转让混合并发）、限额令与计量事件的崩溃恢复幂等（不重复退回/扣减）、晚到计量与已批准转让不符的双边留痕与对账差异，以及既有的重复计量、季节/留存规则、自审冲突等。
