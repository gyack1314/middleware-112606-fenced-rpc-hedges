# 只读 RPC 的条件对冲与会话读下限

key 是读取对象，version 为单调业务修订。epoch 标识副本进程代次。可合并请求须共享相同读取能力、键、下限和有效期限语义。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **capacity**　service 的 max_inflight 和 hedge_budget，上限以实际未结清尝试计。
- **endpoint**　service/replica/epoch/domain/applied，用 offline 标记移出。
- **read**　id/client/service/key/min/deadline/hedge_after。
- **receipt**　写入权威给 client/service/key 证明 min。
- **send**　运输端观察 id 已派发的尝试，含 replica/epoch，as 为尝试身份，backup 标明对冲。
- **result**　as/replica/epoch 判定本次所见 version、value；status 为 value、missing 或 error。
- **cancelled**　对 as 返回 ok 或 unknown。
- **tick**　代理检查到期与候选。
- **inspect**　查看会话、请求、配额。

## 示例

- first-reply-too-old.jsonl
- known-winner-still-uses-capacity.jsonl

## 条件对冲代理（hedgeproxy 包）

`hedgeproxy/engine.py` 实现事件驱动的只读调用代理，把上面的输入消息逐条喂给
`Proxy.push`，输入结束后调用一次 `drain` 封口最后一个秒点。代理保证：

- **会话读下限**：按 `(client, service, key)` 记住已证明的最高版本；新读取的
  有效下限取请求 `min` 与凭据的较大者。写入回执只抬高、永不调低下限。
- **合格性证明**：`value` 与业务上的 `missing` 一视同仁，都要求单次回复所见
  `version >= 下限` 且状态完整；低版本、`error` 局部失读、端点换代的回复一律
  不采用，给出原因后继续等，绝不用抢到的旧值兜底。
- **同秒批次与合并**：同一秒点的读取先挂起，使后到的同能力/键/下限/期限请求
  能并到尚未派发的领导者；携带更高凭据的请求因下限不同不会被并入旧读取。
- **条件对冲**：超过 `hedge_after` 才允许额外尝试，优先选择与先前尝试不同故障
  域、且目录代次与 `applied` 都能兑现下限的端点；同一副本代次只尝试一次。
- **两种配额**：真实在途 `max_inflight` 只统计已上线（含取消未确认）的尝试，
  待运输确认的派发不计；额外尝试另占专门的 `hedge_budget`。首个合格结果胜出
  后取消其余调用，对端确认取消前资源继续计着。
- **换代不继承**：同名副本更换 epoch 后整体替换，旧代次上的成功印象不继承，
  新代次只凭自己的 `applied` 与能力参与选择；迟到低版本不污染会话凭据。
- **截止可重试**：到截止仍无合格结果时返回 `read_failed`（`retryable=true`），
  并附所有已观察回复与被拒原因，值班台可区分版本/故障域不足、截止与配额占用。

### 本地工作台

所有命令都以隔离模式运行（`-I` 屏蔽环境变量与用户 site 包）：

    # 重放演练目录或单个文件，输出代理事件 JSONL
    python3 -I workbench.py replay examples
    python3 -I workbench.py replay examples/first-reply-too-old.jsonl --explain

    # 内置走查：慢副本首发 -> 跨故障域对冲 -> 首回复过旧被拒 -> 合格胜出
    python3 -I workbench.py demo --explain

    # 交互式：从 stdin 逐行喂 JSON 消息，可反复提交新读取与副本进度
    cat my-scenario.jsonl | python3 -I workbench.py interactive

`--explain` 输出中文值班说明；默认输出与输入同构的事件流（`at/kind/record`）。
若演练中的运输回执与代理自己的派发决策不一致，会产生 `trace_mismatch` 事件并
计入结尾小结的 `trace_mismatches`，而不是静默假装一致。

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式、
读取接口与全部代理保证（共 28 个代理用例加 3 个资料读取用例）。

    python3 -I scenario_reader.py examples
    python3 -I run_checks.py

资料读取器会按文件统计各类参与方消息的数量。演练记录只描述各参与方交给系统的输入。
