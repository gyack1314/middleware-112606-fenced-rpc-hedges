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

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会按文件统计各类参与方消息的数量。演练记录只描述各参与方交给系统的输入。
