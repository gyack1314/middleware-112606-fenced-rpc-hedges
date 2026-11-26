"""本地仿真工作台：重放 JSONL 演练、交互式喂消息、内置对冲走查。

工作台只负责搬运输入与展示代理输出，所有判定都在 :mod:`hedgeproxy.engine`。
运行方式见 README，例如::

    python3 -m hedgeproxy.workbench replay examples/first-reply-too-old.jsonl
    echo '{...}' | python3 -m hedgeproxy.workbench interactive
    python3 -m hedgeproxy.workbench demo --explain
"""

import argparse
import json
import sys
from pathlib import Path

from scenario_reader import iter_messages

from .engine import Proxy

MESSAGE_TYPES = [
    "cancelled", "capacity", "endpoint", "inspect", "read",
    "receipt", "result", "send", "tick",
]

# 事件类型 -> 值班台中文说明（--explain 时使用）
EXPLAIN = {
    "capacity_observed": "登记服务的真实在途上限与对冲配额",
    "endpoint_catalog": "目录声明副本代次、故障域、能力与已应用版本",
    "endpoint_offline": "副本移出目录",
    "generation_change": "同名副本换代，旧代次成功不继承",
    "credential_raised": "会话凭据抬高（读取证明或写入回执）",
    "credential_ignored": "回执不高于已持证版本，忽略",
    "floor_raised": "在途请求的有效下限随凭据抬高",
    "read_accepted": "受理只读请求，下限取请求 min 与会话凭据的较大者",
    "read_coalesced": "合并到尚未派发的同能力/键/下限/期限请求",
    "dispatch": "派发一次读取尝试（backup=true 即对冲额外尝试）",
    "attempt_sent": "运输确认尝试已实际派发，开始计入在途",
    "attempt_revoked": "对端确认前撤回，不占对端资源",
    "cancel_requested": "胜出或换代后请求取消，确认前仍占资源",
    "cancel_confirmed": "对端确认取消，资源释放",
    "cancel_unconfirmed": "取消未确认（unknown），资源继续计着",
    "capacity_released": "尝试已由对端终结，在途计数下降",
    "result_rejected": "回复不采用：版本不足/局部失读/换代/迟到",
    "read_result": "合格结果交付，附满足下限的完整性证明",
    "read_failed": "截止无合格结果，给出可重试原因，不用旧值兜底",
    "inspect_snapshot": "值班台快照（请求/配额/目录）",
    "trace_mismatch": "演练输入与代理决策不一致，已如实登记",
    "cancel_ignored": "取消回执找不到对应尝试",
    "result_ignored": "结果找不到对应尝试",
}


def replay_file(path):
    """把一个 JSONL 文件按到达顺序喂给新代理，返回跑完的代理。"""
    proxy = Proxy()
    for message in iter_messages(Path(path), set(MESSAGE_TYPES)):
        proxy.push(message)
    proxy.drain()
    return proxy


def _explain_line(event):
    record = event["record"]
    kind = event["kind"]
    head = EXPLAIN.get(kind, kind)
    bits = [f"t={event['at']:>3} [{kind}] {head}"]
    for key in ("request", "id", "service", "replica", "as", "reason",
                "floor", "version", "state"):
        if key in record:
            bits.append(f"{key}={record[key]}")
    return "　".join(bits)


def render(proxy, explain=False, stream=sys.stdout):
    for event in proxy.events:
        if explain:
            print(_explain_line(event), file=stream)
        else:
            print(json.dumps(event, ensure_ascii=False), file=stream)
    mismatches = len(proxy.trace_notes)
    summary = {
        "events": len(proxy.events),
        "trace_mismatches": mismatches,
        "requests": {rid: req.state for rid, req in proxy.requests.items()},
        "credentials": [
            {"client": c, "service": s, "key": k, "version": v}
            for (c, s, k), v in sorted(proxy.credentials.items())
        ],
        "services": {s: proxy._load(s) for s in proxy.limits},
    }
    banner = "# 演练小结" if explain else None
    if explain:
        print(banner, file=stream)
        for line in json.dumps(summary, ensure_ascii=False, indent=2).splitlines():
            print(line, file=stream)
    else:
        print(json.dumps({"kind": "summary", "record": summary},
                         ensure_ascii=False), file=stream)
    return mismatches


def run_replay(target, explain=False, stream=sys.stdout):
    target = Path(target)
    paths = sorted(target.glob("*.jsonl")) if target.is_dir() else [target]
    total_mismatches = 0
    for path in paths:
        if explain:
            print(f"===== {path.name} =====", file=stream)
        proxy = replay_file(path)
        total_mismatches += render(proxy, explain=explain, stream=stream)
    return total_mismatches


def run_interactive(explain=False, stream=sys.stdout):
    """从标准输入逐行读 JSON 消息，反复提交新的读取与副本进度。"""
    proxy = Proxy()
    for line_number, raw in enumerate(sys.stdin, 1):
        raw = raw.strip()
        if not raw or raw.startswith("#"):
            continue
        try:
            message = json.loads(raw)
            proxy.push(message)
        except (ValueError, KeyError) as error:
            print(json.dumps({"at": proxy.now, "kind": "input_error",
                              "record": {"line": line_number, "error": str(error)}},
                             ensure_ascii=False), file=stream)
            continue
        # 交互式下每条输入后即时刷出新增事件
        while proxy.events:
            event = proxy.events.pop(0)
            if explain:
                print(_explain_line(event), file=stream)
            else:
                print(json.dumps(event, ensure_ascii=False), file=stream)
    proxy.drain()
    while proxy.events:
        event = proxy.events.pop(0)
        if explain:
            print(_explain_line(event), file=stream)
        else:
            print(json.dumps(event, ensure_ascii=False), file=stream)
    return len(proxy.trace_notes)


def demo_messages():
    """内置走查：慢副本拖时间 -> 跨故障域对冲 -> 首回复过旧不采用 -> 合格胜出。"""
    rows = [
        (0, "capacity", {"service": "report", "max_inflight": 2,
                         "hedge_budget": 1}),
        (0, "endpoint", {"service": "report", "replica": "safe", "epoch": 4,
                         "domain": "a", "applied": 10}),
        (0, "endpoint", {"service": "report", "replica": "fast", "epoch": 1,
                         "domain": "z", "applied": 9}),
        (1, "receipt", {"client": "screen", "service": "report",
                        "key": "round", "min": 9}),
        (2, "read", {"id": "r", "client": "screen", "service": "report",
                     "key": "round", "min": 7, "deadline": 18,
                     "hedge_after": 3}),
        (2, "send", {"id": "r", "as": "p", "replica": "safe", "epoch": 4,
                     "backup": False}),
        (6, "tick", {}),
        (6, "send", {"id": "r", "as": "h", "replica": "fast", "epoch": 1,
                     "backup": True}),
        (7, "result", {"as": "h", "replica": "fast", "epoch": 1,
                       "status": "value", "version": 8, "value": "stale"}),
        (9, "result", {"as": "p", "replica": "safe", "epoch": 4,
                       "status": "value", "version": 9, "value": "ok"}),
        (10, "inspect", {"id": "r"}),
        (11, "inspect", {"service": "report"}),
    ]
    return [{"at": at, "kind": kind, "record": record}
            for at, kind, record in rows]


def run_demo(explain=True, stream=sys.stdout):
    proxy = Proxy()
    for message in demo_messages():
        proxy.push(message)
    proxy.drain()
    return render(proxy, explain=explain, stream=stream)


def main(argv=None):
    parser = argparse.ArgumentParser(description="条件对冲只读代理工作台")
    sub = parser.add_subparsers(dest="command", required=True)

    p_replay = sub.add_parser("replay", help="重放一个 JSONL 文件或整个目录")
    p_replay.add_argument("target", type=Path)
    p_replay.add_argument("--explain", action="store_true",
                          help="输出中文值班说明而不是 JSON 事件流")

    p_inter = sub.add_parser("interactive", help="从 stdin 逐行读 JSON 消息")
    p_inter.add_argument("--explain", action="store_true")

    p_demo = sub.add_parser("demo", help="运行内置对冲走查")
    p_demo.add_argument("--explain", action="store_true")

    args = parser.parse_args(argv)
    if args.command == "replay":
        mismatches = run_replay(args.target, explain=args.explain)
    elif args.command == "interactive":
        mismatches = run_interactive(explain=args.explain)
    elif args.command == "demo":
        mismatches = run_demo(explain=True)
    else:  # pragma: no cover - argparse 已拦截
        raise SystemExit(2)
    raise SystemExit(1 if mismatches else 0)


if __name__ == "__main__":
    main()
