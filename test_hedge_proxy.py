"""条件对冲代理的保证级测试。

每个用例直接用消息序列驱动 Proxy，断言的都是业务保证，而不是实现细节：
下限凭据、合格性证明、对冲选择、配额口径、换代不继承、截止可重试等。
"""

import unittest

from hedgeproxy.engine import Proxy
from hedgeproxy.model import (
    CANCEL_REQUESTED,
    CANCEL_UNKNOWN,
    CANCELLED_OK,
    REJECTED,
    REQUESTED,
    SENT,
)


def drive(proxy, rows):
    """rows: (at, kind, record) 元组序列。"""
    for at, kind, record in rows:
        proxy.push({"at": at, "kind": kind, "record": record})
    proxy.drain()
    return proxy


def kinds(proxy):
    return [e["kind"] for e in proxy.events]


def find_events(proxy, kind, **fields):
    # 测试里用 as_ 避开关键字，记录字段实际叫 as。
    if "as_" in fields:
        fields["as"] = fields.pop("as_")
    out = []
    for event in proxy.events:
        if event["kind"] != kind:
            continue
        record = event["record"]
        if all(record.get(k) == v for k, v in fields.items()):
            out.append(event)
    return out


def base_catalog(extra=None):
    rows = [
        (0, "capacity", {"service": "svc", "max_inflight": 3,
                         "hedge_budget": 1}),
        (0, "endpoint", {"service": "svc", "replica": "r1", "epoch": 1,
                         "domain": "a", "applied": 10}),
        (0, "endpoint", {"service": "svc", "replica": "r2", "epoch": 1,
                         "domain": "b", "applied": 10}),
    ]
    if extra:
        rows.extend(extra)
    return rows


class CredentialAndFloorTests(unittest.TestCase):
    def test_read_floor_uses_receipt_credential(self):
        proxy = drive(Proxy(), base_catalog() + [
            (1, "receipt", {"client": "c", "service": "svc",
                            "key": "k", "min": 9}),
            (2, "read", {"id": "r", "client": "c", "service": "svc",
                         "key": "k", "min": 7, "deadline": 20,
                         "hedge_after": 3}),
        ])
        req = proxy.requests["r"]
        self.assertEqual(req.floor, 9)
        accepted = find_events(proxy, "read_accepted", id="r")[0]["record"]
        self.assertEqual(accepted["min"], 7)
        self.assertEqual(accepted["floor"], 9)

    def test_successful_read_raises_credential_for_next_read(self):
        proxy = drive(Proxy(), base_catalog() + [
            (1, "read", {"id": "r1", "client": "c", "service": "svc",
                         "key": "k", "min": 5, "deadline": 20,
                         "hedge_after": 99}),
            (1, "send", {"id": "r1", "as": "p", "replica": "r1",
                         "epoch": 1, "backup": False}),
            (2, "result", {"as": "p", "replica": "r1", "epoch": 1,
                           "status": "value", "version": 8, "value": "x"}),
            (3, "read", {"id": "r2", "client": "c", "service": "svc",
                         "key": "k", "min": 0, "deadline": 20,
                         "hedge_after": 99}),
        ])
        self.assertEqual(proxy.credentials[("c", "svc", "k")], 8)
        self.assertEqual(proxy.requests["r2"].floor, 8)

    def test_lower_receipt_never_lowers_credential_or_floor(self):
        proxy = drive(Proxy(), base_catalog() + [
            (1, "receipt", {"client": "c", "service": "svc",
                            "key": "k", "min": 9}),
            (2, "receipt", {"client": "c", "service": "svc",
                            "key": "k", "min": 6}),
            (3, "read", {"id": "r", "client": "c", "service": "svc",
                         "key": "k", "min": 4, "deadline": 20,
                         "hedge_after": 3}),
        ])
        self.assertEqual(proxy.credentials[("c", "svc", "k")], 9)
        self.assertEqual(proxy.requests["r"].floor, 9)
        self.assertTrue(find_events(proxy, "credential_ignored"))

    def test_receipt_raises_open_request_floor_and_cancels_stale_attempt(self):
        proxy = drive(Proxy(), base_catalog() + [
            (1, "read", {"id": "r", "client": "c", "service": "svc",
                         "key": "k", "min": 5, "deadline": 30,
                         "hedge_after": 99}),
            (1, "send", {"id": "r", "as": "p", "replica": "r1",
                         "epoch": 1, "backup": False}),
            (2, "receipt", {"client": "c", "service": "svc",
                            "key": "k", "min": 12}),
        ])
        req = proxy.requests["r"]
        self.assertEqual(req.floor, 12)
        cancel = find_events(proxy, "cancel_requested", as_="p")
        self.assertEqual(len(cancel), 1)
        self.assertEqual(cancel[0]["record"]["reason"], "version_below_floor")
        # r1 只 applied=10，无法兑现 12；目录里没有可兑现端点时只能等。
        self.assertEqual(req.last_block["reason"], "floor_unpromisable")


class ResultQualificationTests(unittest.TestCase):
    def _open_read(self, proxy, rid="r", floor=9):
        drive(proxy, base_catalog() + [
            (1, "read", {"id": rid, "client": "c", "service": "svc",
                         "key": "k", "min": floor, "deadline": 30,
                         "hedge_after": 99}),
            (1, "send", {"id": rid, "as": "p", "replica": "r1",
                         "epoch": 1, "backup": False}),
        ])

    def test_value_below_floor_rejected_and_waits_on(self):
        proxy = Proxy()
        self._open_read(proxy)
        drive(proxy, [
            (3, "result", {"as": "p", "replica": "r1", "epoch": 1,
                           "status": "value", "version": 8, "value": "old"}),
        ])
        rejected = find_events(proxy, "result_rejected", as_="p")
        self.assertEqual(rejected[0]["record"]["reason"],
                         "version_below_floor")
        self.assertEqual(rejected[0]["record"]["action"], "continue_waiting")
        self.assertEqual(proxy.requests["r"].state, "open")
        self.assertNotIn(("c", "svc", "k"), proxy.credentials)

    def test_missing_requires_same_version_proof(self):
        proxy = Proxy()
        self._open_read(proxy)
        # 低版本 missing 不能用来推断键已删。
        drive(proxy, [
            (3, "result", {"as": "p", "replica": "r1", "epoch": 1,
                           "status": "missing", "version": 8}),
            (4, "tick", {}),
        ])
        self.assertEqual(
            find_events(proxy, "result_rejected", as_="p")[0]["record"]
            ["reason"], "version_below_floor")
        # 代理自行再调度的下一次尝试（仍须满足下限），用其真实身份回执。
        backup = next(a for a in proxy.requests["r"].attempts
                      if a.state == "requested")
        self.assertFalse(backup.backup)  # 终结首发的补派仍是首发身份
        drive(proxy, [
            (4, "send", {"id": "r", "as": "h", "replica": backup.replica,
                         "epoch": backup.epoch, "backup": backup.backup}),
            (5, "result", {"as": "h", "replica": backup.replica,
                           "epoch": backup.epoch,
                           "status": "missing", "version": 9}),
        ])
        won = find_events(proxy, "read_result", request="r")
        self.assertEqual(len(won), 1)
        self.assertEqual(won[0]["record"]["status"], "missing")
        self.assertTrue(won[0]["record"]["proof"]["complete"])

    def test_error_partial_read_rejected(self):
        proxy = Proxy()
        self._open_read(proxy)
        drive(proxy, [
            (3, "result", {"as": "p", "replica": "r1", "epoch": 1,
                           "status": "error", "version": 9,
                           "detail": "partial"}),
        ])
        rejected = find_events(proxy, "result_rejected", as_="p")
        self.assertEqual(rejected[0]["record"]["reason"], "partial_read")
        self.assertEqual(proxy.requests["r"].state, "open")

    def test_qualified_result_carries_version_and_cancels_others(self):
        proxy = drive(Proxy(), base_catalog() + [
            (1, "read", {"id": "r", "client": "c", "service": "svc",
                         "key": "k", "min": 9, "deadline": 30,
                         "hedge_after": 2}),
            (1, "send", {"id": "r", "as": "p", "replica": "r1",
                         "epoch": 1, "backup": False}),
            (4, "send", {"id": "r", "as": "h", "replica": "r2",
                         "epoch": 1, "backup": True}),
            (5, "result", {"as": "h", "replica": "r2", "epoch": 1,
                           "status": "value", "version": 9, "value": "ok"}),
        ])
        result = find_events(proxy, "read_result", request="r")[0]["record"]
        self.assertEqual(result["version"], 9)
        self.assertEqual(result["proof"],
                         {"observed_version": 9, "min_required": 9,
                          "complete": True})
        cancel = find_events(proxy, "cancel_requested", as_="p")
        self.assertEqual(len(cancel), 1)
        self.assertEqual(cancel[0]["record"]["reason"], "winner_known")

    def test_late_low_version_after_finish_is_dropped_and_not_credited(self):
        proxy = drive(Proxy(), base_catalog() + [
            (1, "read", {"id": "r", "client": "c", "service": "svc",
                         "key": "k", "min": 9, "deadline": 30,
                         "hedge_after": 2}),
            (1, "send", {"id": "r", "as": "p", "replica": "r1",
                         "epoch": 1, "backup": False}),
            (4, "send", {"id": "r", "as": "h", "replica": "r2",
                         "epoch": 1, "backup": True}),
            (5, "result", {"as": "h", "replica": "r2", "epoch": 1,
                           "status": "value", "version": 9, "value": "ok"}),
            (6, "result", {"as": "p", "replica": "r1", "epoch": 1,
                           "status": "value", "version": 6, "value": "old"}),
            (7, "read", {"id": "r2", "client": "c", "service": "svc",
                         "key": "k", "min": 0, "deadline": 30,
                         "hedge_after": 99}),
        ])
        late = find_events(proxy, "result_rejected", as_="p")
        self.assertEqual(late[0]["record"]["reason"], "late_after_finish")
        # 迟到的 v6 没有污染会话，续读下限仍是已证明的 9。
        self.assertEqual(proxy.credentials[("c", "svc", "k")], 9)
        self.assertEqual(proxy.requests["r2"].floor, 9)

    def test_result_after_cancel_requested_never_overturns_winner(self):
        proxy = drive(Proxy(), base_catalog() + [
            (1, "read", {"id": "r", "client": "c", "service": "svc",
                         "key": "k", "min": 9, "deadline": 30,
                         "hedge_after": 2}),
            (1, "send", {"id": "r", "as": "p", "replica": "r1",
                         "epoch": 1, "backup": False}),
            (4, "send", {"id": "r", "as": "h", "replica": "r2",
                         "epoch": 1, "backup": True}),
            (5, "result", {"as": "h", "replica": "r2", "epoch": 1,
                           "status": "value", "version": 9, "value": "ok"}),
            # p 已被请求取消；即使它随后回了合格版本，也不能改变已定胜负。
            (6, "result", {"as": "p", "replica": "r1", "epoch": 1,
                           "status": "value", "version": 10, "value": "late"}),
        ])
        late = find_events(proxy, "result_rejected", as_="p")
        self.assertEqual(late[0]["record"]["reason"], "late_after_finish")
        self.assertTrue(late[0]["record"]["detail"]["cancel_in_flight"])
        # 凭据只认胜出者 h 的 v9，迟到的 v10 不抬高也不覆盖。
        self.assertEqual(proxy.credentials[("c", "svc", "k")], 9)
        self.assertEqual(
            find_events(proxy, "read_result", request="r")[0]["record"]["as"],
            "h")


class GenerationTests(unittest.TestCase):
    def test_result_from_old_epoch_rejected_and_new_epoch_reproven(self):
        proxy = drive(Proxy(), base_catalog() + [
            (1, "read", {"id": "r", "client": "c", "service": "svc",
                         "key": "k", "min": 5, "deadline": 30,
                         "hedge_after": 99}),
            (1, "send", {"id": "r", "as": "p", "replica": "r1",
                         "epoch": 1, "backup": False}),
            (2, "endpoint", {"service": "svc", "replica": "r1", "epoch": 2,
                             "domain": "a", "applied": 5}),
            (3, "result", {"as": "p", "replica": "r1", "epoch": 1,
                           "status": "value", "version": 10, "value": "x"}),
        ])
        rejected = find_events(proxy, "result_rejected", as_="p")
        self.assertEqual(rejected[0]["record"]["reason"],
                         "endpoint_generation")
        # 新代次 applied=5，不能兑现 v10 的旧印象；只按自己的声明参与选择。
        self.assertEqual(proxy.requests["r"].state, "open")
        # 旧代次结果被拒后，补派只能选择目录里仍有效的代次（新 r1 或 r2），
        # 绝不能再把请求发回已撤换的 (r1, epoch=1)。
        post_change = [e["record"] for e in find_events(proxy, "dispatch")
                       if e["at"] >= 2]
        self.assertTrue(post_change)
        for rec in post_change:
            if rec["replica"] == "r1":
                self.assertEqual(rec["epoch"], 2)
            self.assertNotEqual((rec["replica"], rec["epoch"]),
                                ("r1", 1))
        last = post_change[-1]
        self.assertIn((last["replica"], last["epoch"]),
                      {("r1", 2), ("r2", 1)})

    def test_generation_change_cancels_sent_attempt_but_keeps_counting(self):
        proxy = drive(Proxy(), [
            (0, "capacity", {"service": "svc", "max_inflight": 3,
                             "hedge_budget": 1}),
            (0, "endpoint", {"service": "svc", "replica": "r1", "epoch": 1,
                             "domain": "a", "applied": 5}),
            (0, "endpoint", {"service": "svc", "replica": "r2", "epoch": 1,
                             "domain": "b", "applied": 5}),
            (1, "read", {"id": "r", "client": "c", "service": "svc",
                         "key": "k", "min": 5, "deadline": 30,
                         "hedge_after": 99}),
            (1, "send", {"id": "r", "as": "p", "replica": "r1",
                         "epoch": 1, "backup": False}),
            (2, "endpoint", {"service": "svc", "replica": "r1", "epoch": 2,
                             "domain": "a", "applied": 8}),
            # 换代后代理自行补派的尝试，用其真实身份确认。
        ])
        self.assertEqual(
            find_events(proxy, "cancel_requested", as_="p")[0]["record"]
            ["reason"], "endpoint_generation")
        self.assertEqual(proxy._load("svc")["inflight"], 1)
        self.assertEqual(proxy._load("svc")["cancel_pending"], 1)


class HedgeAndQuotaTests(unittest.TestCase):
    def test_hedge_prefers_other_domain_after_threshold(self):
        proxy = drive(Proxy(), base_catalog() + [
            (1, "read", {"id": "r", "client": "c", "service": "svc",
                         "key": "k", "min": 5, "deadline": 30,
                         "hedge_after": 3}),
            (1, "send", {"id": "r", "as": "p", "replica": "r1",
                         "epoch": 1, "backup": False}),
            (5, "tick", {}),
        ])
        hedges = [e["record"] for e in find_events(proxy, "dispatch")
                  if e["record"]["backup"]]
        self.assertEqual(len(hedges), 1)
        self.assertEqual(hedges[0]["replica"], "r2")  # 域 b != 首发域 a
        self.assertEqual(hedges[0]["domain"], "b")

    def test_no_hedge_before_threshold(self):
        proxy = drive(Proxy(), base_catalog() + [
            (1, "read", {"id": "r", "client": "c", "service": "svc",
                         "key": "k", "min": 5, "deadline": 30,
                         "hedge_after": 5}),
            (1, "send", {"id": "r", "as": "p", "replica": "r1",
                         "epoch": 1, "backup": False}),
            (3, "tick", {}),
        ])
        self.assertFalse(any(e["record"].get("backup")
                             for e in find_events(proxy, "dispatch")))

    def test_max_inflight_blocks_and_cancel_unknown_still_counts(self):
        rows = [
            (0, "capacity", {"service": "svc", "max_inflight": 1,
                             "hedge_budget": 1}),
            (0, "endpoint", {"service": "svc", "replica": "r1", "epoch": 1,
                             "domain": "a", "applied": 5}),
            (0, "endpoint", {"service": "svc", "replica": "r2", "epoch": 1,
                             "domain": "b", "applied": 5}),
            # 首发到 r1，对冲到 r2（此时占用唯一在途名额）。
            (1, "read", {"id": "r1", "client": "c", "service": "svc",
                         "key": "k", "min": 1, "deadline": 30,
                         "hedge_after": 2}),
            (1, "send", {"id": "r1", "as": "p", "replica": "r1",
                         "epoch": 1, "backup": False}),
            (4, "send", {"id": "r1", "as": "h", "replica": "r2",
                         "epoch": 1, "backup": True}),
            (5, "read", {"id": "r2", "client": "d", "service": "svc",
                         "key": "k2", "min": 1, "deadline": 30,
                         "hedge_after": 99}),
            # r2（备份）合格胜出；首发 r1 被要求取消。
            (6, "result", {"as": "h", "replica": "r2", "epoch": 1,
                           "status": "value", "version": 5, "value": "x"}),
            # 对端不确认取消：r1 的尝试继续占着唯一在途名额。
            (7, "cancelled", {"as": "p", "result": "unknown"}),
        ]
        proxy = drive(Proxy(), rows)
        self.assertEqual(proxy._load("svc")["inflight"], 1)
        self.assertEqual(proxy._load("svc")["cancel_pending"], 1)
        # 新请求一直没有派发机会：在途上限被未确认取消占住。
        self.assertEqual(proxy.requests["r2"].attempts, [])
        self.assertEqual(proxy.requests["r2"].last_block["reason"],
                         "max_inflight")
        # 确认取消后名额释放，排队请求立刻获得首发。
        drive(proxy, [(8, "cancelled", {"as": "p", "result": "ok"})])
        self.assertEqual(proxy._load("svc")["inflight"], 0)
        self.assertTrue(find_events(proxy, "dispatch", request="r2"))

    def test_hedge_budget_is_separate_from_inflight(self):
        rows = [
            (0, "capacity", {"service": "svc", "max_inflight": 3,
                             "hedge_budget": 1}),
            (0, "endpoint", {"service": "svc", "replica": "r1", "epoch": 1,
                             "domain": "a", "applied": 5}),
            (0, "endpoint", {"service": "svc", "replica": "r2", "epoch": 1,
                             "domain": "b", "applied": 5}),
            (0, "endpoint", {"service": "svc", "replica": "r3", "epoch": 1,
                             "domain": "c", "applied": 5}),
            (1, "read", {"id": "r", "client": "c", "service": "svc",
                         "key": "k", "min": 1, "deadline": 40,
                         "hedge_after": 2}),
            (1, "send", {"id": "r", "as": "p", "replica": "r1",
                         "epoch": 1, "backup": False}),
            (4, "tick", {}),
            (4, "send", {"id": "r", "as": "h", "replica": "r2",
                         "epoch": 1, "backup": True}),
            # 首发与唯一备份的端点先后换代，两个尝试都进入取消流程；
            # 代理需要再补备份时，专门的对冲配额已被 h 占满。
            (6, "endpoint", {"service": "svc", "replica": "r2", "epoch": 9,
                             "domain": "b", "applied": 5}),
            (7, "endpoint", {"service": "svc", "replica": "r1", "epoch": 7,
                             "domain": "a", "applied": 5}),
            (8, "tick", {}),
        ]
        proxy = drive(Proxy(), rows)
        backups = [e["record"] for e in find_events(proxy, "dispatch")
                   if e["record"]["backup"]]
        self.assertEqual(len(backups), 1)
        self.assertEqual(proxy._load("svc")["backups"], 1)
        self.assertEqual(proxy.requests["r"].last_block["reason"],
                         "hedge_budget")
        # 真实在途上限 3 远未用尽，挡住补发的是独立对冲配额。
        self.assertLess(proxy._load("svc")["inflight"], 3)

    def test_unpromisable_endpoint_never_receives_dispatch(self):
        rows = [
            (0, "capacity", {"service": "svc", "max_inflight": 3,
                             "hedge_budget": 1}),
            (0, "endpoint", {"service": "svc", "replica": "lag", "epoch": 1,
                             "domain": "a", "applied": 3}),
            (1, "read", {"id": "r", "client": "c", "service": "svc",
                         "key": "k", "min": 8, "deadline": 30,
                         "hedge_after": 2}),
        ]
        proxy = drive(Proxy(), rows)
        self.assertFalse(find_events(proxy, "dispatch"))
        self.assertEqual(proxy.requests["r"].last_block["reason"],
                         "floor_unpromisable")
        # 副本追赶到下限后才允许派发。
        drive(proxy, [
            (5, "endpoint", {"service": "svc", "replica": "lag", "epoch": 1,
                             "domain": "a", "applied": 8}),
        ])
        self.assertTrue(find_events(proxy, "dispatch", request="r"))


class DeadlineTests(unittest.TestCase):
    def test_deadline_returns_retryable_failure_without_stale_fallback(self):
        proxy = drive(Proxy(), base_catalog() + [
            (1, "read", {"id": "r", "client": "c", "service": "svc",
                         "key": "k", "min": 9, "deadline": 10,
                         "hedge_after": 2}),
            (1, "send", {"id": "r", "as": "p", "replica": "r1",
                         "epoch": 1, "backup": False}),
            (4, "send", {"id": "r", "as": "h", "replica": "r2",
                         "epoch": 1, "backup": True}),
            (5, "result", {"as": "p", "replica": "r1", "epoch": 1,
                           "status": "value", "version": 8, "value": "old"}),
            (11, "tick", {}),
        ])
        failed = find_events(proxy, "read_failed", id="r")
        self.assertEqual(len(failed), 1)
        detail = failed[0]["record"]["detail"]
        self.assertTrue(detail["retryable"])
        self.assertEqual(detail["floor"], 9)
        self.assertEqual(proxy.requests["r"].state, "failed")
        # 旧值没有变成结果，也没有写进凭据。
        self.assertNotIn(("c", "svc", "k"), proxy.credentials)
        # 仍在途的对冲尝试被取消，等待对端确认。
        self.assertTrue(find_events(proxy, "cancel_requested", as_="h"))

    def test_result_arriving_at_deadline_second_still_wins(self):
        proxy = drive(Proxy(), base_catalog() + [
            (1, "read", {"id": "r", "client": "c", "service": "svc",
                         "key": "k", "min": 9, "deadline": 10,
                         "hedge_after": 99}),
            (1, "send", {"id": "r", "as": "p", "replica": "r1",
                         "epoch": 1, "backup": False}),
            (10, "result", {"as": "p", "replica": "r1", "epoch": 1,
                            "status": "value", "version": 9, "value": "x"}),
            (10, "tick", {}),
        ])
        self.assertEqual(proxy.requests["r"].state, "won")
        self.assertFalse(find_events(proxy, "read_failed"))


class CoalescingTests(unittest.TestCase):
    def test_same_floor_requests_coalesce_before_dispatch(self):
        proxy = drive(Proxy(), base_catalog() + [
            (1, "receipt", {"client": "c", "service": "svc",
                            "key": "k", "min": 7}),
            (2, "read", {"id": "r1", "client": "c", "service": "svc",
                         "key": "k", "min": 7, "deadline": 20,
                         "hedge_after": 99}),
            (2, "read", {"id": "r2", "client": "c", "service": "svc",
                         "key": "k", "min": 7, "deadline": 20,
                         "hedge_after": 99}),
            (2, "send", {"id": "r1", "as": "p", "replica": "r1",
                         "epoch": 1, "backup": False}),
            (3, "result", {"as": "p", "replica": "r1", "epoch": 1,
                           "status": "value", "version": 7, "value": "x"}),
        ])
        self.assertEqual(proxy.requests["r2"].state, "won")
        self.assertTrue(find_events(proxy, "read_coalesced", request="r2"))
        # 两个调用方各收到一份带同样版本证明的结果。
        winners = {e["record"]["request"]
                   for e in find_events(proxy, "read_result")
                   if e["record"]["status"] == "value"}
        self.assertEqual(winners, {"r1", "r2"})
        # 合并者只共享一次尝试，不会自己再派发。
        self.assertFalse(find_events(proxy, "dispatch", request="r2"))

    def test_higher_credential_request_never_merges_into_started_low_read(self):
        proxy = drive(Proxy(), base_catalog() + [
            (1, "read", {"id": "r1", "client": "c", "service": "svc",
                         "key": "k", "min": 5, "deadline": 30,
                         "hedge_after": 99}),
            (1, "send", {"id": "r1", "as": "p", "replica": "r1",
                         "epoch": 1, "backup": False}),
            (2, "receipt", {"client": "c", "service": "svc",
                            "key": "k", "min": 8}),
            (3, "read", {"id": "r2", "client": "c", "service": "svc",
                         "key": "k", "min": 5, "deadline": 30,
                         "hedge_after": 99}),
        ])
        # r1 已派发且下限抬到 8；r2 的有效下限也是 8，不能并回旧读取。
        self.assertFalse(find_events(proxy, "read_coalesced"))
        self.assertEqual(proxy.requests["r2"].state, "open")
        self.assertEqual(proxy.requests["r2"].floor, 8)

    def test_different_deadline_never_coalesces(self):
        proxy = drive(Proxy(), base_catalog() + [
            (1, "read", {"id": "r1", "client": "c", "service": "svc",
                         "key": "k", "min": 5, "deadline": 20,
                         "hedge_after": 99}),
            (1, "read", {"id": "r2", "client": "c", "service": "svc",
                         "key": "k", "min": 5, "deadline": 25,
                         "hedge_after": 99}),
        ])
        self.assertFalse(find_events(proxy, "read_coalesced"))
        leaders = [e["record"]["request"]
                   for e in find_events(proxy, "dispatch")]
        self.assertEqual(sorted(leaders), ["r1", "r2"])


class InspectAndCatalogTests(unittest.TestCase):
    def test_inspect_distinguishes_block_reasons_and_budget_usage(self):
        proxy = drive(Proxy(), [
            (0, "capacity", {"service": "svc", "max_inflight": 1,
                             "hedge_budget": 1}),
            (0, "endpoint", {"service": "svc", "replica": "r1", "epoch": 1,
                             "domain": "a", "applied": 5}),
            (1, "read", {"id": "r1", "client": "c", "service": "svc",
                         "key": "k", "min": 9, "deadline": 30,
                         "hedge_after": 2}),
            (2, "inspect", {"id": "r1"}),
        ])
        snapshot = find_events(proxy, "inspect_snapshot")[-1]["record"]["view"]
        self.assertEqual(snapshot["last_block"]["reason"],
                         "floor_unpromisable")
        drive(proxy, [
            (3, "endpoint", {"service": "svc", "replica": "r1", "epoch": 1,
                             "domain": "a", "applied": 9}),
            (3, "send", {"id": "r1", "as": "p", "replica": "r1",
                         "epoch": 1, "backup": False}),
            (4, "read", {"id": "r2", "client": "d", "service": "svc",
                         "key": "k2", "min": 1, "deadline": 30,
                         "hedge_after": 1}),
            (5, "inspect", {"service": "svc"}),
        ])
        service_view = find_events(
            proxy, "inspect_snapshot")[-1]["record"]["view"]
        self.assertEqual(service_view["load"]["inflight"], 1)
        self.assertEqual(service_view["budget"]["inflight_remaining"], 0)
        self.assertEqual(service_view["budget"]["hedge_remaining"], 1)

    def test_offline_endpoint_revokes_undispatched_attempt(self):
        proxy = drive(Proxy(), base_catalog() + [
            # 读取与移出目录在同一秒点：批次尚未封口，r1 从未获得上线确认。
            (1, "read", {"id": "r", "client": "c", "service": "svc",
                         "key": "k", "min": 5, "deadline": 30,
                         "hedge_after": 99}),
            (1, "endpoint", {"service": "svc", "replica": "r1", "epoch": 1,
                             "domain": "a", "applied": 10, "offline": True}),
        ])
        dispatches = find_events(proxy, "dispatch", request="r")
        # 已下线的 r1 一次都不能被派发，直接由仍在目录的 r2 首发补派。
        self.assertTrue(all(d["record"]["replica"] != "r1"
                            for d in dispatches))
        self.assertEqual(dispatches[-1]["record"]["replica"], "r2")
        self.assertFalse(dispatches[-1]["record"]["backup"])

    def test_unknown_service_fails_fast_retryably(self):
        proxy = drive(Proxy(), [
            (1, "read", {"id": "r", "client": "c", "service": "ghost",
                         "key": "k", "min": 1, "deadline": 10,
                         "hedge_after": 2}),
        ])
        failed = find_events(proxy, "read_failed", id="r")[0]["record"]
        self.assertEqual(failed["reason"], "service_unknown")
        self.assertEqual(proxy.requests["r"].state, "failed")

    def test_capability_gates_endpoint_choice(self):
        proxy = drive(Proxy(), [
            (0, "capacity", {"service": "svc", "max_inflight": 2,
                             "hedge_budget": 1}),
            (0, "endpoint", {"service": "svc", "replica": "plain",
                             "epoch": 1, "domain": "a", "applied": 5,
                             "capabilities": ["read"]}),
            (0, "endpoint", {"service": "svc", "replica": "strong",
                             "epoch": 1, "domain": "b", "applied": 5,
                             "capabilities": ["read", "read_consistent"]}),
            (1, "read", {"id": "r", "client": "c", "service": "svc",
                         "key": "k", "min": 5, "deadline": 30,
                         "hedge_after": 99,
                         "capability": "read_consistent"}),
        ])
        dispatch = find_events(proxy, "dispatch", request="r")
        self.assertEqual(dispatch[0]["record"]["replica"], "strong")


if __name__ == "__main__":
    unittest.main()
