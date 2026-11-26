"""条件对冲代理的事件驱动核心。

设计要点（对应演练契约）：

- 会话凭据：同一 ``(client, service, key)`` 记住已证明的最高版本；新读取的
  有效下限取请求自带 min 与凭据的较大者。拿到写入回执只会抬高下限，绝不会
  把版本往回调。
- 可合并请求必须共享相同读取能力、键、有效下限与有效期限语义，只能并到
  尚未派发的同下限请求；携带更高凭据的请求不会被并进已经开始的低下限读取。
- 单次回复必须证明自己读的是满足下限的完整版本：``value`` 与业务上的
  ``missing`` 一视同仁，都要求所见 version >= 下限；低版本、局部失读（error）、
  端点换代的回复一律不采用，并给出原因后继续等。
- 每个服务有真实在途上限 ``max_inflight``（以实际未结清尝试计，取消未确认
  也算在途）与专门的额外尝试配额 ``hedge_budget``。首个合格结果回来即胜出，
  其余在途尝试发起取消；对端确认前资源继续计着。
- 对冲优先选择与先前尝试不同故障域、且目录代次与 applied 都能兑现下限的端点。
- 截止仍无合格结果时返回可重试原因，绝不用抢到的旧值兜底。
- 副本撤换后同名新副本的可读下限必须重新证实，旧成功不继承。
"""

from itertools import count

from .model import (
    ATTACHED,
    CANCELLED_OK,
    CANCEL_REQUESTED,
    CANCEL_UNKNOWN,
    DEFAULT_CAPABILITY,
    FAILED,
    OPEN,
    QUALIFIED,
    REJECTED,
    REQUESTED,
    REVOKED,
    SENT,
    WON,
    LATE_REJECTED,
    TERMINAL_STATES,
    Attempt,
    Endpoint,
    Limit,
    ReadRequest,
)

# 回复不被采用的原因（值班台可据此区分）
R_VERSION = "version_below_floor"        # 版本或故障域（applied）不足
R_PARTIAL = "partial_read"               # 局部失读，没有完整版本证据
R_GENERATION = "endpoint_generation"     # 端点已换代，旧代次回复不继承
R_LATE = "late_after_finish"             # 请求已结束后的迟到结果
R_UNKNOWN_TRY = "unknown_attempt"        # 不属于任何在途尝试


class Proxy:
    """按仿真时间推进的只读调用代理。"""

    def __init__(self):
        self.now = -1
        self.limits = {}                       # service -> Limit
        self.endpoints = {}                    # (service, replica) -> Endpoint
        self.credentials = {}                  # (client, service, key) -> version
        self.requests = {}                     # id -> ReadRequest
        self.attempts_by_token = {}            # token -> Attempt
        self.attempts_by_as = {}               # as_id -> Attempt
        self.events = []                       # 代理输出的全部事件
        self._token_seq = count(1)
        self._group_seq = count(1)
        self.trace_notes = []                 # 与输入演练不一致的说明
        self._read_arrivals = {}              # 秒点 -> 本秒到达的读取 id 列表

    # ------------------------------------------------------------------ 基础工具

    def _emit(self, kind, record):
        event = {"at": self.now, "kind": kind, "record": record}
        self.events.append(event)
        return event

    def _credential(self, client, service, key):
        return self.credentials.get((client, service, key), 0)

    def _load(self, service):
        """实际未结清尝试数：取消未确认仍计资源。"""
        total = 0
        backups = 0
        cancel_pending = 0
        for req in self.requests.values():
            for attempt in req.attempts:
                if attempt.live:
                    total += 1
                    if attempt.backup:
                        backups += 1
                    if attempt.state in (CANCEL_REQUESTED, CANCEL_UNKNOWN):
                        cancel_pending += 1
        return {
            "inflight": total,
            "backups": backups,
            "cancel_pending": cancel_pending,
        }

    def _can_promise(self, endpoint, floor, capability):
        """目录层面能否兑现下限：能力支持且已应用版本不低于下限。"""
        if capability not in endpoint.capabilities:
            return False
        return endpoint.applied >= floor

    # ------------------------------------------------------------------ 输入入口

    def push(self, message):
        """处理一条演练消息并推进到该消息的秒点。

        秒点批次语义：同一 ``at`` 内消息按行序到达。该秒点新受理的读取先
        挂起不派发，使同秒后到的同下限读取仍能合并；一旦该秒点出现非 read
        消息（例如运输 send 回执），批次立即封口并统一调度。时间推进到下一
        秒点时，未封口的批次也会在处理新消息前调度。
        """
        at = message["at"]
        kind = message["kind"]
        record = message["record"]
        if at > self.now:
            # 进入新秒点前，封口上一秒挂起的读取批次并做时间驱动的调度
            # （对冲阈值、端点追平、截止结算都在这一步生效）。
            self._flush_arrivals()
            self._advance(at)
            self._schedule()
        if kind == "read":
            self._on_read(record)
            return self.events
        handler = {
            "capacity": self._on_capacity,
            "endpoint": self._on_endpoint,
            "receipt": self._on_receipt,
            "send": self._on_send,
            "result": self._on_result,
            "cancelled": self._on_cancelled,
            "tick": self._on_tick,
            "inspect": self._on_inspect,
        }[kind]
        # 运输回执必须先关联到本秒刚决策的派发，因此先封口批次；
        # 目录/凭据类事件则先处理，使同秒变化能直接作用于挂起尝试，
        # 处理完再封口统一调度。
        if kind in ("send", "result", "cancelled"):
            self._flush_arrivals()
        handler(record)
        self._flush_arrivals()
        return self.events

    def _flush_arrivals(self):
        if self._read_arrivals.pop(self.now, None) is not None:
            self._schedule()

    def drain(self):
        """输入序列结束后封口最后一个秒点挂起的读取批次。"""
        self._flush_arrivals()
        return self.events

    def _on_tick(self, record):
        # tick 到达其秒点：先结算恰好在本秒到期的请求（同秒内行序仍有效，
        # 排在 tick 前面的 result 会先被处理），再做对冲与候选检查。
        self._sweep_deadlines(self.now)
        self._schedule()

    def _advance(self, until):
        # 严格越过截止秒点才结算：截止当秒的消息仍按行序先参与处理；
        # 截止时仍无合格结果，返回可重试原因，不拿旧值兜底。
        self._sweep_deadlines(until)
        self.now = until

    # ------------------------------------------------------------------ 容量与目录

    def _on_capacity(self, record):
        limit = Limit(
            service=record["service"],
            max_inflight=record["max_inflight"],
            hedge_budget=record["hedge_budget"],
        )
        self.limits[limit.service] = limit
        self._emit("capacity_observed", dict(record))

    def _on_endpoint(self, record):
        service = record["service"]
        replica = record["replica"]
        key = (service, replica)
        previous = self.endpoints.get(key)
        capabilities = record.get("capabilities") or [DEFAULT_CAPABILITY]
        endpoint = Endpoint(
            service=service,
            replica=replica,
            epoch=record["epoch"],
            domain=record["domain"],
            applied=record["applied"],
            capabilities=list(capabilities),
            order=previous.order + 1 if previous else 0,
        )
        if record.get("offline"):
            # 移出目录：旧代次在途尝试失去目录依据，须重新证实（这里等同撤换）。
            self.endpoints.pop(key, None)
            self._emit(
                "endpoint_offline",
                {"service": service, "replica": replica,
                 "epoch": record["epoch"]},
            )
            self._revoke_latest(endpoint, generation="offline")
            return
        if previous is not None and previous.epoch != endpoint.epoch:
            self._emit(
                "generation_change",
                {"service": service, "replica": replica,
                 "old_epoch": previous.epoch, "new_epoch": endpoint.epoch,
                 "applied": endpoint.applied},
            )
        self.endpoints[key] = endpoint
        self._emit(
            "endpoint_catalog",
            {"service": service, "replica": replica, "epoch": endpoint.epoch,
             "domain": endpoint.domain, "applied": endpoint.applied,
             "capabilities": endpoint.capabilities},
        )
        # 同名新副本：旧代次在途尝试不能继承旧成功，全部作废并取消。
        if previous is not None and previous.epoch != endpoint.epoch:
            self._revoke_latest(previous, generation="epoch_change",
                                new_epoch=endpoint.epoch)
        self._schedule()

    def _revoke_latest(self, old_endpoint, generation, new_epoch=None):
        """旧代次上的在途尝试：请求中的直接撤回，已派发的请求取消。

        已经处在取消流程中的尝试不重复发取消，只保留“换代不继承”的事实。
        """
        for req in self.requests.values():
            if req.service != old_endpoint.service or req.state != OPEN:
                continue
            for attempt in req.attempts:
                if attempt.replica != old_endpoint.replica:
                    continue
                if attempt.epoch != old_endpoint.epoch or not attempt.live:
                    continue
                if attempt.state == REQUESTED:
                    attempt.state = REVOKED
                    attempt.reject_reason = R_GENERATION
                    self._emit(
                        "attempt_revoked",
                        {"request": req.id, "token": attempt.token,
                         "replica": attempt.replica, "epoch": attempt.epoch,
                         "reason": R_GENERATION, "generation": generation},
                    )
                elif attempt.state == SENT:
                    self._cancel_attempt(
                        req, attempt, reason=R_GENERATION,
                        detail={"generation": generation, "new_epoch": new_epoch},
                    )
        self._schedule()

    # ------------------------------------------------------------------ 凭据

    def _on_receipt(self, record):
        key = (record["client"], record["service"], record["key"])
        old = self.credentials.get(key)
        version = record["min"]
        if old is None or version > old:
            self.credentials[key] = version
            self._emit(
                "credential_raised",
                {"client": record["client"], "service": record["service"],
                 "key": record["key"], "from": old or 0, "to": version},
            )
        else:
            self._emit(
                "credential_ignored",
                {"client": record["client"], "service": record["service"],
                 "key": record["key"], "held": old, "seen": version},
            )
        # 凭据抬高后，同会话未结束请求的下限同步抬高；
        # 已经发出的低下限尝试即使回来也不再够格。
        for req in self.requests.values():
            if req.state != OPEN:
                continue
            if (req.client, req.service, req.key) != key:
                continue
            if version > req.floor:
                req.floor_history.append(
                    {"at": self.now, "from": req.floor, "to": version,
                     "cause": "receipt"})
                req.floor = version
                self._emit(
                    "floor_raised",
                    {"request": req.id, "to": version, "cause": "receipt"},
                )
                self._cancel_unpromising(req, cause="floor_raised")
        self._schedule()

    # ------------------------------------------------------------------ 读取请求

    def _on_read(self, record):
        service = record["service"]
        if service not in self.limits:
            # 目录里没有该服务，立即给出可重试失败，不挂空请求。
            req = ReadRequest(
                id=record["id"], client=record["client"], service=service,
                key=record["key"], capability=record.get("capability", DEFAULT_CAPABILITY),
                submitted_min=record["min"], floor=record["min"],
                deadline=record["deadline"], hedge_after=record["hedge_after"],
                created_at=self.now, state=FAILED, fail_reason="service_unknown",
                fail_detail={"service": service},
            )
            self.requests[req.id] = req
            self._emit("read_failed", self._failure_view(req))
            return
        capability = record.get("capability", DEFAULT_CAPABILITY)
        floor = max(record["min"],
                    self._credential(record["client"], service, record["key"]))
        req = ReadRequest(
            id=record["id"], client=record["client"], service=service,
            key=record["key"], capability=capability,
            submitted_min=record["min"], floor=floor,
            deadline=record["deadline"], hedge_after=record["hedge_after"],
            created_at=self.now,
        )
        self.requests[req.id] = req
        # 本秒到达的读取先挂起：同秒可能还有可合并的兄弟读取，
        # 派发统一放到该秒点输入处理完之后（见 push）。
        self._read_arrivals.setdefault(self.now, []).append(req.id)
        self._emit(
            "read_accepted",
            {"id": req.id, "client": req.client, "service": service,
             "key": req.key, "min": req.submitted_min, "floor": req.floor,
             "deadline": req.deadline, "hedge_after": req.hedge_after,
             "capability": capability},
        )
        merged = self._try_attach(req)
        if merged is not None:
            req.state = ATTACHED
            req.group = merged.group
            merged.waiters.append(
                {"id": req.id, "client": req.client, "at": self.now})
            self._emit(
                "read_coalesced",
                {"request": req.id, "leader": merged.id, "group": merged.group,
                 "floor": req.floor, "deadline": req.deadline},
            )
            return
        req.group = f"g{next(self._group_seq)}"

    def _try_attach(self, req):
        """只合并到尚未派发、且能力/键/下限/期限完全一致的请求。"""
        for leader in self.requests.values():
            if leader is req or leader.state != OPEN or leader.attempts:
                continue
            if (leader.service, leader.key, leader.capability) != (
                req.service, req.key, req.capability):
                continue
            if (leader.floor, leader.deadline) != (req.floor, req.deadline):
                continue
            return leader
        return None

    # ------------------------------------------------------------------ 调度与选择

    def _candidate_endpoints(self, req):
        """按选择优先级返回候选：不同故障域优先，其次 applied 高、目录序新。

        已经尝试过的副本代次（无论在途、取消中还是已被证明读旧/失读）一律
        不再选择：同一代次在目录未变化前给出的证据不会凭空变好，重发只会
        抢占别的租户的正常使用机会。
        """
        active = [a for a in req.attempts if a.state not in TERMINAL_STATES]
        used_domains = {a.domain for a in active}
        tried_generations = {(a.replica, a.epoch) for a in req.attempts}
        candidates = []
        for endpoint in self.endpoints.values():
            if endpoint.service != req.service:
                continue
            if (endpoint.replica, endpoint.epoch) in tried_generations:
                continue
            candidates.append(endpoint)
        candidates.sort(
            key=lambda e: (
                0 if e.domain not in used_domains else 1,   # 优先别的故障域
                0 if self._can_promise(e, req.floor, req.capability) else 1,
                -e.applied,
                e.order,
                e.replica,
            )
        )
        return candidates

    def _choose_endpoint(self, req, backup):
        """选一个可派发端点；不可派发时返回 (None, 原因)。"""
        candidates = self._candidate_endpoints(req)
        if not candidates:
            return None, {"reason": "no_endpoint",
                          "detail": {"service": req.service}}
        capable = [e for e in candidates
                   if self._can_promise(e, req.floor, req.capability)]
        active = [a for a in req.attempts if a.state not in TERMINAL_STATES]
        different_domain = [e for e in capable
                            if not any(a.domain == e.domain
                                       for a in active)]
        if backup and req.attempts and different_domain:
            pool = different_domain
            preference = "different_domain"
        elif capable:
            pool = capable
            preference = "different_domain" if (
                backup and req.attempts) else "catalog_order"
        else:
            # 没有能兑现下限的端点：绝不下发可能读旧的尝试，记录不足原因。
            best = candidates[0]
            return None, {
                "reason": "floor_unpromisable",
                "detail": {
                    "floor": req.floor,
                    "best_replica": best.replica,
                    "best_applied": best.applied,
                    "capability_ok": req.capability in best.capabilities,
                },
            }
        return pool[0], {"reason": None, "detail": {"preference": preference}}

    # 健康在途：已上线或待运输确认，尚未进入取消/终结流程。
    HEALTHY_STATES = frozenset({REQUESTED, SENT})

    def _hedge_due(self, req):
        if req.state != OPEN or not req.attempts:
            return False
        healthy = [a for a in req.attempts
                   if a.state in self.HEALTHY_STATES]
        # 已有健康备份在途：同一请求最多一个未终结备份，避免反复抢占。
        if any(a.backup for a in healthy):
            return False
        # 没有健康首发时由首发补派处理，不走对冲配额。
        if not any(not a.backup for a in healthy):
            return False
        elapsed = self.now - req.created_at
        if elapsed >= req.hedge_after:
            return True
        # 健康首发的端点已无法兑现下限（applied 落后或换代）时也允许补发。
        for attempt in healthy:
            if attempt.backup:
                continue
            endpoint = self.endpoints.get((attempt.service, attempt.replica))
            if endpoint is None or endpoint.epoch != attempt.epoch:
                return True
            if not self._can_promise(endpoint, req.floor, req.capability):
                return True
        return False

    def _schedule(self):
        """每轮调度：先满足首发，再考虑对冲，全程受真实在途与对冲配额约束。

        按服务逐轮推进；某服务在途已满时直接记录阻塞，不再让排队请求
        反复进入选择逻辑而覆盖真正的阻塞原因。一个请求的首发尝试全部终结
        后，补派仍是新的首发（backup=False），不消耗专门的对冲配额。
        """
        progress = True
        while progress:
            progress = False
            open_reqs = [r for r in self.requests.values() if r.state == OPEN]
            open_reqs.sort(key=lambda r: (r.created_at, r.id))
            services = {r.service for r in open_reqs}
            for service in sorted(services):
                limit = self.limits[service]
                reqs = [r for r in open_reqs if r.service == service]
                stop_service = False
                # 1) 首发与首发补派：不存在健康首发（REQUESTED/SENT）时补派，
                #    补派仍是首发身份（backup=False），不消耗专门对冲配额。
                for req in reqs:
                    healthy_primary = any(
                        not a.backup and a.state in self.HEALTHY_STATES
                        for a in req.attempts)
                    if healthy_primary:
                        continue
                    load = self._load(service)
                    if load["inflight"] >= limit.max_inflight:
                        req.last_block = {
                            "at": self.now, "reason": "max_inflight",
                            "inflight": load["inflight"],
                            "max_inflight": limit.max_inflight}
                        continue
                    if self._dispatch(req, backup=False):
                        # REQUESTED 不计真实在途，等 send 确认后才计入。
                        progress = True
                    elif req.last_block and req.last_block["at"] == self.now \
                            and req.last_block["reason"] not in (
                                "max_inflight", "hedge_budget"):
                        # 端点层面不可兑现（无端点/下限无法承诺）：原因已记录，
                        # 本轮不再重试，等目录或 applied 变化后由事件重新调度。
                        stop_service = True
                        break
                if stop_service:
                    continue
                # 2) 满足对冲条件的请求（额外尝试占专门配额，也占真实在途）。
                #    每轮重新计算，确保刚补派的首发不会被同轮当成对冲再发一次。
                open_reqs_now = [r for r in self.requests.values()
                                 if r.state == OPEN and r.service == service]
                for req in open_reqs_now:
                    if not self._hedge_due(req):
                        continue
                    load = self._load(service)
                    if load["inflight"] >= limit.max_inflight:
                        req.last_block = {
                            "at": self.now, "reason": "max_inflight",
                            "inflight": load["inflight"],
                            "max_inflight": limit.max_inflight}
                        continue
                    if load["backups"] >= limit.hedge_budget:
                        req.last_block = {
                            "at": self.now, "reason": "hedge_budget",
                            "backups": load["backups"],
                            "hedge_budget": limit.hedge_budget}
                        continue
                    if self._dispatch(req, backup=True):
                        # REQUESTED 不计真实在途，等 send 确认后才计入。
                        progress = True

    def _dispatch(self, req, backup):
        limit = self.limits.get(req.service)
        load = self._load(req.service)
        if load["inflight"] >= limit.max_inflight:
            req.last_block = {"at": self.now, "reason": "max_inflight",
                              "inflight": load["inflight"],
                              "max_inflight": limit.max_inflight}
            return False
        if backup and load["backups"] >= limit.hedge_budget:
            req.last_block = {"at": self.now, "reason": "hedge_budget",
                              "backups": load["backups"],
                              "hedge_budget": limit.hedge_budget}
            return False
        endpoint, choice = self._choose_endpoint(req, backup)
        if endpoint is None:
            req.last_block = {"at": self.now, **choice}
            return False
        token = f"t{next(self._token_seq)}"
        attempt = Attempt(
            token=token, request_id=req.id, service=req.service,
            replica=endpoint.replica, epoch=endpoint.epoch,
            domain=endpoint.domain, capability=req.capability,
            backup=backup, created_at=self.now,
        )
        req.attempts.append(attempt)
        self.attempts_by_token[token] = attempt
        self._emit(
            "dispatch",
            {"at": self.now, "request": req.id, "token": token,
             "service": req.service, "replica": endpoint.replica,
             "epoch": endpoint.epoch, "domain": endpoint.domain,
             "backup": backup, "floor": req.floor,
             "preference": choice["detail"].get("preference")},
        )
        return True

    # ------------------------------------------------------------------ 运输确认

    def _on_send(self, record):
        request = self.requests.get(record["id"])
        if request is None:
            self.trace_notes.append(
                {"at": self.now, "kind": "send", "as": record.get("as"),
                 "problem": "unknown_request"})
            self._emit("trace_mismatch", {"record": record,
                                          "problem": "unknown_request"})
            return
        backup = bool(record.get("backup"))
        pending = [a for a in request.attempts
                   if a.state == REQUESTED and a.backup == backup]
        attempt = next(
            (a for a in pending
             if a.replica == record["replica"] and a.epoch == record["epoch"]),
            None,
        )
        if attempt is None:
            # 演练里的派发与代理决策不一致：如实登记，不假装是自己的决定。
            self.trace_notes.append(
                {"at": self.now, "kind": "send", "as": record["as"],
                 "problem": "no_matching_dispatch"})
            self._emit(
                "trace_mismatch",
                {"record": record, "problem": "no_matching_dispatch"},
            )
            attempt = Attempt(
                token=f"ext{next(self._token_seq)}", request_id=request.id,
                service=request.service, replica=record["replica"],
                epoch=record["epoch"],
                domain=self._domain_of(request.service, record["replica"]),
                capability=request.capability, backup=backup,
                created_at=self.now, external=True,
            )
            request.attempts.append(attempt)
            self.attempts_by_token[attempt.token] = attempt
        attempt.state = SENT
        attempt.as_id = record["as"]
        attempt.external = attempt.external
        self.attempts_by_as[record["as"]] = attempt
        self._emit(
            "attempt_sent",
            {"request": request.id, "as": record["as"], "token": attempt.token,
             "replica": attempt.replica, "epoch": attempt.epoch,
             "domain": attempt.domain, "backup": backup},
        )

    def _domain_of(self, service, replica):
        endpoint = self.endpoints.get((service, replica))
        return endpoint.domain if endpoint else "unknown"

    # ------------------------------------------------------------------ 结果处理

    def _on_result(self, record):
        attempt = self.attempts_by_as.get(record["as"])
        if attempt is None:
            self._emit(
                "result_ignored",
                {"as": record["as"], "reason": R_UNKNOWN_TRY},
            )
            return
        req = self.requests[attempt.request_id]
        attempt.version = record["version"]
        attempt.status = record["status"]
        endpoint = self.endpoints.get((attempt.service, attempt.replica))

        # 端点换代优先判定：旧代次回复不继承，即使该尝试已在取消流程中。
        if endpoint is None or endpoint.epoch != attempt.epoch:
            self._reject(req, attempt, record, R_GENERATION,
                         {"observed_epoch": attempt.epoch,
                          "catalog_epoch": None if endpoint is None
                          else endpoint.epoch})
            self._schedule()
            return

        # 已进入取消流程的尝试：胜出者已确定或请求已放弃，其迟到回复
        # 不再参与胜负，也不动凭据，只登记后继续等取消确认释放资源。
        if attempt.state in (CANCEL_REQUESTED, CANCEL_UNKNOWN):
            attempt.state = LATE_REJECTED
            attempt.reject_reason = R_LATE
            self._emit(
                "result_rejected",
                self._reject_view(req, attempt, record, R_LATE,
                                  {"cancel_in_flight": True}),
            )
            return

        # 请求已经结束：迟到结果绝不用于该会话未来的续读，也不动凭据。
        if req.state in (WON, FAILED, ATTACHED):
            attempt.state = LATE_REJECTED
            attempt.reject_reason = R_LATE
            self._emit(
                "result_rejected",
                self._reject_view(req, attempt, record, R_LATE,
                                  {"request_state": req.state}),
            )
            return

        # 局部失读：拿不出完整版本证据，error 一律不采用。
        if record["status"] == "error":
            self._reject(req, attempt, record, R_PARTIAL,
                         {"detail": record.get("detail", "error")})
            self._schedule()
            return

        # value 与 missing 一视同仁：都要证明所见版本满足下限。
        if record["version"] < req.floor:
            self._reject(req, attempt, record, R_VERSION,
                         {"floor": req.floor, "version": record["version"]})
            self._schedule()
            return

        self._accept_qualified(req, attempt, record)

    def _reject(self, req, attempt, record, reason, detail):
        was_live = attempt.live
        attempt.state = REJECTED
        attempt.reject_reason = reason
        attempt.reject_detail = detail
        self._emit("result_rejected",
                   self._reject_view(req, attempt, record, reason, detail))
        if was_live:
            self._emit(
                "capacity_released",
                {"request": req.id, "as": attempt.as_id, "service": req.service,
                 "load": self._load(req.service)},
            )

    def _accept_qualified(self, req, attempt, record):
        attempt.state = QUALIFIED
        qualified = {
            "request": req.id, "as": attempt.as_id,
            "replica": attempt.replica, "epoch": attempt.epoch,
            "domain": attempt.domain, "version": record["version"],
            "status": record["status"],
            "value": record.get("value"),
            "floor": req.floor,
            "proof": {"observed_version": record["version"],
                      "min_required": req.floor,
                      "complete": record["status"] in ("value", "missing")},
        }
        req.qualified = qualified
        req.state = WON
        # 合格回复抬高会话凭据；迟到低版本永远走不到这里。
        cred_key = (req.client, req.service, req.key)
        old = self.credentials.get(cred_key, 0)
        if record["version"] > old:
            self.credentials[cred_key] = record["version"]
        self._emit("read_result", qualified)
        # 合并者拿到同一份有依据的结果。
        for waiter in req.waiters:
            self._emit("read_result", {**qualified,
                                       "request": waiter["id"],
                                       "via_group": req.group})
            waiter_req = self.requests[waiter["id"]]
            waiter_req.state = WON
            waiter_req.qualified = {**qualified, "request": waiter["id"]}
            wkey = (waiter_req.client, req.service, req.key)
            wold = self.credentials.get(wkey, 0)
            if record["version"] > wold:
                self.credentials[wkey] = record["version"]
        # 首个合格结果胜出：撤掉没胜出的在途调用。
        for other in req.attempts:
            if other is attempt or not other.live:
                continue
            self._cancel_attempt(req, other, reason="winner_known",
                                 detail={"winner_as": attempt.as_id})
        self._emit(
            "capacity_released",
            {"request": req.id, "as": attempt.as_id, "service": req.service,
             "load": self._load(req.service)},
        )
        # 容量刚释放：排队中的首发/对冲请求立刻获得再调度机会。
        self._schedule()

    def _cancel_attempt(self, req, attempt, reason, detail=None):
        if attempt.state == REQUESTED:
            # 对端还没确认上线，直接撤回，不占对端资源。
            attempt.state = REVOKED
            attempt.reject_reason = reason
            self._emit(
                "attempt_revoked",
                {"request": req.id, "token": attempt.token,
                 "replica": attempt.replica, "epoch": attempt.epoch,
                 "reason": reason, "detail": detail or {}},
            )
            return
        attempt.state = CANCEL_REQUESTED
        self._emit(
            "cancel_requested",
            {"request": req.id, "as": attempt.as_id,
             "replica": attempt.replica, "epoch": attempt.epoch,
             "reason": reason, "detail": detail or {},
             "load": self._load(req.service)},
        )

    def _cancel_unpromising(self, req, cause):
        """下限抬高后，已无法兑现新下限的在途尝试立即取消。"""
        for attempt in req.attempts:
            if not attempt.live or attempt.state == REQUESTED:
                continue
            endpoint = self.endpoints.get((attempt.service, attempt.replica))
            stale = endpoint is None or endpoint.epoch != attempt.epoch
            if stale or not self._can_promise(endpoint, req.floor,
                                              req.capability):
                self._cancel_attempt(req, attempt, reason=R_VERSION,
                                     detail={"cause": cause,
                                             "new_floor": req.floor})

    # ------------------------------------------------------------------ 取消确认

    def _on_cancelled(self, record):
        attempt = self.attempts_by_as.get(record["as"])
        if attempt is None:
            self._emit("cancel_ignored", {"as": record["as"],
                                          "reason": R_UNKNOWN_TRY})
            return
        req = self.requests[attempt.request_id]
        result = record["result"]
        attempt.cancel_result = result
        if result == "ok":
            attempt.state = CANCELLED_OK
            self._emit(
                "cancel_confirmed",
                {"request": req.id, "as": attempt.as_id, "result": "ok",
                 "service": req.service, "load": self._load(req.service)},
            )
        else:
            # 对端尚未确认取消：资源继续计着，不能当已释放。
            attempt.state = CANCEL_UNKNOWN
            self._emit(
                "cancel_unconfirmed",
                {"request": req.id, "as": attempt.as_id, "result": result,
                 "service": req.service, "load": self._load(req.service)},
            )
        self._schedule()

    # ------------------------------------------------------------------ 截止

    def _sweep_deadlines(self, until):
        for req in self.requests.values():
            if req.state != OPEN:
                continue
            if req.deadline < until or req.deadline <= self.now:
                self._fail_request(req)

    def _fail_request(self, req):
        req.state = FAILED
        observed = [
            {"as": a.as_id, "replica": a.replica, "epoch": a.epoch,
             "version": a.version, "status": a.status,
             "reject_reason": a.reject_reason}
            for a in req.attempts
        ]
        req.fail_reason = "deadline_exceeded"
        req.fail_detail = {
            "retryable": True,
            "floor": req.floor,
            "deadline": req.deadline,
            "observed": observed,
            "note": "没有可兑现下限的完整结果，旧值不得兜底",
        }
        self._emit("read_failed", self._failure_view(req))
        for waiter in req.waiters:
            waiter_req = self.requests[waiter["id"]]
            waiter_req.state = FAILED
            waiter_req.fail_reason = req.fail_reason
            waiter_req.fail_detail = req.fail_detail
            self._emit("read_failed", self._failure_view(waiter_req))
        for attempt in req.attempts:
            if attempt.live:
                self._cancel_attempt(req, attempt, reason="deadline",
                                     detail={"deadline": req.deadline})

    # ------------------------------------------------------------------ 值班台检查

    def _on_inspect(self, record):
        if record.get("id"):
            req = self.requests.get(record["id"])
            view = self._request_view(req) if req else {"missing": record["id"]}
        elif record.get("service"):
            view = self._service_view(record["service"])
        else:
            view = {"requests": [self._request_view(r)
                                 for r in self.requests.values()],
                    "services": {s: self._service_view(s)
                                 for s in self.limits}}
        self._emit("inspect_snapshot", {"query": record, "view": view})

    def _request_view(self, req):
        return {
            "id": req.id, "client": req.client, "service": req.service,
            "key": req.key, "state": req.state,
            "submitted_min": req.submitted_min, "floor": req.floor,
            "deadline": req.deadline, "hedge_after": req.hedge_after,
            "group": req.group,
            "attempts": [
                {"as": a.as_id, "token": a.token, "replica": a.replica,
                 "epoch": a.epoch, "domain": a.domain, "backup": a.backup,
                 "state": a.state, "version": a.version,
                 "status": a.status, "reject_reason": a.reject_reason,
                 "reject_detail": a.reject_detail,
                 "cancel_result": a.cancel_result, "external": a.external}
                for a in req.attempts
            ],
            "waiters": list(req.waiters),
            "qualified": req.qualified,
            "fail_reason": req.fail_reason,
            "fail_detail": req.fail_detail,
            "last_block": req.last_block,
            "floor_history": req.floor_history,
            "credential": self._credential(req.client, req.service, req.key),
        }

    def _service_view(self, service):
        limit = self.limits.get(service)
        load = self._load(service)
        endpoints = [
            {"replica": e.replica, "epoch": e.epoch, "domain": e.domain,
             "applied": e.applied, "capabilities": e.capabilities}
            for e in self.endpoints.values() if e.service == service
        ]
        return {
            "service": service,
            "max_inflight": limit.max_inflight if limit else None,
            "hedge_budget": limit.hedge_budget if limit else None,
            "load": load,
            "endpoints": endpoints,
            "budget": None if limit is None else {
                "inflight_remaining": max(0, limit.max_inflight - load["inflight"]),
                "hedge_remaining": max(0, limit.hedge_budget - load["backups"]),
            },
        }

    # ------------------------------------------------------------------ 视图片段

    def _reject_view(self, req, attempt, record, reason, detail):
        return {
            "request": req.id, "as": record["as"],
            "replica": record.get("replica", attempt.replica),
            "epoch": record.get("epoch", attempt.epoch),
            "status": record["status"], "version": record["version"],
            "floor": req.floor, "reason": reason, "detail": detail,
            "action": "continue_waiting" if req.state == OPEN else "dropped",
        }

    def _failure_view(self, req):
        return {"id": req.id, "client": req.client, "service": req.service,
                "key": req.key, "reason": req.fail_reason,
                "detail": req.fail_detail}
