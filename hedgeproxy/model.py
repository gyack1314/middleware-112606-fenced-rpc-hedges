"""代理内部状态的数据结构与状态常量。

事件（代理输出）一律使用普通 dict，保证可以直接 JSON 序列化；
内部实体使用 dataclass，便于表达状态迁移。
"""

from dataclasses import dataclass, field

DEFAULT_CAPABILITY = "read"

# 请求状态
OPEN = "open"            # 仍在等待可兑现下限的结果
ATTACHED = "attached"    # 已合并到同下限的在途请求
WON = "won"              # 已拿到合格结果
FAILED = "failed"        # 截止且无合格结果，返回可重试原因

# 尝试状态
REQUESTED = "requested"                  # 代理已决定派发，等待运输确认
SENT = "sent"                            # 运输确认已实际派发，计入在途
CANCEL_REQUESTED = "cancel_requested"    # 已请求取消，等待对端确认
CANCEL_UNKNOWN = "cancel_unknown"        # 取消未确认，资源继续计着
CANCELLED_OK = "cancelled_ok"            # 对端确认取消，释放资源
REVOKED = "revoked"                      # 尚未上线即被撤回，不占对端资源
QUALIFIED = "qualified"                  # 返回了满足下限的完整版本
REJECTED = "rejected"                    # 已返回但不被采用，尝试本身已结束
LATE_REJECTED = "late_rejected"          # 请求结束后才到的迟到结果，不采用

# 已由对端确认上线、仍占用服务资源的尝试状态（含取消未确认）。
# REQUESTED 只是代理的派发决定，运输尚未确认，不计真实在途。
LIVE_STATES = frozenset(
    {SENT, CANCEL_REQUESTED, CANCEL_UNKNOWN}
)
# 已由对端终结、不再计入在途的状态
TERMINAL_STATES = frozenset(
    {CANCELLED_OK, REVOKED, QUALIFIED, REJECTED, LATE_REJECTED}
)


@dataclass
class Limit:
    """单个服务的真实在途上限与专门的对冲（额外尝试）配额。"""

    service: str
    max_inflight: int
    hedge_budget: int


@dataclass
class Endpoint:
    """服务目录中的一个副本代次。

    同名副本更换 epoch 后整体替换，任何旧代次上的成功印象都不继承，
    新代次只凭自己声明的 applied 与能力参与选择。
    """

    service: str
    replica: str
    epoch: int
    domain: str
    applied: int
    capabilities: list
    order: int


@dataclass
class Attempt:
    """一次具体的读取尝试；``as_id`` 是运输确认后才有的尝试身份。"""

    token: str
    request_id: str
    service: str
    replica: str
    epoch: int
    domain: str
    capability: str
    backup: bool
    created_at: int
    state: str = REQUESTED
    as_id: str | None = None
    external: bool = False
    version: int | None = None
    status: str | None = None
    reject_reason: str | None = None
    reject_detail: dict = field(default_factory=dict)
    cancel_result: str | None = None

    @property
    def live(self) -> bool:
        return self.state in LIVE_STATES


@dataclass
class ReadRequest:
    """一次只读调用及其合并者（waiters）。"""

    id: str
    client: str
    service: str
    key: str
    capability: str
    submitted_min: int
    floor: int
    deadline: int
    hedge_after: int
    created_at: int
    state: str = OPEN
    group: str | None = None
    attempts: list = field(default_factory=list)
    waiters: list = field(default_factory=list)
    qualified: dict | None = None
    fail_reason: str | None = None
    fail_detail: dict | None = None
    last_block: dict | None = None
    floor_history: list = field(default_factory=list)
