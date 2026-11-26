"""命令行入口：在隔离模式下驱动仿真工作台。

用法::

    python3 -I workbench.py replay examples
    python3 -I workbench.py replay examples/first-reply-too-old.jsonl --explain
    python3 -I workbench.py demo --explain
    cat my-scenario.jsonl | python3 -I workbench.py interactive

``-I`` 屏蔽环境变量与用户 site 包；脚本所在目录（仓库根）仍在 sys.path 上，
因此只导入本仓库代码与标准库。
"""

import sys
from pathlib import Path

# 本构建的 -I 不自动保留脚本目录，显式加入仓库根，导入仅限本仓库与标准库。
_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from hedgeproxy.workbench import main

if __name__ == "__main__":
    main()
