"""在隔离模式（python3 -I run_checks.py）下运行全部单元测试。

``-I`` 会忽略环境变量与用户 site 包，避免工作目录外的注入；本构建的 -I
不自动保留脚本目录，这里显式加入仓库根。
"""

import sys
import unittest
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=str(_REPO_ROOT), pattern="test_*.py")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(0 if result.wasSuccessful() else 1)
