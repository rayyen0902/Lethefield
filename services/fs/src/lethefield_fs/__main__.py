"""`python -m lethefield_fs` = sweep worker 主循环（--once 单轮模式）。"""

from lethefield_fs.worker import main

if __name__ == "__main__":
    raise SystemExit(main())
