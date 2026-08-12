"""redline1_exempt 装饰器语义：无操作（返回原函数，运行期零行为变化）。"""

from lethefield_clients import redline1_exempt


def test_returns_original_function():
    def f() -> int:
        return 42

    decorated = redline1_exempt(worker="w", reason="r", cadence="c")(f)
    assert decorated is f
    assert decorated() == 42


def test_works_on_methods_and_nested_defs():
    class S:
        @redline1_exempt(worker="w", reason="r", cadence="c")
        def run(self) -> str:
            return "ok"

    assert S().run() == "ok"
