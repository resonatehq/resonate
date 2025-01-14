from __future__ import annotations

from resonate import Context, Resonate

resonate = Resonate()


@resonate.register
def foo(ctx: Context) -> str:
    return "hi"


def main() -> None:
    handle = foo.run("foo")
    v = handle.result()
    print(v)  # noqa: T201


if __name__ == "__main__":
    main()
