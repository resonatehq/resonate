![resonate banner](./assets/resonate-banner.png)

# Resonate Python SDK

[![ci](https://github.com/resonatehq/resonate-sdk-py/actions/workflows/ci.yml/badge.svg)](https://github.com/resonatehq/resonate-sdk-py/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/resonatehq/resonate-sdk-py/graph/badge.svg?token=61GYC3DXID)](https://codecov.io/gh/resonatehq/resonate-sdk-py)
[![dst](https://github.com/resonatehq/resonate-sdk-py/actions/workflows/dst.yml/badge.svg)](https://github.com/resonatehq/resonate-sdk-py/actions/workflows/dst.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## About this component

The Resonate Python SDK enables developers to build reliable and scalable cloud applications across a wide variety of use cases.

- [How to contribute to this SDK](./CONTRIBUTING.md)
- [Evaluate Resonate for your next project](https://docs.resonatehq.io/evaluate/)
- [Example application library](https://github.com/resonatehq-examples)
- [The concepts that power Resonate](https://www.distributed-async-await.io/)
- [Join the Discord](https://resonatehq.io/discord)
- [Subscribe to the Blog](https://journal.resonatehq.io/subscribe)
- [Follow on Twitter](https://twitter.com/resonatehqio)
- [Follow on LinkedIn](https://www.linkedin.com/company/resonatehqio)
- [Subscribe on YouTube](https://www.youtube.com/@resonatehqio)

## Quickstart

![quickstart banner](./assets/quickstart-banner.png)

1. Install the Resonate Server & CLI

```shell
brew install resonatehq/tap/resonate
```

2. Install the Resonate SDK

```shell
pip install resonate-sdk
```

3. Write your first Resonate Function

A countdown as a loop. Simple, but the function can run for minutes, hours, or days, despite restarts.

```python
from resonate import Resonate, Context
from threading import Event

# Instantiate Resonate
resonate = Resonate.remote()

@resonate.register
def countdown(ctx: Context, count: int, delay: int):
    for i in range(count, 0, -1):
        # Run a function, persist its result
        yield ctx.run(ntfy, i)
        # Sleep
        yield ctx.sleep(delay)
    print("Done!")


def ntfy(_: Context, i: int):
    print(f"Countdown: {i}")


resonate.start() # Start Resonate threads
Event().wait()  # Keep the main thread alive
```

[Working example](https://github.com/resonatehq-examples/example-quickstart-py)

4. Start the server

```shell
resonate dev
```

5. Start the worker

```shell
python countdown.py
```

6. Run the function

Run the function with execution ID `countdown.1`:

```shell
resonate invoke countdown.1 --func countdown --arg 5 --arg 60
```

**Result**

You will see the countdown in the terminal

```shell
python countdown.py
Countdown: 5
Countdown: 4
Countdown: 3
Countdown: 2
Countdown: 1
Done!
```

**What to try**

After starting the function, inspect the current state of the execution using the `resonate tree` command. The tree command visualizes the call graph of the function execution as a graph of durable promises.

```shell
resonate tree countdown.1
```

Now try killing the worker mid-countdown and restarting. **The countdown picks up right where it left off without missing a beat.**
