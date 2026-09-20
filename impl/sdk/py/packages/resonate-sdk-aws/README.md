# resonate-sdk-aws

AWS Lambda support for the [Resonate SDK](https://github.com/resonatehq/resonate-sdk-py).

```shell
pip install resonate-sdk-aws
```

```python
from resonate_aws import Resonate

resonate = Resonate()

@resonate.register
def greet(ctx, name: str) -> str:
    return f"hello {name}"

lambda_handler = resonate.handler()
```
