# Engine equivalence tests (Layer A)

Matched-workload equivalence between the generator engine (`src/`) and the async
engine (`src/async/`): each workload runs on both engines against fresh in-memory
servers and we diff the canonical durable state + root outcome.

```sh
npx jest tests/equivalence
```

This is **Layer A** of the differential-testing strategy. The differential
simulation under failure (**Layer B**, `sim/src/differential.ts`), the oracle
details, and the findings are all documented in
[`../../diff-testing.md`](../../diff-testing.md).
