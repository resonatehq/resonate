# resonate-testing

Shared test doubles for the [Resonate SDK](https://github.com/resonatehq/resonate-sdk-py)'s
own test suites.

This package is **not published**: it lives in `packages/` next to the other
workspace members purely so every suite in the monorepo -- the root `tests/`
and each `packages/*/tests/` -- can depend on it the normal way, instead of
reaching into a `tests.support` internal package via a `pythonpath` hack.

Most of what a test needs is public and lives in `resonate.testing` -- the
same helpers an application gets. This package adds only the pieces that are
specific to testing the SDK *internals*: the network stand-ins, and the golden
file comparison.

The rule from `resonate.testing` holds here too: **a helper never returns an
error, it fails directly.** No call site should have to check a return value
before getting on with the test.
