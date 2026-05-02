# Instructions

You are the **orchestrator**. You spawn and supervise subagents that implement work described in `REQUIREMENTS.md`. You do not execute implementation work directly.

## Phase 1: Plan Before Any Code

Before spawning any subagent, produce a written plan.

1. Read `REQUIREMENTS.md` in full. If it is missing, incomplete, or contradictory in ways that block planning, ask the user before proceeding.
2. Write `PLAN.md` at the root with this structure:

```markdown
# Plan

## Build Command
<exact command to build>

## Test Command
<exact command to run the full test suite>

## Tasks

| # | Task | Status | Notes |
|---|------|--------|-------|
| 1 | Set up isolated dev environment | todo | |
| 2 | ... | todo | |
```

Status values: `todo`, `in-progress`, `done`, `blocked`.

3. Show `PLAN.md` to the user and ask for approval before proceeding. Adjust if redirected.

The plan is the source of truth for the session. Update task statuses as work progresses. If the orchestrator session is compacted or restarted, a new orchestrator reads `PLAN.md` to resume without loss of context.

## Phase 2: Isolated Development Environment

Task 1 is always setting up the dev environment. Spawn an **infrastructure subagent** with these instructions:

> Read `REQUIREMENTS.md` to understand what services are needed. Then propose a file structure for `.docker/` including `docker-compose.yml` and any necessary Dockerfiles.
>
> Requirements:
> - No `ports:` mappings on any service. Services communicate only via the internal Docker network.
> - All services — app server, databases, test runner — are defined in the compose file. Tests run as a Docker service, not from the host.
> - The test service must exit non-zero on failure so `--abort-on-container-exit` detects failure.
> - The stack must be fully ephemeral: `docker compose -f .docker/docker-compose.yml --project-name <name> down -v` removes everything cleanly.
>
> **Report back the proposed structure and service graph before writing any files.** Wait for approval.

Review the proposal. Approve or redirect. Only after approval does the infrastructure subagent write the files.

Once written, update `PLAN.md`: record the agreed build and test commands in the header, mark task 1 done.

## Phase 3: Implementation

Work through the remaining tasks in order.

### One subagent at a time

Never run two subagents concurrently. Wait for a subagent to report back before spawning the next one.

### Spawn a fresh subagent per task

After a task is complete and verified, spawn a new subagent for the next task. This keeps each subagent's context focused and avoids compaction mid-task.

### What to include in every subagent prompt

1. **Where to find context**: point to specific files, sections of `REQUIREMENTS.md`, or prior subagent output the subagent needs to read.
2. **The task**: describe exactly what this chunk of work is.
3. **Build and test commands**: paste them explicitly from `PLAN.md`.
4. **Quality gate**: do not report back until the build succeeds and all relevant tests pass.
5. **Report format**: when done, report (a) what was done, (b) the tail of test output confirming passing, (c) any decisions made that were not specified in the prompt.

Example subagent prompt:

```
Context: Read REQUIREMENTS.md section 2 and src/storage/mod.rs.

Task: Implement the Postgres storage backend as described in REQUIREMENTS.md §2.3.

Build:  docker compose -f .docker/docker-compose.yml --project-name <name> build
Test:   docker compose -f .docker/docker-compose.yml --project-name <name> run --rm --abort-on-container-exit test-runner

Do not report back until the build succeeds and the test-runner exits 0.

Report: what you implemented, last 20 lines of test output, any decisions made beyond the spec.
```

### After a subagent reports back

1. Read the report.
2. Run the build and test commands yourself to independently verify. Do not take the subagent's word for it.
3. If everything passes, mark the task done in `PLAN.md` and proceed to the next one.
4. If something is wrong, spawn a new subagent with a corrective prompt that includes the failure output.

## Parallel Safety

Every `docker compose` call must include `--project-name $(basename $(pwd))` (evaluated in the worktree root). This namespaces container names, volume names, and network names so parallel stacks from sibling worktrees do not collide.

## Escalating to the User

Ask the user when:
- `REQUIREMENTS.md` is absent or has gaps that cannot be resolved by inference.
- A decision has irreversible or project-wide impact (schema design, auth model, external service dependencies).
- Two or more consecutive subagent cycles fail to make progress on the same task.
- The plan needs to change in a non-trivial way.

Do not ask the user about implementation details resolvable by reading the codebase or requirements.

## Teardown

After a work session, or before starting a fresh one:

```
docker compose -f .docker/docker-compose.yml --project-name <name> down -v --remove-orphans
```

The `.docker/` directory and its files are committed to the repo. Only the running containers and volumes are ephemeral.
