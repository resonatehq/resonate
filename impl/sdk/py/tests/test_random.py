from __future__ import annotations

from resonate import random


def test_random_prefix() -> None:
    choices_per_run = 10
    global_prefix: list[float] = []
    steps_taken: list[int] = []
    for i in range(30):
        rand = random.Random(seed=i, prefix=global_prefix)
        remembered_steps = len(global_prefix)
        steps_taken_this_run = [
            rand.randint(0, 100) for _ in range(remembered_steps + choices_per_run)
        ]
        global_prefix.extend(rand.export()[remembered_steps:])
        assert (
            len(steps_taken_this_run) - len(global_prefix) == 0
        ), f"Every run must make {choices_per_run} new choices"

        steps_taken.extend(steps_taken_this_run[remembered_steps:])
        assert (
            steps_taken[:remembered_steps] == steps_taken_this_run[:remembered_steps]
        ), f"The first {remembered_steps} must always be equal."


def test_random_prefix_with_floats() -> None:
    choices_per_run = 10
    global_prefix: list[float] = []
    steps_taken: list[float] = []
    for i in range(30):
        rand = random.Random(seed=i, prefix=global_prefix)
        remembered_steps = len(global_prefix)
        steps_taken_this_run = [
            rand.random() for _ in range(remembered_steps + choices_per_run)
        ]
        global_prefix.extend(rand.export()[remembered_steps:])
        assert (
            len(steps_taken_this_run) - len(global_prefix) == 0
        ), f"Every run must make {choices_per_run} new choices"

        steps_taken.extend(steps_taken_this_run[remembered_steps:])
        assert (
            steps_taken[:remembered_steps] == steps_taken_this_run[:remembered_steps]
        ), f"The first {remembered_steps} must always be equal."
