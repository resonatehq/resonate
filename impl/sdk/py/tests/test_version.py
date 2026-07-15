"""Tests for :func:`resonate.version.check_lockstep_version`."""

from __future__ import annotations

import importlib.metadata
import warnings

import pytest

from resonate.version import check_lockstep_version


def test_warns_when_member_version_skews_from_sdk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        importlib.metadata,
        "packages_distributions",
        lambda: {"resonate_aws": ["resonate-sdk-aws"]},
    )
    versions = {"resonate-sdk": "0.7.3", "resonate-sdk-aws": "0.7.4"}
    monkeypatch.setattr(importlib.metadata, "version", versions.__getitem__)

    with pytest.warns(
        UserWarning,
        match="resonate-sdk-aws 0.7.4 requires resonate-sdk==0.7.4, but "
        "resonate-sdk 0.7.3 is installed",
    ):
        check_lockstep_version("resonate_aws")


def test_silent_when_versions_match(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        importlib.metadata,
        "packages_distributions",
        lambda: {"resonate_aws": ["resonate-sdk-aws"]},
    )
    versions = {"resonate-sdk": "0.7.3", "resonate-sdk-aws": "0.7.3"}
    monkeypatch.setattr(importlib.metadata, "version", versions.__getitem__)

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        check_lockstep_version("resonate_aws")


def test_silent_when_distribution_is_not_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        importlib.metadata,
        "packages_distributions",
        lambda: {"resonate_aws": ["resonate-sdk-aws"]},
    )

    def missing(name: str) -> str:
        raise importlib.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(importlib.metadata, "version", missing)

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        check_lockstep_version("resonate_aws")


def test_silent_when_package_is_not_mapped_to_a_distribution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(importlib.metadata, "packages_distributions", dict)

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        check_lockstep_version("resonate_aws")
