"""Tests for utils.date_normalizer.DateNormalizer."""

from utils.date_normalizer import DateNormalizer


def test_to_iso_us_slash() -> None:
    dn = DateNormalizer()
    assert dn.to_iso("04/15/2026") == "2026-04-15"
