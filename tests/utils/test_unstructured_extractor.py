"""Tests for utils.unstructured_extractor.UnstructuredExtractor."""

from utils.unstructured_extractor import UnstructuredExtractor


def test_extract_amount_finds_currency() -> None:
    ex = UnstructuredExtractor()
    amounts = ex.extract_amounts("Total due is $42.50 for the plan")
    assert 42.5 in amounts
