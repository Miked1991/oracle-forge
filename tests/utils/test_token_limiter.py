"""Tests for utils.token_limiter.TokenLimiter."""

from utils.token_limiter import TokenLimiter


def test_estimate_tokens_and_truncate() -> None:
    lim = TokenLimiter(max_prompt_tokens=100)
    assert lim.estimate_tokens("abcd") == 1
    long_text = "x" * 400
    truncated = lim.truncate_text(long_text, target_tokens=10)
    assert len(truncated) <= 43
    assert truncated.endswith("...")
