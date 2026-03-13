"""
Token Counting Utilities for Whole-Website Extraction

Provides token estimation for GLM-4-Flash and Gemini API calls.
Uses ~4 chars = 1 token heuristic (close enough for threshold checks).
"""

from typing import Optional


def estimate_tokens(text: str) -> int:
    """
    Estimate token count using ~4 chars = 1 token heuristic.
    Works for both GLM and Gemini (close enough for threshold checks).

    Args:
        text: String to estimate tokens for

    Returns:
        Estimated token count (minimum 0)

    Examples:
        >>> estimate_tokens("Hello World")
        2
        >>> estimate_tokens("")
        0
        >>> estimate_tokens("A" * 1000)
        250
    """
    if not text:
        return 0
    return max(0, len(text) // 4)


def estimate_tokens_with_overhead(
    content: str,
    system_prompt: str,
    schema_json: str
) -> int:
    """
    Estimate total tokens including system prompt + schema + content.

    This is the FULL token count that will be sent to the API:
      Total = system_prompt + schema_json + content

    Args:
        content: Main page content (concatenated pages)
        system_prompt: System instruction text
        schema_json: JSON schema as string

    Returns:
        Total estimated tokens (minimum 0)

    Examples:
        >>> system = "You are a data extractor."
        >>> schema = '{"type": "object"}'
        >>> content = "Company info: " + "x" * 1000
        >>> estimate_tokens_with_overhead(content, system, schema)
        264  # (28 + 19 + 1014) / 4 = 265.25 → 265
    """
    total_chars = len(system_prompt) + len(schema_json) + len(content)
    return max(0, total_chars // 4)


def check_token_limit(
    content: str,
    system_prompt: str,
    schema_json: str,
    limit: int
) -> tuple[bool, int]:
    """
    Check if content would exceed token limit.

    Args:
        content: Main page content
        system_prompt: System instruction
        schema_json: JSON schema
        limit: Token limit (e.g., 50000 for GLM, 128000 for Gemini)

    Returns:
        Tuple of (exceeds_limit: bool, estimated_tokens: int)

    Examples:
        >>> system = "Extract data."
        >>> schema = "{}"
        >>> content = "x" * 200000
        >>> check_token_limit(content, system, schema, 50000)
        (True, 50003)  # Exceeds limit
        >>> check_token_limit(content, system, schema, 100000)
        (False, 50003)  # Within limit
    """
    total_tokens = estimate_tokens_with_overhead(content, system_prompt, schema_json)
    exceeds = total_tokens > limit
    return exceeds, total_tokens


# Token limits for reference
GLM_TOKEN_LIMIT = 50000
GEMINI_TOKEN_LIMIT = 128000
