"""
Page Utilities for Whole-Website Extraction

Provides page concatenation and splitting for multi-page extraction.
"""

from typing import List, Dict, Tuple


def concatenate_pages(
    pages: List[Dict[str, str]],
    delimiter_format: str = "=== PAGE: {url} ==="
) -> str:
    """
    Concatenate multiple pages into single text with URL delimiters.

    Format:
        === PAGE: https://example.com/about ===
        [page content]

        === PAGE: https://example.com/products ===
        [page content]

    Args:
        pages: List of dicts with 'url' and 'text' keys
        delimiter_format: Format string for delimiter (must contain {url})

    Returns:
        Concatenated string with delimiters

    Examples:
        >>> pages = [
        ...     {"url": "https://example.com/about", "text": "About us"},
        ...     {"url": "https://example.com/products", "text": "Our products"}
        ... ]
        >>> result = concatenate_pages(pages)
        >>> "=== PAGE: https://example.com/about ===" in result
        True
        >>> "About us" in result
        True
    """
    if not pages:
        return ""

    parts = []
    for page in pages:
        url = page.get('url', 'unknown')
        text = page.get('text', '')

        # Skip empty pages
        if not text.strip():
            continue

        delimiter = delimiter_format.format(url=url)
        parts.append(f"{delimiter}\n{text}")

    return "\n\n".join(parts)


def split_pages_in_half(
    pages: List[Dict[str, str]]
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """
    Split pages list into 2 equal halves (by count, not size).
    Used when total tokens exceed limit.

    Args:
        pages: List of page dicts

    Returns:
        Tuple of (first_half, second_half)

    Examples:
        >>> pages = [{"url": f"page{i}", "text": f"content{i}"} for i in range(10)]
        >>> first, second = split_pages_in_half(pages)
        >>> len(first)
        5
        >>> len(second)
        5

        >>> pages = [{"url": f"page{i}", "text": f"content{i}"} for i in range(11)]
        >>> first, second = split_pages_in_half(pages)
        >>> len(first)
        5
        >>> len(second)
        6
    """
    if not pages:
        return [], []

    mid = len(pages) // 2
    return pages[:mid], pages[mid:]


def validate_pages(pages: List[Dict[str, str]]) -> List[str]:
    """
    Validate pages list and return list of validation errors.

    Args:
        pages: List of page dicts to validate

    Returns:
        List of error messages (empty if valid)

    Examples:
        >>> pages = [{"url": "http://ex.com", "text": "content"}]
        >>> validate_pages(pages)
        []

        >>> pages = [{"text": "missing url"}]
        >>> errors = validate_pages(pages)
        >>> len(errors) > 0
        True
    """
    errors = []

    if not isinstance(pages, list):
        return ["pages must be a list"]

    if not pages:
        errors.append("pages list is empty")

    for i, page in enumerate(pages):
        if not isinstance(page, dict):
            errors.append(f"Page {i}: not a dict")
            continue

        if 'url' not in page:
            errors.append(f"Page {i}: missing 'url' key")

        if 'text' not in page:
            errors.append(f"Page {i}: missing 'text' key")

    return errors


def count_total_characters(pages: List[Dict[str, str]]) -> int:
    """
    Count total characters across all pages.

    Args:
        pages: List of page dicts

    Returns:
        Total character count

    Examples:
        >>> pages = [
        ...     {"url": "page1", "text": "hello"},
        ...     {"url": "page2", "text": "world"}
        ... ]
        >>> count_total_characters(pages)
        10
    """
    total = 0
    for page in pages:
        text = page.get('text', '')
        total += len(text)
    return total
