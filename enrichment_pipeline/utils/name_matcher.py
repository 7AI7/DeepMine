# Enrichment Pipeline — Company Name Fuzzy Matching
#
# Prevents wrong-company data extraction.
# Proven necessary: "SIFL" → Serum Institute, "Pricol" → Parle Agro, etc.

import re
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import NAME_STOP_WORDS


def normalize_name(name: str) -> set[str]:
    """
    Normalize a company name into a set of meaningful tokens.
    
    Process:
      1. Lowercase
      2. Replace '&' with space
      3. Remove punctuation (dots, commas, hyphens)
      4. Split into word tokens
      5. Remove stop words: Pvt, Ltd, Private, Limited, the, and, of, &
      6. "India" is deliberately KEPT — helps disambiguation
    
    Example:
      "J S Auto Cast Foundry India Private Limited"
      → {"j", "s", "auto", "cast", "foundry", "india"}
    """
    name = name.lower()
    name = name.replace("&", " ")
    # Remove dots, commas, hyphens, parentheses — keep letters, digits, spaces
    name = re.sub(r"[^a-z0-9\s]", " ", name)
    tokens = set(name.split())
    tokens -= NAME_STOP_WORDS
    # Remove empty strings
    tokens.discard("")
    return tokens


def match_score(input_name: str, result_name: str) -> float:
    """
    Compute fuzzy match score between input company name and search result name.
    
    Scores computed:
      1. EXACT root check: If it's a very short name (1 token left), ensure it's
         an exact string match in the target (case-insensitive).
      2. Jaccard overlap: |intersection| / |union|
      3. Input coverage:  |intersection| / |input_tokens|
      
    This prevents "Pricol Gourmet" matching "Pricol Limited"
    """
    # Quick exact check to prevent partial substring bleed
    input_norm = re.sub(r'[^a-z0-9\s]', '', input_name.lower())
    result_norm = re.sub(r'[^a-z0-9\s]', '', result_name.lower())
    
    # 1. Stopword token matching
    input_tokens = normalize_name(input_name)
    result_tokens = normalize_name(result_name)
    
    if not input_tokens or not result_tokens:
        return 0.0

    intersection = input_tokens & result_tokens
    union = input_tokens | result_tokens
    
    jaccard = len(intersection) / len(union) if union else 0.0
    coverage = len(intersection) / len(input_tokens) if input_tokens else 0.0
    
    # ROOT-CAUSE FIX: if input only has 1 or 2 tokens, it easily achieves 1.0 coverage
    # against ANY result containing those tokens. E.g "Pricol Limited" -> {"pricol"}
    # matches "Pricol Gourmet Pvt Ltd" -> {"pricol", "gourmet"}.
    # So we penalize severely if the jaccard overlap is too low.
    if len(input_tokens) <= 2:
        if len(result_tokens) > len(input_tokens) + 1:
            # Result has at least 2 extra meaningful words.
            # EXCEPTION: If the original result string directly contains the original input
            # string (e.g. 'Pricol Limited' in 'Pricol Limited | LinkedIn'), allow it.
            if input_norm in result_norm:
                return max(jaccard, coverage)
                
            # Otherwise, it's highly likely a different entity.
            logger_fallback_score = min(jaccard, coverage)
            return logger_fallback_score

    return max(jaccard, coverage)


def is_match(input_name: str, result_name: str, threshold: float) -> bool:
    """
    Check if a search result name matches the input company name.
    
    Args:
      input_name:  The company name we searched for
      result_name: The name that appeared in the search result
      threshold:   Minimum score to accept (0.60 for Bing, 0.70 for Google, 0.75 for Maps)
    
    Returns:
      True if match_score >= threshold
    """
    score = match_score(input_name, result_name)
    return score >= threshold


# ── Quick CLI test ──
if __name__ == "__main__":
    tests = [
        # (input, result, expected: match or not at 0.60)
        ("J S Auto Cast Foundry India Private Limited", "J S Auto Cast Foundry India Private Limited", True),
        ("J S Auto Cast Foundry India", "J. S. Auto Private Limited", False),
        ("Sadhu Forging", "Sadhu Forging Ltd.", True),
        ("Sadhu Forging", "Sadhu Autocom Ltd", False),
        ("SIFL", "Serum Institute Of India Private Limited", False),
        ("Pricol", "Parle Agro Private Limited", False),
        ("Pricol Limited", "Pricol Limited", True),
    ]
    
    print("Name Matcher Tests:")
    print("-" * 80)
    for inp, res, expected in tests:
        score = match_score(inp, res)
        matched = is_match(inp, res, 0.60)
        status = "✅" if matched == expected else "❌ FAIL"
        print(f"  {status}  score={score:.2f}  match={matched}  "
              f"'{inp}' vs '{res}'")
