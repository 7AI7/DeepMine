"""
JSON Merging Utilities for Split Extractions

Provides intelligent merging of extraction results from split requests.
"""

from typing import Dict, Any, List
import json


def prefer_full_name(name1: str | None, name2: str | None) -> str:
    """
    Prefer company name with 'Pvt Ltd', 'Ltd', 'Inc', 'LLC', 'Corp', etc.
    If both have suffix, prefer longer. If neither, prefer first non-null.

    Args:
        name1: First company name
        name2: Second company name

    Returns:
        Preferred name

    Examples:
        >>> prefer_full_name("ABC Inc", "ABC")
        'ABC Inc'
        >>> prefer_full_name("XYZ", "XYZ Private Limited")
        'XYZ Private Limited'
        >>> prefer_full_name("ABC Pvt Ltd", "ABC Limited")
        'ABC Pvt Ltd'
    """
    suffixes = ['pvt ltd', 'ltd', 'limited', 'inc', 'llc', 'corp', 'corporation', 'private limited']

    if not name1:
        return name2 or ""
    if not name2:
        return name1

    n1_lower = name1.lower()
    n2_lower = name2.lower()

    n1_has_suffix = any(suf in n1_lower for suf in suffixes)
    n2_has_suffix = any(suf in n2_lower for suf in suffixes)

    if n1_has_suffix and not n2_has_suffix:
        return name1
    elif n2_has_suffix and not n1_has_suffix:
        return name2
    elif n1_has_suffix and n2_has_suffix:
        # Both have suffix - prefer longer
        return name1 if len(name1) >= len(name2) else name2
    else:
        # Neither has suffix - prefer first
        return name1


def dedup_list(items: List[Any]) -> List[Any]:
    """
    Deduplicate list while preserving order.
    For dicts, uses JSON serialization for comparison.

    Args:
        items: List to deduplicate

    Returns:
        Deduplicated list

    Examples:
        >>> dedup_list([1, 2, 2, 3, 1])
        [1, 2, 3]
        >>> dedup_list([{"a": 1}, {"a": 1}, {"b": 2}])
        [{'a': 1}, {'b': 2}]
    """
    if not isinstance(items, list):
        return items

    seen = set()
    out = []

    for it in items:
        if isinstance(it, dict):
            key = json.dumps(it, sort_keys=True, ensure_ascii=False)
        else:
            key = str(it) if it is not None else None

        if key and key not in seen:
            seen.add(key)
            out.append(it)

    return out


def merge_split_extractions(
    result1: Dict[str, Any],
    result2: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Merge two extraction results from split requests.

    Merge Rules:
    1. Scalars (name, website, etc.): Prefer result1, but use prefer_full_name for 'name'
    2. Contact fields (email, phone): Join with ', ' separator
    3. Arrays (products, clients, etc.): Concatenate and deduplicate
    4. Contact person (all-or-nothing): Take first non-null with all 3 fields

    Args:
        result1: First extraction result (part 1/2)
        result2: Second extraction result (part 2/2)

    Returns:
        Merged extraction result

    Examples:
        >>> r1 = {"company": {"name": "ABC", "email": "a@b.com"}}
        >>> r2 = {"company": {"name": "ABC Ltd", "phone": "123"}}
        >>> merged = merge_split_extractions(r1, r2)
        >>> merged["company"]["name"]
        'ABC Ltd'
        >>> merged["company"]["email"]
        'a@b.com'
        >>> merged["company"]["phone"]
        '123'
    """
    merged = {}

    # ═══════════════════════════════════════════════════════════════════
    # Merge company object
    # ═══════════════════════════════════════════════════════════════════
    comp1 = result1.get('company', {}) or {}
    comp2 = result2.get('company', {}) or {}
    merged_company = {}

    # Special case: name (prefer full legal name)
    if 'name' in comp1 or 'name' in comp2:
        merged_company['name'] = prefer_full_name(comp1.get('name'), comp2.get('name'))

    # Contact fields: join with ', '
    for field in ['email', 'phone']:
        vals = []
        if comp1.get(field):
            vals.append(comp1[field])
        if comp2.get(field):
            vals.append(comp2[field])
        if vals:
            merged_company[field] = ', '.join(vals)

    # Contact person (all-or-nothing): take first complete set
    cp1_valid = all(comp1.get(f'contact_person_{k}') for k in ['name', 'designation', 'contact'])
    cp2_valid = all(comp2.get(f'contact_person_{k}') for k in ['name', 'designation', 'contact'])

    if cp1_valid:
        for k in ['name', 'designation', 'contact']:
            merged_company[f'contact_person_{k}'] = comp1[f'contact_person_{k}']
    elif cp2_valid:
        for k in ['name', 'designation', 'contact']:
            merged_company[f'contact_person_{k}'] = comp2[f'contact_person_{k}']

    # Other scalar fields: prefer first non-null
    scalar_fields = [
        'website', 'address', 'city', 'state', 'country',
        'website_last_updated_on_year', 'linkedin_page',
        'infrastructure_available', 'brochure_link'
    ]
    for field in scalar_fields:
        val = comp1.get(field) if comp1.get(field) is not None else comp2.get(field)
        if val is not None:
            merged_company[field] = val

    if merged_company:
        merged['company'] = merged_company

    # ═══════════════════════════════════════════════════════════════════
    # Merge products object (arrays inside)
    # ═══════════════════════════════════════════════════════════════════
    prod1 = result1.get('products', {}) or {}
    prod2 = result2.get('products', {}) or {}
    merged_products = {}

    for array_field in ['product_category', 'product', 'application', 'service', 'serving_sector']:
        arr1 = prod1.get(array_field, [])
        arr2 = prod2.get(array_field, [])
        if arr1 or arr2:
            combined = (arr1 or []) + (arr2 or [])
            merged_products[array_field] = dedup_list(combined)

    if merged_products:
        merged['products'] = merged_products

    # ═══════════════════════════════════════════════════════════════════
    # Merge array fields at top level
    # ═══════════════════════════════════════════════════════════════════
    for array_field in ['addresses', 'clients', 'management']:
        arr1 = result1.get(array_field, [])
        arr2 = result2.get(array_field, [])
        if arr1 or arr2:
            combined = (arr1 or []) + (arr2 or [])
            merged[array_field] = dedup_list(combined)

    # ═══════════════════════════════════════════════════════════════════
    # Merge infrastructure (has nested structure)
    # ═══════════════════════════════════════════════════════════════════
    infra1 = result1.get('infrastructure', {}) or {}
    infra2 = result2.get('infrastructure', {}) or {}
    merged_infra = {}

    for nested_array in ['infrastructure_blocks', 'machines']:
        arr1 = infra1.get(nested_array, [])
        arr2 = infra2.get(nested_array, [])
        if arr1 or arr2:
            combined = (arr1 or []) + (arr2 or [])
            merged_infra[nested_array] = dedup_list(combined)

    if merged_infra:
        merged['infrastructure'] = merged_infra

    return merged
