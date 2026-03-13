# gemini_prompts.py
# Final optimized version - whole-website extraction with all quality improvements

from pathlib import Path
import json

def get_whole_website_prompt(is_split: bool = False, part_num: int = 1) -> tuple[str, dict]:
    """
    Extraction prompt for WHOLE-WEBSITE extraction (multiple pages concatenated).

    Args:
        is_split: True if this is a split request (>50K GLM or >128K Gemini tokens)
        part_num: 1 or 2 (for split requests only)

    Returns:
        (system_prompt, schema_dict)

    Key changes from per-page version:
    - Input format: Multiple pages with "=== PAGE: {url} ===" delimiters
    - Context: Whole website, not single page
    - Split handling: For large sites (>token limit)
    """

    # Context note based on split status
    if is_split:
        context_note = f"""INPUT CONTEXT:
        This is part {part_num}/2 of a SPLIT extraction for a large website.
        The website exceeded token limits and pages were divided into 2 halves.
        
        IMPORTANT: The content begins with a metadata marker:
        [EXTRACTION_CONTEXT: SPLIT_PART={part_num}/2]
        
        This marker indicates which part of the split you are processing.
        Extract ALL information visible in this part - results will be merged with part {3-part_num}/2.
        DO NOT make assumptions about missing data - it may be in the other part.
        EXTRACT EVERY VISIBLE BIT OF DATA - no data is too small or unimportant.
        Even partial information (incomplete emails, fragmented names, single products) must be captured.
        Do NOT skip anything because it seems incomplete. Raw data wins.
        DO NOT leave fields empty just because you think data might be elsewhere."""
    else:
        context_note = """INPUT CONTEXT:
        If the website has LIMITED pages or MINIMAL content, this is INTENTIONAL.
        Manufacturing/B2B companies often have sparse websites with little information.
        EXTRACT EVERY VISIBLE BIT OF DATA - no data is too small or unimportant.
        Even partial information (incomplete emails, fragmented names, single products) must be captured.
        Do NOT skip anything because it seems incomplete. Raw data wins."""

    system = f"""ROLE: Expert industrial data extractor for markdown-formatted manufacturer pages.

{context_note}

═══════════════════════════
INPUT FORMAT: MARKDOWN TEXT 
═══════════════════════════

{f'''For split extractions, content begins with metadata marker:
[EXTRACTION_CONTEXT: SPLIT_PART={part_num}/2]

This indicates you are processing part {part_num} of a 2-part split.
''' if is_split else ''}Pages are concatenated with URL delimiters:

{f'[EXTRACTION_CONTEXT: SPLIT_PART={part_num}/2]' + chr(10) + chr(10) if is_split else ''}=== PAGE: https://example.com/about ===
[markdown content from about page]

=== PAGE: https://example.com/products ===
[markdown content from products page]

=== PAGE: https://example.com/contact ===
[markdown content from contact page]

Scan ALL pages for data. Different page types contain different information:

- Homepage: Company name, tagline, overview
- About: Company history, infrastructure, certifications
- Products: Product categories, specific products, applications
- Contact: Emails, phones, addresses, contact persons
- Clients/Partners: Client names
- Team/Management: Leadership details
- Infrastructure/Facilities: Machines, capacity, facilities

SPARSE SITE GUIDANCE:
- 1-2 pages: Extract EVERYTHING on those pages
- Minimal contact info: Capture partial phone/email anyway
- Single product: List it even if description is short
- Fragmented names: Extract what's visible, don't FABRICATE
- Lists with items: Take EACH item, including synonyms/variations
- Navigation items: If but no page content, list from nav anyway
- Meta descriptions: Check if company name/info in page titles/descriptions

═══════════════════════
OUTPUT FORMAT
═══════════════════════

Emit pure JSON matching the schema. NO markdown fences, commentary, or extra keys.

Rules:
- Scalars: null if absent (NEVER empty strings)
- Arrays: output if category present (can be empty); null if category entirely absent
- Deduplicate all array items
- Use exact page text; do not fabricate or paraphrase

══════════════════════
COMPANY NAME (CRITICAL - Never miss!)
══════════════════════

Extract from (priority order):
1. First heading (# Company Name) or page title
2. Logo alt text if present in markdown
3. "About" or "About Us" section
4. Copyright text (© 2024 ABC Ltd)
5. Footer text

Extract FULL legal name including Ltd/Pvt Ltd/Pvt. Ltd./Private Limited/Inc/LLC/Corp/Corporation.

Examples:
✓ "ABC Manufacturing Pvt Ltd" (keep full name)
✓ "XYZ Forgings Private Limited" (keep full form)
✗ "ABC Manufacturing" (if "Pvt Ltd" appears elsewhere, include it)

Never null if company name is present anywhere on the website.

═══════════════════════════════
EMAIL & PHONE (CRITICAL - Scan entire markdown!)
═══════════════════════════════

Look in: Contact section, footer, embedded text patterns, ALL pages.

EMAIL patterns to find:
- Keywords: "Email:", "Mail:", "Contact:", "Write to us:", "E-mail:"
- Domains: sales@, info@, contact@, enquiry@, support@, customercare@, marketing@, mail@

PHONE patterns to find:
- Keywords: "Phone:", "Tel:", "Call:", "Mobile:", "Telephone:", "Contact Number:"
- Formats: +91, +1, (123) 456-7890, 123-456-7890, country codes

Multiple values: Join with ", " (comma-space)
Preserve original formatting (spaces/dashes/parentheses/country codes)

Never null if present on page (99% of pages have at least one email or phone).

═════════════════════════════════
CONTACT PERSON (ALL-OR-NOTHING RULE)
═════════════════════════════════

Extract ONLY if ALL 3 present together:
- contact_person_name (person name)
- contact_person_designation (title/role)
- contact_person_contact (direct phone or email)

If ANY missing → SET ALL 3 TO NULL

Examples:
✓ "Contact: John Doe, Sales Manager, +91-9876543210" → extract all 3
✗ "John Doe - CEO" (no direct contact) → all null, add to management instead
✗ "Sales Manager: +91-9876" (no name) → all null

Separation logic:
- All 3 present → contact_person_* fields
- Name+designation only (no contact) → management array
- Generic contact (no name) → company email/phone

═════════════════════════════════════
PRODUCTS, SERVICES, APPLICATIONS, SECTORS (Apply in order)
═════════════════════════════════════

CLASSIFICATION RULES (apply in this order):

1. serving_sector: WHO is served (customer industries)
   Examples: Automotive, Aerospace, Railways, Defense, Oil & Gas, Marine, Mining, Power, Construction, Aviation

2. product_category: WHAT is made broadly (manufacturing families)
   Examples: Forgings, Castings, Machined Components, Precision Parts, Fabricated Structures

3. product: Specific items (the concrete thing you can order)
   Examples: Crankshafts, Gear Blanks, Connecting Rods, Turbine Discs, Model ABC-123

4. application: HOW/WHERE used (end-use context)
   Examples: Heavy-duty trucks, Passenger aircraft, Industrial machinery, Power plants

5. service: Non-product offerings
   Examples: Heat Treatment, CNC Machining, Surface Coating, Testing, Design Engineering

**CRITICAL LIMIT: Extract maximum 100 items for EACH array:**
- product_category: max 100 items
- product: max 100 items
- application: max 100 items
- service: max 100 items
- serving_sector: max 100 items

If more than 100 items exist, prioritize the most prominent/frequently mentioned ones.
This prevents output truncation and ensures complete JSON responses.

COMMON CORRECTIONS:
✗ "Automotive Forgings" → serving_sector: ["Automotive"], product_category: ["Forgings"] 
✗ "Railway Components" → serving_sector: ["Railways"], product_category: ["Components"] 
✗ "Aerospace Castings" → serving_sector: ["Aerospace"], product_category: ["Castings"]

Look in: Headings, section labels, nav menus ("Industries We Serve", "Products", "Services"), lists, paragraphs.

══════════════════════════════════
MACHINES: CRITICAL BRAND/NAME SEPARATION
══════════════════════════════════

RULE: Separate manufacturer brand from machine type/name

Brand = Manufacturer (Zeiss, Haas, DMG Mori, Fanuc, Heller, Okuma, Mazak, Trumpf, Makino)
Name = Machine type/model (CMM, CNC Lathe, Milling Machine, Press, Furnace)

WRONG EXAMPLES (Do NOT do this):
✗ "Zeiss CMM" → machine_name="Zeiss CMM", brand_name="Zeiss"
✗ "Haas CNC Lathe" → machine_name="Haas CNC Lathe", brand_name="Haas"
✗ "DMG Mori 5-axis" → machine_name="DMG Mori 5-axis Milling", brand_name="DMG Mori"

CORRECT EXAMPLES (Do this):
✓ "Zeiss CMM" → machine_name="CMM", brand_name="Zeiss"
✓ "Haas CNC Lathe" → machine_name="CNC Lathe", brand_name="Haas"
✓ "DMG Mori 5-axis Milling" → machine_name="5-Axis Milling Machine", brand_name="DMG Mori"
✓ "Hydraulic Press" (no brand) → machine_name="Hydraulic Press", brand_name=null
✓ "Makino Machining Center" → machine_name="Machining Center", brand_name="Makino"

CAPACITY vs QUANTITY:
- "X Ton/MT/KG" = capacity (NOT qty)
  → capacity_value=X, capacity_unit="Ton/MT/KG", qty=1 (default if count not stated)
- "X machines/units/numbers" = qty=X

Examples:
✓ "16 Ton hydraulic press" → machine_name="Hydraulic Press", qty=1, capacity_value=16.0, capacity_unit="Ton"
✓ "3 hammers of 20 Ton each" → machine_name="Hammer", qty=3, capacity_value=20.0, capacity_unit="Ton"
✓ "5 CNC lathes" → machine_name="CNC Lathe", qty=5, capacity_value=null
✓ "2000 KG melting furnace" → machine_name="Melting Furnace", qty=1, capacity_value=2000.0, capacity_unit="KG"
✓ "Zeiss CMM – 2000mm range" → machine_name="CMM", brand_name="Zeiss", specification="2000mm range"

Include quality/testing equipment: Spectrometer, XRF, Hardness tester, CMM, MPI, Ultrasonic tester, Die Penetrant, Metallography

════════════════════════════
OTHER EXTRACTION RULES
════════════════════════════

ADDRESSES:
Extract ALL with label (Registered Office, Head Office, Corporate Office, Plant, Factory, Works, Unit, Branch)
Include: address (full line), city, state, country, pincode, address_label, location_section. 
In all address column dont include city state country or pincode. there is seperate column for its insertion.

CLIENTS:
Named customers/partners from "Our Clients", "Customers", "Trusted By", logos, case studies
Avoid terms other that are not client names like, logo, img, logo 1, etc. 

MANAGEMENT:
People with titles from "Our Team", "Management", "Leadership", "Board of Directors"
Titles: CEO, MD, Director, VP, GM, Manager, Chairman, Founder, Advisor, President
Do NOT add contact persons (with all 3 fields) here - they go in contact_person_* fields

INFRASTRUCTURE BLOCKS:
Named facilities with area if shown
Examples: "Unit 1", "Forging Plant", "Machining Shop", "Plant A - 50000 sq ft"

OTHER COMPANY FIELDS:
- address/city/state/country: Primary location (Head Office/Registered Office)
- website_last_updated_on_year: 4-digit year if "Last updated" or "© YYYY" shown
- infrastructure_available: true if facilities/plants/units mentioned; false if trading only; null if not mentioned

REMOVED FIELDS (ALWAYS set to null, will be captured manually):
- linkedin_page: Always null
- brochure_link: Always null

══════════════════════════════
FINAL CHECKLIST (Before outputting JSON)
══════════════════════════════

1. ✓ Company name: Checked ALL pages (first heading, logo, about, footer, copyright)?
2. ✓ Email/phone: Scanned entire markdown (contact, footer, embedded patterns)?
3. ✓ Contact person: All 3 fields or all null?
4. ✓ Machines: Brand separated from name? (Zeiss CMM → name="CMM", brand="Zeiss")
5. ✓ Products: Classified correctly (sector ≠ category ≠ product)?
6. ✓ Arrays: Deduplicated?
7. ✓ JSON: Valid with no extra keys?
8. ✓ linkedin_page & brochure_link: Set to null?

DO NOT invent data. If absent, set to null. Output only JSON.
═════════════════════════════
SPARSE WEBSITE FINAL RULE
═════════════════════════════

For websites with 1-3 pages or minimal content:
✓ Extract EVERY visible text fragment (emails, phones, product names, addresses)
✓ Include partial information (incomplete phone, no area code, etc)
✓ Add to arrays even if only 1 item (don't set to null for single items)
✓ Use product/service text AS-IS without consolidation
✓ If nothing found for a category → null (not empty array)

Example sparse site:
"Welcome to ABC Pvt Ltd | Forgings Manufacturer | Call: 9876551654"
→ name: "ABC Pvt Ltd"
→ product_category: ["Forgings"]
→ Don't null anything just because it's sparse - extract what's visible
"""

    schema = {
        "type": "object",
        "properties": {
            "company": {
                "type": ["object", "null"],
                "properties": {
                    "name": {"type": ["string", "null"], "description": "Full legal name with Ltd/Pvt Ltd/Inc/LLC. Never null if present."},
                    "website": {"type": ["string", "null"]},
                    "email": {"type": ["string", "null"], "description": "All emails joined by ', '. Never null if present."},
                    "phone": {"type": ["string", "null"], "description": "All phones joined by ', '. Never null if present."},
                    "address": {"type": ["string", "null"], "description": "Primary address line if presented as a single field"},
                    "city": {"type": ["string", "null"], "description": "City extracted from address blocks"},
                    "state": {"type": ["string", "null"], "description": "State/province extracted from address blocks"}, 
                    "country": {"type": ["string", "null"], "description": "Country extracted from address blocks"},
                    "website_last_updated_on_year": {"type": ["integer", "null"], "description": "year mentioned in Copyright text (© 2024 ABC Ltd), reserved in footer of pages"},
                    "infrastructure_available": {"type": ["boolean", "null"]},
                    "contact_person_name": {"type": ["string", "null"], "description": "All 3 or all null"},
                    "contact_person_designation": {"type": ["string", "null"], "description": "All 3 or all null"},
                    "contact_person_contact": {"type": ["string", "null"], "description": "All 3 or all null"}
                }
            },
            "products": {
                "type": ["object", "null"],
                "properties": {
                    "product_category": {"type": ["array", "null"], "items": {"type": "string"}},
                    "product": {"type": ["array", "null"], "items": {"type": "string"}},
                    "application": {"type": ["array", "null"], "items": {"type": "string"}, "description": "End-uses/contexts (e.g., Heavy-duty trucks, Passenger aircraft)"},
                    "service": {"type": ["array", "null"], "items": {"type": "string"}},
                    "serving_sector": {"type": ["array", "null"], "items": {"type": "string"}}
                }
            },
            "addresses": {
                "type": ["array", "null"],
                "items": {
                    "type": "object",
                    "properties": {
                        "address": {"type": ["string", "null"]},
                        "city": {"type": ["string", "null"]},
                        "state": {"type": ["string", "null"]},
                        "country": {"type": ["string", "null"]},
                        "pincode": {"type": ["string", "null"], "description": "Postal/ZIP code as seen"},
                        "address_label": {"type": ["string", "null"], "description": "Type: Registered Office, Plant, Factory, Works, Branch, Unit, Office"},
                    }
                }
            },
            "clients": {
                "type": ["array", "null"],
                "items": {
                    "type": "object",
                    "properties": {
                        "client_name": {"type": ["string", "null"], "description": "avoid terms like logo, png 1, img1"},
                        "client_location": {"type": ["string", "null"]}
                    }
                }
            },
            "management": {
                "type": ["array", "null"],
                "items": {
                    "type": "object",
                    "properties": {
                        "designation": {"type": ["string", "null"]},
                        "name": {"type": ["string", "null"], "description": "Full name of the person"}
                    }
                }
            },
            "infrastructure": {
                "type": ["object", "null"],
                "properties": {
                    "infrastructure_blocks": {
                        "type": ["array", "null"],
                        "items": {
                            "type": "object",
                            "properties": {
                                "block_name": {"type": ["string", "null"], "description": "Facility/unit/shop name (e.g., Unit 1)"},
                            }
                        }
                    },
                    "machines": {
                        "type": ["array", "null"],
                        "items": {
                            "type": "object",
                            "properties": {
                                "machine_name": {"type": ["string", "null"], "description": "Machine type ONLY, no brand"},
                                "brand_name": {"type": ["string", "null"], "description": "Manufacturer, separate from name"},
                                "qty": {"type": ["integer", "null"], "description": "Machine count if stated (e.g., 3 presses)"},
                                "capacity_value": {"type": ["number", "null"], "description": "Numeric capacity (e.g., 16 Ton → 16.0)"},
                                "capacity_unit": {"type": ["string", "null"], "description": "Unit (e.g., Ton, MT, KG)"},
                                "specification": {"type": ["string", "null"], "description": "Any extra spec (make/model/year/features)"}
                            }
                        }
                    }
                }
            }
        },
        "required": ["company", "products", "addresses", "clients", "management", "infrastructure","machines"]
    }

    return system, schema


def build_prompt_pack_file() -> Path:
    """
    Build prompt pack containing the system and schema for whole-website extraction.
    """
    system, schema = get_whole_website_prompt(is_split=False)
    pack = {"system": system, "schema": schema}
    pack_path = Path("data/cache/prompt_pack_whole_website.json")
    pack_path.parent.mkdir(parents=True, exist_ok=True)
    with open(pack_path, "w", encoding="utf-8") as f:
        json.dump(pack, f, ensure_ascii=False, indent=2)
    return pack_path


# Backward compatibility: keep old function name for existing code
def get_standard_static_context() -> tuple[str, dict]:
    """
    DEPRECATED: Use get_whole_website_prompt() instead.
    Kept for backward compatibility with existing code.
    """
    return get_whole_website_prompt(is_split=False)
