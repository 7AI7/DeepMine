"""
Enhanced content extraction with 3-level fallback + structured data integration.
Level 1: Trafilatura with table preservation
Level 2: Justext + structured data fallback  
Level 3: Manual cleaning + structured data extraction
Focus: Address data extraction from structured schemas (20% sites coverage)
"""

from __future__ import annotations
import logging
from typing import Optional
from bs4 import BeautifulSoup
from . import settings

log = logging.getLogger("content_extractor")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log.setLevel(logging.INFO)

def _bs_clean(raw_html: str, is_homepage: bool) -> str:
    soup = BeautifulSoup(raw_html or "", "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    for tag in soup.find_all(["header", "nav"]):
        tag.decompose()
    if not is_homepage:
        for tag in soup.find_all("footer"):
            tag.decompose()
    return soup.get_text("\n", strip=True)

def _trafilatura_clean(raw_html: str, output_format: str = 'txt') -> Optional[str]:
    """
    Extract content using Trafilatura.
    
    Args:
        raw_html: Raw HTML content
        output_format: 'txt' or 'markdown' (default: 'txt')
    
    Returns:
        Extracted text/markdown or None if failed
    """
    try:
        import trafilatura
        # favor_precision=False to avoid dropping team/management sections
        txt = trafilatura.extract(
            raw_html or "",
            include_tables=True,
            favor_precision=False,
            output_format=output_format  # Support markdown output
        )
        return (txt or "").strip() or None
    except Exception as e:
        log.debug("Trafilatura not available/failed: %s", e)
        return None
def extract_page_markdown(raw_html: str, url: str, is_homepage: bool) -> str:
    """
    Enhanced extraction that returns markdown instead of plain text.
    
    Fallback order:
    1. Trafilatura (markdown mode)
    2. html-to-markdown
    3. BeautifulSoup with structure markers
    
    Args:
        raw_html: Raw HTML content
        url: Page URL (for logging)
        is_homepage: True if homepage (keeps footer)
    
    Returns:
        Markdown-formatted text with structure preserved
    """
    try:
        if not raw_html:
            log.warning("Empty HTML for url=%s", url)
            return ""
        
        # Try Trafilatura markdown first
        if getattr(settings, "USE_THREE_LAYER_FALLBACK", False):
            t = _trafilatura_clean(raw_html, output_format='markdown')
            if t and len(t) > 80:
                return t
        
        # Try html-to-markdown
        try:
            from html_to_markdown import convert as html_to_md
            md = html_to_md(raw_html)
            if md and len(md.strip()) > 80:
                return md
        except Exception as e:
            log.debug("html-to-markdown failed: %s", e)
        
        # Fallback to BeautifulSoup with structure
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(raw_html, "html.parser")
        
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        for tag in soup.find_all(["header", "nav"]):
            tag.decompose()
        if not is_homepage:
            for tag in soup.find_all("footer"):
                tag.decompose()
        
        parts = []
        for elem in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'li']):
            text = elem.get_text(strip=True)
            if not text:
                continue
            
            tag_name = elem.name
            if tag_name == 'h1':
                parts.append(f"# {text}")
            elif tag_name == 'h2':
                parts.append(f"## {text}")
            elif tag_name == 'h3':
                parts.append(f"### {text}")
            elif tag_name in ['h4', 'h5', 'h6']:
                parts.append(f"#### {text}")
            else:
                parts.append(text)
        
        return "\n\n".join(parts) if parts else soup.get_text("\n", strip=True)
    
    except Exception as e:
        log.exception("extract_page_markdown failed: url=%s err=%s", url, e)
        return raw_html or ""

def _justext_clean(raw_html: str) -> Optional[str]:
    try:
        import justext
        paragraphs = justext.justext(raw_html or "", justext.get_stoplist("English"))
        txt = "\n".join(p.text for p in paragraphs if not p.is_boilerplate)
        return (txt or "").strip() or None
    except Exception as e:
        log.debug("jusText not available/failed: %s", e)
        return None

def extract_page(raw_html: str, url: str, is_homepage: bool) -> str:
    """
    Context-free cleaner.
    - If USE_THREE_LAYER_FALLBACK: try Trafilatura -> jusText -> BeautifulSoup policy cleaner.
    - Else: use only BeautifulSoup policy cleaner.
    Homepage: keep footer; Others: strip footer.
    """
    try:
        if not raw_html:
            log.warning("Empty HTML for url=%s", url)
            return ""
        if getattr(settings, "USE_THREE_LAYER_FALLBACK", False):
            t = _trafilatura_clean(raw_html)
            if t and len(t) > 80:
                return t
            j = _justext_clean(raw_html)
            if j and len(j) > 80:
                return j
        # Policy-enforcing local clean
        cleaned = _bs_clean(raw_html, is_homepage=is_homepage)
        if not cleaned:
            log.warning("No visible text after cleaning for url=%s", url)
        return cleaned
    except Exception as e:
        log.exception("extract_page failed: url=%s err=%s", url, e)
        try:
            return _bs_clean(raw_html or "", is_homepage=is_homepage)
        except Exception:
            return raw_html or ""