"""
Central keyword management for consistent matching across all extractors.
Compiled from all original files to maintain consistency.
"""
from urllib.parse import urlparse

# Contact page keywords (from site_crawler.py) 
CONTACT_KEYWORDS = [
    'contact', 'contact us', 'get in touch', 'reach us',
    'enquiry', 'enquire', 'inquiry', 'find us', 'locate us',
    'office address', 'how to reach', 'get in contact'
]

# Product keywords (from site_crawler.py - AUTHORITATIVE)
PRODUCT_KEYWORDS = [
    "products", "our products", "product", "our product", "what we offer", "product categories",
    "our speciality", "speciality", "our expertise", "expertise",
    "solutions", "business and solutions", "what we do",
    "product range", "our range", "portfolio", "job folio",
    "what we make", "what we manufacture", "our offerings",
    "manufacturing", "production", "our work", "catalogue",
    "product catalog", "product catalogue", "our catalog"
]

# Service keywords (from site_crawler.py - AUTHORITATIVE)
SERVICE_KEYWORDS = [
    "services", "our services", "business verticals", "expertise",
    "what we provide", "service offerings", "our solutions", "what we do", "how we help",
    "our capabilities", "service portfolio", "professional services"
]

# Application keywords (from site_crawler.py - AUTHORITATIVE) 
APPLICATION_KEYWORDS = [
    "applications", "application areas", "application industries"
]

# Sector keywords (from site_crawler.py - AUTHORITATIVE)
SECTOR_KEYWORDS = [
    "serving sector", "sectors served", "industries served",
    "serving", "sectors", "industries", "markets", "verticals",
    "industry segments", "market segments", "who we serve", "industry we serve",
    "industries we serve"
]

# Management keywords (from site_crawler.py - AUTHORITATIVE)
MANAGEMENT_KEYWORDS = [
    "management", "team", "leadership", "directors",
    "board", "our founder", "chairman", "ceo", "board of directors", "core team",
    'board', 'directors', 
]

# Client keywords (from site_crawler.py - AUTHORITATIVE)
CLIENT_KEYWORDS = [
    "client", "customers", "our client", "partners", "customer list",
    "who trusts us", "they trust us", "our customers", "clientele",
    "our partners", "trusted by", "our associations"
]

# Infrastructure keywords (COMPILED from site_crawler.py + infrastructure.py + fetcher.py)
INFRASTRUCTURE_KEYWORDS = [
    "infrastructure", "facilities", "our facility", "capabilities",
    "manufacturing", "plant & machinery", "equipment", "machine shop",
    "quality lab", "quality control", "quality assurance", "quality management system",
    "production", "workshop", "factory", "plant", "machinery",
    "machining facility", "forging facility", "testing", "approach", "machine",
    "heat treatment", "quality", "cnc", "lathe", "manufacturing facility", "production facility", "our infrastructure",
    "technical capabilities", "manufacturing capabilities", "our equipment"
]

# Navigation skip terms (compiled from all extractors)
SKIP_NAVIGATION_TERMS = [
     'iso certificate',
    'ped certificate', 'quality certificates' , 'ISO certification' 
    'investors', 'sustainability'
]
UI_SKIP_TERMS = [
    # UI/Marketing elements  
    'subscribe', 'insights', 'newsletter', 'our video',
    'corporate video', 'company video', 'our vision', 'our mission',
    'subscribe to insights', 'subscribe to newsletter',
    "careers","jobs","legal","privacy","terms","cookies","disclaimer",
    "news","blog","events","press","sitemap","media","gallery","login","signup",
    "rss","calendar","search","whatsapp","telegram",'news', 'photos',"blogs",
    
    # Generic sections
    'about us', 'home', 'login', 'register', 'search',
    'privacy', 'terms', 'cookies', 'feedback',
    
    # Footer elements
    'copyright', 'all rights reserved', 'powered by', 'designed by',
    'follow us', 'social media',
]

# Language indicators to avoid (from fetcher.py)
LANGUAGE_SKIP_INDICATORS = [
    '/malayalam/', '/hindi/', '/tamil/', '/telugu/',
    '/chinese/', '/japanese/', '/spanish/', '/french/',
    '?lang=', '&lang=', '/locale/', '/language/',
    'annual', 'report', 'financial', 'investor',
    'shareholder', 'ethics', 'pact', 'grievance',
    'redressal', 'right-to-information', 'rti',
    'tender', 'bidding', 'auction'
]

# Policy and irrelevant page indicators
IRRELEVANT_PAGE_INDICATORS = [
    'policy', 'policies', 'terms', 'privacy',
    'disclaimer', 'legal', 'compliance',
    'gallery', 'photo', 'video', 'media',
    'news', 'event', 'blog', 'article', 'press',
    'award', 'achievement', 'certification',
    'login', 'register', 'account', 'signin',
    'career', 'job', 'recruitment', 'opportunity',
    'vacancy', 'opening', 'hire', 'hiring',
    'download', 'brochure', 'catalog', 'pdf',
    'covid', 'csr', 'sustainability', 'environmental',
    'sitemap', 'search', 'feedback'
]

# Machine terms for infrastructure extraction (from infrastructure.py + implementation guide)
MACHINE_TERMS = [
    "vmc", "cnc", "lathe", "turning center", "milling machine", "shaper",
    "hydraulic press", "hammer", "pneumatic hammer", "forging press",
    "furnace", "pit furnace", "drop bottom furnace", "ring rolling",
    "compressor", "crane", "eot crane", "overhead crane", "spectrometer", 
    "hardness tester", "shot blasting", "robotic fettling", "induction heater", 
    "boring machine", "thread rolling", "straightening machine", "disa", 
    "auto pour", "die casting", "machining center", "radial drill",
    "surface grinder", "cylindrical grinder", "heat treatment",
    "annealing furnace", "normalizing furnace", "tempering furnace",
    "cad", "3d", "testing","die","tools","machining","die and tool",
    "pressing","hammer",
]

# Content deny list (from infrastructure.py implementation guide)
CONTENT_DENY_LIST = [
    "quality policy", "privacy", "terms", "careers", "csr", 
    "navigation", "cookie", "disclaimer", "sitemap",'youtube', 'tenders','tender',
    "privacy policy", "annual report", "financial results","reports",'investors',
    'FINANCIAL_HIGHLIGHTS', 'certifications', 'Safety_policy', "ISO","Ethics", "Pact",
    "Grievance", "Redressal","right-to-information", "RTI"
]
COMPREHENSIVE_MANAGEMENT_TERMS = [
    # English terms
    'director', 'board', 'manager', 'ceo', 'chairman', 'secretary', 'managing director',
    'chief executive', 'president', 'vice president', 'executive director', 'non-executive',
    'independent director', 'founder', 'co-founder', 'partner', 'principal', 'owner',
    'proprietor', 'head', 'lead', 'senior manager', 'general manager', 'deputy manager',
    'assistant manager', 'joint secretary', 'additional secretary', 'joint managing director',
    'whole time director', 'nominee director', 'government nominee', 'promoter director',
    'executive chairman', 'non-executive chairman', 'managing partner', 'senior partner',
    'chief financial officer', 'cfo', 'chief operating officer', 'coo', 'chief technology officer', 'cto',
    'chief marketing officer', 'cmo', 'chief human resources officer',
    # Indian specific terms
    'sri', 'smt', 'shri', 'kumari', 'dr', 'prof', 'adv', 'advocate', 'ca', 'chartered accountant',
    'retd', 'retired', 'ias', 'ips', 'ifs', 'bureaucrat', 'officer',
    'joint secretary finance', 'psu'
]
# Geographic terms for deduplication (from TODO)
GEOGRAPHIC_TERMS = [
    'mumbai', 'delhi', 'chennai', 'pune', 'bangalore', 'kolkata', 'hyderabad',
    'india', 'usa', 'uk', 'uae', 'singapore', 'germany', 'japan',
    'gurgaon', 'noida', 'faridabad', 'nashik', 'coimbatore'
]

SOCIAL_HOSTS = {"linkedin.com","www.linkedin.com","facebook.com","twitter.com","x.com",
                "instagram.com","youtube.com","t.me","wa.me"}
# Merge with existing if they exist
UNIVERSAL_SKIP_TERMS = list(set(
    UI_SKIP_TERMS + 
    CONTENT_DENY_LIST +   # If exists  
    SKIP_NAVIGATION_TERMS # If exists
))
# Utility functions for keyword matching
def contains_keywords(text: str, keywords: list, exact_match: bool = False) -> bool:
    """
    Enhanced keyword matching with exact and fuzzy options.
    """
    if not text or not keywords:
        return False
    
    text_lower = text.lower().strip()
    
    for keyword in keywords:
        keyword_lower = keyword.lower()
        
        if exact_match:
            # Exact phrase match
            if keyword_lower == text_lower:
                return True
        else:
            # Contains match (original behavior)
            if keyword_lower in text_lower:
                return True
    
    return False
def is_mailtel_or_anchor(href: str) -> bool:
    if not href: return True
    h = href.strip().lower()
    return h.startswith("mailto:") or h.startswith("tel:") or h.startswith("#") or "javascript:void" in h

def contains_language_indicator(url: str) -> bool:
    u = (url or "").lower()
    return any(ind in u for ind in LANGUAGE_SKIP_INDICATORS)

def contains_skip_term(path: str) -> bool:
    p = (path or "").lower()
    return any((term or "").lower() in p for term in UNIVERSAL_SKIP_TERMS)

def contains_skip_prefix(path: str) -> bool:
    p = (path or "").lower().strip("/")
    if not p:
        return False
    segments = p.split("/")
    for seg in segments:
        for term in UNIVERSAL_SKIP_TERMS:
            t = (term or "").lower().strip()
            if not t:
                continue
            if seg == t or seg.startswith(t):
                return True
    return False

def same_domain(url: str, base: str) -> bool:
    u, b = urlparse(url), urlparse(base)
    return (u.netloc or b.netloc) == b.netloc

def is_linkedin(url: str) -> bool:
    return urlparse(url).netloc.lower() in SOCIAL_HOSTS and "linkedin" in url.lower()

def contains_keywords_word_boundary(text: str, keywords: list) -> bool:
    """
    Word boundary keyword matching for more precise matching.
    """
    if not text or not keywords:
        return False
    
    import re
    text_lower = text.lower().strip()
    text_words = set(re.findall(r'\b\w+\b', text_lower))
    
    for keyword in keywords:
        keyword_lower = keyword.lower()
        keyword_words = set(re.findall(r'\b\w+\b', keyword_lower))
        
        # If all keyword words are present in text
        if keyword_words.issubset(text_words):
            return True
    
    return False

# Export commonly used keyword sets for backward compatibility
PRODUCT_HEADS = PRODUCT_KEYWORDS  # For existing code compatibility
SERVICE_HEADS = SERVICE_KEYWORDS  # For existing code compatibility
APP_HEADS = APPLICATION_KEYWORDS  # For existing code compatibility
SECTOR_HEADS = SECTOR_KEYWORDS    # For existing code compatibility
MANAGEMENT_KW = MANAGEMENT_KEYWORDS  # For existing code compatibility
CLIENT_KW = CLIENT_KEYWORDS       # For existing code compatibility
INFRA_HEADS = INFRASTRUCTURE_KEYWORDS  # For existing code compatibility
