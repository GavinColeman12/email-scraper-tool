"""
Industry-specific email pattern priors based on aggregated B2B email pattern research.

Sources:
- Hunter.io published pattern distributions
- Industry-specific analyses (dental, medical, legal SMB email data)
- B2B email pattern studies showing size-based distributions

Pattern size bias:
- 1-10 employees: first@ dominates (sole practitioner style)
- 11-50 employees: first.last@ and flast@ emerge as dominant
- 51-500 employees: flast@ peaks (42-45%)
- 500+ employees: first.last@ dominates (48-56%)
"""
from typing import List, Optional, Tuple


# Default ordered priors per industry
# Format: list of (pattern_name, prior_weight) where weight sums to 1.0
# Higher weight = more likely to try first

INDUSTRY_PATTERN_PRIORS = {
    # Healthcare: dental, medical, dermatology
    # Most practices 5-30 staff. first.last@ and flast@ dominate.
    # Dr. prefix is VERY common in dental/medical (~25% of practices)
    "dental": [
        ("first.last", 0.32),
        ("flast", 0.22),
        ("first", 0.18),
        ("drlast", 0.10),
        ("dr.last", 0.08),
        ("firstlast", 0.05),
        ("f.last", 0.03),
        ("last", 0.02),
    ],
    "medical": [
        ("first.last", 0.35),
        ("flast", 0.22),
        ("first", 0.14),
        ("drlast", 0.10),
        ("dr.last", 0.08),
        ("firstlast", 0.05),
        ("f.last", 0.04),
        ("last", 0.02),
    ],
    "dermatology": [
        ("first.last", 0.34),
        ("flast", 0.22),
        ("first", 0.14),
        ("drlast", 0.10),
        ("dr.last", 0.10),
        ("firstlast", 0.05),
        ("f.last", 0.03),
        ("last", 0.02),
    ],
    "chiropractic": [
        ("first", 0.28),
        ("first.last", 0.25),
        ("drlast", 0.15),
        ("dr.last", 0.12),
        ("flast", 0.10),
        ("firstlast", 0.05),
        ("last", 0.03),
        ("f.last", 0.02),
    ],
    "physical_therapy": [
        ("first.last", 0.30),
        ("first", 0.22),
        ("flast", 0.20),
        ("firstlast", 0.10),
        ("last", 0.08),
        ("f.last", 0.05),
        ("drlast", 0.03),
        ("dr.last", 0.02),
    ],
    "urgent_care": [
        ("first.last", 0.40),
        ("flast", 0.25),
        ("first", 0.15),
        ("firstlast", 0.08),
        ("f.last", 0.05),
        ("last", 0.04),
        ("dr.last", 0.02),
        ("drlast", 0.01),
    ],

    # Legal: partners use last name heavily. Law firms skew to formal patterns.
    "law": [
        ("first.last", 0.38),
        ("flast", 0.22),
        ("last", 0.14),
        ("first", 0.10),
        ("firstlast", 0.06),
        ("f.last", 0.05),
        ("lastf", 0.03),
        ("last.first", 0.02),
    ],

    # Medspa: more informal, personal branding common
    "medspa": [
        ("first", 0.30),
        ("first.last", 0.25),
        ("flast", 0.18),
        ("firstlast", 0.10),
        ("last", 0.07),
        ("drlast", 0.05),
        ("dr.last", 0.03),
        ("f.last", 0.02),
    ],

    # Home services: very informal, first-name-only dominates
    "plumbing": [
        ("first", 0.45),
        ("flast", 0.18),
        ("first.last", 0.15),
        ("last", 0.10),
        ("firstlast", 0.06),
        ("f.last", 0.04),
        ("last.first", 0.02),
    ],
    "hvac": [
        ("first", 0.42),
        ("flast", 0.20),
        ("first.last", 0.16),
        ("last", 0.10),
        ("firstlast", 0.07),
        ("f.last", 0.03),
        ("last.first", 0.02),
    ],
    "moving": [
        ("first", 0.40),
        ("flast", 0.22),
        ("first.last", 0.18),
        ("last", 0.10),
        ("firstlast", 0.06),
        ("f.last", 0.03),
        ("last.first", 0.01),
    ],

    # Salons: personal branding, first-name often = brand
    "salon": [
        ("first", 0.48),
        ("first.last", 0.18),
        ("flast", 0.15),
        ("firstlast", 0.08),
        ("last", 0.06),
        ("f.last", 0.03),
        ("last.first", 0.02),
    ],

    # Restaurants, gyms: mixed
    "restaurant": [
        ("first", 0.38),
        ("first.last", 0.22),
        ("flast", 0.18),
        ("firstlast", 0.10),
        ("last", 0.08),
        ("f.last", 0.03),
        ("last.first", 0.01),
    ],
    "gym": [
        ("first", 0.40),
        ("first.last", 0.22),
        ("flast", 0.18),
        ("firstlast", 0.08),
        ("last", 0.08),
        ("f.last", 0.03),
        ("last.first", 0.01),
    ],

    # Auto dealerships
    "auto": [
        ("first.last", 0.32),
        ("flast", 0.25),
        ("first", 0.18),
        ("firstlast", 0.12),
        ("last", 0.06),
        ("f.last", 0.04),
        ("last.first", 0.03),
    ],
}

DEFAULT_PATTERN_PRIOR = [
    ("first.last", 0.30),
    ("flast", 0.22),
    ("first", 0.20),
    ("firstlast", 0.10),
    ("last", 0.08),
    ("f.last", 0.05),
    ("dr.last", 0.03),
    ("last.first", 0.02),
]

# Size-based multipliers applied on top of industry priors
# If we know headcount, shift the distribution
SIZE_MULTIPLIERS = {
    # Solo/micro (1-10): boost first@, penalize formal patterns
    "micro": {"first": 1.8, "first.last": 0.7, "flast": 0.8, "last": 0.6},
    # Small (11-50): balanced
    "small": {"first": 1.0, "first.last": 1.1, "flast": 1.1, "last": 1.0},
    # Medium (51-500): boost formal, penalize first-only
    "medium": {"first": 0.5, "first.last": 1.3, "flast": 1.4, "last": 1.2},
    # Large (500+): strongly formal
    "large": {"first": 0.3, "first.last": 1.6, "flast": 1.2, "last": 1.0},
}


# Vertical aliases — map raw Google Maps business_type strings to our keys
_VERTICAL_ALIASES = {
    # Dental
    "dental clinic": "dental", "dentist": "dental", "dental office": "dental",
    "dental practice": "dental", "orthodontist": "dental",
    "pediatric dentist": "dental", "cosmetic dentist": "dental",
    # Medical
    "medical clinic": "medical", "doctor": "medical", "physician": "medical",
    "family medicine": "medical", "general practitioner": "medical",
    "pediatrician": "medical",
    "dermatologist": "dermatology", "dermatology clinic": "dermatology",
    "chiropractor": "chiropractic", "chiropractic clinic": "chiropractic",
    "physical therapist": "physical_therapy",
    "pt clinic": "physical_therapy", "rehab clinic": "physical_therapy",
    # Legal
    "law firm": "law", "attorney": "law", "lawyer": "law", "law office": "law",
    "legal services": "law", "attorneys": "law", "law firms": "law",
    # Wellness
    "med spa": "medspa", "medical spa": "medspa",
    "aesthetic clinic": "medspa", "medspa": "medspa",
    # Home services
    "plumber": "plumbing", "plumbing service": "plumbing",
    "plumbing contractor": "plumbing",
    "hvac contractor": "hvac", "heating and cooling": "hvac",
    "ac repair": "hvac",
    "moving company": "moving", "movers": "moving",
    # Beauty
    "hair salon": "salon", "beauty salon": "salon",
    "styling studio": "salon",
    # Food
    "restaurant": "restaurant", "eatery": "restaurant",
    "diner": "restaurant", "cafe": "restaurant", "bistro": "restaurant",
    # Fitness
    "gym": "gym", "fitness center": "gym", "fitness studio": "gym",
    # Auto
    "auto repair": "auto", "auto shop": "auto",
    "mechanic": "auto", "car dealership": "auto",
}


def normalize_vertical(raw_type: str) -> str:
    """Map a raw Google Maps business_type string to our standardized vertical key.
    Returns the vertical key, or empty string if no match."""
    if not raw_type:
        return ""
    t = raw_type.lower().strip()
    # Direct match first
    if t in INDUSTRY_PATTERN_PRIORS:
        return t
    if t in _VERTICAL_ALIASES:
        return _VERTICAL_ALIASES[t]
    # Substring match as last resort
    for key in INDUSTRY_PATTERN_PRIORS:
        if key in t:
            return key
    for alias, vertical in _VERTICAL_ALIASES.items():
        if alias in t:
            return vertical
    return ""


def get_patterns_for(industry, headcount=None):
    """
    Return pattern priors for a given industry, adjusted by headcount if known.
    Returns list of (pattern_name, weight) sorted by weight descending.
    """
    industry = (industry or "").lower().strip()
    # Try to normalize raw business_type (e.g. "Dental clinic" → "dental")
    if industry not in INDUSTRY_PATTERN_PRIORS:
        normalized = normalize_vertical(industry)
        if normalized:
            industry = normalized
    priors = INDUSTRY_PATTERN_PRIORS.get(industry, DEFAULT_PATTERN_PRIOR).copy()

    if headcount:
        size_bucket = _size_bucket(headcount)
        multipliers = SIZE_MULTIPLIERS.get(size_bucket, {})
        priors = [
            (pattern, weight * multipliers.get(pattern, 1.0))
            for pattern, weight in priors
        ]

    # Renormalize
    total = sum(w for _, w in priors)
    if total > 0:
        priors = [(p, w / total) for p, w in priors]

    # Sort by weight descending
    priors.sort(key=lambda x: x[1], reverse=True)
    return priors


def _size_bucket(headcount):
    if headcount <= 10:
        return "micro"
    elif headcount <= 50:
        return "small"
    elif headcount <= 500:
        return "medium"
    else:
        return "large"


# Pattern application functions
PATTERN_BUILDERS = {
    "first": lambda f, l: f,
    "last": lambda f, l: l if l else None,
    "first.last": lambda f, l: f"{f}.{l}" if l else None,
    "flast": lambda f, l: f"{f[0]}{l}" if f and l else None,
    "f.last": lambda f, l: f"{f[0]}.{l}" if f and l else None,
    "firstlast": lambda f, l: f"{f}{l}" if l else None,
    "last.first": lambda f, l: f"{l}.{f}" if l else None,
    "lastf": lambda f, l: f"{l}{f[0]}" if f and l else None,
    "dr.last": lambda f, l: f"dr.{l}" if l else None,
    "drlast": lambda f, l: f"dr{l}" if l else None,
}


def build_email(pattern, first, last, domain):
    """Apply a pattern to generate an email address. Returns str or None."""
    builder = PATTERN_BUILDERS.get(pattern)
    if not builder:
        return None
    try:
        f = (first or "").lower().strip()
        l = (last or "").lower().strip()
        if not f:
            return None
        local = builder(f, l)
        if not local:
            return None
        return f"{local}@{domain}".lower()
    except Exception:
        return None
