"""
Volume-mode industry priors.

Strict, intentional, short. Each vertical has 2–3 patterns — the
minimum needed to have a reasonable shot at the right address while
keeping NB budget predictable.

HARD RULE: `{first}@` is NEVER in this table. A bare-first-name
local part is too ambiguous to guess at — it's only acceptable if
literally scraped from the business's own website. The existing
triangulation pipeline may construct it from evidence, but volume
mode's last-resort guess always uses a formal pattern with the
last name attached.

Pattern tokens:
  {first}   first name, lowercase
  {last}    last name, lowercase
  {f}       first initial
  {l}       last initial
"""
from __future__ import annotations

import re
from typing import Optional


# Per-vertical priors. First entry is the primary guess; secondary are
# backup options (not currently used in volume mode — we build ONE
# industry-prior candidate per DM — but kept for future expansion).
INDUSTRY_PRIORS: dict[str, list[str]] = {
    "law":           ["{first}.{last}", "{f}{last}"],
    "dental":        ["{first}.{last}", "{f}{last}", "dr{last}"],
    "medspa":        ["{first}.{last}", "{f}{last}", "{first}{l}"],
    "urgent_care":   ["{first}.{last}", "{f}{last}"],
    "specialty_med": ["{first}.{last}", "{f}{last}"],
    "cpa":           ["{first}.{last}", "{f}{last}", "{last}"],
    "ria":           ["{first}.{last}", "{f}{last}", "{last}"],
    "agency":        ["{first}.{last}", "{f}{last}", "{first}{l}"],
    "construction":  ["{first}.{last}", "{f}{last}", "{last}"],
    "cre":           ["{first}.{last}", "{f}{last}"],
    "manufacturing": ["{first}.{last}", "{f}{last}"],
    "tech_saas":     ["{first}.{last}", "{f}{last}", "{first}{l}"],
    "nonprofit":     ["{first}.{last}", "{f}{last}"],
    "education":     ["{first}.{last}", "{f}{last}"],
}


DEFAULT_PRIOR: list[str] = ["{first}.{last}", "{f}{last}"]


# Map raw Google Maps business_type strings to a vertical key.
# Substring match against any of these triggers the vertical.
_VERTICAL_ALIASES: list[tuple[str, str]] = [
    # Legal
    ("law firm", "law"), ("attorney", "law"), ("lawyer", "law"),
    ("law office", "law"), ("legal services", "law"),
    # Dental
    ("dental", "dental"), ("dentist", "dental"),
    ("orthodontist", "dental"), ("pediatric dent", "dental"),
    ("cosmetic dent", "dental"),
    # Medspa
    ("med spa", "medspa"), ("medical spa", "medspa"), ("medspa", "medspa"),
    ("aesthetic clinic", "medspa"), ("aesthetic", "medspa"),
    ("dermatology", "specialty_med"), ("dermatologist", "specialty_med"),
    # Urgent care
    ("urgent care", "urgent_care"),
    # Specialty medical (catch-all for healthcare that isn't dental / medspa / urgent)
    ("medical clinic", "specialty_med"), ("physician", "specialty_med"),
    ("pediatrician", "specialty_med"), ("chiropractor", "specialty_med"),
    ("physical therap", "specialty_med"), ("family medicine", "specialty_med"),
    ("doctor", "specialty_med"), ("clinic", "specialty_med"),
    ("veterinar", "specialty_med"),
    # Accounting / wealth
    ("cpa", "cpa"), ("accountant", "cpa"), ("accounting firm", "cpa"),
    ("tax prepar", "cpa"), ("bookkeep", "cpa"),
    ("registered investment", "ria"), ("wealth manag", "ria"),
    ("financial advisor", "ria"), ("investment advisor", "ria"),
    # Agency (marketing, design, consulting)
    ("marketing agency", "agency"), ("digital agency", "agency"),
    ("advertising agency", "agency"), ("design agency", "agency"),
    ("consult", "agency"), ("agency", "agency"),
    ("branding", "agency"), ("pr firm", "agency"),
    # Construction / trades (grouped under construction for volume mode)
    ("construction", "construction"), ("contractor", "construction"),
    ("builder", "construction"), ("roofing", "construction"),
    ("plumb", "construction"), ("hvac", "construction"),
    ("electric", "construction"), ("remodel", "construction"),
    ("landscap", "construction"), ("painting", "construction"),
    ("flooring", "construction"),
    # Commercial real estate
    ("commercial real estate", "cre"), ("real estate broker", "cre"),
    ("real estate agency", "cre"), ("property management", "cre"),
    ("realtor", "cre"),
    # Manufacturing
    ("manufactur", "manufacturing"), ("industrial", "manufacturing"),
    ("fabrication", "manufacturing"), ("machine shop", "manufacturing"),
    # Tech / SaaS
    ("software", "tech_saas"), ("saas", "tech_saas"),
    ("technology", "tech_saas"), ("tech company", "tech_saas"),
    ("it services", "tech_saas"), ("cloud", "tech_saas"),
    # Nonprofit
    ("nonprofit", "nonprofit"), ("non-profit", "nonprofit"),
    ("charity", "nonprofit"), ("foundation", "nonprofit"),
    # Education
    ("school", "education"), ("university", "education"),
    ("college", "education"), ("tutoring", "education"),
    ("educational", "education"),
]


def normalize_vertical(raw: str) -> str:
    """
    Map a Google Maps business_type string (like 'Dental clinic',
    'Law firm', 'Marketing agency') to a vertical key in INDUSTRY_PRIORS.
    Returns '' when no known vertical matches — caller uses DEFAULT_PRIOR.
    """
    if not raw:
        return ""
    t = raw.lower().strip()
    # Direct key match
    if t in INDUSTRY_PRIORS:
        return t
    # Substring match (order-preserving — first hit wins; list is
    # ordered to prefer more-specific aliases)
    for alias, vertical in _VERTICAL_ALIASES:
        if alias in t:
            return vertical
    return ""


def get_priors(raw_business_type: str) -> list[str]:
    """Return the (usually 2-3) prior pattern templates for a business."""
    key = normalize_vertical(raw_business_type)
    if key and key in INDUSTRY_PRIORS:
        return INDUSTRY_PRIORS[key]
    return DEFAULT_PRIOR


# ─── Pattern application ──────────────────────────────────────────────

def _slug(s: str) -> str:
    """Lowercase, strip, remove non-alpha. 'José-María' → 'josémaría'."""
    if not s:
        return ""
    # Keep alpha + basic unicode; drop punctuation/whitespace
    return re.sub(r"[^a-z\u00c0-\u024f]", "", s.lower().strip())


def build_email(pattern: str, first: str, last: str, domain: str) -> Optional[str]:
    """
    Apply a pattern template like '{first}.{last}' or '{f}{last}'
    to a name + domain, returning the email address or None if any
    required field is missing.

    Never generates a bare {first}@ email — that pattern is not in
    any INDUSTRY_PRIORS entry, and we hard-reject it here as a
    defence-in-depth against a caller passing '{first}'.
    """
    if pattern == "{first}":
        # Hard guard — bare-first is too ambiguous for a last-resort guess.
        return None
    first_s = _slug(first)
    last_s = _slug(last)
    if not first_s or not last_s:
        return None
    f_initial = first_s[0] if first_s else ""
    l_initial = last_s[0] if last_s else ""
    try:
        local = pattern.format(
            first=first_s,
            last=last_s,
            f=f_initial,
            l=l_initial,
        )
    except (KeyError, IndexError):
        return None
    if not local:
        return None
    return f"{local}@{domain}".lower()
