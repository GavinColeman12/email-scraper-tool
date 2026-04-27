"""
universal_pipeline.py — Single-file drop-in for Claude Code.

Industry-agnostic B2B email discovery pipeline with aggressive caching.
Replaces the NPI-only v3 pipeline with a universal system that works for
dental, construction, legal, HVAC, restaurants, and every other B2B vertical.

═══════════════════════════════════════════════════════════════════════════════
COST OPTIMISATIONS vs v4 (what saves you money)
═══════════════════════════════════════════════════════════════════════════════

  1. MERGED SEARCHES    owner_search + press_search → one combined query
                        that parses BOTH names AND @domain emails from the
                        same result set. Saves 1 SearchApi credit / biz.

  2. GATED LINKEDIN     LinkedIn-specific query only fires if the primary
                        Google search didn't already surface linkedin.com/in
                        URLs. Skips ~60% of businesses → saves 0.6 credit / biz.

  3. SQLITE CACHE       Tiered TTLs per data type:
                          • place_id:           90 days
                          • place_details:      30 days
                          • owner_candidates:   30 days
                          • domain_emails:      14 days (scrape + Google)
                          • whois:              90 days
                          • nb_verify:          30 days
                          • detected_pattern:   60 days (per-domain)

                        Re-running the same business = ~$0. New email scraper
                        test runs on old data = FREE. Domain-level results are
                        shared across multiple businesses at the same domain.

  4. NPI FIELD FIX      p.get("number") not p.get("npi"). Fixes the null-npi
                        bug from prior runs. Taxonomy desc parsing for real
                        credentials instead of the "NPI-registered" fallback.

  5. SMTP GATE REMOVED  NeverBounce no longer blocked behind SMTP success.
                        Google Workspace / O365 domains block SMTP probing
                        but NB still works fine — so NB now always runs.

  6. CATCHALL REHAB     NB=catchall flagged as `risky_catchall` with score 55
                        instead of forcing safe_to_send=False. Downstream
                        can decide per-campaign whether to send or suppress.

═══════════════════════════════════════════════════════════════════════════════
COST ESTIMATES
═══════════════════════════════════════════════════════════════════════════════

                        First run      Re-run same biz    1000 biz / month
  v3 (NPI only)         $0.050         $0.050             $50.00
  v4 (universal)        $0.030         $0.030             $30.00
  THIS (cached+merged)  $0.012         $0.000–0.003       $6–12 first month,
                                                            $3–6 every month after

  Breakdown of first-run cost:
    - Owner search (1 merged query)       $0.005
    - LinkedIn (gated, ~40% of biz)        $0.002
    - Domain email search                  $0.005
    - Places Text Search + Details         $0.003 (cached 90d → one-time)
    - NeverBounce                          $0.003
    Total first run:                       ~$0.012–0.018 / business

═══════════════════════════════════════════════════════════════════════════════
INTEGRATION
═══════════════════════════════════════════════════════════════════════════════

  1. Drop this file into your src/ folder as `universal_pipeline.py`
  2. Your existing modules should be importable from the same folder:
       - email_sources.extract_all_hidden_emails (optional)
       - industry_patterns.get_patterns_for / build_email
       - neverbounce.verify
       - email_verifier.verify_smtp
     If any are missing, safe stubs kick in (returns empty / conservative).
  3. Cache DB is created automatically at ./cache/discovery.db (override with
     UNIVERSAL_PIPELINE_CACHE_PATH env var).
  4. Env vars required:
       SEARCHAPI_KEY            — for Google / LinkedIn search
       NEVERBOUNCE_API_KEY      — already set, unchanged
     Optional:
       GOOGLE_PLACES_API_KEY    — enables GMB owner field agent
       UNIVERSAL_PIPELINE_CACHE_PATH — custom cache file location
  5. In your Streamlit bulk-scrape page, add a new mode called
     "Universal (cached)". Call `scrape_with_triangulation(business)`.
  6. To see cache stats in your UI:  `from universal_pipeline import cache_stats`

═══════════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import os
import re
import json
import time
import sqlite3
import hashlib
import logging
import threading
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from typing import Optional, Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# OPTIONAL IMPORTS (safe stubs if user's real modules aren't present)
# ─────────────────────────────────────────────────────────────────────────────

try:
    from .email_sources import extract_all_hidden_emails
except Exception:
    def extract_all_hidden_emails(html: str) -> dict:
        return {}

try:
    from .industry_patterns import get_patterns_for, build_email
except Exception:
    def get_patterns_for(industry: str) -> list[tuple[str, float]]:
        return [("flast", 0.25), ("firstlast", 0.15), ("first.last", 0.35)]

    def build_email(pattern: str, first: str, last: str, domain: str) -> Optional[str]:
        f, l, d = first.lower(), last.lower(), domain.lower()
        if not f or not l or not d:
            return None
        table = {
            "first.last": f"{f}.{l}@{d}",
            "firstlast":  f"{f}{l}@{d}",
            "flast":      f"{f[0]}{l}@{d}",
            "f.last":     f"{f[0]}.{l}@{d}",
            "first":      f"{f}@{d}",
            "last":       f"{l}@{d}",
            "drlast":     f"dr{l}@{d}",
            "dr.last":    f"dr.{l}@{d}",
            "last.first": f"{l}.{f}@{d}",
            "lastf":      f"{l}{f[0]}@{d}",
        }
        return table.get(pattern.lower())

try:
    from .neverbounce import verify as _nb_verify_raw
except Exception:
    class _NBStub:
        def __init__(self): self.result = "unknown"; self.safe_to_send = False
    def _nb_verify_raw(email: str):
        return _NBStub()

try:
    from .email_verifier import verify_smtp as _verify_smtp_raw
except Exception:
    def _verify_smtp_raw(email: str, timeout: int = 8) -> dict:
        return {"status": "unknown", "catchall": False}


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 1: DATA MODELS
# ═════════════════════════════════════════════════════════════════════════════

@dataclass
class OwnerCandidate:
    full_name: str
    first_name: str
    last_name: str
    title: str = ""
    source: str = ""
    source_url: str = ""
    confidence: int = 0
    raw_snippet: str = ""


@dataclass
class DetectedPattern:
    pattern_name: str
    confidence: int
    evidence_emails: list[str]
    evidence_names: list[str]
    method: str = "triangulation"


@dataclass
class TriangulationResult:
    decision_maker: Optional[OwnerCandidate] = None
    all_owners: list[OwnerCandidate] = field(default_factory=list)
    detected_pattern: Optional[DetectedPattern] = None

    best_email: Optional[str] = None
    best_email_confidence: int = 0
    best_email_evidence: list[str] = field(default_factory=list)
    safe_to_send: bool = False
    risky_catchall: bool = False

    candidate_emails: list[dict] = field(default_factory=list)
    agents_run: list[str] = field(default_factory=list)
    agents_succeeded: list[str] = field(default_factory=list)
    cache_hits: list[str] = field(default_factory=list)
    time_seconds: float = 0.0
    cost_estimate: float = 0.0
    cost_saved_by_cache: float = 0.0

    evidence_trail: dict = field(default_factory=dict)
    debug: dict = field(default_factory=dict)


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 2: SQLITE CACHE LAYER
# ═════════════════════════════════════════════════════════════════════════════
#
# Tiered TTLs. Schema is plain: (cache_key, namespace, value_json, expires_at).
# Thread-safe via a single lock — SQLite handles concurrent reads fine, writes
# are rare (just post-fetch) so a single lock is fine at our volume.
# ═════════════════════════════════════════════════════════════════════════════

class _Cache:
    DEFAULT_TTLS = {
        "place_id":          90 * 86400,   # business location rarely moves
        "place_details":     30 * 86400,
        "owner_candidates":  30 * 86400,   # owners change slowly
        "domain_emails":     14 * 86400,   # websites update occasionally
        "whois":             90 * 86400,   # registrant rarely changes
        "nb_verify":         30 * 86400,   # email validity stable ~30d
        "detected_pattern":  60 * 86400,   # once a domain's pattern is known
        "smtp_probe":         7 * 86400,
    }
    # Approximate USD cost we'd have paid if not cached (for "saved" reporting)
    COST_MAP = {
        "place_id":          0.017,
        "place_details":     0.017,
        "owner_candidates":  0.010,
        "domain_emails":     0.005,
        "whois":             0.000,
        "nb_verify":         0.003,
        "detected_pattern":  0.000,
        "smtp_probe":        0.000,
    }

    def __init__(self, path: Optional[str] = None):
        self.path = path or os.getenv(
            "UNIVERSAL_PIPELINE_CACHE_PATH",
            os.path.join(os.getcwd(), "cache", "discovery.db"),
        )
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._lock = threading.Lock()
        self._init_schema()

    def _init_schema(self):
        with self._lock, sqlite3.connect(self.path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cache (
                    key         TEXT NOT NULL,
                    namespace   TEXT NOT NULL,
                    value_json  TEXT NOT NULL,
                    expires_at  INTEGER NOT NULL,
                    created_at  INTEGER NOT NULL,
                    PRIMARY KEY (key, namespace)
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_expires ON cache(expires_at)")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cache_stats (
                    namespace TEXT PRIMARY KEY,
                    hits      INTEGER DEFAULT 0,
                    misses    INTEGER DEFAULT 0,
                    saved_usd REAL DEFAULT 0.0
                )
            """)
            conn.commit()

    @staticmethod
    def _hash_key(parts: list[str]) -> str:
        raw = "|".join(p.strip().lower() for p in parts if p)
        return hashlib.sha256(raw.encode()).hexdigest()[:32]

    def get(self, namespace: str, *parts) -> Optional[Any]:
        key = self._hash_key(list(parts))
        now = int(time.time())
        with self._lock, sqlite3.connect(self.path) as conn:
            row = conn.execute(
                "SELECT value_json FROM cache WHERE key=? AND namespace=? AND expires_at>?",
                (key, namespace, now),
            ).fetchone()
            if row:
                self._bump_stats(conn, namespace, hit=True)
                conn.commit()
                try:
                    return json.loads(row[0])
                except Exception:
                    return None
            self._bump_stats(conn, namespace, hit=False)
            conn.commit()
        return None

    def set(self, namespace: str, value: Any, *parts, ttl: Optional[int] = None):
        key = self._hash_key(list(parts))
        now = int(time.time())
        expires = now + (ttl or self.DEFAULT_TTLS.get(namespace, 86400))
        with self._lock, sqlite3.connect(self.path) as conn:
            conn.execute(
                """INSERT OR REPLACE INTO cache
                   (key, namespace, value_json, expires_at, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (key, namespace, json.dumps(value, default=str), expires, now),
            )
            conn.commit()

    def _bump_stats(self, conn, namespace: str, hit: bool):
        if hit:
            cost_saved = self.COST_MAP.get(namespace, 0.0)
            conn.execute(
                """INSERT INTO cache_stats(namespace, hits, misses, saved_usd)
                   VALUES (?, 1, 0, ?)
                   ON CONFLICT(namespace) DO UPDATE SET
                       hits = hits + 1,
                       saved_usd = saved_usd + excluded.saved_usd""",
                (namespace, cost_saved),
            )
        else:
            conn.execute(
                """INSERT INTO cache_stats(namespace, hits, misses, saved_usd)
                   VALUES (?, 0, 1, 0)
                   ON CONFLICT(namespace) DO UPDATE SET misses = misses + 1""",
                (namespace,),
            )

    def stats(self) -> dict:
        with self._lock, sqlite3.connect(self.path) as conn:
            rows = conn.execute(
                "SELECT namespace, hits, misses, saved_usd FROM cache_stats"
            ).fetchall()
            total_rows = conn.execute("SELECT COUNT(*) FROM cache").fetchone()[0]
            expired = conn.execute(
                "SELECT COUNT(*) FROM cache WHERE expires_at < ?",
                (int(time.time()),),
            ).fetchone()[0]
        by_ns = {
            ns: {
                "hits": h,
                "misses": m,
                "hit_rate": round(h / (h + m), 3) if (h + m) else 0.0,
                "saved_usd": round(saved, 4),
            }
            for ns, h, m, saved in rows
        }
        total_saved = sum(v["saved_usd"] for v in by_ns.values())
        return {
            "total_cached_entries": total_rows,
            "expired_entries": expired,
            "total_saved_usd": round(total_saved, 2),
            "by_namespace": by_ns,
        }

    def purge_expired(self) -> int:
        with self._lock, sqlite3.connect(self.path) as conn:
            cur = conn.execute(
                "DELETE FROM cache WHERE expires_at < ?", (int(time.time()),)
            )
            conn.commit()
            return cur.rowcount


# Singleton cache accessor
_cache_instance: Optional[_Cache] = None
_cache_lock = threading.Lock()


def get_cache() -> _Cache:
    global _cache_instance
    if _cache_instance is None:
        with _cache_lock:
            if _cache_instance is None:
                _cache_instance = _Cache()
    return _cache_instance


def cache_stats() -> dict:
    """Public helper: expose cache stats to your UI."""
    return get_cache().stats()


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 3: CONSTANTS / HELPERS
# ═════════════════════════════════════════════════════════════════════════════

OWNER_TITLES = {
    "owner": 25, "founder": 25, "co-founder": 24, "cofounder": 24,
    "ceo": 23, "chief executive": 23, "president": 20, "principal": 20,
    "managing partner": 20, "managing member": 18, "managing director": 18,
    "partner": 15, "proprietor": 22, "director": 10, "dds": 8, "dmd": 8,
    "md": 8, "dvm": 8, "esq": 8,
}

GENERIC_LOCALS = {
    "info", "hello", "contact", "admin", "office", "team", "help",
    "support", "sales", "billing", "reception", "appointments",
    "booking", "inquiries", "service", "services", "customer",
}

STOPWORD_NAMES = {
    # Medical / dental
    "dental", "dentistry", "medical", "clinic", "care", "wellness",
    "smile", "smiles", "family", "pediatric", "orthodontic", "oral",
    # Legal (was MISSING — caused "Injury Attorneys", "Spodek Law",
    # "Manhattan Legal", "Law Tsigler" to pass validation)
    "law", "legal", "attorney", "attorneys", "lawyer", "lawyers",
    "injury", "criminal", "divorce", "immigration", "estate",
    "counsel", "advocate", "advocates", "firm", "esq", "esquire",
    # Construction / trades / real estate (missing — caused "Best
    # Contractor", "Bedroom Apartments", "Homes Our")
    "construction", "builders", "builder", "contracting", "contractor",
    "contractors", "plumbing", "electric", "electrical", "hvac",
    "roofing", "landscaping", "renovation", "renovations",
    "apartments", "apartment", "bedroom", "bathroom", "kitchen",
    "homes", "housing", "property", "properties", "real", "realty",
    "development", "developer", "developments", "frontier",
    # Generic business / UI text
    "center", "centre", "group", "associates", "practice", "office",
    "services", "service", "solutions", "company", "corporation",
    "llc", "inc", "pllc", "pc", "ltd", "corp",
    # Website / navigation / generic content
    "about", "contact", "team", "home", "staff", "patients", "clients",
    "privacy", "terms", "copyright", "reserved", "welcome", "menu",
    "search", "send", "email", "visit", "find", "call", "now",
    "online", "website", "forbes", "business", "phone", "number",
    "announces", "interim", "updated", "chief", "executive", "principal",
    "president", "vice", "director", "managing", "owner", "founder",
    "partner", "ceo", "cfo", "coo", "cto", "cmo",
    # Articles / pronouns / common English words (caused "The Rushmore",
    # "Homes Our", "Are History", "Our Work", "Our Process")
    "the", "our", "your", "my", "his", "her", "their", "this", "that",
    "these", "those", "a", "an", "are", "is", "was", "were", "has",
    "have", "had", "be", "been", "being", "you", "we", "they",
    # Question / conjunction words (caused "When Laurence" — partial
    # sentence fragment like "When Laurence Brooks founded...")
    "when", "where", "who", "what", "why", "how", "which",
    # Adjectives / superlatives common in business names and SEO copy
    "best", "top", "new", "old", "great", "good", "premium", "premier",
    "luxury", "highly", "engaged", "leading", "trusted", "expert",
    "experienced", "featured", "visionary", "combined", "trade",
    "highly", "more", "most", "very", "much",
    # Places / neighborhoods often mistaken for surnames
    "village", "villa", "district", "borough", "neighborhood",
    "downtown", "uptown", "midtown", "hills", "heights", "park",
    "grove", "lake", "long", "short", "view", "viewed",
    # Months + days (caused "Updated March", "April Showers", etc.)
    "january", "february", "march", "april", "may", "june", "july",
    "august", "september", "october", "november", "december",
    "monday", "tuesday", "wednesday", "thursday", "friday", "saturday",
    "sunday", "today", "tomorrow", "yesterday",
    # Misc UI/navigation words observed in real scraper output
    "customer", "customers", "employee", "employees", "directory",
    "leader", "leadership", "recognition", "aviation", "civil",
    "government", "healthcare", "sectors", "market", "markets",
    "partnering", "opportunities", "trade", "year", "history",
    "ethics", "safety", "repairs", "insurance", "claims", "process",
    "method", "methods", "values", "core", "original", "related",
    "companies", "corp", "work", "works", "project", "projects",
    "news", "mesh", "hudson", "yards", "instagram", "facebook",
    "twitter", "linkedin", "yelp", "email", "phone",
    # Search #28 additions — more verbs/adjectives that got through
    "get", "see", "named", "cool", "kids", "already", "started",
    "working", "too", "late", "continue", "reading", "read", "less",
    "request", "certified", "free", "consultation", "consultations",
    "social", "media", "blog", "blogs", "post", "posts",
    # Legal/PI industry terms frequently caught as "names"
    "client", "clients", "testimonials", "testimonial", "testimony",
    "car", "cars", "vehicle", "vehicles", "accident", "accidents",
    "bicycle", "motorcycle", "truck", "trucks", "pedestrian",
    "premises", "liability", "liabilities",
    # Marketing copy phrases
    "straightforward", "personalized", "advocacy", "results",
    "continue", "working", "already", "too", "late", "combined",
    "experience", "trusted", "dedicated", "personal", "attention",
    # Geographic + demographic
    "county", "state", "bar", "city", "town", "mayor", "council",
    "member", "members", "citizen", "citizens", "resident", "residents",
    "texans", "served", "veterans", "navy", "army", "police", "turned",
    "association", "homeowners", "free",
    # Honorific/rank qualifiers
    "high", "net", "worth", "worthy", "honorable", "honored",
    "adjunct", "professor", "assistant",
    # Directional + highway/city terms (caught "North Mopac", "San Marcos")
    "north", "south", "east", "west", "northeast", "northwest",
    "southeast", "southwest", "mopac", "interstate", "highway",
    "expressway", "expy", "parkway", "turnpike", "freeway",
    "san", "marcos", "antonio", "francisco", "diego", "jose",
    "santa", "fort", "lake", "port", "saint", "mount",
    # Geographic (US states + major cities frequently scraped as "names")
    "york", "brooklyn", "queens", "bronx", "manhattan", "staten",
    "boston", "chicago", "seattle", "portland", "denver", "austin",
    "dallas", "houston", "phoenix", "atlanta", "miami", "tampa",
    "united", "states", "usa", "america", "american",
    "alabama", "alaska", "arizona", "arkansas", "california",
    "colorado", "connecticut", "delaware", "florida", "georgia",
    "hawaii", "idaho", "illinois", "indiana", "iowa", "kansas",
    "kentucky", "louisiana", "maine", "maryland", "massachusetts",
    "michigan", "minnesota", "mississippi", "missouri", "montana",
    "nebraska", "nevada", "hampshire", "jersey", "mexico",
    "carolina", "dakota", "ohio", "oklahoma", "oregon",
    "pennsylvania", "rhode", "tennessee", "texas", "utah",
    "vermont", "virginia", "washington", "wisconsin", "wyoming",
    # Streets / generic location words
    "street", "avenue", "boulevard", "road", "suite", "floor",
    # UI/SEO spam
    "experienced", "special", "review", "reviews", "meet", "like",
    "comment", "share", "search", "profile", "profiles", "listing",
    "listings",
}

# Common English words that are sometimes used as surnames but NEVER as
# first names. If the first OR last name token IS one of these alone,
# reject — it's almost certainly extracted UI text, not a person.
NEVER_A_PERSON = {
    "law", "legal", "email", "menu", "search", "send", "visit", "call",
    "find", "now", "home", "about", "contact", "team", "staff",
    "meet", "review", "reviews", "center", "office", "services",
    "announces", "interim", "executive", "chief", "vice", "partner",
    "managing", "owner", "founder", "president", "director", "principal",
    "attorney", "attorneys", "lawyer", "lawyers", "esq", "esquire",
    "law", "legal", "firm", "group", "company", "united", "states",
    "forbes", "business", "phone", "number",
}

HEADERS = {
    # Real Chrome UA — some sites block custom UAs with stricter WAF rules,
    # costing us real data. Impersonating Chrome avoids those blocks without
    # any downside.
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
NAME_RE = re.compile(r"\b(?:Dr\.?\s+)?([A-Z][a-z]{2,}(?:\s+[A-Z]\.)?\s+[A-Z][a-z]{2,})\b")
STATE_RE = re.compile(r",\s*([A-Z]{2})\s+\d{5}")


def _norm_name(n: str) -> str:
    return re.sub(r"[^a-z]", "", n.lower())


def _strip_html(html: str) -> str:
    html = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.S | re.I)
    html = re.sub(r"<[^>]+>", " ", html)
    return re.sub(r"\s+", " ", html)


def _extract_state(addr: str) -> str:
    m = STATE_RE.search(addr or "")
    return m.group(1) if m else ""


def _extract_city_state(addr: str) -> tuple[str, str]:
    m = re.search(r",\s*([^,]+),\s*([A-Z]{2})\s+\d{5}", addr or "")
    if m:
        return m.group(1).strip(), m.group(2)
    return "", _extract_state(addr)


def _extract_postal_code(addr: str) -> str:
    m = re.search(r"\b(\d{5})(?:-\d{4})?\b", addr or "")
    return m.group(1) if m else ""


def _is_wrong_territory(
    cand: "OwnerCandidate", business_name: str, domain: str, address: str,
) -> bool:
    """
    Reject candidates whose source_url is on an UNRELATED domain AND
    whose source URL doesn't carry any token from the business address
    (city/state or long business-name tokens). Defense-in-depth against
    cases where the location-anchored query still returns an off-territory
    hit — e.g., "Manhattan Dental" in Manhattan, MT attributed to
    "One Manhattan Dental" in NYC.
    """
    src = (cand.source_url or "").lower()
    if not src:
        return False
    if domain and domain.lower() in src:
        return False  # same registrable domain as the business → trust it
    # LinkedIn profile slugs virtually never carry geographic tokens —
    # exempting them here preserves the primary owner-sourcing channel.
    # A bad LinkedIn attribution is caught by the name_classifier + synthesis
    # layers, not this filter.
    if "linkedin.com/in/" in src:
        return False
    city, state = _extract_city_state(address or "")
    tokens: set[str] = set()
    if city:
        tokens.add(city.lower().replace(" ", ""))
        tokens.add(city.lower().replace(" ", "-"))
    if state:
        tokens.add(state.lower())
    # Long tokens from the business name (4+ chars) — "manhattan" alone
    # isn't specific enough to anchor, but the state token above makes it
    # specific ("mt" present in onemanhattandental.com? no → rejects).
    for tok in re.findall(r"[A-Za-z]{4,}", business_name or ""):
        tokens.add(tok.lower())
    # Require state OR (city AND a business-name token) to be in source URL
    if state and state.lower() in src:
        return False
    city_tok = city.lower().replace(" ", "") if city else ""
    if city_tok and city_tok in src:
        # Only count a city token if a business-name token also matches,
        # avoids "Manhattan" matching onemanhattandental.com
        for tok in re.findall(r"[A-Za-z]{4,}", business_name or ""):
            if tok.lower() != city_tok and tok.lower() in src:
                return False
    return True  # no territory signal in source_url → off-territory


def _is_junk_name(name: str, business_name: str = "") -> bool:
    """
    Reject anything that's clearly NOT a person's name.

    Rules (fail-fast — any one rejects):
      1. Fewer than 2 tokens OR any token shorter than 2 chars.
      2. ANY token appears in STOPWORD_NAMES (industry terms, cities,
         UI text, generic business words). A real person's name
         shouldn't have "Attorneys", "Law", "Manhattan" in it.
      3. First OR last name alone is in NEVER_A_PERSON (words that
         are commonly scraped as names but never actually ARE names:
         "Law", "Menu", "Search", role titles, etc.).
      4. All tokens appear somewhere in the business name (then this
         is a business-name fragment, not a person — e.g., "Spodek
         Law" at "Spodek Law Group" or "Injury Attorneys" at "NYC
         Injury Attorneys P.C.").

    business_name is optional for backward compat but should be
    passed whenever available.
    """
    tokens = name.lower().split()
    if len(tokens) < 2:
        return True
    if any(len(t) < 2 for t in tokens):
        return True
    if any(t in STOPWORD_NAMES for t in tokens):
        return True
    # NEVER_A_PERSON check — first OR last alone disqualifies
    if tokens[0] in NEVER_A_PERSON or tokens[-1] in NEVER_A_PERSON:
        return True
    # Business-name-fragment check. Fires ONLY when every token is in
    # the business name AND at least one of those tokens is also a
    # stopword. This catches "Spodek Law" / "Injury Attorneys" /
    # "Manhattan Legal" (stopword + surname/adjective from the biz name)
    # while still ACCEPTING owner-operators whose real name happens
    # to match the business name: "Daniel Clement" at "Clement Law",
    # "Todd Spodek" at "Spodek Law Group", "Robert Tsigler" at
    # "Law Offices of Robert Tsigler". Owner-match is the STRONGEST
    # signal they actually are the owner — don't reject it.
    if business_name:
        biz_tokens = set(re.findall(r"[a-z]+", business_name.lower()))
        if (biz_tokens
            and all(t in biz_tokens for t in tokens)
            and any(t in STOPWORD_NAMES for t in tokens)):
            return True
    return False


def _title_weight(title: str) -> int:
    if not title:
        return 0
    t = title.lower()
    return max((w for k, w in OWNER_TITLES.items() if k in t), default=5)


def _extract_title(text: str) -> str:
    t = text.lower()
    best = ("", 0)
    for title, weight in OWNER_TITLES.items():
        if title in t and weight > best[1]:
            best = (title, weight)
    if not best[0]:
        verb_to_role = {
            "owns": "Owner", "runs": "Owner", "manages": "Owner",
            "founded": "Founder", "established": "Founder", "started": "Founder",
            "launched": "Founder", "opened": "Founder",
            "leads": "President", "heads": "President",
        }
        for verb, role in verb_to_role.items():
            if verb in t:
                return role
    return best[0].title() if best[0] else ""


def _parse_name(raw: str, source: str, title: str = "",
                business_name: str = "") -> Optional[OwnerCandidate]:
    if not raw:
        return None
    clean = re.sub(r",?\s*(DDS|DMD|MD|DO|PhD|Esq|PA|LLC|Inc\.?)\.?\s*$", "", raw.strip(), flags=re.I)
    clean = re.sub(r"^(Dr\.?|Mr\.?|Mrs\.?|Ms\.?|Miss)\s+", "", clean, flags=re.I)
    # Strip trailing ellipsis / truncation markers from malformed
    # snippets (e.g., "Stephen ..." from "Stephen ... Founder").
    clean = re.sub(r"\s*\.{2,}\s*$", "", clean)
    parts = [p for p in clean.split() if p]
    parts = [p for p in parts if not re.match(r"^[A-Z]\.?$", p)]
    if len(parts) < 2:
        return None
    first, last = parts[0], parts[-1]
    # Both first and last name must be purely alphabetic (allowing
    # hyphenated last names). Rejects "..." and other punctuation junk.
    if not re.match(r"^[A-Za-z][A-Za-z'\-]*$", first):
        return None
    if not re.match(r"^[A-Za-z][A-Za-z'\-]*$", last):
        return None
    if _is_junk_name(f"{first} {last}", business_name=business_name):
        return None
    return OwnerCandidate(
        full_name=f"{first} {last}",
        first_name=first, last_name=last,
        title=title, source=source,
    )


def _extract_names_with_titles(
    text: str, window: int = 80, business_name: str = ""
) -> list[OwnerCandidate]:
    out: list[OwnerCandidate] = []
    if not text:
        return out
    for m in NAME_RE.finditer(text):
        name = m.group(1).strip()
        if _is_junk_name(name, business_name=business_name):
            continue
        start = max(0, m.start() - window)
        end = min(len(text), m.end() + window)
        context = text[start:end].lower()
        title = _extract_title(context)
        if not title:
            continue
        parsed = _parse_name(name, source="", title=title,
                             business_name=business_name)
        if parsed:
            parsed.raw_snippet = context[:200]
            out.append(parsed)
    return out


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 4: OWNER DISCOVERY AGENTS (with caching)
# ═════════════════════════════════════════════════════════════════════════════

def _agent_combined_owner_and_press(
    business_name: str, domain: str, address: str, cache: _Cache
) -> tuple[list[OwnerCandidate], list[str], list[str]]:
    """
    MERGED AGENT (was 2 searches in v4, now 1).

    One SearchApi query surfaces:
      - owner / founder names (via title-keyword extraction from snippets)
      - @domain emails (via regex across same snippets)
      - LinkedIn URLs (used by the gating logic to skip linkedin_via_google)

    The query is location-anchored (city + state) so "Manhattan Dental" in
    Montana doesn't get attributed to "One Manhattan Dental" in NYC. The
    cache key includes city|state too so the MT result and the NYC result
    don't collide.

    Saves ~1 SearchApi credit per business.
    """
    city, state = _extract_city_state(address or "")
    cache_geo = f"{city}|{state}"
    cached = cache.get("owner_candidates", "combined_v2", business_name, domain, cache_geo)
    if cached is not None:
        raw = [OwnerCandidate(**c) for c in cached.get("candidates", [])]
        candidates = [
            c for c in raw
            if not _is_junk_name(c.full_name, business_name=business_name)
        ]
        if raw and not candidates:
            logger.info(
                f"cache miss-through: all {len(raw)} cached owner "
                f"candidates for {business_name!r} failed validation; "
                f"re-querying"
            )
        else:
            return (candidates, cached.get("emails", []),
                    cached.get("linkedin_urls", []))

    api_key = os.getenv("SEARCHAPI_KEY")
    if not api_key:
        return [], [], []

    loc_clause = ""
    if city and state:
        loc_clause = f' "{city}, {state}"'
    elif state:
        loc_clause = f' "{state}"'
    role_clause = "(owner OR founder OR CEO OR president OR principal)"
    if domain:
        query = f'"{business_name}"{loc_clause} {role_clause} (email OR "@{domain}")'
    else:
        query = f'"{business_name}"{loc_clause} {role_clause} email'
    try:
        resp = requests.get(
            "https://www.searchapi.io/api/v1/search",
            params={"q": query, "engine": "google", "num": 10, "api_key": api_key},
            timeout=15,
        )
        data = resp.json()
    except Exception as e:
        logger.warning(f"combined search: {e}")
        return [], [], []

    candidates: list[OwnerCandidate] = []
    emails: set[str] = set()
    linkedin_urls: list[str] = []

    email_pat = re.compile(r"[A-Za-z0-9._%+-]+@" + re.escape(domain), re.I) if domain else None

    for r in data.get("organic_results", []):
        url = r.get("link", "") or ""
        blob = " ".join(filter(None, [r.get("title"), r.get("snippet")]))

        if "linkedin.com/in/" in url:
            linkedin_urls.append(url)
            # LinkedIn title format: "John Smith - Owner - Smith Dental | LinkedIn"
            name_part = r.get("title", "").split(" - ")[0].split(" | ")[0].strip()
            name_part = re.sub(r"\s*\(.*?\)\s*$", "", name_part)
            parsed = _parse_name(name_part, source="google_owner_search",
                                 title=_extract_title(blob),
                                 business_name=business_name)
            if parsed:
                parsed.source_url = url
                parsed.raw_snippet = blob[:200]
                candidates.append(parsed)
            continue

        for cand in _extract_names_with_titles(blob, business_name=business_name):
            cand.source = "google_owner_search"
            cand.source_url = url
            cand.raw_snippet = blob[:200]
            candidates.append(cand)

        if email_pat:
            from src.email_scraper import _is_rejected
            emails.update(
                m.lower() for m in email_pat.findall(blob) if not _is_rejected(m)
            )

    cache.set(
        "owner_candidates",
        {
            "candidates": [asdict(c) for c in candidates],
            "emails": list(emails),
            "linkedin_urls": linkedin_urls,
        },
        "combined_v2", business_name, domain, cache_geo,
    )
    return candidates, list(emails), linkedin_urls


def _agent_linkedin_gated(
    business_name: str, already_have_linkedin: bool, cache: _Cache
) -> list[OwnerCandidate]:
    """Only fires if the combined search didn't already surface LinkedIn results."""
    if already_have_linkedin:
        return []

    cached = cache.get("owner_candidates", "linkedin", business_name)
    if cached is not None:
        raw = [OwnerCandidate(**c) for c in cached]
        filt = [c for c in raw
                if not _is_junk_name(c.full_name, business_name=business_name)]
        # Fall through to fresh query if everything cached is now garbage
        if not (raw and not filt):
            return filt

    api_key = os.getenv("SEARCHAPI_KEY")
    if not api_key:
        return []

    query = f'site:linkedin.com/in "{business_name}" (owner OR founder OR CEO OR principal)'
    try:
        resp = requests.get(
            "https://www.searchapi.io/api/v1/search",
            params={"q": query, "engine": "google", "num": 10, "api_key": api_key},
            timeout=15,
        )
        data = resp.json()
    except Exception as e:
        logger.warning(f"linkedin gated: {e}")
        return []

    candidates: list[OwnerCandidate] = []
    for r in data.get("organic_results", []):
        title = r.get("title", "") or ""
        snippet = r.get("snippet", "") or ""
        url = r.get("link", "") or ""
        name_part = title.split(" - ")[0].split(" | ")[0].strip()
        name_part = re.sub(r"\s*\(.*?\)\s*$", "", name_part)
        parsed = _parse_name(name_part, source="linkedin_via_google",
                             title=_extract_title(title + " " + snippet),
                             business_name=business_name)
        if parsed:
            parsed.source_url = url
            parsed.raw_snippet = (title + " :: " + snippet)[:200]
            candidates.append(parsed)

    cache.set("owner_candidates", [asdict(c) for c in candidates],
              "linkedin", business_name)
    return candidates


def _agent_website_scrape(
    website: str, domain: str, business_name: str, cache: _Cache
) -> tuple[list[OwnerCandidate], list[str]]:
    """Scrape /about /team etc. for names + @domain emails. Cached per-domain."""
    if not website:
        return [], []

    cached = cache.get("domain_emails", "scrape", domain)
    if cached is not None:
        raw = [OwnerCandidate(**c) for c in cached.get("candidates", [])]
        candidates = [
            c for c in raw
            if not _is_junk_name(c.full_name, business_name=business_name)
        ]
        # Lazy re-filter cached emails against the current PLACEHOLDER_LOCALS
        # rules so old entries holding "first@domain" template leaks get
        # scrubbed on next read without a cache bust.
        from src.email_scraper import _is_rejected
        cached_emails = [e for e in cached.get("emails", []) if not _is_rejected(e)]
        # If ALL cached names are junk, don't just return emails with an
        # empty candidate list — re-scrape the website and get fresh
        # candidates. Website scrape is FREE (no API credit), so the
        # cost of the retry is just bandwidth.
        if not (raw and not candidates):
            return candidates, cached_emails

    # Fixed paths — expanded to cover the full range of how small businesses
    # label their people pages across legal, medical, dental, trades, B2B
    # services, real estate, and accounting/consulting.
    paths = [
        # General / contact
        "/", "/about", "/about-us", "/about_us", "/our-story", "/who-we-are",
        "/contact", "/contact-us", "/contact_us",
        # Team pages — generic
        "/team", "/our-team", "/our_team", "/meet-the-team", "/meet-our-team",
        "/staff", "/our-staff", "/people", "/our-people", "/faculty",
        "/bios", "/meet-us",
        # Leadership
        "/leadership", "/our-leadership", "/principals", "/our-principals",
        "/management", "/executives",
        # Legal
        "/attorneys", "/our-attorneys", "/lawyers", "/our-lawyers",
        "/partners", "/our-partners", "/associates", "/our-associates",
        "/attorney-bios", "/attorneys-staff",
        # Medical + dental
        "/providers", "/our-providers", "/doctors", "/our-doctors",
        "/dentists", "/our-dentists", "/physicians", "/our-physicians",
        "/specialists", "/our-specialists", "/meet-the-doctor",
        "/meet-the-dentist", "/meet-the-doctors", "/our-practice",
        # Real estate
        "/agents", "/our-agents", "/brokers", "/realtors", "/team-agents",
        # Consulting / accounting
        "/consultants", "/advisors", "/our-advisors", "/cpas", "/our-cpas",
    ]
    base = website.rstrip("/")
    candidates: list[OwnerCandidate] = []
    emails: set[str] = set()

    email_pat = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

    # Keywords that, when found in a link's visible text OR href, indicate
    # that link goes to a team/people/staff page. Used to DISCOVER custom
    # URLs the fixed path list would miss (e.g. /company/our-team,
    # /firm/lawyers, /practice/dentists).
    TEAM_LINK_KEYWORDS = (
        "team", "attorneys", "attorney-", "lawyers", "lawyer-",
        "doctors", "doctor-", "physicians", "physician-",
        "dentists", "dentist-", "providers", "provider-",
        "specialists", "specialist-", "staff", "people", "principals",
        "partners", "associates", "agents", "brokers", "realtors",
        "consultants", "advisors", "leadership", "faculty", "our-team",
        "meet-the-", "meet-our-", "about-us", "about_us", "bios", "bio-",
    )

    try:
        from src.email_scraper import _is_rejected
    except Exception:
        def _is_rejected(_e): return False

    def _process_page(url: str, html: str) -> None:
        """Extract emails + name candidates from a fetched page."""
        try:
            hidden = extract_all_hidden_emails(html) or {}
            for src_emails in hidden.values():
                emails.update(e for e in src_emails if not _is_rejected(e))
        except Exception:
            pass
        emails.update(e for e in email_pat.findall(html) if not _is_rejected(e))

        text = _strip_html(html)
        for cand in _extract_names_with_titles(text, business_name=business_name):
            cand.source = "website_scrape"
            cand.source_url = url
            candidates.append(cand)

    visited: set[str] = set()
    discovered_paths: set[str] = set()
    bio_page_paths: set[str] = set()
    # Overall safety cap so a pathological site can't keep the crawler
    # running forever. 80 fetches easily covers the fixed list + ~25
    # discovered team pages + ~15 per-person bio pages.
    MAX_FETCHES = 80
    # Same-domain deadline — we also have the 60s future timeout upstream,
    # but budgeting inside gives us a clean stop with partial results.
    import time as _time
    _t_start = _time.time()
    MAX_SECONDS = 55

    def _fetch(url: str) -> Optional[str]:
        """GET a URL, return HTML or None. Respects caps + timeouts."""
        if len(visited) >= MAX_FETCHES:
            return None
        if _time.time() - _t_start > MAX_SECONDS:
            return None
        if url in visited:
            return None
        visited.add(url)
        try:
            r = requests.get(url, timeout=10, headers=HEADERS, allow_redirects=True)
            if r.status_code != 200:
                return None
            ct = r.headers.get("content-type", "")
            if "text/html" not in ct and "application/xhtml" not in ct and "application/xml" not in ct:
                return None
            return r.text
        except Exception:
            return None

    def _harvest_team_links(html: str) -> list[str]:
        """Parse <a> tags for links whose href/text looks like a team page."""
        out: list[str] = []
        try:
            anchors = re.findall(
                r'<a[^>]+href=["\']([^"\'#]+)["\'][^>]*>([^<]{0,160})</a>',
                html, flags=re.I,
            )
        except Exception:
            return out
        for href, link_text in anchors:
            href_l = href.lower().strip()
            text_l = link_text.lower().strip()
            if not any(k in href_l or k in text_l for k in TEAM_LINK_KEYWORDS):
                continue
            if href_l.startswith("http"):
                if domain and domain.lower() not in href_l:
                    continue
                out.append(href)
            elif href_l.startswith("/"):
                out.append(base + href)
        return out

    def _harvest_pdf_links(html: str, base_url: str) -> list[str]:
        """Absolute URLs to .pdf assets linked from this page. Bounded
        to the business's own domain so we don't follow off-site PDFs."""
        out: list[str] = []
        try:
            hrefs = re.findall(r'<a[^>]+href=["\']([^"\'#]+\.pdf(?:\?[^"\']*)?)["\']',
                                html, flags=re.I)
        except Exception:
            return out
        for h in hrefs:
            h_l = h.lower().strip()
            if h_l.startswith("http"):
                if domain and domain.lower() not in h_l:
                    continue
                out.append(h)
            elif h_l.startswith("/"):
                out.append(base + h)
            else:
                out.append(base + "/" + h.lstrip("./"))
        return out


    def _harvest_bio_links(html: str, source_url: str) -> list[str]:
        """
        Second-layer discovery: on a team page, look for links that appear
        to be individual bios — paths like /attorneys/jane-doe,
        /team/john-smith, /bios/dr-brown, /providers/sarah-chen-md.
        These are where personal emails usually live.
        """
        out: list[str] = []
        try:
            anchors = re.findall(
                r'<a[^>]+href=["\']([^"\'#?]+)["\'][^>]*>([^<]{0,160})</a>',
                html, flags=re.I,
            )
        except Exception:
            return out
        # Bio-link heuristics: path after a team-like prefix that looks
        # like a person slug (contains a hyphen + 2+ alpha segments, or
        # matches typical "dr-name" / "firstname-lastname" patterns).
        bio_prefixes = (
            "/attorneys/", "/attorney/", "/lawyers/", "/lawyer/",
            "/doctors/", "/doctor/", "/physicians/", "/physician/",
            "/dentists/", "/dentist/", "/providers/", "/provider/",
            "/specialists/", "/specialist/",
            "/team/", "/staff/", "/people/", "/bios/", "/bio/",
            "/partners/", "/partner/", "/associates/", "/associate/",
            "/agents/", "/agent/", "/brokers/", "/broker/",
            "/consultants/", "/consultant/", "/advisors/", "/advisor/",
            "/meet/", "/leadership/", "/principals/", "/principal/",
            "/our-team/", "/our-attorneys/", "/our-doctors/",
        )
        for href, _text in anchors:
            href_l = href.lower().strip()
            # Make absolute
            if href_l.startswith("http"):
                if domain and domain.lower() not in href_l:
                    continue
                abs_url = href
            elif href_l.startswith("/"):
                abs_url = base + href
            else:
                continue
            abs_l = abs_url.lower()
            # Must contain one of the bio-prefixes AND have a slug-like tail
            if not any(p in abs_l for p in bio_prefixes):
                continue
            # Tail after the last "/" should look like a slug (has a hyphen
            # or is at least 4 alpha chars), otherwise it's probably the
            # team index page itself.
            tail = abs_l.rstrip("/").split("/")[-1]
            if len(tail) < 4:
                continue
            # Exclude pagination + filter links
            if any(skip in tail for skip in ("?", "=", "page", "filter", "all")):
                continue
            out.append(abs_url)
        return out

    # Strict path prefixes for sitemap filtering. We only pull sitemap
    # URLs that match these patterns — the loose TEAM_LINK_KEYWORDS match
    # would pull in case-study pages like /experience/strategic-partnership
    # that eat the time budget without delivering bios.
    SITEMAP_TEAM_PREFIXES = (
        "/team", "/our-team", "/people", "/our-people", "/staff",
        "/bios", "/bio/", "/faculty",
        "/leadership", "/principals", "/management",
        "/attorneys", "/attorney/", "/lawyers", "/lawyer/",
        "/partners/", "/partner/", "/associates", "/associate/",
        "/doctors", "/doctor/", "/physicians", "/physician/",
        "/dentists", "/dentist/", "/providers", "/provider/",
        "/specialists", "/specialist/", "/consultants", "/consultant/",
        "/advisors", "/advisor/", "/cpas", "/cpa/",
        "/agents", "/agent/", "/brokers", "/broker/", "/realtors", "/realtor/",
        "/about/", "/about-us",
        "/meet-the-", "/meet-our-",
    )

    def _parse_sitemap(xml: str) -> list[str]:
        """Extract URLs from sitemap.xml that look like team/bio pages."""
        urls = re.findall(r"<loc[^>]*>\s*([^<]+?)\s*</loc>", xml, flags=re.I)
        out: list[str] = []
        for u in urls:
            u_l = u.lower().strip()
            if domain and domain.lower() not in u_l:
                continue
            # Strict path-prefix match (not substring in query strings)
            from urllib.parse import urlparse as _up
            try:
                path_l = _up(u_l).path
            except Exception:
                path_l = u_l
            if any(path_l.startswith(p) or path_l == p.rstrip("/")
                   for p in SITEMAP_TEAM_PREFIXES):
                out.append(u.strip())
        return out

    def _extract_jsonld_people(html: str) -> list[OwnerCandidate]:
        """
        Parse <script type="application/ld+json"> blocks for Person /
        Employee / OrganizationMember schemas. Many clinic / firm sites
        publish their staff this way (often with `email` populated).
        """
        out: list[OwnerCandidate] = []
        try:
            blocks = re.findall(
                r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
                html, flags=re.S | re.I,
            )
        except Exception:
            return out
        import json as _json
        for blob in blocks:
            try:
                data = _json.loads(blob.strip())
            except Exception:
                continue
            nodes = data if isinstance(data, list) else [data]
            stack = list(nodes)
            while stack:
                node = stack.pop()
                if isinstance(node, list):
                    stack.extend(node); continue
                if not isinstance(node, dict):
                    continue
                # Traverse nested structures
                for v in node.values():
                    if isinstance(v, (list, dict)):
                        stack.append(v)
                t = node.get("@type", "") or ""
                if isinstance(t, list):
                    t = t[0] if t else ""
                if not any(t == k or t.endswith(k) for k in
                           ("Person", "Employee", "OrganizationMember")):
                    continue
                name = (node.get("name") or node.get("givenName", "") + " " +
                        node.get("familyName", "")).strip()
                if not name or len(name.split()) < 2:
                    continue
                title = node.get("jobTitle") or ""
                if isinstance(title, list):
                    title = title[0] if title else ""
                email = (node.get("email") or "").lower().strip()
                if email.startswith("mailto:"):
                    email = email[7:]
                if email and not _is_rejected(email):
                    emails.add(email)
                parts = name.split()
                out.append(OwnerCandidate(
                    full_name=name, first_name=parts[0], last_name=parts[-1],
                    title=str(title)[:80], source="website_jsonld",
                    source_url="",  # filled in by caller
                ))
        return out

    # PDF URLs we discover during crawling — fetched & scraped in Phase E.
    pdf_urls: set[str] = set()

    # ── Phase A: fixed path sweep + link discovery on homepage/about ──
    for path in paths:
        if len(visited) >= MAX_FETCHES or _time.time() - _t_start > MAX_SECONDS:
            break
        url = base + path
        html = _fetch(url)
        if html is None:
            continue
        _process_page(url, html)
        # Structured-data (JSON-LD) people — check every page
        for jld in _extract_jsonld_people(html):
            jld.source_url = url
            candidates.append(jld)
        # Harvest PDFs linked from every page (firm brochures, CVs,
        # engagement letters often have staff emails in footers).
        for pu in _harvest_pdf_links(html, url):
            pdf_urls.add(pu)
        # Link discovery fires on homepage + /about family
        if path in ("/", "/about", "/about-us", "/about_us") and len(discovered_paths) < 25:
            for u in _harvest_team_links(html):
                discovered_paths.add(u)
                if len(discovered_paths) >= 25:
                    break

    # Early-exit optimization: if Phase A already produced a solid haul
    # (≥5 real emails, or ≥10 plausible names + ≥2 emails), skip Phases
    # B/C/D. Preserves budget for sites that actually need the deeper
    # crawl and avoids diminishing returns on sites that already
    # volunteered their directory on the homepage + /about pages.
    _real_names = sum(
        1 for c in candidates
        if not _is_junk_name(c.full_name, business_name=business_name)
    )
    _run_phase_bcd = not (len(emails) >= 5 or (_real_names >= 10 and len(emails) >= 2))
    if not _run_phase_bcd:
        logger.debug(
            f"website_scrape: early-exit after Phase A for {domain} "
            f"({_real_names} names, {len(emails)} emails, "
            f"{round(_time.time()-_t_start,1)}s)"
        )

    # ── Phase B: sitemap.xml (authoritative page list) ──
    # Many sites publish a sitemap that enumerates every URL. Free signal.
    if _run_phase_bcd:
      for sitemap_url in (base + "/sitemap.xml", base + "/sitemap_index.xml"):
        if len(visited) >= MAX_FETCHES or _time.time() - _t_start > MAX_SECONDS:
            break
        sitemap_html = _fetch(sitemap_url)
        if not sitemap_html:
            continue
        for u in _parse_sitemap(sitemap_html)[:30]:
            discovered_paths.add(u)
        # Handle sitemap-of-sitemaps: if we got index URLs, fetch one level deep
        if "sitemapindex" in sitemap_html.lower():
            for child in re.findall(r"<loc[^>]*>\s*([^<]+?)\s*</loc>", sitemap_html, flags=re.I)[:3]:
                if _time.time() - _t_start > MAX_SECONDS:
                    break
                child_xml = _fetch(child.strip())
                if child_xml:
                    for u in _parse_sitemap(child_xml)[:30]:
                        discovered_paths.add(u)

    # ── Phase C: follow discovered team-page links ──
    if _run_phase_bcd:
      for url in list(discovered_paths)[:25]:
        if len(visited) >= MAX_FETCHES or _time.time() - _t_start > MAX_SECONDS:
            break
        html = _fetch(url)
        if html is None:
            continue
        _process_page(url, html)
        for jld in _extract_jsonld_people(html):
            jld.source_url = url
            candidates.append(jld)
        # On team pages, harvest links to individual bio pages
        for bio_url in _harvest_bio_links(html, url):
            bio_page_paths.add(bio_url)
            if len(bio_page_paths) >= 20:
                break

    # ── Phase D: follow per-person bio pages ──
    # This is where personal emails usually live (jane@firm.com on
    # /attorneys/jane-doe). Free to fetch, high-value signal.
    if _run_phase_bcd:
      for url in list(bio_page_paths)[:20]:
        if len(visited) >= MAX_FETCHES or _time.time() - _t_start > MAX_SECONDS:
            break
        html = _fetch(url)
        if html is None:
            continue
        _process_page(url, html)
        for jld in _extract_jsonld_people(html):
            jld.source_url = url
            candidates.append(jld)
        for pu in _harvest_pdf_links(html, url):
            pdf_urls.add(pu)

    # ── Phase E: PDF scraping (firm brochures, CVs, engagement letters) ──
    # Emails in PDFs are often missed by HTML regex. Bounded to 5 PDFs,
    # 6 MB each, same-domain only.
    if pdf_urls and _time.time() - _t_start < MAX_SECONDS:
        try:
            from src.pdf_scraper import harvest_pdf_emails
            pdf_result = harvest_pdf_emails(
                sorted(pdf_urls)[:5], domain=domain,
                max_pdfs=5, timeout_s=8,
            )
            for e in pdf_result.get("emails", []):
                if not _is_rejected(e):
                    emails.add(e)
        except Exception as e:
            logger.debug(f"pdf_harvest failed: {e}")

    if domain:
        emails = {e.lower() for e in emails if e.lower().endswith("@" + domain.lower())}

    cache.set(
        "domain_emails",
        {"candidates": [asdict(c) for c in candidates], "emails": list(emails)},
        "scrape", domain,
    )
    return candidates, list(emails)


def _agent_colleague_emails(
    owners: list[OwnerCandidate], domain: str, cache: _Cache,
    max_colleagues: int = 3,
) -> list[str]:
    """
    Per-colleague email harvesting for NPI-less industries (legal, trades,
    consumer services). The primary owner-search sometimes misses employee
    emails because it AND's the query with owner/founder keywords, so
    pages that publish `jperez@firm.com` on a /team page without those
    keywords never match.

    This agent takes the top N validated colleague names and does a
    targeted Google query per colleague:  "Colleague Name" "@firm.com"

    Gated by the caller — fires ONLY when the first-pass triangulation
    didn't form a pattern. Costs up to N × $0.005 SearchApi credits on
    the firms that need it; zero cost on firms that already have a
    confident pattern. Results cached per (colleague, domain) for 30 days.

    Returns a list of @domain emails found across the queries.
    """
    if not domain or not owners:
        return []
    api_key = os.getenv("SEARCHAPI_KEY")
    if not api_key:
        return []

    email_pat = re.compile(r"[A-Za-z0-9._%+-]+@" + re.escape(domain), re.I)
    found: set[str] = set()

    # Pick top N colleagues by confidence. 3 is a good balance between
    # triangulation signal (need 2+ matches to confirm a pattern) and
    # cost (3 queries ≈ $0.015/business).
    top = sorted(owners, key=lambda x: -x.confidence)[:max_colleagues]

    for owner in top:
        if not owner.full_name or " " not in owner.full_name.strip():
            continue
        query = f'"{owner.full_name}" "@{domain}"'
        cache_key = (owner.full_name.lower(), domain.lower())
        cached = cache.get("colleague_emails", *cache_key)
        if cached is not None:
            found.update(cached)
            continue

        try:
            resp = requests.get(
                "https://www.searchapi.io/api/v1/search",
                params={"q": query, "engine": "google", "num": 5, "api_key": api_key},
                timeout=15,
            )
            data = resp.json()
        except Exception as e:
            logger.warning(f"colleague_emails({owner.full_name}): {e}")
            continue

        per_query: set[str] = set()
        for r in data.get("organic_results", []):
            blob = (r.get("title") or "") + " " + (r.get("snippet") or "")
            per_query.update(m.lower() for m in email_pat.findall(blob))

        cache.set("colleague_emails", list(per_query), *cache_key)
        found.update(per_query)

    return list(found)


def _agent_whois(domain: str, cache: _Cache) -> list[OwnerCandidate]:
    if not domain:
        return []
    cached = cache.get("whois", domain)
    if cached is not None:
        return [OwnerCandidate(**c) for c in cached]

    try:
        resp = requests.get(
            f"https://rdap.org/domain/{domain}",
            timeout=10, headers={"Accept": "application/json"},
        )
        if resp.status_code != 200:
            cache.set("whois", [], domain)
            return []
        data = resp.json()
    except Exception:
        return []

    candidates: list[OwnerCandidate] = []
    for entity in data.get("entities", []):
        roles = [r.lower() for r in entity.get("roles", [])]
        if not any(r in roles for r in ("registrant", "administrative", "technical")):
            continue
        vcard = entity.get("vcardArray", [])
        if len(vcard) < 2:
            continue
        for item in vcard[1]:
            if len(item) >= 4 and item[0] == "fn" and item[3]:
                parsed = _parse_name(item[3], source="whois", title="Registrant")
                if parsed:
                    candidates.append(parsed)
                    break
    cache.set("whois", [asdict(c) for c in candidates], domain)
    return candidates


def _agent_places(
    business_name: str, address: str, cache: _Cache
) -> list[OwnerCandidate]:
    """
    Google Places GMB lookup with two-tier caching:
    - place_id cached 90d (most stable)
    - place_details cached 30d (reviews / summary can change)
    """
    api_key = os.getenv("GOOGLE_PLACES_API_KEY")
    if not api_key:
        return []

    place_id = cache.get("place_id", business_name, address)
    if not place_id:
        try:
            s = requests.get(
                "https://maps.googleapis.com/maps/api/place/textsearch/json",
                params={"query": f"{business_name} {address}", "key": api_key},
                timeout=10,
            ).json()
            if not s.get("results"):
                cache.set("place_id", "", business_name, address, ttl=14 * 86400)
                return []
            place_id = s["results"][0]["place_id"]
            cache.set("place_id", place_id, business_name, address)
        except Exception as e:
            logger.warning(f"places text search: {e}")
            return []

    if not place_id:
        return []

    details = cache.get("place_details", place_id)
    if not details:
        try:
            d = requests.get(
                "https://maps.googleapis.com/maps/api/place/details/json",
                params={"place_id": place_id,
                        "fields": "name,editorial_summary,reviews",
                        "key": api_key},
                timeout=10,
            ).json()
            details = d.get("result", {})
            cache.set("place_details", details, place_id)
        except Exception:
            return []

    texts = []
    if details.get("editorial_summary", {}).get("overview"):
        texts.append(details["editorial_summary"]["overview"])
    for rev in details.get("reviews", [])[:5]:
        if rev.get("text"):
            texts.append(rev["text"])

    candidates: list[OwnerCandidate] = []
    for t in texts:
        for cand in _extract_names_with_titles(t, business_name=business_name):
            cand.source = "google_places"
            cand.confidence = 55
            candidates.append(cand)
    return candidates


def _agent_npi_healthcare(
    business_name: str, address: str, industry: str, cache: _Cache
) -> list[OwnerCandidate]:
    """Healthcare-only NPI fallback. Queries NPI-1 (individual providers)
    by postal_code + taxonomy, then filters by street/business tokens."""
    # Cache key bumped to v2 — the old npi cache holds all-empty results
    # from the buggy organization_name+NPI-2 query.
    cached = cache.get("owner_candidates", "npi_v2", business_name, address)
    if cached is not None:
        raw = [OwnerCandidate(**c) for c in cached]
        filt = [c for c in raw
                if not _is_junk_name(c.full_name, business_name=business_name)]
        # Fall through on empty or all-garbage; NPI is free (US gov API)
        if raw and not (raw and not filt):
            return filt

    city, state = _extract_city_state(address)
    postal = _extract_postal_code(address)
    if not (state or postal):
        return []

    i = industry.lower() if industry else ""
    taxonomy_filter = "Dentist" if "dent" in i else \
                      "Chiropractor" if "chiro" in i else \
                      "Physician" if ("physic" in i or "medic" in i) else ""

    # NPI Registry has TWO disjoint record types:
    #   NPI-1 (individuals) — have first_name / last_name
    #   NPI-2 (organizations) — have organization_name, no first/last
    # The previous impl queried by organization_name (only matches NPI-2)
    # then parsed basic.first_name (only exists on NPI-1) → zero matches.
    # Fix: query NPI-1 by postal_code + taxonomy, which returns every
    # licensed provider in the ZIP. We then trust the LLM/synthesis layer
    # (and Phase 2 junk-name filter) to pick the ones affiliated with
    # this specific practice.
    params = {
        "version": "2.1",
        "enumeration_type": "NPI-1",
        "limit": 50,
    }
    if postal:
        params["postal_code"] = postal
    else:
        # Fallback to city/state when the address didn't include a ZIP
        params["state"] = state
        if city:
            params["city"] = city
    if taxonomy_filter:
        params["taxonomy_description"] = taxonomy_filter

    try:
        resp = requests.get(
            "https://npiregistry.cms.hhs.gov/api/", params=params, timeout=10,
        )
        data = resp.json()
    except Exception as e:
        logger.warning(f"npi: {e}")
        return []

    # Lightly filter by address tokens so we don't flood synthesis with
    # every dentist in the ZIP. Match on street-name tokens OR business-name
    # tokens from the NPI record's LOCATION address.
    street_tokens: set[str] = set()
    for tok in re.findall(r"[A-Za-z]{3,}", address or ""):
        street_tokens.add(tok.lower())
    biz_tokens: set[str] = set()
    for tok in re.findall(r"[A-Za-z]{4,}", business_name or ""):
        biz_tokens.add(tok.lower())

    def _record_matches_location(rec: dict) -> bool:
        if not (street_tokens or biz_tokens):
            return True
        for addr_rec in rec.get("addresses", []) or []:
            if (addr_rec.get("address_purpose") or "").upper() != "LOCATION":
                continue
            line = " ".join(
                str(addr_rec.get(k, "")) for k in ("address_1", "address_2")
            ).lower()
            if any(t in line for t in street_tokens):
                return True
        # Secondary: organization/practice name match on NPI-2 link (unlikely for NPI-1)
        return False

    candidates: list[OwnerCandidate] = []
    for rec in data.get("results", []):
        basic = rec.get("basic", {})
        first = (basic.get("first_name") or "").strip().title()
        last = (basic.get("last_name") or "").strip().title()
        if not first or not last:
            continue
        if not _record_matches_location(rec):
            continue
        taxonomies = rec.get("taxonomies", [])
        primary_tax = next((t for t in taxonomies if t.get("primary")),
                           taxonomies[0] if taxonomies else {})
        credential = basic.get("credential") or primary_tax.get("desc") or ""
        candidates.append(OwnerCandidate(
            full_name=f"{first} {last}",
            first_name=first, last_name=last,
            title=credential, source="npi",
            source_url=f"https://npiregistry.cms.hhs.gov/provider-view/{rec.get('number')}",
            raw_snippet=f"NPI {rec.get('number')} | {credential}",
        ))

    cache.set("owner_candidates", [asdict(c) for c in candidates],
              "npi_v2", business_name, address)
    return candidates


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 5: OWNER SYNTHESIS (dedupe + rank)
# ═════════════════════════════════════════════════════════════════════════════

def _synthesise_owners(
    candidates: list[OwnerCandidate], business_name: str
) -> list[OwnerCandidate]:
    if not candidates:
        return []
    # Safety net: even if earlier extract/cache paths somehow allowed a
    # junk name through, this is the last gate before a name becomes a
    # decision maker. Any post-fix run should see the garbage cleared
    # here as a defense-in-depth check.
    candidates = [
        c for c in candidates
        if not _is_junk_name(c.full_name, business_name=business_name)
    ]
    if not candidates:
        return []
    groups: dict[str, list[OwnerCandidate]] = {}
    for c in candidates:
        k = _norm_name(c.full_name)
        if not k or len(k) < 4:
            continue
        groups.setdefault(k, []).append(c)

    biz_clean = re.sub(r"[^a-z]", "", business_name.lower())
    merged: list[OwnerCandidate] = []

    for group in groups.values():
        primary = max(group, key=lambda x: _title_weight(x.title))
        sources = list({c.source for c in group if c.source})
        titles = [c.title for c in group if c.title]
        # Cross-page corroboration — count distinct source_urls the
        # name appeared on. A name on 3+ pages/sources is much more
        # likely to be real than a name extracted once from a footer
        # snippet. Scales the base score.
        distinct_urls = len({c.source_url for c in group if c.source_url})
        corroboration_bonus = 0
        if distinct_urls >= 3:
            corroboration_bonus = 20
        elif distinct_urls == 2:
            corroboration_bonus = 10

        score = _title_weight(primary.title) * 3
        score += (len(sources) - 1) * 15
        score += corroboration_bonus

        # Business-name surname match is the STRONGEST signal for
        # owner-operators. "Daniel Clement" at "Clement Law", "Todd
        # Spodek" at "Spodek Law Group". Bumped from +40 → +65 so
        # this beats generic LinkedIn "Founder" hits for random
        # employees who happen to be in Google's snippet results.
        last_norm = re.sub(r"[^a-z]", "", primary.last_name.lower())
        if last_norm and len(last_norm) >= 3 and last_norm in biz_clean:
            score += 65

        if sources == ["whois"]:
            score = min(score, 25)

        primary.confidence = max(0, min(100, score))
        primary.source = " + ".join(sorted(sources))
        primary.title = max(titles, key=_title_weight) if titles else primary.title
        # Stash the page-corroboration count on raw_snippet so the CSV
        # export can surface it for operators.
        if distinct_urls >= 2:
            primary.raw_snippet = (
                (primary.raw_snippet or "") +
                f" [seen on {distinct_urls} pages]"
            )[:200]
        merged.append(primary)

    merged.sort(key=lambda x: x.confidence, reverse=True)
    return merged


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 6: TRIANGULATION + CANDIDATES
# ═════════════════════════════════════════════════════════════════════════════

def _classify_pattern(local: str, first: str, last: str) -> Optional[str]:
    if not first or not last:
        return None
    if local == f"{first}.{last}":    return "first.last"
    if local == f"{first}{last}":     return "firstlast"
    if local == f"{first[0]}{last}":  return "flast"
    if local == f"{first[0]}.{last}": return "f.last"
    if local == first and len(first) >= 4: return "first"
    if local == last:                 return "last"
    if local == f"{last}.{first}":    return "last.first"
    if local == f"{last}{first[0]}":  return "lastf"
    if local == f"dr.{last}":         return "dr.last"
    if local == f"dr{last}":          return "drlast"
    return None


def _triangulate_pattern(
    emails: list[str], owners: list[OwnerCandidate], domain: str, cache: _Cache
) -> Optional[DetectedPattern]:
    """Per-domain pattern cache: once detected, valid 60d."""
    cached = cache.get("detected_pattern", domain)
    if cached:
        return DetectedPattern(**cached)

    if not emails or not owners:
        return None

    votes: dict[str, list[tuple[str, str]]] = {}
    for email in emails:
        if "@" not in email:
            continue
        local = email.split("@")[0].lower()
        # Use the canonical stopword checker — covers every variant
        # the duplicate GENERIC_LOCALS set was missing (connect, reach,
        # info-substring, practice-area compounds, etc.). A generic
        # mailbox should never contribute to pattern votes since its
        # local part isn't related to any DM's name.
        from src.volume_mode.stopwords import is_generic as _is_generic_lp
        if _is_generic_lp(local):
            continue
        for owner in owners:
            first = owner.first_name.lower()
            last = owner.last_name.lower()
            pat = _classify_pattern(local, first, last)
            if pat:
                votes.setdefault(pat, []).append((email, owner.full_name))
                break

    if not votes:
        return None

    name, evidence = max(votes.items(), key=lambda x: len(x[1]))
    n = len(evidence)
    confidence = 95 if n >= 3 else 88 if n == 2 else 70
    pat = DetectedPattern(
        pattern_name=name, confidence=confidence,
        evidence_emails=[e for e, _ in evidence],
        evidence_names=[nm for _, nm in evidence],
    )
    cache.set("detected_pattern", asdict(pat), domain)
    return pat


def _generate_candidates(
    decision_maker: OwnerCandidate,
    domain: str,
    detected_pattern: Optional[DetectedPattern],
    industry: str,
    allow_first_only_pattern: bool = False,
    scraped_emails: Optional[list[str]] = None,
) -> list[dict]:
    first, last = decision_maker.first_name, decision_maker.last_name
    candidates: list[dict] = []
    seen: set[str] = set()
    scraped_emails = scraped_emails or []
    d_lower = domain.lower()

    # Guardrail: the bare `first@domain` pattern is off by default for
    # INDUSTRY-PRIOR guesses — it's only valid for solo practitioners and a
    # few consumer-facing verticals where it's conventional. But when a
    # `first@` pattern is triangulated from real evidence (an actual email
    # like marc@domain tied to owner Marc), that's not a guess — it's proof.
    # Triangulated evidence always flows through regardless of this guard.
    def _block_first_only(pat: str) -> bool:
        return pat == "first" and not allow_first_only_pattern

    # A detected_pattern is "proven" when triangulation tied it to at least
    # one real email. In that case we treat it as evidence, not a prior.
    pattern_is_proven = bool(
        detected_pattern
        and getattr(detected_pattern, "evidence_emails", None)
        and len(detected_pattern.evidence_emails) >= 1
    )

    # Emit scraped-direct candidates FIRST (up to 2 slots). These carry
    # the highest source-evidence weight in the scorer because the business
    # literally published them. Scorer's specificity cap correctly ranks
    # generic inboxes (info@) below personal mailboxes (drjones@).
    #
    # Deterministic ordering: normalize + dedupe + sort so set-iteration
    # order doesn't pick different emails on different runs. Then PREFER
    # emails already cached as NB-valid — preserves safe_to_send leads
    # across replays instead of picking whichever scraped email happened
    # to come first in set iteration order.
    try:
        _cache_for_nb = get_cache()
    except Exception:
        _cache_for_nb = None

    normalized = []
    for scraped in scraped_emails:
        email = (scraped or "").lower().strip()
        if not email or "@" not in email:
            continue
        if not email.endswith("@" + d_lower):
            continue
        normalized.append(email)
    normalized = sorted(set(normalized))

    def _nb_valid_rank(email: str) -> int:
        # Returns 0 if cached NB-valid, 1 if cached catchall/unknown, 2 if uncached.
        if _cache_for_nb is None:
            return 2
        cached = _cache_for_nb.get("nb_verify", email)
        if not cached:
            return 2
        return 0 if cached.get("result") == "valid" else 1

    normalized.sort(key=_nb_valid_rank)

    scraped_slot_budget = 2
    for email in normalized:
        if scraped_slot_budget <= 0:
            break
        if email in seen:
            continue
        candidates.append({
            "email": email,
            "pattern": "scraped",
            "source": "scraped_direct",
            "base_confidence": 75,
        })
        seen.add(email)
        scraped_slot_budget -= 1

    # Apply the detected pattern to the DM's name. Triangulated evidence
    # (pattern_is_proven) bypasses the first-only guard since it's not a
    # blind guess — we've seen the pattern in a real email at this domain.
    # A proven pattern's base_confidence is boosted to 80 (above the 75
    # that scraped_direct gets) so the DM's personalized email outranks
    # generic scraped inboxes like info@ when both pass NB verification.
    if detected_pattern and detected_pattern.confidence >= 70 and (
        pattern_is_proven or not _block_first_only(detected_pattern.pattern_name)
    ):
        email = build_email(detected_pattern.pattern_name, first, last, domain)
        if email and email not in seen:
            base = max(detected_pattern.confidence, 80) if pattern_is_proven \
                else detected_pattern.confidence
            candidates.append({"email": email, "pattern": detected_pattern.pattern_name,
                               "source": "detected_pattern",
                               "base_confidence": base})
            seen.add(email)

    try:
        priors = get_patterns_for(industry) or []
    except Exception:
        priors = [("flast", 0.25), ("firstlast", 0.15)]

    # Walk ALL priors until we reach the NB budget (not just top-3). This
    # lets us try patterns like drlast / dr.last that sit outside the top
    # slice but frequently correspond to the real mailbox (e.g.
    # drjones@mikejonesdds.com was NB-valid in our tests).
    for pattern_name, weight in priors:
        if _block_first_only(pattern_name):
            continue
        email = build_email(pattern_name, first, last, domain)
        if email and email not in seen:
            candidates.append({"email": email, "pattern": pattern_name,
                               "source": "industry_prior",
                               "base_confidence": int(weight * 50)})
            seen.add(email)
            if len(candidates) >= 4:
                break

    # first.last@ is the universal last-resort default. Appended AFTER
    # evidence-backed and industry-prior candidates so NB verification
    # burns on stronger signals first, but it's always present so we
    # never ship a business with zero candidates.
    fl = build_email("first.last", first, last, domain)
    if fl and fl not in seen:
        candidates.append({"email": fl, "pattern": "first.last",
                           "source": "first_last_fallback",
                           "base_confidence": 45})
        seen.add(fl)

    # Cap at 5 so industry-prior patterns (drlast, flast) still enter the
    # pool when scraped_direct fills 2 slots + detected_pattern fills 1.
    # NB budget is 4 main + 1 guaranteed DM probe = 5, which matches.
    return candidates[:5]


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 7: SMTP + NEVERBOUNCE (cached)
# ═════════════════════════════════════════════════════════════════════════════

def _probe_smtp_cached(email: str, cache: _Cache) -> dict:
    cached = cache.get("smtp_probe", email)
    if cached is not None:
        return cached
    try:
        r = _verify_smtp_raw(email, timeout=8)
        result = {"valid": r.get("status") == "valid",
                  "catchall": r.get("catchall", False)}
    except Exception:
        result = {"valid": False, "catchall": False}
    cache.set("smtp_probe", result, email)
    return result


def _nb_verify_cached(email: str, cache: _Cache, *,
                       force_refresh: bool = False) -> dict:
    """force_refresh=True bypasses the cache — used by the NB-unknown
    retry path in volume mode where the prior verdict was 'unknown'
    and we want a fresh NB call (often flips on retry due to transient
    rate-limit / timeout / server-refused conditions)."""
    if not force_refresh:
        cached = cache.get("nb_verify", email)
        if cached is not None:
            # If the cached entry is from a credit-exhaustion event, don't
            # trust it — re-query so a topped-up account picks up fresh
            # results. `credit_exhausted` flag is set below whenever NB
            # reported 0 credits.
            if not cached.get("credit_exhausted"):
                return cached

    # Pre-flight #1: domain must have MX records (free DNS lookup).
    # If it doesn't, the domain can't receive mail at all — skip the
    # NB call and mark invalid. Free to check, saves $0.003/call on
    # dead domains.
    try:
        from src.mx_check import email_has_mx
        if not email_has_mx(email):
            result = {"safe_to_send": False, "result": "invalid",
                      "error": "no_mx_records",
                      "skipped_nb": True}
            cache.set("nb_verify", result, email)
            return result
    except Exception:
        pass  # dns module missing or lookup failed — fall through to NB

    # Pre-flight #2: skip entirely if we know the account has 0 credits.
    # Cached for 5 minutes so we don't poll NB on every call.
    if not _nb_credits_available():
        return {"safe_to_send": False, "result": "unknown",
                "credit_exhausted": True, "error": "nb_credits_depleted"}

    try:
        r = _nb_verify_raw(email)
        result = {"safe_to_send": bool(r.safe_to_send), "result": r.result}
        # Detect the specific credit-exhaustion error from the NB
        # integration's flag set, and mark the cache entry so it gets
        # re-queried once credits are topped up.
        if getattr(r, "flags", None) and any(
            "Insufficient credit" in str(f) for f in r.flags
        ):
            result["credit_exhausted"] = True
            _mark_nb_credits_exhausted()
    except Exception as e:
        result = {"safe_to_send": False, "result": None, "error": str(e)}
    cache.set("nb_verify", result, email)
    return result


# ── NB credit availability (5-minute cache) ──
_NB_CREDITS_STATE = {"checked_at": 0.0, "available": True}


def _nb_credits_available() -> bool:
    """
    Return False when we know the NB account has 0 remaining credits.
    Checks once every 5 minutes (or on explicit _mark_nb_credits_*).
    Defaults to True when we haven't checked yet or the account check
    itself fails (fail open — let the real API call surface the error).
    """
    import time as _t
    now = _t.time()
    if now - _NB_CREDITS_STATE["checked_at"] < 300:
        return _NB_CREDITS_STATE["available"]
    try:
        from src.neverbounce import get_account_info
        info = get_account_info()
        credits = info.get("credits_info") or {}
        available = (
            (credits.get("paid_credits_remaining") or 0)
            + (credits.get("free_credits_remaining") or 0)
        ) > 0
    except Exception:
        available = True  # fail open — don't block on account-check errors
    _NB_CREDITS_STATE["checked_at"] = now
    _NB_CREDITS_STATE["available"] = available
    return available


def _mark_nb_credits_exhausted() -> None:
    """Called when an NB response indicates 0 credits — stops the bleed."""
    import time as _t
    _NB_CREDITS_STATE["checked_at"] = _t.time()
    _NB_CREDITS_STATE["available"] = False


def _candidate_confidence(candidate: dict, pattern: Optional[DetectedPattern]) -> int:
    score = candidate.get("base_confidence", 30)

    if candidate.get("nb_result") == "invalid":
        return 0
    if candidate.get("nb_result") == "catchall":
        score += 10
    if candidate.get("smtp_valid") and not candidate.get("smtp_catchall"):
        score += 20
    elif candidate.get("smtp_catchall"):
        score -= 5
    if pattern and candidate["pattern"] == pattern.pattern_name:
        score += 15

    # NB-valid is AUTHORITATIVE: floor at 85 so that even a low-prior
    # pattern (drlast base 5, flast base 11) clears the SAFE threshold
    # when NeverBounce has explicitly confirmed the mailbox.
    if candidate.get("nb_valid"):
        score = max(score + 35, 85)

    return max(0, min(100, score))


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 8: MAIN PIPELINE ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════

def triangulate_email(
    business_name: str,
    website: str,
    domain: str,
    address: str,
    industry: str,
    decision_maker_hint: Optional[str] = None,
    scraped_emails: Optional[list[str]] = None,
    use_neverbounce: bool = True,
    confidence_threshold: int = 70,
    allow_first_only_pattern: bool = False,
) -> TriangulationResult:
    start = time.time()
    result = TriangulationResult()
    scraped_emails = scraped_emails or []
    cache = get_cache()

    # ── PHASE 1: Owner discovery + email evidence (parallel, cached) ──
    all_candidates: list[OwnerCandidate] = []
    all_emails: set[str] = set(scraped_emails)
    linkedin_urls: list[str] = []

    # Parse the explicit hint once — we use it both to seed discovery AND
    # as a tiebreaker against search-derived candidates. Without this tie-
    # breaker, common surnames (e.g. "Jones" in "Jones Dental Clinic")
    # can cause a random "Michael Jones" returned by Google search to
    # outrank the real "Timothy Jones" the operator already identified.
    hint_seed: Optional[OwnerCandidate] = None
    if decision_maker_hint and len(decision_maker_hint.strip()) > 3:
        hint_seed = _parse_name(decision_maker_hint, source="existing_hint", title="")
        if hint_seed:
            hint_seed.confidence = 40
            all_candidates.append(hint_seed)

    with ThreadPoolExecutor(max_workers=5) as ex:
        fut_combined = ex.submit(
            _agent_combined_owner_and_press, business_name, domain, address, cache
        )
        fut_website = ex.submit(
            _agent_website_scrape, website, domain, business_name, cache
        )
        fut_whois = ex.submit(_agent_whois, domain, cache)
        fut_places = (
            ex.submit(_agent_places, business_name, address, cache)
            if os.getenv("GOOGLE_PLACES_API_KEY") else None
        )
        fut_npi = (
            ex.submit(_agent_npi_healthcare, business_name, address, industry, cache)
            if industry and any(k in industry.lower()
                                for k in ("dent", "medic", "health", "clinic",
                                          "physician", "chiro")) else None
        )

        # Combined (names + emails + linkedin URLs)
        result.agents_run.append("combined_owner_press")
        try:
            c_cands, c_emails, c_lnk = fut_combined.result(timeout=20)
            if c_cands or c_emails:
                result.agents_succeeded.append("combined_owner_press")
            # Reject candidates whose source URL is off-territory (e.g.
            # NYC dentist returned for a Montana business). See L7.
            before_n = len(c_cands)
            c_cands = [
                c for c in c_cands
                if not _is_wrong_territory(c, business_name, domain, address)
            ]
            if before_n != len(c_cands):
                result.evidence_trail["territory_filtered"] = before_n - len(c_cands)
            all_candidates.extend(c_cands)
            all_emails.update(c_emails)
            linkedin_urls.extend(c_lnk)
        except Exception as e:
            logger.warning(f"combined: {e}")

        # Website scrape
        result.agents_run.append("website_scrape")
        try:
            # 60s gives the deep crawler room to follow sitemap + link
            # discovery + per-person bio pages. Website scrape is free
            # (bandwidth only), so we optimise for completeness.
            w_cands, w_emails = fut_website.result(timeout=60)
            if w_cands or w_emails:
                result.agents_succeeded.append("website_scrape")
            all_candidates.extend(w_cands)
            all_emails.update(w_emails)
        except Exception as e:
            logger.warning(f"website: {e}")

        # WHOIS
        result.agents_run.append("whois")
        try:
            wh = fut_whois.result(timeout=15)
            if wh:
                result.agents_succeeded.append("whois")
                all_candidates.extend(wh)
        except Exception:
            pass

        if fut_places:
            result.agents_run.append("google_places")
            try:
                p = fut_places.result(timeout=15)
                if p:
                    result.agents_succeeded.append("google_places")
                    all_candidates.extend(p)
            except Exception:
                pass

        if fut_npi:
            result.agents_run.append("npi_healthcare")
            try:
                n = fut_npi.result(timeout=15)
                if n:
                    result.agents_succeeded.append("npi_healthcare")
                    all_candidates.extend(n)
            except Exception:
                pass

    # ── COST SAVING: LinkedIn only fires if combined didn't surface any ──
    if not linkedin_urls:
        result.agents_run.append("linkedin_gated")
        try:
            li = _agent_linkedin_gated(business_name, False, cache)
            if li:
                result.agents_succeeded.append("linkedin_gated")
                all_candidates.extend(li)
        except Exception:
            pass

    # ── PHASE 1.5: LLM name classifier (Haiku, cached) ──
    # Before synthesis, ask Haiku which names are actually real people vs.
    # SEO spam / UI fragments / business descriptors. Replaces the stopword
    # treadmill. ~$0.001-0.002 per business, cached per candidate list so
    # re-runs are free. On failure (no API key / API error) we fall through
    # to the existing stopword-based _is_junk_name filter in synthesis.
    if all_candidates:
        result.agents_run.append("llm_name_filter")
        try:
            from src.name_classifier import filter_real_people
            filtered = filter_real_people(
                all_candidates, business_name, domain, cache
            )
            if filtered is not None:
                before = len(all_candidates)
                all_candidates = filtered
                result.agents_succeeded.append("llm_name_filter")
                result.evidence_trail["llm_filter_removed"] = before - len(filtered)
        except Exception as e:
            logger.warning(f"llm name filter failed: {e}")

    # ── PHASE 2: Synthesise owners ──
    # _synthesise_owners still runs _is_junk_name as a secondary stopword
    # defense — belt + suspenders against Haiku edge cases.
    ranked = _synthesise_owners(all_candidates, business_name)
    result.all_owners = ranked

    # When the operator supplied an explicit hint (a scraped contact_name
    # from the website), prefer it as the decision-maker as long as it
    # made it through synthesis. The synthesizer can demote the hinted
    # person if someone else shows a much stronger signal (NPI match,
    # 3+ corroborating sources) — but for common surnames the hint is
    # usually right. Only fall through to the top-ranked candidate if
    # the hint dropped out entirely.
    dm = None
    if hint_seed:
        hint_key = _norm_name(hint_seed.full_name)
        for cand in ranked:
            if _norm_name(cand.full_name) == hint_key:
                dm = cand
                break
    if dm is None:
        dm = ranked[0] if ranked else None
    result.decision_maker = dm

    if not result.decision_maker or not domain:
        result.time_seconds = round(time.time() - start, 2)
        result.debug["exit_reason"] = "no_decision_maker" if not result.decision_maker else "no_domain"
        _record_cache_savings(result, cache)
        return result

    result.evidence_trail["discovered_emails"] = list(all_emails)
    result.evidence_trail["owners_count"] = len(ranked)

    # ── PHASE 3: Triangulation (per-domain cache) ──
    result.detected_pattern = _triangulate_pattern(
        list(all_emails), ranked, domain, cache
    )

    # ── PHASE 3B: Colleague-email harvest (gated) ──
    # If the first-pass triangulation didn't form a confident pattern AND
    # we have 2+ validated colleagues AND the domain is known, do one
    # targeted Google search per top colleague to find emails we might
    # have missed (common for legal/trades where NPI is unavailable and
    # the primary owner-search query can't catch employee emails on
    # /team pages). Costs up to 3 × $0.005 SearchApi credits; fires
    # only on the businesses that need it; results cached per-colleague.
    if (
        (result.detected_pattern is None or result.detected_pattern.confidence < 70)
        and len(ranked) >= 2
        and domain
    ):
        result.agents_run.append("colleague_emails")
        try:
            harvested = _agent_colleague_emails(ranked, domain, cache)
            if harvested:
                result.agents_succeeded.append("colleague_emails")
                new_emails = [e for e in harvested if e not in all_emails]
                all_emails.update(harvested)
                if new_emails:
                    # Re-triangulate with the enriched email pool
                    refined = _triangulate_pattern(
                        list(all_emails), ranked, domain, cache
                    )
                    # Only overwrite if refined is a stronger signal
                    if refined and (
                        result.detected_pattern is None
                        or refined.confidence > result.detected_pattern.confidence
                    ):
                        result.detected_pattern = refined
                    result.evidence_trail["colleague_emails_added"] = new_emails
        except Exception as e:
            logger.warning(f"colleague_emails agent failed: {e}")

    result.evidence_trail["discovered_emails"] = list(all_emails)

    # ── PHASE 4: Candidate generation ──
    candidates = _generate_candidates(
        result.decision_maker, domain, result.detected_pattern, industry,
        allow_first_only_pattern=allow_first_only_pattern,
        scraped_emails=list(all_emails),
    )

    if not candidates:
        result.time_seconds = round(time.time() - start, 2)
        _record_cache_savings(result, cache)
        return result

    # ── PHASE 5: SMTP probe (cached, parallel) ──
    with ThreadPoolExecutor(max_workers=5) as ex:
        probe_futures = {ex.submit(_probe_smtp_cached, c["email"], cache): c
                         for c in candidates}
        for fut in as_completed(probe_futures):
            c = probe_futures[fut]
            try:
                p = fut.result(timeout=15)
                c["smtp_valid"] = p.get("valid", False)
                c["smtp_catchall"] = p.get("catchall", False)
            except Exception:
                c["smtp_valid"] = False
                c["smtp_catchall"] = False
    result.agents_run.append("smtp_probe")
    if any(c.get("smtp_valid") for c in candidates):
        result.agents_succeeded.append("smtp_probe")

    # ── PHASE 6: NeverBounce — walk candidate list up to NB_BUDGET ──
    # NB-invalid responses mean the mailbox doesn't exist, not that we
    # should give up. Walk the list in confidence order, calling NB on
    # each, until one comes back valid OR we hit catchall/unknown
    # (which tells us no further NB call will help). SMTP gate removed
    # — hosted envs (Streamlit Cloud, most VMs) block port 25 so
    # smtp_valid is almost always False and would gate NB off.
    #
    # ADDITIONALLY, even when the primary walk stops early (first valid),
    # we always guarantee the triangulated DM candidate gets NB-tested so
    # the user sees its verdict alongside the generic inbox. Otherwise a
    # scraped info@ that returns valid would silently hide whether the
    # DM's personalized email is deliverable.
    if use_neverbounce:
        for c in candidates:
            c["confidence"] = _candidate_confidence(c, result.detected_pattern)
        candidates.sort(key=lambda x: x["confidence"], reverse=True)

        NB_BUDGET = 4
        verified = 0
        for cand in list(candidates):
            if verified >= NB_BUDGET:
                break
            nb = _nb_verify_cached(cand["email"], cache)
            cand["nb_valid"] = nb.get("safe_to_send", False)
            cand["nb_result"] = nb.get("result")
            verified += 1
            if nb.get("result") == "valid":
                break
            if nb.get("result") == "invalid":
                continue  # try the next pattern
            # catchall / unknown / error — no point probing further
            break

        # Guarantee: if the triangulated DM candidate exists and wasn't
        # tested in the main walk, run +1 NB on it. Budget cap is 5
        # total (4 + 1 guaranteed DM probe). Cached NB results are free.
        dm_candidate = next(
            (c for c in candidates if c.get("source") == "detected_pattern"),
            None,
        )
        if dm_candidate and "nb_result" not in dm_candidate:
            nb = _nb_verify_cached(dm_candidate["email"], cache)
            dm_candidate["nb_valid"] = nb.get("safe_to_send", False)
            dm_candidate["nb_result"] = nb.get("result")

        result.agents_run.append("neverbounce")
        if any(c.get("nb_result") for c in candidates):
            result.agents_succeeded.append("neverbounce")

        for c in candidates:
            c["confidence"] = _candidate_confidence(c, result.detected_pattern)
        candidates.sort(key=lambda x: x["confidence"], reverse=True)

    result.candidate_emails = candidates

    # ── PHASE 7: Decision gate ──
    # Generic inbox rejection — same rule volume mode enforces. info@,
    # contact@, admin@, partners@, intakes@, info-* etc. never win as
    # primary pick, even when NB says valid. Analysis of search #41
    # showed triangulation was picking 13 generic inboxes that wouldn't
    # reach the DM (shared mailboxes at law firms auto-filter cold
    # outreach). Better to return empty than send to a dead end.
    try:
        from src.volume_mode.stopwords import is_generic as _is_generic
    except Exception:
        def _is_generic(local: str, business_name: str = "") -> bool:
            return False

    biz_name_for_filter = business_name or ""

    def _acceptable(c: dict) -> bool:
        email = (c.get("email") or "")
        if "@" not in email:
            return False
        local = email.split("@", 1)[0]
        return not _is_generic(local, business_name=biz_name_for_filter)

    eligible = [c for c in candidates if _acceptable(c)]

    if not eligible:
        # No non-generic candidate survived — return empty with a
        # reason the operator can read.
        result.best_email = ""
        result.best_email_confidence = 0
        result.best_email_evidence = [
            "⚫ No deliverable non-generic email produced. Every scraped "
            "email on this domain is a shared inbox (info@, contact@, etc.) "
            "and every constructed DM pattern NB-invalided. Genuinely "
            "unreachable via cold outreach."
        ]
        result.safe_to_send = False
        result.time_seconds = round(time.time() - start, 2)
        _record_cache_savings(result, cache)
        return result

    # LLM final gate — same Haiku picker volume mode uses. Catches
    # semantic shared-inbox variants we haven't listed (new industries
    # invent new ones: `connect@`, `engageus@`, `letstalk@`) AND
    # wrong-person picks (jake@firm when DM is Matthew). One cached
    # call per biz, ~$0.001.
    if result.decision_maker and domain and eligible:
        try:
            from src.email_picker_llm import pick_email_with_llm
            llm_result = pick_email_with_llm(
                candidates=[{
                    "email": c.get("email") or "",
                    "bucket": c.get("source", ""),
                    "pattern": c.get("pattern", ""),
                    "nb_result": c.get("nb_result"),
                } for c in eligible],
                dm_name=result.decision_maker.full_name,
                dm_title=getattr(result.decision_maker, "title", ""),
                business_name=business_name, domain=domain, cache=cache,
            )
            if llm_result is not None:
                picked_email, reason = llm_result
                if picked_email is None:
                    # Haiku: all candidates are shared inboxes or
                    # wrong-person. Return empty with a clear reason.
                    result.best_email = ""
                    result.best_email_confidence = 0
                    result.best_email_evidence = [
                        f"⚫ LLM gate rejected every candidate: {reason}",
                    ]
                    result.safe_to_send = False
                    result.time_seconds = round(time.time() - start, 2)
                    _record_cache_savings(result, cache)
                    return result
                # Reorder so the LLM's pick is first
                eligible = (
                    [c for c in eligible if c.get("email") == picked_email]
                    + [c for c in eligible if c.get("email") != picked_email]
                )
        except Exception as e:
            logger.debug(f"triangulation LLM gate failed: {e}")

    top = eligible[0]
    # Skip bucket-D/E-style constructed candidates that came back
    # NB-invalid — proven-bounce addresses should never win. Walk
    # down the eligible list until we find one that isn't a confirmed
    # bounce, or run out (return empty).
    for cand in eligible:
        if cand.get("nb_result") == "invalid" and cand.get("source") in (
            "industry_prior", "first_last_fallback"
        ):
            continue
        top = cand
        break
    else:
        result.best_email = ""
        result.best_email_confidence = 0
        result.best_email_evidence = [
            "⚫ Every constructed DM pattern came back NB-invalid. "
            "Domain confirmed to reject mail at first.last / flast / etc."
        ]
        result.safe_to_send = False
        result.time_seconds = round(time.time() - start, 2)
        _record_cache_savings(result, cache)
        return result

    result.best_email = top["email"]
    result.best_email_confidence = top["confidence"]
    result.best_email_evidence = _build_evidence(top, result)

    if top.get("nb_result") == "catchall":
        result.risky_catchall = True
        result.safe_to_send = False
        result.best_email_evidence.append(
            "⚠️ Catch-all domain — recommended but unverified; track bounces"
        )
    else:
        result.safe_to_send = top["confidence"] >= confidence_threshold
        if not result.safe_to_send:
            result.best_email_evidence.append(
                f"⚠️ Below threshold ({top['confidence']} < {confidence_threshold})"
            )

    # If the eligible list differed from the full candidate list, note
    # which generic inboxes were suppressed so the evidence trail
    # shows the operator what we skipped.
    suppressed = [c["email"] for c in candidates
                   if c not in eligible and c.get("email")]
    if suppressed:
        result.best_email_evidence.append(
            f"ℹ️ Suppressed {len(suppressed)} generic-inbox candidate"
            f"{'s' if len(suppressed) != 1 else ''}: "
            f"{', '.join(suppressed[:3])}"
            f"{'…' if len(suppressed) > 3 else ''}"
        )

    result.time_seconds = round(time.time() - start, 2)
    _record_cache_savings(result, cache)
    return result


def _record_cache_savings(result: TriangulationResult, cache: _Cache):
    """Approximate the dollar savings this run captured from cache hits."""
    # Lightweight: re-read stats and attribute delta to this run isn't precise,
    # so we just expose the total cache savings via cache_stats() and report
    # per-run cost_estimate optimistically.
    # Leave concrete per-run savings to future instrumentation.
    pass


def _build_evidence(candidate: dict, result: TriangulationResult) -> list[str]:
    ev: list[str] = []
    if result.detected_pattern and candidate["pattern"] == result.detected_pattern.pattern_name:
        names = ", ".join(result.detected_pattern.evidence_names[:2])
        n = len(result.detected_pattern.evidence_names)
        ev.append(f"✅ Pattern '{candidate['pattern']}' triangulated from {n} owner(s): {names}")
    elif candidate["source"] == "first_last_fallback":
        ev.append("ℹ️ Default pattern: first.last@ (B2B prior)")
    elif candidate["source"] == "industry_prior":
        ev.append(f"ℹ️ Industry prior for {result.decision_maker.title or 'generic'}")

    if candidate.get("nb_valid"):
        ev.append("✅ NeverBounce: VALID")
    elif candidate.get("nb_result") == "catchall":
        ev.append("⚠️ NeverBounce: CATCH-ALL")
    elif candidate.get("nb_result") == "invalid":
        ev.append("❌ NeverBounce: INVALID")

    if candidate.get("smtp_valid") and not candidate.get("smtp_catchall"):
        ev.append("✅ SMTP: accepted")
    elif candidate.get("smtp_catchall"):
        ev.append("⚠️ SMTP: catch-all")

    ev.append(f"Owner via: {result.decision_maker.source}")
    return ev


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 9: INTEGRATION SHIM (drop-in for your existing scrape_with_triangulation)
# ═════════════════════════════════════════════════════════════════════════════

def scrape_with_triangulation(business: dict) -> TriangulationResult:
    """Drop-in replacement for v3/v4 entry point — universal + cached."""
    website = business.get("website", "")
    if not website:
        return TriangulationResult()
    domain = urlparse(website).netloc.replace("www.", "")
    return triangulate_email(
        business_name=business["business_name"],
        website=website,
        domain=domain,
        address=business.get("address", ""),
        industry=(business.get("business_type") or "").lower(),
        decision_maker_hint=business.get("contact_name"),
        scraped_emails=[],
    )


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 10: CLI TEST HARNESS (run directly to verify everything works)
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Universal pipeline CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run", help="Run pipeline on one business")
    r.add_argument("--name", required=True)
    r.add_argument("--website", required=True)
    r.add_argument("--address", default="")
    r.add_argument("--industry", default="")
    r.add_argument("--contact", default="")

    sub.add_parser("stats", help="Show cache stats")
    sub.add_parser("purge", help="Purge expired cache entries")

    args = parser.parse_args()

    if args.cmd == "run":
        domain = urlparse(args.website).netloc.replace("www.", "")
        result = triangulate_email(
            business_name=args.name,
            website=args.website,
            domain=domain,
            address=args.address,
            industry=args.industry,
            decision_maker_hint=args.contact or None,
        )
        out = {
            "best_email": result.best_email,
            "confidence": result.best_email_confidence,
            "safe_to_send": result.safe_to_send,
            "risky_catchall": result.risky_catchall,
            "decision_maker": (
                asdict(result.decision_maker) if result.decision_maker else None
            ),
            "detected_pattern": (
                asdict(result.detected_pattern) if result.detected_pattern else None
            ),
            "agents_run": result.agents_run,
            "agents_succeeded": result.agents_succeeded,
            "time_seconds": result.time_seconds,
            "evidence": result.best_email_evidence,
        }
        print(json.dumps(out, indent=2, default=str))

    elif args.cmd == "stats":
        print(json.dumps(cache_stats(), indent=2))

    elif args.cmd == "purge":
        removed = get_cache().purge_expired()
        print(f"Purged {removed} expired entries.")
