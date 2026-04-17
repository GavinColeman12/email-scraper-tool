# Phone Cross-Reference via WHOIS + Licensing Boards — Design

**Date:** 2026-04-17
**Project:** email-scraper
**Goal:** Use the business's listed phone number to (1) cross-verify scraped email domains via WHOIS and (2) discover authoritative provider names via national licensing registries — both feeding into the existing confidence-tier system.

## Problem

Phone numbers are mostly a dead end for email discovery. But two narrow applications add real signal:

1. **WHOIS cross-verification** — if the domain's WHOIS registrant phone matches the business's listed phone, the scraped email is proven to belong to this business (not a squatter or misattribution). Works on ~40% of domains (rest are privacy-protected).

2. **Licensing board lookups** — national registries (NPI for medical, ADA for dental) return authoritative provider names for a practice. These names anchor email construction with higher confidence than team-page extraction.

## Design

### 1. WHOIS verifier — `src/whois_verifier.py`

```python
def lookup_whois_phone(domain: str) -> dict:
    """Returns {phone, registrant_name, privacy_protected, error}."""

def normalize_phone(phone: str) -> str:
    """Strip formatting, return digits only with optional leading 1."""

def phones_match(a: str, b: str) -> bool:
    """True if last 10 digits match (ignores country code / format)."""

def verify_against_business_phone(domain: str, business_phone: str) -> dict:
    """Returns {matches: True|False|None, registrant_phone, privacy_protected}.
    matches=None when WHOIS is privacy-protected (no signal)."""
```

**Phone normalization:** strip `+`, `-`, `(`, `)`, spaces, dots. Keep only digits. Take last 10 digits for comparison (tolerates `1` country code).

**Privacy detection:** if registrant_phone contains "REDACTED", "DATA REDACTED", "WhoisGuard", or is literally empty after parsing, mark `privacy_protected=True` and `matches=None`.

**Rate limiting:** WHOIS queries are rate-limited by registrar. Cache by domain in-memory for the session. Add a 0.2s delay between lookups to be polite.

**Error handling:** any exception → return `{matches: None, error: "..."}`. Never blocks the pipeline.

### 2. Licensing lookup — `src/licensing_lookup.py`

```python
def lookup_medical_providers(business_name: str, city: str, state: str) -> list:
    """Query NPI Registry API. Returns [{name, first, last, npi, specialty}]."""

def lookup_dentists(business_name: str, city: str, state: str) -> list:
    """Scrape ADA Find-A-Dentist public search. Returns same shape."""

def lookup_licensed_providers(vertical: str, business_name: str,
                               city: str, state: str) -> list:
    """Dispatcher — picks the right backend based on vertical."""
```

**NPI Registry API:** `https://npiregistry.cms.hhs.gov/api/?version=2.1&...`
- Free, no API key required
- Search by `organization_name` + `state` + `city`
- Returns JSON with provider list
- Rate limit: generous (~100/sec)

**ADA Find-A-Dentist:** `https://findadentist.ada.org/api/...`
- Has a public-facing search page; the underlying JSON API returns dentist records
- No auth required
- Rate limit: add 1s delay per request to be polite
- Fallback: if API returns error, skip silently

**Vertical mapping:**
- `dental_practice` → `lookup_dentists()`
- `medical_clinic`, `chiropractic`, `physical_therapy` → `lookup_medical_providers()`
- All others → return `[]` (V1 scope)

### 3. Integration in `src/deep_scraper.py`

New step inserted between website scrape and synthesizer:

```
Step 1: Base scrape (existing)
Step 2: Research agents (existing)
Step 3: [NEW] WHOIS verification if we have listed phone
Step 4: [NEW] Licensing lookup if vertical is dental/medical AND <2 people found
Step 5: Merge results into context
Step 6: Claude synthesizer (existing)
```

**WHOIS integration:** results go into `agent_findings["whois"] = {matches: bool|None, registrant_phone}`.

**Licensing integration:** returned providers get added to `agent_findings["website"]["people"]` with `authority=15` (higher than team page). Also fed to Claude synthesizer so it can cite the registry.

### 4. Confidence tier bumps — `src/email_scraper.py`

After `_pick_top_contact` picks a primary_email, a new `_apply_whois_boost` helper runs:

```
if whois_result["matches"] == True:
    # Scraped personal email → already HIGH, stays HIGH, add suffix
    # Constructed email with pattern evidence → bumps MEDIUM to HIGH
    # Constructed email without pattern → bumps LOW to MEDIUM
    email_source += "_whois_confirmed"

if whois_result["matches"] == False:
    # Actively mismatched — rare but important
    email_source += "_whois_mismatch_warning"
    # Don't auto-downgrade; let user see the warning
```

Privacy-protected (matches=None) doesn't change anything — neutral signal.

### 5. Gating rules (keep costs + latency low)

| Enhancement | Fires when |
|---|---|
| WHOIS lookup | Always when we have `audit_data.phone` AND a resolved domain |
| NPI medical lookup | Vertical is medical/chiro/PT AND rule-based extraction found <2 people |
| ADA dental lookup | Vertical is dental AND rule-based extraction found <2 people |

Both paid modes (Verified, Deep) call these. Basic mode skips licensing lookup but still runs WHOIS (it's free and fast).

### 6. Testing

- **Phone normalization** unit tests — `(555) 123-4567` + `+1-555-123-4567` + `1.555.123.4567` all normalize identically
- **WHOIS live test** — `lookup_whois_phone("google.com")` returns a phone, `lookup_whois_phone("privacy-protected-domain.com")` returns `privacy_protected=True`
- **NPI API live test** — `lookup_medical_providers("Kaiser Permanente", "Oakland", "CA")` returns >0 providers
- **Confidence boost** unit test — WHOIS match + MEDIUM construction → HIGH with suffix
- **Regression test** — existing test businesses still produce same output when WHOIS is privacy-protected

## Error handling

| Scenario | Behavior |
|---|---|
| WHOIS registrar rate limits | Skip this domain, no signal |
| NPI API down | Log, continue with rule-based only |
| ADA site HTML structure changes | Fallback to empty list |
| Missing `audit_data.phone` | WHOIS skipped silently |
| Malformed domain | Skipped silently |

Nothing blocks the pipeline. All failures are logged to stderr for observability.

## Files changed

| File | Change |
|---|---|
| `src/whois_verifier.py` | NEW |
| `src/licensing_lookup.py` | NEW |
| `src/deep_scraper.py` | New integration step + `agent_findings` additions |
| `src/email_scraper.py` | New `_apply_whois_boost` helper, wired after `_pick_top_contact` |
| `requirements.txt` | Add `python-whois>=0.9.0` |

## Out of scope

- State-specific licensing scrapers (lawyers, real estate) — too fragmented across 50 states
- Reverse-phone lookup services (Whitepages, Truecaller) — consumer data, not B2B
- Phone carrier lookups — no email signal
