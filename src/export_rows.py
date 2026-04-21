"""
Shared row-builder for the Export CSV and Bulk Scrape pages.

Single source of truth for:
  - The full export schema (all fields we know about a business lead)
  - The visual Badge column (🟢 HIGH / 🟡 MED / 🔴 LOW / ⛔ SKIP / ❔ ?)
  - Name parsing (first/last from "Dr. Jane Smith, DDS")
  - Evidence-trail derivation from professional_ids JSON

Lives in src/ so the Streamlit pages stay thin glue and can't drift from
each other.
"""
from __future__ import annotations

import json
from typing import Iterable


# ── Name parsing ──────────────────────────────────────────────────────

_TITLE_PREFIXES = {
    "dr", "dr.", "doctor", "mr", "mr.", "mrs", "mrs.", "ms", "ms.",
    "miss", "prof", "prof.", "professor", "attorney", "atty", "atty.",
    "sir", "madam", "rev", "rev.", "reverend", "hon", "hon.",
    "honorable", "capt", "capt.", "captain", "lt", "lt.",
}

_CREDENTIAL_SUFFIXES = {
    "dmd", "dds", "md", "do", "phd", "dpm", "od", "dc", "dvm", "edd",
    "mba", "jd", "esq", "esquire", "cpa", "rn", "np", "pa", "pa-c",
    "bsn", "msn", "aprn", "fnp", "mph", "mha", "ms", "ma", "ba", "bs",
    "facp", "faap", "facog", "facs",
}


def split_contact_name(full_name: str) -> tuple[str, str]:
    """
    Parse "Dr. Caleb Martin, DMD" → ("Caleb", "Martin").
    Strips prefix titles and credential suffixes. Middle names are dropped.
    """
    if not full_name:
        return ("", "")
    name = full_name.split(",")[0].strip()
    tokens = name.split()
    if not tokens:
        return ("", "")
    while tokens and tokens[0].lower().rstrip(".") in _TITLE_PREFIXES:
        tokens.pop(0)
    while tokens and tokens[-1].lower().rstrip(".") in _CREDENTIAL_SUFFIXES:
        tokens.pop()
    if not tokens:
        return ("", "")
    if len(tokens) == 1:
        return (tokens[0], "")
    return (tokens[0], tokens[-1])


# ── Badge (confidence + verification + catch-all awareness) ──────────

def verify_badge(biz: dict) -> str:
    """
    Combine confidence + SMTP evidence + safe_to_send into one visual label.
    Demotes 🟢 HIGH → 🟡 MED when the row is actually unsafe (catch-all or
    triangulation flagged it below threshold), so operators don't ship
    unverified sends.

    Volume mode adds a distinct 🟣 REVIEW tier for NB-unknown rows — cold
    outreach should NOT auto-send these. The operator filters them out
    or manually inspects before adding to a send batch.
    """
    conf = (biz.get("confidence") or "").lower()
    src = (biz.get("email_source") or "").lower()
    reason = (biz.get("email_verification_reason") or "").lower()
    safe_flag = biz.get("email_safe_to_send")
    is_unsafe = (safe_flag == 0 or safe_flag is False) or "catch-all" in reason
    if conf == "high" and "smtp_verified" in src:
        return "🟢 VERIFIED" if not is_unsafe else "🟡 MED (catch-all)"
    if conf == "high":
        return "🟢 HIGH" if not is_unsafe else "🟡 MED (catch-all)"
    if conf == "review":
        return "🟣 REVIEW (NB unknown)"
    if conf == "medium" and "unverified_with_signal" in src:
        return "🟡 MED (unverified)"
    if conf == "medium":
        return "🟡 MEDIUM"
    if conf == "skip":
        return "⛔ SKIP"
    if conf == "low":
        return "🔴 LOW"
    return "❔ ?"


# ── Evidence-trail parsing ───────────────────────────────────────────

def _parse_prof_ids(biz: dict) -> dict:
    raw = biz.get("professional_ids") or {}
    if isinstance(raw, str):
        try:
            return json.loads(raw) or {}
        except Exception:
            return {}
    return raw if isinstance(raw, dict) else {}


def evidence_ticks(biz: dict) -> dict:
    """Returns {'smtp': '✓/blank', 'whois': '✓/✗/blank', 'npi_pattern': '✓/blank'}."""
    source = (biz.get("email_source") or "").lower()
    prof = _parse_prof_ids(biz)
    agents_ok = set(prof.get("agents_succeeded") or [])
    pattern_confirmed = bool(prof.get("detected_pattern"))
    smtp = "✓" if ("smtp_verified" in source
                   or "smtp_probe" in agents_ok
                   or "neverbounce" in agents_ok) else ""
    whois = ("✓" if ("whois_confirmed" in source or "whois" in agents_ok)
             else ("✗" if "whois_mismatch" in source else ""))
    npi_pat = "✓" if (
        "npi_registry" in source
        or "_pattern_confirmed" in source
        or "_npi_anchored" in source
        or "npi_healthcare" in agents_ok
        or pattern_confirmed
    ) else ""
    return {"smtp": smtp, "whois": whois, "npi_pattern": npi_pat}


# ── Full export schema (used by Export CSV + Bulk Scrape CSV) ────────

# Canonical column order. Both pages MUST preserve this for downstream
# audit-tool compatibility.
EXPORT_COLUMNS = [
    "Badge", "Score", "Tier",
    # Evidence columns (conditionally included — see build_row argument)
    "SMTP ✓", "WHOIS ✓", "NPI/Pattern ✓",
    # Business identity
    "Business Name", "Business Type", "Location", "Phone", "Website",
    # Contact
    "Email", "First Name", "Last Name", "Contact Name", "Contact Title",
    # Provenance
    "Email Source", "Confidence",
    # Google signals
    "Rating", "Review Count", "Place ID",
    # Verification
    "Email Status",
    # Raw triangulation evidence (large JSON blob; audit-tool reads this)
    "Professional IDs",
]


def build_row(biz: dict, *, include_evidence: bool = False) -> dict:
    """
    Build the full export row for a business. `include_evidence=True` keeps
    the SMTP/WHOIS/NPI tick columns; False drops them (they widen the
    table without giving a human operator actionable info).
    """
    ticks = evidence_ticks(biz)
    email = biz.get("primary_email") or ""
    first, last = split_contact_name(biz.get("contact_name") or "")

    # Carry full evidence trail for audit-tool consumers
    prof = biz.get("professional_ids") or ""
    if not isinstance(prof, str):
        try:
            prof = json.dumps(prof)
        except Exception:
            prof = ""

    row = {
        "Badge": verify_badge(biz),
        "Score": biz.get("lead_quality_score") or "",
        "Tier": biz.get("lead_tier") or "",
    }
    if include_evidence:
        row["SMTP ✓"] = ticks["smtp"]
        row["WHOIS ✓"] = ticks["whois"]
        row["NPI/Pattern ✓"] = ticks["npi_pattern"]
    row.update({
        "Business Name": biz.get("business_name") or "",
        "Business Type": biz.get("business_type") or "",
        "Location":      biz.get("address") or biz.get("location") or "",
        "Phone":         biz.get("phone") or "",
        "Website":       biz.get("website") or "",
        "Email":         email,
        "First Name":    first,
        "Last Name":     last,
        "Contact Name":  biz.get("contact_name") or "",
        "Contact Title": biz.get("contact_title") or "",
        "Email Source":  biz.get("email_source") or "",
        "Confidence":    biz.get("confidence") or "",
        "Rating":        biz.get("rating"),
        "Review Count":  biz.get("review_count"),
        "Place ID":      biz.get("place_id") or "",
        "Email Status":  biz.get("email_status") or "",
        "Professional IDs": prof,
    })
    return row


def build_rows(businesses: Iterable[dict], *, include_evidence: bool = False) -> list[dict]:
    return [build_row(b, include_evidence=include_evidence) for b in businesses]


# ── Narrow view for the on-screen Bulk-Scrape table ──────────────────

# The full row has 20+ columns. Streamlit's dataframe renders best with
# ~10. Use this subset for the visible table; keep the full row for CSV
# download.
BULK_DISPLAY_COLUMNS = [
    "Badge", "Score", "Tier", "Business Name", "Location",
    "Rating", "Review Count", "Email", "Email Source",
    "Contact Name", "Contact Title", "Website",
]
