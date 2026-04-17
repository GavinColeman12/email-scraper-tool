# Verified Email Scraping — Design

**Date:** 2026-04-17
**Project:** email-scraper
**Goal:** Reduce cold-email bounce rate from ~15% to <2% while keeping scrape cost under $0.30 per 200 businesses.

## Problem

Four emails bounced in the last batch, all with "address not found". All were **constructed** emails where the scraper guessed the wrong pattern (`amy.morgan@` when the real pattern was different). MX verification passed because the domain accepts mail — but the specific mailbox didn't exist.

The scraper's MEDIUM confidence tier is currently too broad. It bundles:
- Blind pattern guesses with no supporting evidence
- Pattern-detected constructions from multiple scraped emails
- LinkedIn-identified names with no website match

That makes "filter by HIGH + MEDIUM" unreliable. Users can't trust MEDIUM without a second look.

## Objectives

1. **Primary:** Drop bounce rate to <2% by verifying constructed emails against the recipient's SMTP server before saving them as `primary_email`.
2. **Secondary:** Improve extraction quality (names, titles, email-to-person matching) on messy pages where rule-based parsing fails, without burning Claude calls on clean pages.
3. **Tertiary:** Tighten MEDIUM tier semantics so the HIGH+MEDIUM filter is trustworthy for direct send.

Non-goals: replacing the Plotly PDF charts, changing the search/pagination logic, re-architecting storage.

## One-click workflow

After finding businesses on Page 1, user should be able to run the full pipeline with one button:

```
1. Find businesses → save to search
2. Click "🚀 Run full pipeline" on Find Businesses page
3. Background job runs Verified mode (scrape + Haiku + SMTP) on all pending businesses
4. User can navigate to Bulk Scrape to watch live progress, OR come back later
5. When complete, Export CSV page shows the ranked export-ready list with verification badges
```

The button on Find Businesses triggers the same background job as Bulk Scrape's "Start in background" — just wired one-shot so the user doesn't need to navigate pages. Verified mode is forced (not user-selectable in this entry point) because we're standardizing on the recommended pipeline.

## Architecture

### Tightened confidence tiers

New semantic definitions replace the current ad-hoc usage:

| Tier | Criteria |
|---|---|
| **HIGH** | SMTP-verified as deliverable, OR scraped personal email from the site, OR cross-verified across 2+ independent sources (website + LinkedIn + press). |
| **MEDIUM** | Pattern detected from ≥2 real emails at the same domain AND person identified via LinkedIn / team page / press. Requires **two positive signals** — never a blind guess. |
| **LOW** | Single weak signal (e.g., one scraped email + no person, or one person + no pattern). Hidden from default Bulk Scrape filters; users can unhide. |
| **SKIP** | SMTP probe returned INVALID, OR SMTP UNKNOWN without any positive signal. Business gets no primary email and is dropped from top-N ranking. |

### SMTP verification integrated into scrape pipeline

`verify_smtp_patterns(first, last, domain)` already exists in `src/email_verifier.py` but is only called when the user manually triggers the verify page. The scrape pipeline needs to call it automatically on constructed candidates.

Decision table for SMTP output:

| SMTP Result | Positive signal present | Action | Confidence |
|---|---|---|---|
| VALID | any | Save verified email as `primary_email` | HIGH |
| INVALID (all patterns) | any | Skip (no primary) | SKIP |
| UNKNOWN (probe blocked) | Yes (pattern + person) | Save best-guess pattern | MEDIUM |
| UNKNOWN | No | Skip | SKIP |
| CATCH-ALL | any | Save best-guess with note | MEDIUM (flagged catch-all) |

"Positive signal" = pattern confidence >= medium (from ≥2 real emails) AND identified person (LinkedIn match or team-page entry).

### Conditional Haiku fallbacks

Claude Haiku runs **only when rule-based extraction is weak**, not on every business. Gating rules:

| Haiku function | Triggers when | Returns |
|---|---|---|
| `haiku_extract_people(html, business_name)` | Rules found <2 named people on team/about pages | `[{name, title, email_if_present, bio_excerpt}]` |
| `haiku_match_emails_to_people(emails, people, domain)` | ≥1 scraped email has an ambiguous prefix AND ≥1 person found with no email match | `{email: matched_person_name_or_null}` |
| `haiku_filter_false_positives(candidates)` | ≥5 candidate emails at the domain (signals noisy page) | Filtered list; junk like `services@`, `2x.png@` removed |
| `haiku_cross_reference(name, linkedin_snippet, press_snippet)` | Same name appears in ≥2 sources with conflicting titles | `{name, consolidated_title, confidence_reasoning}` |

**Cost control:** ~30% of businesses trigger a Haiku call; each call is ~500-1500 tokens = ~$0.002. Total ~$0.24 per 200 businesses.

### Three modes on Bulk Scrape page

| Mode | Pipeline | Cost / 200 | Time / biz | Expected bounce |
|---|---|---|---|---|
| **Basic** | Rules only; no Claude; no SMTP | $0 | ~2s | ~15% |
| **Verified** (new default) | Rules + conditional Haiku + SMTP pattern testing + tightened tiers | ~$0.30 | ~6s | <2% |
| **Deep** | 4 research agents (website, schema.org, LinkedIn, press) + Sonnet synthesizer + SMTP | ~$2 | ~10s | <2% |

Verified becomes the default — Basic stays for fast reconnaissance sweeps.

### Data flow — per business in Verified mode

```
1. Base scrape (rules)      → scraped_emails, extracted_people
2. If <2 people found       → Haiku extract_people
3. If >=5 candidates        → Haiku filter_false_positives
4. Pattern detection        → pattern + confidence from multiple emails
5. Pick best person (rules + LinkedIn cross-ref)
6. If ambiguous email→person → Haiku match_emails_to_people
7. Construct candidate using detected pattern
8. verify_smtp_patterns     → VALID / INVALID / UNKNOWN
9. Apply tiered confidence rules above
10. Compute lead_quality_score (unchanged)
```

Each step composes — failures fall back to lower-quality signals rather than erroring.

## Components

### New files

- `src/haiku_scraper.py` (~300 lines) — 4 Haiku functions, each with strict JSON schema for the response. Uses the current Haiku model (exact name resolved during implementation). Each function gracefully degrades to empty list / identity transform if Haiku unavailable or JSON parse fails.

### Modified files

- `src/email_scraper.py`
  - `scrape_business_emails()` gets new parameter `auto_verify: bool = False`.
  - When True, after `_pick_top_contact` produces a constructed email, calls `verify_smtp_patterns()` and applies the decision table above.
  - Adds hooks to call `haiku_extract_people()` when `len(uniq_persons) < 2`.
  - Adds hooks for `haiku_filter_false_positives` and `haiku_match_emails_to_people`.

- `src/deep_scraper.py`
  - Enables `auto_verify=True` by default in deep mode.
  - Passes Haiku-extracted people to the synthesizer alongside rule-based extraction.

- `src/lead_scoring.py`
  - Adjust `_VERIFY_ADJUSTMENT` so SMTP-SKIP businesses get `lead_quality_score = 0` (drops them below the threshold).

- `pages/5_🚀_Bulk_Scrape.py`
  - 3-mode radio instead of 2.
  - Updated cost estimate per mode.
  - Verified becomes the default selected option.

## Error handling

- **SMTP timeout / connection refused:** Treat as UNKNOWN; fall through to signal-check logic.
- **SMTP catch-all detected:** Tag with `is_catchall=True`; confidence capped at MEDIUM; shown to user with a warning badge on the Bulk Scrape UI.
- **Haiku API failure:** Log to stderr, continue with rule-based extraction (graceful degradation — we never block on Haiku).
- **Haiku returns invalid JSON:** Log, return empty result, continue with rules.
- **No `ANTHROPIC_API_KEY`:** Verified mode silently skips Haiku hooks but still runs SMTP verification. Cost drops to $0; quality drops toward Basic.

## Testing

Each component tested independently:

1. **SMTP decision table** — unit test with mocked `verify_smtp_patterns` results (VALID / INVALID / UNKNOWN / CATCH-ALL) paired with signal variations. Assert correct tier assignment.
2. **Haiku gating** — unit test that rule-based pipeline with 3+ people doesn't call Haiku; pipeline with 0 people does call it.
3. **Haiku JSON parsing** — unit test with real Haiku responses + adversarial responses (malformed JSON, missing fields). Assert graceful fallback.
4. **End-to-end smoke** — run Verified mode on 5 known businesses (1 clean site, 1 Cloudflare-protected, 1 messy team page, 1 small business with no team page, 1 known-bad email for SMTP INVALID path). Assert correct outcomes.
5. **Bounce regression** — the 4 businesses that bounced yesterday. Run Verified mode. Expected: 3 of 4 get a verified working pattern; 1 gets SKIP. Zero of 4 get an unverified primary email.

## Risks and mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| SMTP port 25 blocked on Streamlit Cloud after platform change | High — Verified mode degrades to Basic | Detection on startup; fall back and warn user in UI |
| Haiku rate limits or API errors | Medium — partial coverage | Graceful degradation + retry with jitter |
| Too many businesses hit SKIP (over-pruning) | Medium — user gets fewer leads | Track and surface SKIP count on the UI; user can loosen to Basic |
| Latency on large batches (200 × 6s = 20min) | Low — already using background jobs | No change needed; jobs survive page nav |

## Out of scope

- Paid verification integration (ZeroBounce) — already exists and works independently.
- Changes to the Google Maps search / pagination / synonym logic.
- UI overhaul of the results table.
- Cross-session persistence of Haiku JSON responses (no caching optimization in v1).
