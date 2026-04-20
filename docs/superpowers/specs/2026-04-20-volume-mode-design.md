# Volume Mode — cheap mass email discovery

**Date:** 2026-04-20
**Status:** Approved

## Context

Current triangulation costs $0.03–$0.06/biz ($6–12 per 200) and aims for
60–80% decision-maker hit rate with NeverBounce verification. That's the
right tool for high-stakes outreach (consulting engagements, named
accounts). It's the wrong tool for mass spaghetti-throw outreach where
you want to churn through 200+ leads at a time and are fine sending to
40% of them.

Volume Mode is a separate pipeline for the mass-outreach case:

- Use what's free (website crawl, NPI, WHOIS) aggressively
- One SearchApi call ONLY as LinkedIn fallback when the crawl missed
  the owner
- Selective NeverBounce — verify only the candidates we actually
  believe in (directly scraped emails, DM emails built from a pattern
  triangulated from ≥2 real scraped evidence emails). Skip NB on
  industry-prior guesses — $0.003 is not worth spending on a
  16-confidence shot in the dark.
- **Never pick a generic-inbox email as the primary**: info@, contact@,
  smile@, hello@, etc. are demoted from "winner" to "evidence only".
  Cold outreach to those inboxes is filtered by admins and never reaches
  the decision maker.
- Emits the same 23-column export schema as triangulation so the
  downstream audit + send pipeline doesn't care which mode produced
  the row.

**Expected economics:** ~$0.009/biz (~$1.80 per 200), 40–60% DM-email
hit rate. Triangulation pipeline stays available for the leads that
warrant it.

## Goals

- One toggle in Bulk Scrape to run 200 businesses for <$2 and get a
  filterable list of 🟢 volume-verified / 🟡 volume-scraped / 🔴
  volume-guess / ⚫ empty leads.
- Export CSV is identical shape to triangulation's — no downstream
  consumer cares which mode produced the row.
- Bounce tracker picks up volume-mode sends same as triangulation
  sends, so `pattern_success` / `industry_pattern_success` tables
  continue to learn regardless of mode.
- Volume mode primary email NEVER lands on a generic shared inbox.

## Non-goals (this spec)

- Replacing triangulation. Volume mode is additive — pick at run time.
- Wiring `pattern_success` / `industry_pattern_success` into scoring.
  That's Phase 2 and a separate spec.
- An automated send pipeline. Volume mode produces a CSV; sending is
  downstream.

## Architecture

New module `src/volume_pipeline.py` with one public entry point:

```python
def scrape_volume(business: dict, *, use_neverbounce: bool = True) -> VolumeResult
```

Reuses existing free agents from `src/universal_pipeline.py`:
- `_agent_website_scrape` — deep 4-phase crawler (sitemap + bio pages + JSON-LD)
- `_agent_npi_healthcare` — NPI-1 by postal code (healthcare only)
- `_agent_whois` — WHOIS registrant (usually null, but free)
- `_agent_linkedin_gated` — LinkedIn Google search ($0.005, **fallback only**)
- `_synthesise_owners` — rank + dedup candidates
- `_triangulate_pattern` — detect pattern from scraped email evidence
- `_nb_verify_cached` — NeverBounce (called selectively)

Skips (vs triangulation):
- `_agent_combined_owner_and_press` — the main $0.005/biz SearchApi call
- `_agent_colleague_emails` — up to 3× $0.005/biz
- Haiku LLM classifier (`filter_real_people`) — ~$0.001–0.002/biz
- Aggressive NB walk (up to 5 calls/biz → capped at 3 here, selective)

## Priority (the product rule)

**Decision maker first. Industry pattern guess only as last resort.**
Generic inboxes are never picked, period.

## Per-business flow

```
┌──────────────────────────────────────────────────────────────────────┐
│ 1. Deep website crawl (existing, free)                               │
│    → names + @domain emails + detected pattern (if ≥2 evidence)      │
├──────────────────────────────────────────────────────────────────────┤
│ 2. NPI (free, healthcare only) + WHOIS (free)                        │
│    → federal/registry-backed DMs                                     │
├──────────────────────────────────────────────────────────────────────┤
│ 3. Stopword filter (no Haiku in volume mode — bounce tracker fixes)  │
│    → ranked DM list via _synthesise_owners                           │
├──────────────────────────────────────────────────────────────────────┤
│ 4. LinkedIn fallback — FIRES ONLY IF: no plausible DM from 1+2+3.    │
│    One $0.005 SearchApi call.                                        │
├──────────────────────────────────────────────────────────────────────┤
│ 5. Build candidates — STRICT RANKING:                                │
│    a. Scraped personal email matching DM name       ← DM proven      │
│         (e.g. marc@firm.com when DM is Marc)                         │
│    b. DM email built from TRIANGULATED pattern       ← DM likely     │
│         (pattern evidenced from ≥2 real emails at this domain)       │
│    c. Scraped personal email (non-generic, any person) ← person real │
│    d. DM email from INDUSTRY-PRIOR pattern           ← LAST RESORT   │
│         law firms: firstname.lastname@                               │
│         dental/medical: first.last@ or flast@                        │
│         (see src/industry_patterns.py)                               │
│    e. first.last@ universal fallback if still no DM                  │
│                                                                      │
│    GENERIC INBOXES (info@, contact@, smile@, hello@, sales@, etc.)   │
│    are stored in evidence_trail.discovered_emails for pattern        │
│    detection ONLY. They can never be picked as best_email. Even if   │
│    NB-valid. Even if no other option exists. → volume_empty instead. │
├──────────────────────────────────────────────────────────────────────┤
│ 6. Selective NeverBounce (cap 3 calls):                              │
│    ✓ NB every candidate in buckets (a), (b), (c)                     │
│    ✗ Skip NB for (d) and (e) — industry priors aren't worth $0.003   │
│      they get trusted/distrusted via bounce tracker over time        │
├──────────────────────────────────────────────────────────────────────┤
│ 7. Pick best_email:                                                  │
│    Walk buckets a → b → c → d → e in order.                          │
│    Within a bucket, NB-valid > NB-catchall > NB-unknown > not-tested │
│    First winner stops the walk.                                      │
│    If no bucket had a valid candidate → return empty.                │
└──────────────────────────────────────────────────────────────────────┘
```

## Industry priors used in bucket (d)

`src/industry_patterns.py` already maps industries to pattern priors.
Volume mode reads that module unchanged:

| Industry | Primary prior | Secondary |
|---|---|---|
| Law firm | `firstname.lastname@` | `flastname@` |
| Dental | `first.last@` | `flast@`, `drlast@` |
| Medical | `first.last@` | `flast@` |
| Construction / trades | `first@` (solo) or `first.last@` | `flast@` |
| SaaS / tech | `first@` | `first.last@` |
| Accounting / consulting | `first.last@` | `flast@` |
| Real estate | `first.last@` | `first@` |

These are exactly the patterns that win in each industry. Volume mode
constructs ONE primary-prior email per DM; bucket (d) doesn't spray
multiple guesses because every extra guess is an extra $0.003 NB call
we're intentionally NOT making.

## Generic-inbox blacklist

```python
VOLUME_GENERIC_BLACKLIST = {
    "info", "contact", "contactus", "contact-us",
    "hello", "hi", "team", "support", "admin", "office", "mail",
    "enquiries", "inquiries", "sales", "marketing", "help", "service",
    "customercare", "customer-care", "customerservice", "customer-service",
    "reception", "frontdesk", "front-desk", "appointments", "bookings",
    "smile", "welcome", "intake", "noreply", "no-reply", "donotreply",
    "webmaster", "postmaster", "general", "inbox",
}
```

Exact local-part match (case-insensitive). Substring matches (e.g. the
existing `GENERIC_SUBSTRINGS`) would be too aggressive and catch names
like `salesy@firm.com` from a real person Sal Salesy.

Generic emails still:
- Appear in `evidence_trail.discovered_emails` (pattern detection can
  use them)
- Appear in `professional_ids.candidate_emails` in the export
- Count toward pattern triangulation (seeing `smile@firm.com` +
  `marc@firm.com` tells us the domain accepts first@ format)

They do NOT:
- Get picked as `best_email`
- Get an NB call in volume mode (saves budget)

## Output contract

`VolumeResult` matches the shape of `TriangulationResult` so the shared
`src/export_rows.py` builder works unchanged. New confidence tiers
stored in the `confidence` field (mapped to the existing Badge logic):

| Tier | Meaning | Expected % of 200-biz run | Badge |
|---|---|---|---|
| `volume_verified` | Scraped OR pattern-matched + NB-valid | 25–40% | 🟢 HIGH |
| `volume_scraped` | Scraped personal, NB-catchall/unknown | 10–20% | 🟡 MEDIUM |
| `volume_guess` | Industry-prior constructed, no NB | 30–50% | 🔴 LOW |
| `volume_empty` | No plausible non-generic email | 5–15% | ❔ ? |

`email_source` uses the same descriptive labels from
`_describe_email_source` so operators can see WHY each pick was made
(triangulated pattern vs scraped personal vs industry prior).

## UI integration

**Bulk Scrape page** (`pages/5_🚀_Bulk_Scrape.py`):
- Add a new mode: **"🚀 Volume Mode — cheap mass (< $2 per 200)"**
- Position alongside existing Triangulation mode
- Same progress UI, same background-job flow
- Same CSV download (all 23 columns via `src/export_rows.py`)

**Confidence filter dropdown** picks up the new tiers automatically
because Badge logic is already driven by `confidence` + `email_safe_to_send`.

## Cost model

Per business:
- Phase 1 (crawl + NPI + WHOIS): **$0**
- LinkedIn fallback (fires on ~30% of biz): **~$0.0015 avg**
- Selective NB (avg 2 calls per biz that has scraped or pattern-match,
  ~70% of biz): **~$0.0042 avg**
- **Total avg: ~$0.006/biz, ~$1.20 per 200**

Worst case: every biz needs LinkedIn + 3 NB = $0.005 + $0.009 =
$0.014/biz, $2.80 per 200. Still 4× cheaper than triangulation.

Budget cap per run: exposed as a `volume_budget_usd` parameter with
$5.00 default. When reached, remaining businesses get crawl-only
(no LinkedIn, no NB) and fall through to the industry-prior tier.

## Learning loop

Volume mode is the PRIMARY feeder for the bounce-tracker learning loop:
- Every volume-mode send flows through `src/bounce_tracker.py`
- Bounces update `pattern_success` / `industry_pattern_success` tables
- Future volume runs consult `get_patterns_for(industry)` which reads
  from those tables — so the industry-prior guesses improve over time
- This is how `volume_guess` hit rate rises from 20% → 40%+ over weeks
  without changing any code

Wiring `pattern_success` into scoring is a separate spec — this one
just ensures volume-mode output flows cleanly into the tracker.

## Files

| Path | Change |
|---|---|
| `src/volume_pipeline.py` | **NEW** — orchestrator + generic-inbox filter + selective NB |
| `src/universal_pipeline.py` | No change — volume pipeline imports agents |
| `src/export_rows.py` | No change — shared schema already handles new confidence tiers |
| `src/email_scraper.py` | No change — `_describe_email_source` already covers volume-mode cases |
| `pages/5_🚀_Bulk_Scrape.py` | Add "Volume Mode" option + route to `volume_pipeline.scrape_volume()` |
| `src/background_jobs.py` | No change — existing `bulk_scrape` job type runs the function you pass |
| `tests/` | **NEW** — unit tests for generic-inbox filter, selective NB branching, ranking |

## Verification plan

1. **Unit tests** for `src/volume_pipeline.py`:
   - generic-inbox blacklist rejects all listed locals exactly
   - ranking prefers scraped-personal > triangulated-DM > industry-prior
   - selective NB fires on buckets (a/b/c) but not (d/e)
   - LinkedIn fallback fires iff no plausible DM after crawl+NPI

2. **Live A/B** via the replay tool:
   - Run volume mode on searches #28, #29, #24, #14
   - Compare to existing triangulation baselines
   - Measure: cost delta, DM-email delta, generic-email-picked (should be 0)

3. **Stress test** on 8 diverse industries (reuse `scripts/stress_test.py`):
   - Expect 0 generic-inbox picks
   - Expect 40–60% DM-email picks
   - Expect cost <$0.10 total

## Rollout

Single branch, one commit:
1. `src/volume_pipeline.py` + unit tests
2. `pages/5_🚀_Bulk_Scrape.py` integration
3. Replay A/B numbers captured in commit message

Triangulation mode stays default. Operators explicitly opt into Volume
Mode when budget matters.

## Open items (future specs)

- Wire `pattern_success` reads into volume-mode industry-prior scoring
  (the learning loop's second half)
- Auto-upgrade rule: if volume-mode returns `volume_empty` on a
  business, optionally re-run in triangulation mode for that one biz
  ($0.03 per upgrade, cheaper than re-running 200 in full triangulation)
- Export "secondary email" column with the best generic inbox as a
  manual-fallback option (operator can still try info@ if cold outreach
  to the DM fails)
