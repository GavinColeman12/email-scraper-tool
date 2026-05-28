# Crescendo CRM + Cold Outreach — Development Handoff

**Date:** 2026-05-28
**For:** Continuing development in a new chat session
**Owner:** Gavin Coleman (Founder & Principal, Crescendo Consulting)

---

## TL;DR — what this is

Built a complete cold-outreach engine for Crescendo Consulting over several sessions:
**scraper → Neon Postgres → NeverBounce-verified cohort → HubSpot CRM → personalized auto-send via Gmail SMTP.**

Everything lives in the git repo at `/Users/gavincoleman/Downloads/email-scraper` on branch **`feature/hubspot-crm-sync`** (pushed to GitHub: `GavinColeman12/email-scraper-tool`). ~30 commits.

---

## The business (context for any new session)

- **Crescendo Consulting** — Gavin (founder) + Joanna Webber (focuses on London/UK market).
- Sells an **AI diagnostic platform ("Sonar")** to SMBs across verticals (dental, law, restaurants, med spas, etc.). Pitch: "we find $100K+ in combined risk + opportunity."
- Demo/platform: `https://crescendo-consulting.net/sonar/demo` and the explore tool at `https://crescendo-platform.crescendo-consulting.net/explore`
- Admin demo dashboard (Railway): `https://truthful-mercy-production.up.railway.app/admin/demos`
- Per-vertical demos: `https://truthful-mercy-production.up.railway.app/demo/<vertical>` (restaurant, law, dental, bar, cafe, medical, med_spa — NO veterinarian yet)

---

## Current state — what's DONE

### HubSpot CRM (portal 246276084, data center na2)
- **Pipeline:** "SMB Sales Pipeline" (id=`default`), 7 stages: Cold Outreach → Warm Outreach → Demo Booked → Demo Done → Reviewing Materials → Closed Won — Purchased / Closed Lost
- **~768 contacts**, all from verified/deliverable sources (706 scraper cohort + 62 April-deliverable)
- **Custom properties** (Company): `business_vertical`, `of_locations`, `audit_sent_date`, `audit_url`, `source__list_batch`. (Contact): `role`, `lead_score`, `lead_quality_score_raw`, `email_source_category`, `priority_tier`, `best_time_to_call`
  - NOTE the quirky internal names: `of_locations` (not num_locations), `source__list_batch` (double underscore)
  - HubSpot's built-in `industry` has 148 locked options — we use `business_vertical` instead
- **4 dynamic lists:** UK Priority—LinkedIn (id=17), UK Pipeline All (id=18), Direct Scrape Cohort (id=19), US Priority—LinkedIn (id=20)
- **Plan tier:** Sales Hub Starter ($30/mo for 2 seats) — contact cap effectively removed for non-marketing contacts
- **Joanna invited:** jo.webber@crescendo-consulting.net (Standard user)

### Email sending (live + working)
- **SMTP via Gmail App Password** — sends from `gavin.coleman@crescendo-consulting.net`
- Domain auth is CLEAN: SPF + DKIM + DMARC at `p=quarantine` (enforcement). Established domain (Workspace since 2021, sending since 2016). 0 spam blocks.
- April campaign analysis: 340 sent, 40 hard bounces (all invalid addresses, ~0 spam blocks) — confirms deliverability is healthy, list quality was the only issue (now fixed by NeverBounce-verified pipeline)

### Live sync (Neon → HubSpot)
- `HUBSPOT_SYNC_ENABLED=true` — new scraped leads auto-sync to HubSpot (Company + Contact + Deal at Cold Outreach)
- Wired into BOTH `pages/1_🔎_Find_Businesses.py` AND `pages/5_🚀_Bulk_Scrape.py` workers
- One-way only: Neon → HubSpot. Updates to existing records don't auto-push (use backfill script).

---

## The scripts (all in `email-scraper/scripts/`)

| Script | What it does |
|---|---|
| `bootstrap_csv_export.py` | Export Neon leads → HubSpot-import CSV. Filter: "best cohort" (A/B tier + high conf OR LinkedIn) → ~629 leads. Modes via `BOOTSTRAP_FILTER` env. |
| `auto_send_outreach.py` | **The main sender.** Pulls a HubSpot list → personalized HTML email → Gmail SMTP. `SENDER=gavin/joanna`, `HUBSPOT_LIST_ID`, `LIMIT`, `DELAY_SEC`, `DRY_RUN`. Smart title-cases company names, vertical-aware phrasing, BCC auto-logs to HubSpot. |
| `generate_outreach_batch.py` | Alternative: generates an HTML page with one-click "Open in Gmail" buttons per lead (manual send). |
| `backfill_hubspot_sync.py` | Sync cohort leads from Neon → HubSpot (catch-up). `BACKFILL_HOURS`, `BACKFILL_SEARCH_ID`. |
| `dedupe_hubspot_companies.py` | Merge duplicate companies by domain. `DEDUPE_APPLY=1` to apply. |
| `cleanup_noise_contacts.py` | Delete Gmail-sync noise contacts. `CLEANUP_MODE=conservative/aggressive`, `CLEANUP_APPLY=1`. |
| `enrich_email_only_contacts.py` | For email-only contacts: fetch website title → company name → vertical → sync. Skips free-email/junk. |
| `reauth_gmail.py` | Re-auth Gmail OAuth (only needed for the IMAP/read path; SMTP send uses app password). |
| `london_batch_scrape.py` / `london_batch_expansion.py` | Bulk-scrape London verticals (hospitality + professional services). |

**Credentials** (all in `email-scraper/.env`, gitignored):
- `HUBSPOT_ACCESS_TOKEN` (Private App, all CRM scopes incl. contacts)
- `HUBSPOT_PIPELINE_ID=default`, `HUBSPOT_STAGE_*` (7 stage IDs)
- `GMAIL_ADDRESS=gavin.coleman@crescendo-consulting.net`, `GMAIL_APP_PASSWORD` (⚠️ exposed in old chat — regenerate)
- `LINKEDIN_URL`, `DEMO_URL`, `PLATFORM_URL=https://crescendo-consulting.net/sonar/demo`
- `DATABASE_URL` (Neon Postgres)
- `HUBSPOT_SYNC_ENABLED=true`

**Core modules:** `src/hubspot_client.py` (low-level REST wrapper, 13 tests), `src/hubspot_sync.py` (orchestrator + `_classify_vertical` + `_smart_title_case`, 11 tests). 24 tests passing via `python3 -m pytest tests/test_hubspot_*.py`.

---

## PRIORITY TASKS (next session) — ordered

### 🔴 #1 — Wire pre-computed audit findings into the email (THE personalization unlock)
**Problem Gavin hit:** deep personalization (April-style: "Baker has 119 reviews, you have 86; missing from 6 of 11 AI searches") was abandoned because regenerating LLM analysis per email made CAC skyrocket.

**The fix:** the audit findings are ALREADY computed by the Sonar/reputation-audit platform. Pull STORED findings into the email template instead of regenerating. Marginal cost ≈ $0/email = April quality at volume-play economics.

**The blocker:** those findings are NOT in this Neon `businesses` table (which only has rating, review_count, business_type). They live in the **Sonar/reputation-audit-tool platform** — likely:
- `/Users/gavincoleman/Downloads/reputation-audit-tool-V2/` (its own DB or data store), OR
- The Railway-hosted platform `truthful-mercy-production` (has its own backend DB)

**Next steps:**
1. Find where audit findings are stored — check `reputation-audit-tool-V2/src/` and its DB config. Look for review-gap, competitor, AI-search-miss, complaint-theme data per business.
2. Build a mapping: HubSpot contact email → audit findings (by domain or business name)
3. Add 2-3 structured personalization fields per lead (e.g., `review_gap_line`, `ai_search_line`, `top_finding`)
4. Update `auto_send_outreach.py compose_email()` to inject these when available, fall back to the generic template when not
5. **Tier it:** only deep-personalize tier-A leads (top ~15%) to keep it focused
**Lightweight interim option:** even without the audit platform, Neon HAS `rating` + `review_count`. Could add "I noticed your {rating}★ across {review_count} reviews" as a cheap personalization hook. Modest but free + immediate.

### 🔴 #2 — Watch a real prospect use `/sonar/demo` cold (no code — Gavin does this)
The demo is the actual conversion. The email's only job is the click. If the demo doesn't wow, nothing upstream matters. Highest-ROI non-code action.

### 🔴 #3 — Build the reply → booking → close flow
The engine is all front-end (sending). When someone replies "tell me more," what's the sequence? Booking link → demo → proposal → close. Revenue lives here, not in the first touch. Currently undefined.

### 🟠 #4 — Set up HubSpot meeting/booking links (Gavin + Joanna)
Sales → Meetings → create scheduling page. Add to email signatures. Doubles demo-booking conversion (self-serve booking, no back-and-forth).

### 🟠 #5 — Tier the outreach
Deep-personalize tier-A (with #1's audit data), generic template for the long tail. Keeps CAC low, best leads get best treatment.

### 🟡 Lower priority
- Ramp send volume 10→25→50/day (protect fresh-ish sending pattern)
- Dedicated cold-sending domain before scaling past ~50/day (protects primary domain reputation) — Gavin wants to look into this later
- Fix ~8 junk company names from enrichment (SEO-title artifacts like "Car Accident Lawyers in Chicago, IL")
- Build bounce-monitor script (run after each batch, flag if bounce rate >3%)
- BIMI logo + blue checkmark ($1,500/yr) — phase 2, eligible (DMARC at quarantine)
- Add veterinarian demo to the platform + template

---

## Key decisions + gotchas learned (don't re-learn these)

1. **HubSpot `industry` is locked** (148 system options). Use custom `business_vertical`.
2. **HubSpot internal names get mangled:** `of_locations`, `source__list_batch`, `hs_linkedin_url` (not `linkedinbio`). Always verify after creating properties.
3. **`batch/read` doesn't return associations** — use the v4 batch associations endpoint to get contact→company links.
4. **Company names from Google Maps are ALL-CAPS** — `_smart_title_case()` fixes them (preserves P.A., LLC, DDS).
5. **Gmail avatars can't be animated** — static only. Use the chevron-dark avatar at `/Users/gavincoleman/Downloads/avatar_chevron_dark.png`.
6. **App passwords beat OAuth for sending** — OAuth tokens expire weekly in testing mode. SMTP + app password is durable. (Needs 2FA on the account.)
7. **Send confirmation** in auto_send accepts "yes"/"y" (was too strict before).
8. **Don't send cold from bu.edu** — use crescendo-consulting.net (done).
9. **Cohort filter** = decision-maker title AND (LinkedIn-confirmed OR A/B-tier+high-confidence). ~629 leads.
10. **Lead Quality Score:** Neon stores 0-100 (`lead_quality_score`), HubSpot has both `lead_score` (1-10 normalized) and `lead_quality_score_raw` (0-100).

---

## How to continue in a new session

1. Open the repo: `cd /Users/gavincoleman/Downloads/email-scraper` (on branch `feature/hubspot-crm-sync`)
2. Read this handoff + the design spec at `docs/superpowers/specs/2026-05-20-crm-funnel-tracking-design.md`
3. Memory file `crm_stack.md` has the durable context
4. Start with PRIORITY #1 (wire audit findings) — first investigate `reputation-audit-tool-V2/` to find where findings are stored
5. Tests: `python3 -m pytest tests/test_hubspot_*.py` (24 should pass)
6. The branch is NOT merged to main — decide whether to merge or keep iterating

## Immediate to-do for Gavin (non-code)
- [ ] Upload `avatar_chevron_dark.png` to Google account
- [ ] Regenerate the Gmail app password (old one exposed in chat) → update `.env`
- [ ] Watch someone use /sonar/demo cold
- [ ] Set up HubSpot meeting links
- [ ] Ramp sends gradually (started at 5/day)
