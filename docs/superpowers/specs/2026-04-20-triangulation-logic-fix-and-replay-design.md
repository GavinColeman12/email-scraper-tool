# Triangulation Logic Fix + Replay Regression Tool — Design

**Date:** 2026-04-20
**Status:** Approved (plan C — replay tool built alongside logic fixes)

## Context

Audit of search #29 (dental, 15 businesses) exposed three classes of problem in the triangulation pipeline:

1. **Decision logic bugs** — when triangulation produces a strong `first` pattern with evidence (e.g. `marc@onemanhattandental.com` → Marc Sclafani), the pipeline discards it because `_block_first_only` was intended to block industry-prior guesses and is wrongly also blocking triangulated evidence. This is silent: the user sees a "LOW" confidence industry-prior first.last@ candidate instead of the correct triangulated marc@ at confidence 70+.

2. **Evidence loss in the export** — the CSV has `SMTP ✓`, `WHOIS ✓`, `NPI/Pattern ✓` columns that are always empty for triangulation runs. Reason: the columns derive from substring matches on `email_source` ("whois_confirmed" etc.), but the triangulation pipeline writes `email_source = "triangulation"`. The JSON evidence trail (`professional_ids.agents_succeeded`) carries the real signal but the CSV never reads it.

3. **No regression safety net** — we cannot tell whether a logic change helps or hurts until the next live campaign produces bounces. Live campaigns cost SearchApi + NB credits and take hours. With Phase 1-3 caches already populated at 14-90d TTL, replaying a historical search with new logic is near-zero-cost and produces an apples-to-apples A/B.

## Goals

- Eliminate the logic bugs so triangulation emits the DM's personalized email when a pattern is triangulated, not the industry-prior fallback.
- Make the CSV honest about what evidence was collected.
- Ship a replay tool so every future logic change comes with a before/after diff against historical data.
- Preserve the vision of an evolving system: the replay tool is the mechanism that lets the decision logic learn.

## Non-goals (this spec)

- Full bounce-feedback learning into `pattern_success` / `industry_pattern_success` tables. That's Phase 2. These tables exist but are effectively empty (2 test rows). Wiring them up is its own spec.
- Automated email sending pipeline. Out of scope.
- UI for the replay tool beyond a Streamlit page that lists past runs.

## Two tracks

### Track L — Logic fixes

| # | Fix | File | Change |
|---|---|---|---|
| L1 | Triangulated `first` pattern bypasses `_block_first_only` guard | [universal_pipeline.py:1269-1297](src/universal_pipeline.py:1269) | When `detected_pattern.method == "triangulation"` AND `confidence ≥ 70` AND `len(evidence_emails) ≥ 1`, allow first-only. The guard's intent is to block *unevidenced* guesses, not triangulated evidence. |
| L2 | NB walk also verifies the triangulated DM candidate | [universal_pipeline.py:1647-1666](src/universal_pipeline.py:1647) | After stopping the main walk on first valid/catchall/unknown, if the top `detected_pattern` candidate is untested, run one extra NB on it (budget +1, still ≤ 5 total). This way when `info@` wins first, the DM email is still verified for the user to choose from. |
| L3 | Badge logic respects `risky_catchall` and `safe_to_send` | [pages/4_📥_Export_CSV.py:240-255](pages/4_📥_Export_CSV.py:240) | When row has `risky_catchall` evidence OR `email_safe_to_send=0`, demote 🟢 HIGH to 🟡 MEDIUM. |
| L4 | CSV evidence columns read `professional_ids.agents_succeeded` | [pages/4_📥_Export_CSV.py:264-268](pages/4_📥_Export_CSV.py:264) | Parse `professional_ids` JSON if present; set WHOIS/NPI/Pattern ticks from `agents_succeeded` list. Fallback to substring match for legacy deep_scraper rows. |
| L5 | Debug NPI 0/15 match rate | [universal_pipeline.py:1444-1502](src/universal_pipeline.py:1444), `_agent_npi_healthcare` | Confirm query shape, taxonomy codes, city/state parsing. Run live test against 3 known dentists. |
| L6 | Investigate `first@domain` template leak | `_agent_website_scrape`, email regex | "first@onemanhattandental.com" appeared as a scraped email. Trace whether scraper is grabbing placeholder text or a real published mail template. Filter local parts that match pattern placeholder tokens (`first`, `last`, `firstname`, `lastname`, `fname`, `lname`, `your`, `you`, `example`). |
| L7 | Location-anchor owner search | `_agent_combined_owner_and_press` | Include `" + city + state"` in the Google query and reject source URLs whose domain doesn't overlap the business name or domain. Prevents Manhattan-MT dentist getting attributed to Manhattan-NYC founder. |
| L8 | Relabel "Verify Deliverability" page to "Re-verify Existing Emails" | [pages/3_✅_Verify_Deliverability.py](pages/3_✅_Verify_Deliverability.py) | Header note: "Triangulation already verifies in bulk. Use this for ad-hoc re-verification." |

### Track R — Replay tool

**Storage:** new table `replay_runs`:
```sql
CREATE TABLE replay_runs (
    id INTEGER PRIMARY KEY,
    original_search_id INTEGER REFERENCES searches(id),
    label TEXT,                 -- e.g. "baseline-2026-04-20", "post-L1-L4"
    git_sha TEXT,               -- HEAD at replay time
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    businesses_json TEXT,       -- full list of re-triangulated TriangulationResult dicts
    metrics_json TEXT           -- precomputed {n, safe_to_send, dm_emails, info_emails, patterns, ...}
);
CREATE INDEX idx_replay_original ON replay_runs(original_search_id);
```

**CLI:** `python scripts/replay_search.py --search-id 29 --label baseline` (and `--diff <replay_id_a> <replay_id_b>`).
- Pulls businesses from `searches`/`businesses` tables.
- Calls `scrape_with_triangulation()` on each (reuses all cache namespaces → near-zero cost).
- Computes metrics: `safe_to_send_pct`, `dm_email_pct` (emails matching detected_pattern applied to DM), `generic_email_pct` (info/hello/contact), `pattern_detected_pct`, `nb_valid_pct`.
- Writes a `replay_runs` row.
- `--diff` subcommand prints a side-by-side comparison of two replay_runs rows.

**Streamlit page (optional this session):** `pages/7_🔁_Replay.py` lists past replays with filter by `original_search_id`, shows metrics diff between any two.

**Cost model:** the Phase 1-3 data is cached 14-90d. A replay of a 100-biz search incurs:
- 0 SearchApi calls (cached)
- 0-10 NB calls (only if fix changed candidate set and new emails need verification)
- ~30s of CPU for Phase 4-7 per business
- Total: ~$0.03 per 100-biz replay, minutes of CPU.

## Success criteria

After Track L ships, a replay of search #29 should show:

- `pattern_detected_pct` unchanged (triangulation logic for detection is unchanged)
- **`dm_email_pct` rises** from ~40% to ~60%+ (triangulated `first` patterns now flow through)
- **Generic-email pick rate falls** correspondingly
- `safe_to_send_pct` unchanged or slightly up
- CSV badge column matches `safe_to_send` (no 🟢 HIGH + `safe=false` mismatch)

## Rollout

Single branch, single bundled PR. Each commit is one fix. Final commit includes the baseline + post-fix replay metrics in the message.

## Open questions (follow-up specs, not this one)

- Bounce-feedback learning: how do `email_sends` results flow back into `pattern_success`/`industry_pattern_success` and influence future scoring?
- Automated outreach: pipeline to generate + send consulting requests on safe_to_send leads.
- NPI fallback: if NPI doesn't match, try the state dental board API.
