import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import streamlit as st

from src import storage
from src.email_scraper import scrape_business_emails
from src.deep_scraper import deep_scrape_business_emails
from src.secrets import get_secret

st.set_page_config(page_title="Scrape Emails", page_icon="📧", layout="wide")
st.title("📧 Scrape Business Websites for Emails")

storage.init_db()

searches = storage.list_searches()
if not searches:
    st.warning("No searches yet. Go to **🔎 Find Businesses** first.")
    st.stop()

# ── Pick a search ──
labels = {s["id"]: f"#{s['id']} — {s['query']} ({s.get('with_email', 0)}/{s.get('business_count', 0)} scraped)"
          for s in searches}
search_id = st.selectbox("Search", options=list(labels.keys()),
                          format_func=lambda k: labels[k])

businesses = storage.list_businesses(search_id=search_id)
pending = [b for b in businesses if not b.get("primary_email") and b.get("website")]
already_scraped = [b for b in businesses if b.get("primary_email")]
no_website = [b for b in businesses if not b.get("website")]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total", len(businesses))
c2.metric("Pending scrape", len(pending))
c3.metric("Already scraped", len(already_scraped))
c4.metric("No website", len(no_website))

# Count enhance candidates: already-scraped but LOW or MEDIUM or blank confidence
enhance_candidates = [
    b for b in already_scraped
    if b.get("website")
    and (b.get("confidence") or "") in ("", "low", "medium")
]

st.caption(
    "💡 Scraping is free — just web requests. Each business fetches ~8 pages "
    "(homepage + contact/about/team) and extracts emails via regex + mailto: links."
)

# ── Deep research toggle ──
try:
    has_claude = bool(get_secret("ANTHROPIC_API_KEY"))
except Exception:
    has_claude = False

st.markdown("### Research mode")
mode_col1, mode_col2 = st.columns([3, 2])
with mode_col1:
    deep_mode = st.checkbox(
        "🧠 **Deep research mode** (multi-agent + AI synthesis)",
        value=False,
        help="Runs 4 research agents in parallel (website, schema.org, LinkedIn, "
             "press/news search) then uses Claude AI to pick the best decision-maker "
             "email with reasoning. Much higher accuracy. Costs ~$0.02/business "
             "(1 extra SearchApi call + Claude Sonnet call)."
    )
with mode_col2:
    if deep_mode:
        if has_claude:
            st.caption("✅ Claude Sonnet synthesizer enabled")
        else:
            st.caption("⚠️ No `ANTHROPIC_API_KEY` — will use rules-based synthesizer (free but less accurate)")

if deep_mode:
    est_cost_per = 0.02 if has_claude else 0.005
    total_est = len(pending) * est_cost_per
    st.info(
        f"💰 Deep research estimated cost: **${total_est:.2f}** for {len(pending)} businesses "
        f"(~${est_cost_per:.3f}/business)"
    )

# ── Enhance existing results with Claude ──
st.markdown("### 🧠 Enhance existing results with Claude AI")
st.caption(
    f"Re-runs the multi-agent pipeline + Claude synthesizer on the **{len(enhance_candidates)}** "
    f"already-scraped businesses that are currently LOW, MEDIUM, or blank confidence. "
    f"Skips HIGH-confidence ones (no point enhancing what's already good). "
    f"Cost: ~${len(enhance_candidates) * 0.02:.2f} at ~$0.02/business."
)

if not has_claude:
    st.caption("⚠️ `ANTHROPIC_API_KEY` not set — enhance will use rules-based synthesizer (still better than basic mode, but no AI).")

if st.button(
    f"🧠 Enhance {len(enhance_candidates)} low/medium/blank businesses with Claude",
    disabled=not enhance_candidates,
    type="secondary",
):
    e_prog = st.progress(0)
    e_status = st.empty()
    e_results_preview = st.empty()
    upgraded = 0
    unchanged = 0
    e_upgrades = []

    CONFIDENCE_RANK = {"": 0, "low": 1, "medium": 2, "high": 3}

    def _enhance_one(biz):
        addr = biz.get("address", "") or biz.get("location", "")
        city = addr.split(",")[0].strip() if addr else ""
        result = deep_scrape_business_emails(
            business_name=biz["business_name"],
            website=biz.get("website", ""),
            location=city,
            verify_with_mx=True,
        )
        storage.update_business_emails(biz["id"], result)
        return biz, result

    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = [ex.submit(_enhance_one, b) for b in enhance_candidates]
        for i, fut in enumerate(as_completed(futures)):
            try:
                biz, result = fut.result()
            except Exception as e:
                biz, result = {"business_name": "?", "confidence": "", "primary_email": ""}, {"confidence": ""}
                st.write(f"⚠️ Error: {e}")
            old_conf = (biz.get("confidence") or "")
            new_conf = (result.get("confidence") or "")
            if CONFIDENCE_RANK.get(new_conf, 0) > CONFIDENCE_RANK.get(old_conf, 0):
                upgraded += 1
                e_upgrades.append({
                    "Business": biz.get("business_name"),
                    "Before": f"{old_conf or '—'} / {biz.get('primary_email') or '—'}",
                    "After": f"{new_conf} / {result.get('primary_email', '')}",
                    "Reasoning": (result.get("synthesis_reasoning", "") or "")[:100],
                })
            else:
                unchanged += 1
            e_prog.progress((i + 1) / len(enhance_candidates))
            e_status.write(
                f"**{i + 1}/{len(enhance_candidates)}** · {biz.get('business_name')} → "
                f"{old_conf or '—'} → **{new_conf or '—'}** · {result.get('primary_email', '')}"
            )

    e_prog.empty()
    e_status.success(f"✅ Enhanced — **{upgraded}** upgraded, **{unchanged}** unchanged")
    if e_upgrades:
        st.markdown("#### Upgrades")
        st.dataframe(pd.DataFrame(e_upgrades), use_container_width=True, hide_index=True)
    st.rerun()

st.divider()

# ── Bulk scrape ──
if st.button(f"▶️ Scrape {len(pending)} pending businesses",
              disabled=not pending, type="primary"):
    progress = st.progress(0)
    status_box = st.empty()
    results_preview = st.empty()

    def _scrape_one(biz):
        # Parse city from address for more targeted LinkedIn search
        addr = biz.get("address", "") or biz.get("location", "")
        city = addr.split(",")[0].strip() if addr else ""
        if deep_mode:
            result = deep_scrape_business_emails(
                business_name=biz["business_name"],
                website=biz.get("website", ""),
                location=city,
                verify_with_mx=True,
            )
        else:
            result = scrape_business_emails(
                business_name=biz["business_name"],
                website=biz.get("website", ""),
                find_decision_makers=True,
                location=city,
            )
        storage.update_business_emails(biz["id"], result)
        return biz, result

    completed = 0
    scraped_so_far = []
    max_workers = 4 if deep_mode else 8
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_scrape_one, b) for b in pending]
        for fut in as_completed(futures):
            try:
                biz, result = fut.result()
            except Exception as e:
                biz, result = {"business_name": "?"}, {"primary_email": "", "scraped_emails": []}
                st.write(f"⚠️ Error on {biz.get('business_name')}: {e}")
            completed += 1
            progress.progress(completed / len(pending))
            status_box.write(
                f"**{completed}/{len(pending)}** · {biz.get('business_name')} → "
                f"{result.get('primary_email') or '(no email found)'}"
            )
            if result.get("primary_email"):
                scraped_so_far.append({
                    "Name": biz.get("business_name"),
                    "Email": result["primary_email"],
                    "Scraped?": "✅" if result.get("scraped_emails") else "🤔 pattern-based",
                })
                if len(scraped_so_far) % 5 == 0:
                    results_preview.dataframe(
                        pd.DataFrame(scraped_so_far),
                        use_container_width=True, hide_index=True,
                    )

    progress.empty()
    status_box.success(f"✅ Done — scraped {completed} businesses")
    st.rerun()

# ── Results table ──
st.divider()
st.subheader("All businesses in this search")

filter_option = st.radio("Show",
                         ["all", "with_email", "scraped_found", "pattern_only",
                          "no_email", "no_website"],
                         horizontal=True,
                         format_func=lambda k: {
                             "all": "All",
                             "with_email": "With email",
                             "scraped_found": "Scraped (found on site)",
                             "pattern_only": "Pattern-based only",
                             "no_email": "No email",
                             "no_website": "No website",
                         }[k])


def matches(b):
    has_email = bool(b.get("primary_email"))
    has_site = bool(b.get("website"))
    scraped = bool(b.get("scraped_emails"))
    if filter_option == "with_email":
        return has_email
    if filter_option == "scraped_found":
        return scraped
    if filter_option == "pattern_only":
        return has_email and not scraped
    if filter_option == "no_email":
        return has_site and not has_email
    if filter_option == "no_website":
        return not has_site
    return True


filtered = [b for b in businesses if matches(b)]

rows = []
CONFIDENCE_EMOJI = {"high": "🟢", "medium": "🟡", "low": "🔴"}
SOURCE_LABEL = {
    "scraped_mailto_or_regex": "🎯 scraped",
    "scraped_personal_email": "🎯 scraped",
    "constructed_from_linkedin": "👔 LinkedIn",
    "constructed_from_website_decision_maker": "🏢 site (owner)",
    "team_page_decision_maker": "🏢 team page",
    "team_page_verified_by_linkedin": "✅ cross-verified",
    "linkedin_verified_by_website": "✅ cross-verified",
    "team_page_person": "🏢 team",
    "constructed_from_website_name": "👤 site (name)",
    "generic_inbox": "📬 generic",
    "generic_fallback": "📬 generic",
    "website_cross_verified": "✅ cross-verified",
    "press_cross_verified": "✅ press + verified",
    "schema": "🏷️ schema.org",
    "press": "📰 press",
    "linkedin": "👔 LinkedIn",
}
for b in filtered:
    scraped_emails = b.get("scraped_emails") or []
    src_raw = b.get("email_source") or ""
    source = SOURCE_LABEL.get(src_raw, "—")
    if not src_raw and b.get("primary_email"):
        source = "🎯 scraped" if b["primary_email"] in scraped_emails else "🤔 pattern"
    confidence = b.get("confidence") or ""
    conf_display = f"{CONFIDENCE_EMOJI.get(confidence, '')} {confidence}".strip() or "—"
    rows.append({
        "id": b["id"],
        "Name": b["business_name"],
        "Email": b.get("primary_email") or "",
        "Source": source,
        "Confidence": conf_display,
        "Contact": b.get("contact_name") or "—",
        "Title": b.get("contact_title") or "—",
        "All scraped": ", ".join(scraped_emails[:3]) if scraped_emails else "—",
        "Website": b.get("website") or "—",
        "Status": b.get("email_status") or "not verified",
    })

if rows:
    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True,
                 column_config={"id": None, "Website": st.column_config.LinkColumn()})

    # Show reasoning for any deeply-researched businesses
    deep_researched = [b for b in filtered if b.get("reasoning")]
    if deep_researched:
        with st.expander(f"🧠 AI reasoning for {len(deep_researched)} deeply-researched businesses"):
            for b in deep_researched[:20]:
                synth = b.get("synthesizer", "rules")
                icon = "🤖" if synth == "claude" else "⚙️"
                st.markdown(
                    f"{icon} **{b['business_name']}** → `{b.get('primary_email', '')}` "
                    f"(*{b.get('confidence', '')}*)  \n"
                    f"<span style='color:#666;font-size:0.9em'>{b.get('reasoning', '')}</span>",
                    unsafe_allow_html=True,
                )
                st.divider()
else:
    st.info("No businesses match the current filter.")

# ── Manual override ──
with st.expander("✏️ Override email for a specific business"):
    names = {b["id"]: b["business_name"] for b in filtered}
    if names:
        pick = st.selectbox("Business", options=list(names.keys()),
                             format_func=lambda k: names[k], key="ov_pick")
        chosen = next((b for b in filtered if b["id"] == pick), None)
        if chosen:
            options = [""]
            if chosen.get("scraped_emails"):
                options.extend(chosen["scraped_emails"])
            if chosen.get("constructed_emails"):
                options.extend(chosen["constructed_emails"])
            options.append("Custom...")
            current = chosen.get("primary_email", "")
            selected = st.selectbox(
                "Pick an email",
                options=options,
                index=options.index(current) if current in options else 0,
                key="ov_email",
            )
            custom = ""
            if selected == "Custom...":
                custom = st.text_input("Custom email", key="ov_custom")
            if st.button("💾 Save override"):
                new_email = custom if selected == "Custom..." else selected
                storage.override_primary_email(pick, new_email)
                st.success(f"Updated to {new_email or '(blank)'}")
                st.rerun()
