"""
decision_log.py — Per-business decision-logic export for the scraper.

Serializes every signal the scraper used to pick the final email so you
can grep a bad result and see EXACTLY which agent / pattern / score
drove the decision.

Public API:
  build_business_decision_log(business) -> dict
    Shape:
      {
        "business": {id, business_name, website, place_id, ...},
        "final_email": {address, confidence, source, status},
        "scoring": {score, tier, breakdown, specificity, is_catchall, ...},
        "triangulation": {decision_maker, all_providers, detected_pattern, ...},
        "agents_run": [...], "agents_succeeded": [...],
        "candidates": [{email, pattern, source, smtp_valid, nb_result,
                         confidence, ...}, ...],
        "gate_decision": {should_send, reason},
        "generated_at": ISO timestamp,
      }

  build_search_decision_log(search_id) -> dict
    Wraps build_business_decision_log for every business in a search.

Called from the Bulk Scrape + Export CSV pages as a download button.
"""
import json
from datetime import datetime


def build_business_decision_log(business: dict) -> dict:
    """Self-contained decision log for a single business row."""
    # Decode professional_ids JSON if it came from the DB as a string
    prof_raw = business.get("professional_ids")
    if isinstance(prof_raw, str) and prof_raw:
        try:
            prof = json.loads(prof_raw)
        except Exception:
            prof = {"_parse_error": True, "_raw": prof_raw[:500]}
    else:
        prof = prof_raw or {}

    # Score the email RIGHT NOW using the same scorer the pipeline uses.
    # This shows what the gate would produce if the pipeline ran today.
    scoring_block = _compute_scoring_block(business)

    return {
        "business": {
            "id": business.get("id"),
            "business_name": business.get("business_name"),
            "business_type": business.get("business_type"),
            "address": business.get("address"),
            "phone": business.get("phone"),
            "website": business.get("website"),
            "place_id": business.get("place_id"),
            "rating": business.get("rating"),
            "review_count": business.get("review_count"),
        },
        "final_email": {
            "address": business.get("primary_email"),
            "confidence": business.get("confidence"),
            "source": business.get("email_source"),
            "status": business.get("email_status"),
            "contact_name": business.get("contact_name"),
            "contact_title": business.get("contact_title"),
        },
        "scoring": scoring_block,
        "triangulation": {
            "decision_maker": prof.get("decision_maker"),
            "all_providers": prof.get("all_providers") or [],
            "detected_pattern": prof.get("detected_pattern"),
            "risky_catchall": prof.get("risky_catchall"),
            "time_seconds": prof.get("time_seconds"),
            "cost_estimate": prof.get("cost_estimate"),
        },
        "agents_run": prof.get("agents_run") or [],
        "agents_succeeded": prof.get("agents_succeeded") or [],
        "candidates": prof.get("candidate_emails") or [],
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }


def _compute_scoring_block(business: dict) -> dict:
    """
    Re-run the scorer in debug mode so we can return a full breakdown
    instead of just the integer stored in lead_quality_score.
    """
    try:
        from src.lead_scoring import (
            compute_lead_quality_score, _business_dict_to_inputs,
        )
        from src.email_scoring import score_email_candidate, gate_decision
    except Exception as e:
        return {"_error": f"could not import scorer: {e}"}

    try:
        # Primary path — the same API the pipeline uses.
        primary = compute_lead_quality_score(business)
    except Exception as e:
        primary = {"_error": str(e)}

    # Deep path — recover the structured EmailScore + the gate call.
    deep = {}
    try:
        inputs = _business_dict_to_inputs(business)
        if inputs is not None:
            email_score = score_email_candidate(inputs)
            deep["email_score"] = email_score.to_dict()
            decision = gate_decision(email_score)
            deep["gate_decision"] = {
                "should_send": decision.should_send,
                "should_verify_further": decision.should_verify_further,
                "should_manual_review": decision.should_manual_review,
                "should_skip": decision.should_skip,
                "reason": decision.reason,
            }
    except Exception as e:
        deep["_deep_error"] = str(e)

    return {
        "stored_score": business.get("lead_quality_score"),
        "stored_tier": business.get("lead_tier"),
        "recomputed": primary,
        **deep,
    }


def build_search_decision_log(search_id: int) -> dict:
    """Decision log for every business in a given search."""
    try:
        from src import storage
    except Exception as e:
        return {"_error": f"storage unavailable: {e}", "businesses": []}

    businesses = storage.list_businesses(search_id=search_id)
    logs = [build_business_decision_log(b) for b in businesses]
    # Aggregate summary stats so you can eyeball the run
    total = len(logs)
    with_score = sum(1 for log in logs if log["scoring"].get("stored_score"))
    safe_to_send = sum(
        1 for log in logs
        if (log["scoring"].get("gate_decision") or {}).get("should_send")
    )
    return {
        "search_id": search_id,
        "total_businesses": total,
        "with_score": with_score,
        "safe_to_send": safe_to_send,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "businesses": logs,
    }
