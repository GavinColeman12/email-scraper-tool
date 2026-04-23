"""
Generic-inbox blacklist + is_generic() helper.

Rule (absolute, no exceptions): a local part matching any name in
GENERIC_LOCAL_PARTS can never be picked as a primary email for
outreach. Generic inboxes are auto-routed, filtered by admins, and
rarely reach the decision maker — sending to them wastes bandwidth
and hurts sender reputation.

Generic emails still flow through as evidence for pattern detection
(seeing smile@firm.com tells us the domain accepts mail) but they
cannot win the best_email slot.
"""

GENERIC_LOCAL_PARTS = {
    # Info / contact
    "info", "information", "contact", "contactus", "contact-us",
    "get-in-touch", "getintouch", "hello", "hi", "hey", "hiya",
    "howdy", "welcome", "ola",
    # Team / office
    "team", "theteam", "team-us", "admin", "administration", "office",
    "general", "inbox", "mail", "mailbox",
    # Revenue / ops
    "sales", "marketing", "bizdev", "newbusiness", "biz",
    "support", "help", "helpdesk", "service", "customercare",
    "customerservice", "customer-care", "customer-service",
    "care", "wecare",
    # Front-of-house
    "reception", "receptionist", "frontdesk", "front-desk", "desk",
    "frontoffice", "front-office",
    "appointments", "bookings", "booking", "appointment",
    "scheduling", "intake", "scheduling",
    # Finance
    "billing", "accounts", "accounting", "ap", "ar", "payments",
    "invoices", "invoicing", "orders", "shipping", "returns", "refunds",
    # Restaurant / hospitality shared inboxes — catering@, management@,
    # reservations@, specialevents@, gifts@ all auto-route to whoever
    # is on front-of-house duty that day, not to the owner.
    "catering", "cater", "reservations", "reservation",
    "specialevents", "special-events", "privateevents", "private-events",
    "events", "parties", "party", "groups", "group-sales",
    "gifts", "gift", "giftcards", "giftcard", "vouchers",
    "management", "managers", "gm", "ops", "operations",
    "kitchen", "chef", "chefs", "dining", "foh", "boh",
    "takeout", "delivery", "orders", "togo", "to-go",
    "tickets", "ticketing", "rsvp",
    # Accessibility / compliance — these are WCAG/ADA contact inboxes
    "accessibility", "a11y", "ada", "wcag", "accommodations",
    # "We are" style brand inboxes — weare@, hello-we-are@, us@
    "weare", "we-are", "wearethe", "us", "theus", "allofus",
    # Law-firm case-intake compounds the rules below might miss
    "caseevaluation", "caseevaluations", "casereview", "casereviews",
    "freeconsultation", "freeconsult", "freeevaluation",
    "complimentarycasereview", "complimentaryconsultation",
    "howcanwehelp", "wecanhelp", "canwehelp",
    "contactanattorney", "talktoanattorney", "talktoalawyer",
    # HR
    "hr", "careers", "jobs", "recruiting", "recruitment", "hiring",
    # Legal / policy
    "legal", "compliance", "privacy", "gdpr", "dpo",
    # Security
    "abuse", "security", "it", "tech", "technical", "dev", "developer",
    "api", "devops", "webmaster", "postmaster", "hostmaster",
    # Media
    "media", "press", "pr", "news", "newsletter", "subscribe",
    "updates", "notifications", "alerts",
    # Automated / no-reply
    "noreply", "no-reply", "donotreply", "do-not-reply", "mailer",
    "mailer-daemon", "bounce", "bounces",
    # Vague feedback
    "enquiries", "inquiries", "questions", "ask", "feedback", "comments",
    # Industry-specific brand aliases that act like shared inboxes
    "smile", "smiles", "dentist", "doctor", "practice", "clinic",
    "office-manager", "officemanager", "events", "rsvp",
    "concierge", "hospitality", "front",
    # Alumni / community / investor relations — shared distribution lists
    "alumni", "community", "communications", "comms", "donations",
    "donors", "volunteer", "volunteers", "investor", "investors",
    "grants", "partnerships",
    # Team-level aliases (plural = shared list, singular = could be personal)
    "partners", "leadership", "advisors", "managers", "founders",
    "directors", "executives", "principals",
    "clients", "customers", "members",
    # More news / content aliases
    "blog", "podcast", "editorial", "editor", "editors", "writer", "writers",
    "content", "social", "community-manager",
    # Legal practice-area / role inboxes — attorney@, divorce@, cases@
    # are all shared mailboxes that auto-route to whoever handles intake,
    # not to the founder/DM
    "attorney", "attorneys", "lawyer", "lawyers", "legal", "law",
    "cases", "case", "claim", "claims", "litigation", "litigator",
    "divorce", "custody", "criminal", "dui", "dwi", "accident", "injury",
    "personalinjury", "pi", "pilaw", "estateplanning", "estate",
    "probate", "immigration", "bankruptcy", "employment",
    "workerscomp", "workcomp", "ssd", "disability",
    "intake", "intakes", "newclient", "newclients", "consultation",
    "consultations", "consult", "consults",
    # Medical / dental practice-area inboxes
    "dentalpractice", "medicalpractice", "practice", "clinic",
    "dr", "drs", "dentists", "doctors",
    "ortho", "endo", "perio", "oral", "surgery", "cosmetic", "implants",
    "braces", "invisalign", "whitening", "cleaning", "cleanings",
    "extraction", "extractions", "pediatric", "emergency",
}


# Practice-area + role tokens that, when found INSIDE a longer local
# part (not exact match), still flag as a shared inbox. These catch
# compound names like "divorceattorney@firm.com", "pilawyer@firm.com",
# "intakemanager@firm.com". Kept tighter than GENERIC_LOCAL_PARTS because
# substring matching has higher false-positive risk.
PRACTICE_AREA_SUBSTRINGS = (
    "lawfirm", "lawoffice", "lawoffices", "firmadmin",
    "divorceattorney", "divorcelawyer", "piattorney", "pilawyer",
    "personalinjury", "carcrash", "caraccident",
    "intakemanager", "intakespecialist", "caseworker",
    "practicemanager", "officemanager", "officeadmin",
    # Marketing / funnel locals — these phrase-compounds appear on a
    # ton of law-firm sites as "casereview@", "freeconsultation@",
    # "complimentarycasereview@", "howcanwehelp@", etc. If a local part
    # contains any of these substrings it's a funnel inbox, not a DM.
    "casereview", "caseevaluation", "caseintake", "freeconsult", "freeevaluation",
    "complimentary", "consultation", "evaluation", "intakeform",
    "newmatter", "howcanwehelp", "wecanhelp", "contactanattorney",
    "talktoanattorney", "talktoalawyer", "speakwith",
    # Restaurant marketing funnels
    "privateevents", "specialevents", "bookaprivate", "bookatable",
    "planyourevent", "hostyourevent", "cateringinquiries",
    # Generic funnel
    "contactform", "contactus", "getintouch", "workwithus",
    "joinus", "applynow", "requestquote", "getquote", "getademo",
    "bookademo", "schedulecall", "schedulemeeting",
)


def is_generic(local_part: str, *, business_name: str = "") -> bool:
    """
    Return True if the email local part is a generic / shared inbox
    keyword, a numeric-only string, 2 chars or shorter, contains a
    practice-area substring, or matches the business-name tokens.

    Accepts an optional `business_name` so we can dynamically reject
    firm-name-as-local patterns like `hlawfirm@hildebrandlaw.com` or
    `martinlaw@martin-law.com` (both are shared inboxes, not people).

    Intentionally strict: if we're wrong and reject a real person named
    "Hi" or "Sales", the cost is one lost lead; if we're wrong and
    accept info@, the cost is a wasted send + reputation damage.
    """
    if not local_part:
        return True
    lp = local_part.lower().strip()
    if lp in GENERIC_LOCAL_PARTS:
        return True
    if len(lp) <= 2:
        return True
    if lp.isdigit():
        return True
    # Venue/location-code prefix — e.g. "233thompson@", "90park@",
    # "gct@" (Grand Central Terminal), "felice56@", "felice83@".
    # Pattern: starts with 2+ digits followed by a word, OR is a short
    # location acronym. These are shop-front aliases that auto-route to
    # whatever manager happens to be on shift, not to the owner.
    if len(lp) >= 4:
        import re as _re_v
        if _re_v.match(r"^\d{2,}[a-z]+$", lp):
            return True
        if _re_v.match(r"^[a-z]+\d{2,}$", lp) and len(_re_v.sub(r"\d", "", lp)) <= 8:
            # "felice56", "store12" — brand + number. Longer brand-like
            # locals (e.g. a real person "smith1990" is unlikely here)
            # only flagged when the letter portion is short (≤8 chars).
            return True
    # Placeholder / demo locals that shouldn't be picked
    for prefix in ("test", "demo", "sample", "example", "temp", "placeholder"):
        if lp.startswith(prefix):
            return True
    # "info" is such a strong shared-inbox indicator that ANY local
    # part containing it is demoted — infosp, smithinfo, drinfo,
    # practiceinfo, 2024info, info-team, et al. "Info" appears in
    # essentially zero real person names, so the false-positive risk
    # is near zero and the downside (accidentally cold-mailing a
    # shared inbox) is real.
    if "info" in lp:
        return True
    # Shared-inbox prefix + short location/variant suffix —
    #   contactnyc = contact + NYC
    #   salesmn    = sales + MN
    # Rule: local starts with a shared-inbox keyword AND the remainder
    # is ≤ 4 characters (so real names like "helloworld" 5+ chars stay
    # unaffected, but location-variant aliases are rejected).
    for prefix in ("contact", "hello", "sales", "support",
                   "admin", "office", "team", "help", "service",
                   "reception", "billing", "intake"):
        if lp.startswith(prefix) and 0 < len(lp) - len(prefix) <= 4:
            return True
    # Compound generic (e.g. "info123", "contact-us-today", "team-sf")
    # Strip trailing digits / common separators and re-check.
    import re as _re
    stripped = _re.sub(r"[\d_\-]+$", "", lp)
    if stripped and stripped in GENERIC_LOCAL_PARTS:
        return True
    # Practice-area compound patterns
    for sub in PRACTICE_AREA_SUBSTRINGS:
        if sub in lp:
            return True
    # Firm-name-as-local: if the local part contains a substantive
    # business-name token (≥4 chars that aren't filler), it's almost
    # certainly a shared "firm@" alias. E.g.:
    #   martin-law.com → local "martinlaw" is generic
    #   hildebrand → local "hildebrandlaw" is generic
    #   weaver-law.com → local "weaverlaw" is generic
    # We keep bare-lastname-in-local OK because it can be a real
    # person's email (e.g. weaver@weaver-law.com could be the founder
    # Roger Weaver — the scraped-DM-match check handles that case).
    if business_name:
        filler = {"the", "and", "llc", "inc", "co", "corp", "group",
                  "of", "firm", "law", "clinic", "practice", "center",
                  "labs", "lab", "pllc", "pc", "llp", "office", "offices",
                  "attorney", "attorneys", "lawyer", "lawyers", "a"}
        import re as _re2
        biz_tokens = [t.lower() for t in _re2.findall(r"[A-Za-z]+", business_name)
                      if len(t) >= 4 and t.lower() not in filler]
        # Require BOTH a firm-name token AND a generic modifier token
        # (law, firm, office, etc.) in the local — so bare last name
        # `weaver@` stays OK but `weaverlaw@` or `hildebrandfirm@` is
        # classified generic.
        modifier_tokens = {"law", "firm", "office", "offices", "legal",
                            "attorney", "attorneys", "lawyers",
                            "clinic", "practice", "group", "team",
                            "associates"}
        has_biz_token = any(t in lp for t in biz_tokens)
        has_modifier = any(m in lp for m in modifier_tokens)
        if has_biz_token and has_modifier:
            return True
    return False


def email_is_generic(email: str, *, business_name: str = "") -> bool:
    """Extract local part from email and call is_generic()."""
    if not email or "@" not in email:
        return True
    return is_generic(email.split("@", 1)[0], business_name=business_name)
