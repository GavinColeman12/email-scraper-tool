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
}


def is_generic(local_part: str) -> bool:
    """
    Return True if the email local part is a generic / shared inbox
    keyword, a numeric-only string, or 2 characters or shorter
    (prefixes like "ny@" are always non-person).

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
    # Placeholder / demo locals that shouldn't be picked
    for prefix in ("test", "demo", "sample", "example", "temp", "placeholder"):
        if lp.startswith(prefix):
            return True
    # Compound generic (e.g. "info123", "contact-us-today", "team-sf")
    # Strip trailing digits / common separators and re-check.
    import re as _re
    stripped = _re.sub(r"[\d_\-]+$", "", lp)
    if stripped and stripped in GENERIC_LOCAL_PARTS:
        return True
    return False


def email_is_generic(email: str) -> bool:
    """Extract local part from email and call is_generic()."""
    if not email or "@" not in email:
        return True
    return is_generic(email.split("@", 1)[0])
