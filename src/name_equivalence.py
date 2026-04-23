"""
Nickname + diminutive map for name-matching.

Used wherever we need to decide "does this email local-part reference
this person" — e.g. does `jeff@jeffbriscoe.com` refer to Jeffrey Briscoe?

The map is bidirectional: looking up 'jeff' returns 'jeffrey' (and vice
versa via the reverse index). Only short, unambiguous diminutives are
included — rare or regional variants (e.g. "Bobbi" for Barbara) are
intentionally excluded to keep false positives low.
"""
from __future__ import annotations


# Canonical form → {nicknames/diminutives}
_CANONICAL_TO_NICK: dict[str, set[str]] = {
    "alexander": {"alex", "al", "xander"},
    "alexandra": {"alex", "alexa", "allie", "sandy"},
    "andrew": {"andy", "drew"},
    "anthony": {"tony"},
    "benjamin": {"ben", "benji"},
    "bradford": {"brad"},
    "catherine": {"cathy", "cate", "kate", "katie"},
    "charles": {"charlie", "chuck", "chas"},
    "christopher": {"chris", "kit", "topher"},
    "daniel": {"dan", "danny"},
    "david": {"dave", "davey"},
    "deborah": {"deb", "debbie"},
    "douglas": {"doug"},
    "edward": {"ed", "eddie", "ted"},
    "elizabeth": {"liz", "beth", "eliza", "betty", "lizzy"},
    "emily": {"em", "emmy"},
    "francis": {"frank", "fran"},
    "frederick": {"fred", "freddie"},
    "gregory": {"greg"},
    "harold": {"harry", "hal"},
    "henry": {"hank", "harry"},
    "jacob": {"jake"},
    "james": {"jim", "jimmy", "jamie"},
    "jeffrey": {"jeff", "jeffy"},
    "jennifer": {"jen", "jenny", "jenn"},
    "john": {"jack", "johnny", "jon"},
    "jonathan": {"jon", "jonny", "johnny"},
    "joseph": {"joe", "joey", "jos"},
    "joshua": {"josh"},
    "katherine": {"kate", "kathy", "katie", "kat"},
    "kenneth": {"ken", "kenny"},
    "lawrence": {"larry", "laurie"},
    "margaret": {"maggie", "meg", "peggy"},
    "matthew": {"matt"},
    "michael": {"mike", "mick", "mickey"},
    "nathaniel": {"nate", "nat"},
    "nicholas": {"nick", "nicky"},
    "patrick": {"pat", "paddy"},
    "peter": {"pete"},
    "philip": {"phil"},
    "rebecca": {"becca", "becky"},
    "richard": {"rich", "rick", "dick", "ricky"},
    "robert": {"bob", "rob", "bobby", "robby"},
    "ronald": {"ron", "ronny"},
    "samuel": {"sam", "sammy"},
    "stephanie": {"steph"},
    "stephen": {"steve", "steph"},
    "steven": {"steve", "stevie"},
    "susan": {"sue", "susie"},
    "theodore": {"ted", "teddy"},
    "thomas": {"tom", "tommy"},
    "timothy": {"tim", "timmy"},
    "victor": {"vic", "vicky"},
    "vincent": {"vince", "vinny"},
    "william": {"will", "bill", "billy", "willy"},
}

# Reverse index: nickname → canonical forms
_NICK_TO_CANONICAL: dict[str, set[str]] = {}
for canonical, nicks in _CANONICAL_TO_NICK.items():
    for n in nicks:
        _NICK_TO_CANONICAL.setdefault(n, set()).add(canonical)


def equivalents(name: str) -> set[str]:
    """Return all name forms equivalent to the given one (canonical +
    all known nicknames/diminutives). Always includes the input."""
    n = (name or "").lower().strip()
    if not n:
        return set()
    out = {n}
    out.update(_CANONICAL_TO_NICK.get(n, set()))
    out.update(_NICK_TO_CANONICAL.get(n, set()))
    # Also add all canonicals that map to this nickname (transitive)
    for canonical in _NICK_TO_CANONICAL.get(n, set()):
        out.update(_CANONICAL_TO_NICK.get(canonical, set()))
    return out


def names_match(a: str, b: str) -> bool:
    """
    Return True if a and b refer to the same person via canonical or
    nickname equivalence. Case-insensitive; handles Jeff↔Jeffrey,
    Mike↔Michael, Liz↔Elizabeth, etc.
    """
    if not a or not b:
        return False
    a_low = a.lower().strip()
    b_low = b.lower().strip()
    if a_low == b_low:
        return True
    return bool(equivalents(a_low) & equivalents(b_low))
