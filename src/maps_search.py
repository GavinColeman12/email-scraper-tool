"""
Paginated Google Maps business search via SearchApi.io.
Returns up to `max_results` businesses with name, address, website, phone, place_id.
"""
import time
from typing import Optional

import requests

from src.secrets import get_secret

BASE_URL = "https://www.searchapi.io/api/v1/search"


class SearchError(Exception):
    pass


def _call(params: dict) -> dict:
    api_key = get_secret("SEARCHAPI_KEY")
    if not api_key:
        raise SearchError("SEARCHAPI_KEY not set. Add it to .env or Streamlit Cloud secrets.")

    call_params = dict(params)
    call_params["api_key"] = api_key

    try:
        resp = requests.get(BASE_URL, params=call_params, timeout=30)
    except requests.RequestException as e:
        raise SearchError(f"Network error: {e}") from e

    if resp.status_code == 401 or resp.status_code == 403:
        raise SearchError("SearchApi authentication failed — check SEARCHAPI_KEY.")
    if resp.status_code == 429:
        raise SearchError("SearchApi quota exceeded. Upgrade your plan or wait.")

    try:
        data = resp.json()
    except Exception as e:
        raise SearchError(f"Invalid JSON from SearchApi: {e}") from e

    if isinstance(data, dict) and data.get("error"):
        raise SearchError(f"SearchApi error: {data['error']}")

    return data


# Businesses sometimes list a social-media page as their "website" on
# Google Maps. We can't scrape those for owner emails (they're walled
# gardens, not the business's own domain). Track them so the triangulation
# pipeline can skip the website-scrape agent cleanly instead of pulling
# irrelevant candidates off a social page.
_SOCIAL_ONLY_HOSTS = (
    "facebook.com", "fb.com", "instagram.com", "linkedin.com",
    "twitter.com", "x.com", "tiktok.com", "youtube.com",
    "yelp.com", "tripadvisor.com", "opentable.com", "doordash.com",
    "ubereats.com", "grubhub.com", "seamless.com", "pinterest.com",
    "wa.me", "wame.me",  # WhatsApp share links
)


def _is_real_business_website(url: str) -> bool:
    """Return False for social/review-only URLs that can't be scraped."""
    if not url:
        return False
    u = url.lower().strip()
    if not (u.startswith("http://") or u.startswith("https://")):
        u = "https://" + u  # tolerate bare domains
    for host in _SOCIAL_ONLY_HOSTS:
        if f"://{host}/" in u or f"://www.{host}/" in u or u.endswith(f"://{host}") or u.endswith(f"://www.{host}"):
            return False
    return True


def _parse_business(biz: dict) -> dict:
    """Normalize a SearchApi local_result into our format."""
    btype = biz.get("type") or biz.get("types") or ""
    if isinstance(btype, list):
        btype = btype[0] if btype else ""

    place_id = biz.get("place_id", "") or biz.get("data_id", "")
    maps_url = biz.get("link") or ""
    if not maps_url and place_id:
        maps_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"

    raw_website = biz.get("website", "") or ""
    # Keep the raw value visible so operators can inspect, but flag
    # social-only URLs so the triangulation pipeline can bail out cleanly.
    website = raw_website if _is_real_business_website(raw_website) else ""

    return {
        "business_name": biz.get("title", "") or biz.get("name", ""),
        "address": biz.get("address", ""),
        "phone": biz.get("phone", ""),
        "website": website,
        "website_raw": raw_website,
        "website_is_social_only": bool(raw_website and not website),
        "rating": float(biz.get("rating") or 0),
        "review_count": int(biz.get("reviews") or biz.get("reviews_count") or 0),
        "place_id": place_id,
        "google_maps_url": maps_url,
        "business_type": btype,
    }


# When Google Maps caps a single query at 20-60 results, these synonyms
# let us keep going by re-querying with related terms. Picked to be
# semantically equivalent — all return the same kind of business.
QUERY_SYNONYMS = {
    # ════════════════════════════════════════════════════════════════
    # DENTAL
    # ════════════════════════════════════════════════════════════════
    "dental": ["dentist", "dental office", "dental clinic", "dental practice",
                "family dentistry", "cosmetic dentist", "general dentist"],
    "dental office": ["dentist", "dental clinic", "dental practice",
                       "family dentistry", "cosmetic dentist", "general dentist",
                       "emergency dentist"],
    "dentist": ["dental office", "dental clinic", "dental practice",
                 "family dentistry", "cosmetic dentist", "general dentist"],
    "dental clinic": ["dentist", "dental office", "dental practice",
                       "family dentistry", "cosmetic dentist", "general dentist"],
    "dental practice": ["dentist", "dental office", "dental clinic",
                         "family dentistry", "cosmetic dentist"],
    "dental care": ["dentist", "dental office", "dental clinic", "dental practice"],
    "orthodontist": ["orthodontics", "braces clinic", "invisalign provider",
                      "orthodontic office"],
    "endodontist": ["endodontic clinic", "root canal specialist", "endodontics"],
    "periodontist": ["periodontal clinic", "periodontics", "gum specialist"],
    "oral surgeon": ["oral surgery", "maxillofacial surgeon", "oral and maxillofacial surgery"],
    "pediatric dentist": ["children's dentist", "kids dentist", "pediatric dentistry"],

    # ════════════════════════════════════════════════════════════════
    # MEDICAL — primary care + specialties
    # ════════════════════════════════════════════════════════════════
    "medical": ["medical clinic", "doctor", "physician", "urgent care",
                 "family medicine", "primary care", "medical practice"],
    "doctor": ["physician", "medical clinic", "medical practice",
                "family medicine", "primary care"],
    "physician": ["doctor", "medical practice", "primary care", "internal medicine"],
    "clinic": ["medical clinic", "medical practice", "family medicine",
                "urgent care", "doctor"],
    "urgent care": ["walk-in clinic", "immediate care", "emergency clinic",
                     "minor emergency"],
    "primary care": ["family medicine", "internal medicine", "general practitioner",
                      "family doctor"],
    "family medicine": ["family doctor", "primary care", "general practice"],
    "pediatrician": ["pediatric clinic", "children's doctor", "kids doctor",
                      "pediatrics"],
    "dermatologist": ["dermatology clinic", "skin clinic", "skin doctor",
                       "dermatology"],
    "cardiologist": ["cardiology", "heart doctor", "heart specialist"],
    "psychiatrist": ["mental health clinic", "psychiatric services", "psychiatry"],
    "psychologist": ["therapist", "mental health counselor", "counseling office"],
    "therapist": ["therapy office", "counseling", "mental health counselor",
                   "psychotherapist"],
    "counselor": ["therapist", "counseling office", "mental health counselor"],
    "ob/gyn": ["obstetrician", "gynecologist", "women's health clinic"],
    "physical therapy": ["physical therapist", "PT clinic", "rehab clinic",
                          "physiotherapy", "sports rehab"],
    "physical therapist": ["physical therapy", "PT clinic", "physiotherapy"],
    "chiropractic": ["chiropractor", "chiropractic clinic", "chiropractic office",
                      "spine care"],
    "chiro": ["chiropractor", "chiropractic clinic", "chiropractic office"],
    "chiropractor": ["chiropractic clinic", "chiropractic office",
                      "spine care", "back pain clinic"],
    "med spa": ["medspa", "medical spa", "aesthetic clinic", "botox clinic"],
    "medspa": ["med spa", "medical spa", "aesthetic clinic", "botox clinic"],
    "podiatrist": ["foot doctor", "podiatry", "foot and ankle clinic"],

    # ════════════════════════════════════════════════════════════════
    # VISION / EYECARE
    # ════════════════════════════════════════════════════════════════
    "eye": ["optometrist", "eye doctor", "vision center", "optical shop",
             "eye care center"],
    "optometrist": ["eye doctor", "optical shop", "vision center",
                     "eye care center", "eye exam"],
    "ophthalmologist": ["eye surgeon", "eye specialist", "vision specialist"],

    # ════════════════════════════════════════════════════════════════
    # VETERINARY / PET
    # ════════════════════════════════════════════════════════════════
    "veterinary": ["veterinarian", "vet clinic", "animal hospital", "pet clinic"],
    "vet": ["veterinarian", "vet clinic", "animal hospital", "pet clinic"],
    "veterinarian": ["vet clinic", "animal hospital", "pet clinic",
                      "veterinary hospital"],
    "pet": ["pet store", "pet supplies", "pet groomer", "pet boarding"],
    "pet groomer": ["pet grooming", "dog groomer", "pet salon"],
    "pet boarding": ["pet hotel", "dog boarding", "kennel", "pet daycare"],
    "dog trainer": ["dog training", "pet training", "obedience training"],

    # ════════════════════════════════════════════════════════════════
    # LEGAL
    # ════════════════════════════════════════════════════════════════
    "legal": ["law firm", "attorney", "lawyer", "law office"],
    "law": ["law firm", "attorney", "lawyer", "law office", "legal services",
             "attorneys", "law practice", "attorneys at law"],
    "law firm": ["attorney", "lawyer", "legal services", "law office",
                  "law practice", "legal counsel"],
    "attorney": ["law firm", "lawyer", "legal services", "law office"],
    "lawyer": ["law firm", "attorney", "legal services", "law office"],
    "law office": ["law firm", "attorney", "lawyer", "legal services"],
    "personal injury attorney": ["personal injury lawyer", "accident attorney",
                                  "PI lawyer", "injury law firm"],
    "criminal attorney": ["criminal lawyer", "criminal defense", "DUI attorney"],
    "divorce attorney": ["divorce lawyer", "family law", "family attorney"],
    "estate attorney": ["estate planning", "probate attorney", "estate lawyer"],
    "immigration attorney": ["immigration lawyer", "immigration law"],
    "bankruptcy attorney": ["bankruptcy lawyer", "debt relief attorney"],
    "tax attorney": ["tax lawyer", "tax law", "tax controversy"],

    # ════════════════════════════════════════════════════════════════
    # FOOD & BEVERAGE
    # ════════════════════════════════════════════════════════════════
    "food": ["restaurant", "cafe", "diner", "eatery"],
    "restaurant": ["eatery", "dining", "bistro", "cafe", "grill", "kitchen"],
    "cafe": ["coffee shop", "coffeehouse", "bakery cafe", "espresso bar"],
    "coffee shop": ["cafe", "coffeehouse", "espresso bar"],
    "coffee": ["coffee shop", "cafe", "coffeehouse", "espresso bar"],
    "bakery": ["pastry shop", "bread shop", "patisserie", "donut shop"],
    "pizza": ["pizzeria", "pizza place", "pizza restaurant"],
    "bar": ["sports bar", "cocktail bar", "wine bar", "tavern", "pub"],
    "pub": ["bar", "tavern", "gastropub", "irish pub"],
    "brewery": ["craft brewery", "brewpub", "taproom", "microbrewery"],
    "winery": ["wine tasting", "vineyard", "wine bar"],
    "ice cream": ["ice cream shop", "gelato", "frozen yogurt", "ice cream parlor"],
    "food truck": ["mobile food", "street food vendor"],
    "catering": ["catering service", "caterer", "catering company",
                  "event catering"],
    "deli": ["delicatessen", "sandwich shop", "lunch counter"],
    "sushi": ["sushi restaurant", "japanese restaurant", "sushi bar"],
    "mexican restaurant": ["mexican food", "taqueria", "tacos"],
    "italian restaurant": ["italian food", "pasta restaurant", "trattoria"],
    "chinese restaurant": ["chinese food", "asian restaurant"],
    "thai restaurant": ["thai food", "asian cuisine"],
    "indian restaurant": ["indian food", "curry house"],
    "steakhouse": ["steak restaurant", "chophouse", "grill"],
    "seafood restaurant": ["seafood", "fish restaurant", "oyster bar"],
    "fast food": ["quick service restaurant", "drive-thru", "fast casual"],
    "vegan restaurant": ["vegan food", "plant-based restaurant", "vegetarian restaurant"],
    "breakfast restaurant": ["breakfast spot", "brunch restaurant", "diner"],
    "diner": ["breakfast restaurant", "american restaurant", "lunch spot"],

    # ════════════════════════════════════════════════════════════════
    # WELLNESS / BEAUTY / FITNESS
    # ════════════════════════════════════════════════════════════════
    "fitness": ["gym", "fitness center", "fitness studio", "health club"],
    "gym": ["fitness center", "fitness studio", "health club", "crossfit"],
    "crossfit": ["crossfit gym", "fitness center", "functional fitness"],
    "yoga studio": ["yoga", "hot yoga", "yoga classes"],
    "yoga": ["yoga studio", "hot yoga", "yoga classes"],
    "pilates": ["pilates studio", "reformer pilates", "pilates classes"],
    "personal trainer": ["personal training", "fitness coach", "private trainer"],
    "martial arts": ["karate", "jiu jitsu", "martial arts school", "mma gym"],
    "dance studio": ["dance school", "dance classes", "ballet studio"],

    "beauty": ["salon", "hair salon", "beauty salon", "med spa"],
    "salon": ["hair salon", "beauty salon", "styling studio"],
    "hair salon": ["beauty salon", "styling studio", "hair stylist"],
    "barber": ["barbershop", "men's haircuts", "barber shop"],
    "barbershop": ["barber", "men's haircuts"],
    "nail salon": ["nail bar", "manicure pedicure", "nail spa"],
    "spa": ["day spa", "wellness spa", "med spa"],
    "day spa": ["spa", "massage spa", "wellness spa"],
    "massage": ["massage therapy", "massage spa", "massage clinic", "bodywork"],
    "tanning salon": ["tanning", "spray tan", "sun tan"],
    "lash extensions": ["eyelash extensions", "lash studio", "lash bar"],
    "waxing": ["waxing salon", "wax studio", "hair removal"],
    "tattoo": ["tattoo shop", "tattoo studio", "tattoo parlor"],
    "piercing": ["body piercing", "piercing studio"],

    # ════════════════════════════════════════════════════════════════
    # HOME SERVICES / TRADES
    # ════════════════════════════════════════════════════════════════
    "plumbing": ["plumber", "plumbing service", "plumbing contractor"],
    "plumber": ["plumbing service", "plumbing company", "plumbing contractor",
                 "emergency plumber"],
    "electrical": ["electrician", "electrical contractor", "electrical service"],
    "electrician": ["electrical service", "electrical contractor",
                     "electrical company"],
    "hvac": ["hvac contractor", "heating and cooling", "air conditioning",
              "ac repair", "furnace repair"],
    "heating": ["hvac", "furnace repair", "heating contractor"],
    "air conditioning": ["hvac", "ac repair", "ac installation"],
    "roofing": ["roofer", "roofing contractor", "roofing company"],
    "roofer": ["roofing contractor", "roofing company", "roof repair"],
    "landscaper": ["landscaping company", "lawn care", "landscape design"],
    "landscaping": ["landscaper", "lawn care", "landscape design"],
    "lawn care": ["lawn service", "lawn maintenance", "landscaping"],
    "tree service": ["arborist", "tree removal", "tree trimming"],
    "pest control": ["exterminator", "bug control", "pest management"],
    "exterminator": ["pest control", "bug control", "termite control"],
    "cleaning service": ["house cleaning", "maid service", "janitorial"],
    "house cleaning": ["maid service", "cleaning service", "residential cleaning"],
    "carpet cleaning": ["rug cleaning", "upholstery cleaning"],
    "window cleaning": ["window washer", "window washing"],
    "painter": ["painting contractor", "house painter", "painting company",
                 "commercial painter"],
    "painting": ["painter", "painting contractor", "house painter"],
    "carpenter": ["carpentry", "finish carpentry", "custom carpentry"],
    "handyman": ["handyman service", "home repair", "general handyman"],
    "flooring": ["flooring contractor", "tile installer", "hardwood flooring",
                  "carpet installer"],
    "tile installer": ["tile contractor", "tile and grout"],
    "locksmith": ["locksmith service", "lockout service", "key cutting"],
    "moving company": ["movers", "moving service", "relocation service"],
    "movers": ["moving company", "moving service"],
    "storage": ["self storage", "storage facility", "storage units"],
    "self storage": ["storage facility", "storage units"],
    "junk removal": ["hauling service", "trash removal", "rubbish removal"],
    "appliance repair": ["appliance service", "fridge repair", "washer repair"],
    "garage door": ["garage door repair", "garage door installation"],
    "fencing": ["fence contractor", "fence installer", "fence company"],
    "concrete": ["concrete contractor", "masonry", "concrete work"],
    "masonry": ["mason", "stonework", "brickwork", "concrete"],
    "pool service": ["pool maintenance", "pool cleaning", "pool repair"],
    "pool installation": ["pool builder", "pool company", "pool contractor"],
    "solar": ["solar installation", "solar panels", "solar contractor"],

    # ════════════════════════════════════════════════════════════════
    # CONSTRUCTION / BUILDING
    # ════════════════════════════════════════════════════════════════
    "construction": ["contractor", "construction company", "general contractor",
                      "builder", "remodeling contractor"],
    "contractor": ["construction company", "general contractor", "builder",
                    "construction"],
    "general contractor": ["construction company", "builder", "construction"],
    "builder": ["home builder", "custom home builder", "construction"],
    "remodeling": ["remodeling contractor", "home renovation", "kitchen remodel"],
    "kitchen remodel": ["kitchen renovation", "kitchen contractor", "remodeling"],
    "bathroom remodel": ["bathroom renovation", "bathroom contractor", "remodeling"],
    "demolition": ["demolition contractor", "demo service"],
    "excavation": ["excavating contractor", "earth moving"],

    # ════════════════════════════════════════════════════════════════
    # MANUFACTURING
    # ════════════════════════════════════════════════════════════════
    "manufacturing": ["manufacturer", "factory", "production company",
                       "industrial company"],
    "manufacturer": ["manufacturing company", "factory", "production"],
    "machine shop": ["cnc machining", "metal fabrication", "precision machining"],
    "metal fabrication": ["metal fabricator", "welding shop", "machine shop"],
    "welding": ["welder", "welding shop", "metal fabrication"],

    # ════════════════════════════════════════════════════════════════
    # FINANCE / PROFESSIONAL SERVICES
    # ════════════════════════════════════════════════════════════════
    "accounting": ["accountant", "CPA", "accounting firm", "tax preparation"],
    "accountant": ["CPA", "accounting firm", "tax preparation", "tax advisor",
                    "bookkeeper"],
    "cpa": ["accountant", "accounting firm", "tax preparation", "tax advisor"],
    "tax": ["tax preparation", "accountant", "CPA", "tax advisor"],
    "tax preparation": ["tax preparer", "CPA", "accountant", "tax service"],
    "bookkeeper": ["bookkeeping service", "accountant", "accounting firm"],
    "financial": ["financial advisor", "financial planner", "wealth manager",
                   "investment advisor"],
    "financial advisor": ["financial planner", "wealth manager", "investment advisor",
                           "wealth advisor"],
    "wealth management": ["wealth manager", "financial advisor", "investment advisor"],
    "investment advisor": ["financial advisor", "investment firm", "wealth manager"],
    "insurance": ["insurance agent", "insurance broker", "insurance agency"],
    "insurance agent": ["insurance broker", "insurance agency"],
    "insurance broker": ["insurance agent", "insurance agency"],
    "mortgage": ["mortgage broker", "mortgage lender", "home loans"],
    "mortgage broker": ["mortgage lender", "mortgage company", "home loans"],
    "credit union": ["bank", "financial institution"],

    "consulting": ["consultant", "consulting firm", "management consulting",
                    "business consultant"],
    "consultant": ["consulting firm", "business consultant", "advisor"],
    "management consulting": ["business consulting", "strategy consulting",
                               "consulting firm"],
    "business coach": ["executive coach", "business coaching", "leadership coach"],
    "hr consultant": ["human resources consulting", "HR services", "hr firm"],
    "staffing agency": ["staffing firm", "recruitment agency", "employment agency"],
    "recruiter": ["recruiting firm", "recruitment agency", "staffing agency"],

    "agency": ["marketing agency", "digital agency", "advertising agency",
                "design agency", "branding agency"],
    "marketing": ["marketing agency", "digital agency", "advertising agency",
                   "marketing firm"],
    "marketing agency": ["digital marketing", "advertising agency", "marketing firm"],
    "digital marketing": ["seo agency", "online marketing", "digital agency"],
    "seo": ["seo agency", "search engine optimization", "digital marketing"],
    "ppc": ["ppc agency", "google ads management", "paid search"],
    "advertising": ["advertising agency", "ad agency", "marketing agency"],
    "branding": ["branding agency", "brand consultancy", "design agency"],
    "graphic design": ["design agency", "graphic designer", "design studio"],
    "web design": ["website design", "web designer", "web development"],
    "web development": ["web developer", "website development", "web design"],
    "social media management": ["social media agency", "social media marketing"],
    "pr firm": ["public relations", "pr agency", "communications firm"],

    # ════════════════════════════════════════════════════════════════
    # REAL ESTATE
    # ════════════════════════════════════════════════════════════════
    "realty": ["real estate agent", "realtor", "real estate agency"],
    "real estate": ["real estate agent", "realtor", "real estate agency",
                     "realty office", "real estate broker"],
    "real estate agent": ["realtor", "real estate agency", "realty office"],
    "realtor": ["real estate agent", "real estate agency"],
    "real estate broker": ["real estate agency", "broker", "realtor"],
    "property management": ["property manager", "rental management", "property services"],
    "commercial real estate": ["commercial broker", "CRE", "commercial property"],
    "title company": ["title services", "real estate closing"],
    "appraiser": ["real estate appraiser", "property appraiser"],
    "home inspector": ["home inspection", "property inspector"],

    # ════════════════════════════════════════════════════════════════
    # AUTO
    # ════════════════════════════════════════════════════════════════
    "auto": ["auto repair", "mechanic", "auto shop", "car repair"],
    "auto repair": ["mechanic", "auto shop", "car repair", "auto service"],
    "mechanic": ["auto repair", "auto shop", "car repair"],
    "car dealer": ["car dealership", "auto dealer", "used car dealer"],
    "car wash": ["auto wash", "car detailing", "carwash"],
    "auto detailing": ["car detailing", "auto detail", "mobile detailing"],
    "tire shop": ["tire dealer", "tire installation", "tire service"],
    "auto body shop": ["body shop", "collision repair", "auto body"],
    "transmission repair": ["transmission shop", "transmission service"],
    "windshield replacement": ["auto glass", "windshield repair"],
    "motorcycle dealer": ["motorcycle shop", "motorcycle dealership"],
    "rv dealer": ["rv dealership", "recreational vehicle dealer", "rv sales"],
    "boat dealer": ["boat dealership", "marine dealer", "marine sales"],

    # ════════════════════════════════════════════════════════════════
    # RETAIL
    # ════════════════════════════════════════════════════════════════
    "jewelry store": ["jeweler", "jewelry shop", "fine jewelry"],
    "jeweler": ["jewelry store", "jewelry shop", "fine jewelry"],
    "florist": ["flower shop", "flower delivery", "florists"],
    "flower shop": ["florist", "flower delivery"],
    "boutique": ["clothing boutique", "fashion boutique", "women's boutique"],
    "clothing store": ["apparel store", "fashion store", "boutique"],
    "shoe store": ["shoe shop", "footwear store"],
    "furniture store": ["furniture shop", "home furnishings"],
    "antique store": ["antique shop", "antique dealer", "vintage shop"],
    "thrift store": ["thrift shop", "consignment shop", "second hand store"],
    "consignment shop": ["consignment store", "thrift store"],
    "bookstore": ["book shop", "independent bookstore"],
    "music store": ["instrument store", "music shop"],
    "art gallery": ["art studio", "gallery", "fine art"],
    "hardware store": ["home improvement store", "tool store"],
    "garden center": ["nursery", "plant store", "garden shop"],
    "pharmacy": ["drugstore", "drug store", "compounding pharmacy"],
    "convenience store": ["corner store", "mini mart"],
    "liquor store": ["wine shop", "liquor shop", "spirits store"],
    "vape shop": ["vape store", "smoke shop"],
    "smoke shop": ["tobacco shop", "vape shop", "head shop"],
    "gun store": ["gun shop", "firearms dealer", "sporting goods"],

    # ════════════════════════════════════════════════════════════════
    # EDUCATION / CHILDCARE
    # ════════════════════════════════════════════════════════════════
    "tutoring": ["tutor", "tutoring center", "academic tutoring"],
    "tutor": ["tutoring", "tutoring center", "private tutor"],
    "preschool": ["daycare", "child care", "early learning center", "nursery school"],
    "daycare": ["preschool", "child care", "early learning center"],
    "child care": ["daycare", "preschool", "kids care"],
    "kindergarten": ["preschool", "early learning"],
    "private school": ["independent school", "academy", "preparatory school"],
    "music school": ["music lessons", "music academy", "music teacher"],
    "driving school": ["driving instructor", "driver education"],
    "language school": ["language classes", "english school"],

    # ════════════════════════════════════════════════════════════════
    # HOSPITALITY / TRAVEL
    # ════════════════════════════════════════════════════════════════
    "hotel": ["hotels", "lodging", "boutique hotel", "inn", "resort"],
    "motel": ["lodging", "inn", "budget hotel"],
    "bed and breakfast": ["b&b", "inn", "guest house"],
    "resort": ["spa resort", "destination resort", "vacation resort"],
    "vacation rental": ["short term rental", "airbnb host", "rental property"],
    "travel agency": ["travel agent", "tour operator", "vacation planner"],
    "tour operator": ["tour company", "travel agency", "guided tours"],
    "event venue": ["banquet hall", "wedding venue", "conference venue"],
    "wedding venue": ["event venue", "banquet hall", "reception venue"],

    # ════════════════════════════════════════════════════════════════
    # PHOTOGRAPHY / EVENTS / CREATIVE
    # ════════════════════════════════════════════════════════════════
    "photographer": ["photography studio", "wedding photographer",
                      "portrait photographer", "photography service"],
    "photography": ["photographer", "photography studio"],
    "videographer": ["videography", "wedding videographer", "video production"],
    "wedding planner": ["event planner", "wedding coordinator", "event design"],
    "event planner": ["event coordinator", "wedding planner", "event design"],
    "dj service": ["wedding dj", "event dj", "mobile dj"],

    # ════════════════════════════════════════════════════════════════
    # NONPROFIT / RELIGIOUS
    # ════════════════════════════════════════════════════════════════
    "nonprofit": ["non-profit", "charity", "foundation", "501c3"],
    "charity": ["nonprofit", "non-profit", "charitable organization"],
    "church": ["place of worship", "religious organization", "congregation"],
    "synagogue": ["jewish congregation", "place of worship"],
    "mosque": ["islamic center", "place of worship"],
    "temple": ["place of worship", "religious organization"],
    "funeral home": ["funeral services", "mortuary", "cremation services"],
    "cemetery": ["memorial park", "burial grounds"],

    # ════════════════════════════════════════════════════════════════
    # TECH / IT / SAAS
    # ════════════════════════════════════════════════════════════════
    "software": ["software development", "software company", "saas company",
                  "tech company"],
    "tech company": ["software company", "technology", "tech firm"],
    "it services": ["it support", "managed it services", "tech support",
                     "computer services"],
    "managed it": ["it services", "msp", "managed service provider"],
    "computer repair": ["pc repair", "laptop repair", "tech repair"],
    "data center": ["colocation", "cloud services", "hosting provider"],
    "cybersecurity": ["security company", "infosec firm", "cyber security"],

    # ════════════════════════════════════════════════════════════════
    # LOGISTICS / TRANSPORT
    # ════════════════════════════════════════════════════════════════
    "trucking": ["trucking company", "freight services", "logistics"],
    "logistics": ["logistics company", "freight forwarder", "supply chain"],
    "courier": ["courier service", "delivery service", "messenger service"],
    "shipping": ["shipping service", "freight services", "courier"],
    "warehouse": ["warehousing", "storage facility", "distribution center"],

    # ════════════════════════════════════════════════════════════════
    # MISC PROFESSIONAL
    # ════════════════════════════════════════════════════════════════
    "engineer": ["engineering firm", "civil engineer", "structural engineer"],
    "architect": ["architectural firm", "architecture studio", "design firm"],
    "interior design": ["interior designer", "interior decorator", "design studio"],
    "surveyor": ["land surveyor", "surveying company"],
    "notary": ["notary public", "notary services", "mobile notary"],
    "translator": ["translation service", "interpreter", "language services"],

    # ════════════════════════════════════════════════════════════════
    # SECURITY
    # ════════════════════════════════════════════════════════════════
    "security": ["security service", "security company", "security guard"],
    "alarm": ["alarm company", "security alarm", "alarm installation"],
    "private investigator": ["pi", "investigator", "detective agency"],
}


def _normalize_query(q: str) -> str:
    """Lowercase + strip trailing 's' from the last word (dental clinics -> dental clinic)."""
    q = q.strip().lower()
    if q.endswith("s") and not q.endswith("ss"):
        # Only strip 's' if removing it leaves a known synonym key
        singular = q[:-1]
        if singular in QUERY_SYNONYMS:
            return singular
    return q


def fuzzy_synonym_key(query: str) -> Optional[str]:
    """
    When the user's query doesn't exactly match any synonym key, find
    the closest one via edit distance. Catches typos like 'restaraunt'
    → 'restaurant', 'dentsit' → 'dentist'. Returns None if no key is
    close enough (cutoff 0.78 — strict to avoid wrong corrections).

    Standard library only (difflib) — no extra deps.
    """
    if not query:
        return None
    import difflib
    q = query.strip().lower()
    if q in QUERY_SYNONYMS:
        return None  # exact match already covered upstream
    matches = difflib.get_close_matches(
        q, QUERY_SYNONYMS.keys(), n=1, cutoff=0.78,
    )
    return matches[0] if matches else None


def _query_variants(query: str) -> list:
    """Return ordered list of queries to try, starting with the user's input.

    If the user's query is a typo (e.g. 'restaraunt') we ALSO fan out
    on the corrected form ('restaurant') and its synonyms, so they
    don't get stuck on Google Maps' ~30-result single-query cap.
    """
    q = _normalize_query(query)
    variants = [query]  # always try the exact user query first

    # Look up synonyms for normalized form, then for plural-stripped form
    synonyms = list(QUERY_SYNONYMS.get(q, []))
    if q.endswith("s") and q[:-1] in QUERY_SYNONYMS:
        synonyms.extend(QUERY_SYNONYMS[q[:-1]])

    # Typo correction — if normalized query has zero exact synonyms,
    # try fuzzy-matching to a known synonym key. Add the corrected
    # term + its synonyms to the variant list. Original query still
    # leads (Google Maps may match on the typo too).
    if not synonyms:
        corrected = fuzzy_synonym_key(q)
        if corrected:
            variants.append(corrected)
            synonyms.extend(QUERY_SYNONYMS.get(corrected, []))

    variants.extend(synonyms)

    # Also try the singular form of the original query if it differs
    raw_lower = query.strip().lower()
    if raw_lower != q:
        variants.append(q)

    # Dedupe case-insensitively, preserving order
    seen = set()
    out = []
    for v in variants:
        k = v.strip().lower()
        if k and k not in seen:
            seen.add(k)
            out.append(v)
    return out


def _single_query_paginated(full_query: str, max_results: int,
                             seen_place_ids: set) -> list:
    """Run one query with full pagination, deduped against seen_place_ids."""
    results = []
    start = 0
    empty_pages = 0

    while len(results) < max_results:
        params = {"engine": "google_maps", "q": full_query}
        if start > 0:
            params["start"] = start

        try:
            data = _call(params)
        except SearchError:
            raise

        local_results = data.get("local_results", []) or []
        if not local_results:
            # Edge case: single-match shapes
            for key in ("place_result", "place_results", "knowledge_graph"):
                pr = data.get(key)
                if pr and isinstance(pr, dict) and (pr.get("title") or pr.get("name")):
                    parsed = _parse_business(pr)
                    pid = parsed.get("place_id")
                    if pid and pid not in seen_place_ids:
                        seen_place_ids.add(pid)
                        results.append(parsed)
            empty_pages += 1
            if empty_pages >= 2:  # two empty pages in a row = genuine end
                break
            start += 20
            time.sleep(0.3)
            continue

        empty_pages = 0
        new_this_page = 0
        for biz in local_results:
            parsed = _parse_business(biz)
            pid = parsed.get("place_id")
            if pid and pid in seen_place_ids:
                continue
            if pid:
                seen_place_ids.add(pid)
            results.append(parsed)
            new_this_page += 1
            if len(results) >= max_results:
                break

        # If the page produced nothing new (all dupes), Google is recycling —
        # further pagination won't help on this query.
        if new_this_page == 0:
            break

        start += 20
        # Softer upper bound — try up to 10 pages per query (200 offset)
        # before giving up. Google Maps rarely goes deeper than this.
        if start >= 200:
            break
        time.sleep(0.3)

    return results


def search_businesses(query: str, location: str = "",
                      max_results: int = 50) -> list:
    """
    Search Google Maps for businesses matching the query in the location.

    Strategy:
    1. Run the exact user query with full pagination
    2. If still below max_results, retry with synonym variants (e.g.
       "dental office" → "dentist" → "dental clinic")
    3. Dedupe everything by place_id across all variants

    Typical query forms:
      search_businesses("dental clinic", "Manhattan NYC")
      search_businesses("law firm", "Brooklyn NY", max_results=100)
    """
    all_results = []
    seen_place_ids = set()

    for variant in _query_variants(query):
        needed = max_results - len(all_results)
        if needed <= 0:
            break
        full_query = f"{variant} {location}".strip()
        try:
            batch = _single_query_paginated(full_query, needed, seen_place_ids)
        except SearchError:
            # Propagate auth/quota errors; they won't be fixed by more queries
            if not all_results:
                raise
            break
        all_results.extend(batch)
        if len(all_results) >= max_results:
            break

    return all_results[:max_results]


def estimate_cost(max_results: int, query: str = "") -> dict:
    """
    Estimate SearchApi cost + API-call count for a given search.

    Returns {"avg_usd": float, "max_usd": float, "avg_calls": int,
             "max_calls": int, "variants": int}.

    The previous version under-estimated by ~5× because it used a flat
    pages = max_results/20 + 2 and ignored synonym fan-out. In practice
    Google Maps returns ~15-25 unique results per query per location
    for common verticals, so hitting 100 results usually means running
    5+ synonym variants — each paginated, each deduped.
    """
    pages_per_variant = max(1, min(10, (max_results + 19) // 20))

    # How many synonym variants will actually fire?
    if query:
        try:
            variants = _query_variants(query)
            n_variants = len(variants)
        except Exception:
            n_variants = 1
    else:
        n_variants = 1

    # Average-case: variants short-circuit once we hit max_results.
    # Assume each variant contributes ~20 new results after dedup; once
    # we've accumulated max_results, the loop exits early.
    avg_variants_fired = min(n_variants, max(1, (max_results + 19) // 20))
    avg_calls = avg_variants_fired * pages_per_variant

    # Worst-case: every variant runs full pagination because dupes/thin
    # results force us to keep querying.
    max_calls = n_variants * pages_per_variant

    # SearchApi: ~$0.005 per credit/call
    return {
        "avg_usd": round(avg_calls * 0.005, 2),
        "max_usd": round(max_calls * 0.005, 2),
        "avg_calls": avg_calls,
        "max_calls": max_calls,
        "variants": n_variants,
    }
