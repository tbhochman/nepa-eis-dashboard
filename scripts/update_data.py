#!/usr/bin/env python3
"""
Daily NEPA EIS Dashboard updater.

Queries the Federal Register API for recent EIS-related documents
(NOIs, Draft EIS, Final EIS, RODs) and updates data.js accordingly.

Data flow:
  1. Fetch recent Federal Register documents mentioning EIS
  2. Classify each as NOI, DEIS, FEIS, or ROD
  3. New NOIs -> add to DATA_UNDERWAY
  4. FEIS/ROD for existing underway projects -> move to DATA_COMPLETED
  5. Update elapsed times for all underway projects
  6. Write updated data.js
"""

import json
import re
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime, date
from pathlib import Path

DATA_JS = Path(__file__).resolve().parent.parent / "data.js"

FR_API = "https://www.federalregister.gov/api/v1/documents.json"

# Agency name normalization: Federal Register uses full names,
# dashboard uses abbreviations
AGENCY_MAP = {
    "forest-service": "USFS",
    "land-management-bureau": "BLM",
    "bureau-of-land-management": "BLM",
    "federal-energy-regulatory-commission": "FERC",
    "federal-highway-administration": "FHWA",
    "army-corps-of-engineers": "USACE",
    "corps-of-engineers": "USACE",
    "ocean-energy-management-bureau": "BOEM",
    "bureau-of-ocean-energy-management": "BOEM",
    "reclamation-bureau": "USBR",
    "bureau-of-reclamation": "USBR",
    "national-oceanic-and-atmospheric-administration": "NOAA",
    "fish-and-wildlife-service": "USFWS",
    "national-park-service": "NPS",
    "environmental-protection-agency": "EPA",
    "energy-department": "DOE",
    "department-of-energy": "DOE",
    "nuclear-regulatory-commission": "NRC",
    "federal-aviation-administration": "FAA",
    "federal-transit-administration": "FTA",
    "federal-railroad-administration": "FRA",
    "defense-department": "DOD",
    "navy-department": "USN",
    "air-force-department": "USAF",
    "army-department": "USA",
    "marine-corps": "USMC",
    "coast-guard": "USCG",
    "national-aeronautics-and-space-administration": "NASA",
    "tennessee-valley-authority": "TVA",
    "indian-affairs-bureau": "BIA",
    "bureau-of-indian-affairs": "BIA",
    "surface-transportation-board": "STB",
    "natural-resources-conservation-service": "NRCS",
    "animal-and-plant-health-inspection-service": "APHIS",
    "agriculture-department": "USDA",
    "interior-department": "DOI",
    "transportation-department": "USDT",
    "state-department": "DOS",
    "bonneville-power-administration": "BPA",
    "western-area-power-administration": "WAPA",
    "rural-utilities-service": "RUS",
    "general-services-administration": "GSA",
    "national-highway-traffic-safety-administration": "NHTSA",
    "surface-mining-reclamation-and-enforcement-office": "OSMRE",
    "national-marine-fisheries-service": "NMFS",
    "national-nuclear-security-administration": "NNSA",
    "national-science-foundation": "NSF",
    "veterans-affairs-department": "VA",
    "housing-and-urban-development-department": "HUD",
    "maritime-administration": "MARAD",
    "missile-defense-agency": "MDA",
    "customs-and-border-protection-bureau": "CBP",
    "food-and-drug-administration": "FDA",
    "postal-service": "USPS",
    "international-boundary-and-water-commission-united-states-and-mexico": "USIBWC",
    "farm-service-agency": "FSA",
}

# Category keywords for classification
CATEGORY_RULES = [
    (r"\b(pipeline|lng|oil|gas|petroleum|fossil|coal|refiner)", "Energy Fossil"),
    (r"\b(solar|wind|hydro|geotherm|renewable|battery storage|pumped storage|hydrogen hub|direct air capture)", "Energy Renewable"),
    (r"\b(nuclear|plutonium|uranium|reactor|spent fuel)", "Nuclear"),
    (r"\b(highway|interstate|i-\d+|freeway|transit|rail|bridge|airport|port |harbor|ship channel|ferry|parkway|turnpike|corridor.*highway|expressway)", "Transportation"),
    (r"\b(mine|mining|lithium|copper|gold|molybdenum|quarr)", "Mining"),
    (r"\b(flood|dam|reservoir|water supply|irrigation|canal|levee|watershed|coastal storm)", "Water Infrastructure"),
    (r"\b(sage.grouse|wildlife|habitat|endangered|fish.*management|sanctuary|tortoise|wolf|bison|elk|conservation plan|marine.*sanctuary|estuarine)", "Wildlife & Habitat"),
    (r"\b(military|air force|navy|army|marine corps|missile|defense|bomber|fighter|f-\d+|training.*range|munitions)", "Defense"),
    (r"\b(forest plan|resource management plan|land management|national forest|timber|vegetation management|fire resilien|fuels|grazing|recreation area|ski area|travel management)", "Land & Resource Mgmt"),
]

# State abbreviation extraction from title
STATE_ABBREVS = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY",
}

# Two-letter state codes that appear in titles
STATE_CODE_RE = re.compile(
    r"\b(A[KLRZ]|C[AOT]|D[CE]|FL|GA|HI|I[ADLN]|K[SY]|LA|M[ADEINOST]|"
    r"N[CDEHJMVY]|O[HKR]|P[AR]|RI|S[CD]|T[NX]|UT|V[AT]|W[AIVY])\b"
)


def fetch_json(url, retries=3):
    """Fetch JSON from URL with retries and rate-limit awareness."""
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "NEPA-EIS-Dashboard/1.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                print(f"  ERROR fetching {url}: {e}", file=sys.stderr)
                return None


def query_federal_register(days_back=3):
    """Query Federal Register for recent EIS-related documents."""
    today = date.today()
    from datetime import timedelta
    start = today - timedelta(days=days_back)

    all_results = []

    # Search for EIS-related notices
    search_terms = [
        '"environmental impact statement"',
        '"notice of intent" "environmental impact"',
        '"record of decision" "environmental impact statement"',
    ]

    for term in search_terms:
        params = {
            "conditions[term]": term,
            "conditions[publication_date][gte]": start.isoformat(),
            "conditions[publication_date][lte]": today.isoformat(),
            "per_page": 200,
            "page": 1,
            "fields[]": [
                "title", "abstract", "publication_date", "agencies",
                "type", "document_number", "html_url", "action",
            ],
        }

        # Build URL manually since fields[] needs special handling
        parts = []
        for k, v in params.items():
            if isinstance(v, list):
                for item in v:
                    parts.append(f"{urllib.parse.quote(k)}={urllib.parse.quote(str(item))}")
            else:
                parts.append(f"{urllib.parse.quote(k)}={urllib.parse.quote(str(v))}")
        url = f"{FR_API}?{'&'.join(parts)}"

        print(f"  Querying: {term[:50]}...")
        data = fetch_json(url)
        if data and "results" in data:
            all_results.extend(data["results"])
            total = data.get("count", 0)
            print(f"    Found {len(data['results'])} results (total: {total})")

            # Paginate if needed
            page = 2
            while len(data.get("results", [])) == 200 and page <= 5:
                params_copy = dict(params)
                params_copy["page"] = page
                parts2 = []
                for k, v in params_copy.items():
                    if isinstance(v, list):
                        for item in v:
                            parts2.append(f"{urllib.parse.quote(k)}={urllib.parse.quote(str(item))}")
                    else:
                        parts2.append(f"{urllib.parse.quote(k)}={urllib.parse.quote(str(v))}")
                url2 = f"{FR_API}?{'&'.join(parts2)}"
                data = fetch_json(url2)
                if data and "results" in data:
                    all_results.extend(data["results"])
                    page += 1
                else:
                    break

        time.sleep(1)  # Rate limit courtesy

    # Deduplicate by document_number
    seen = set()
    deduped = []
    for doc in all_results:
        dn = doc.get("document_number", "")
        if dn and dn not in seen:
            seen.add(dn)
            deduped.append(doc)
        elif not dn:
            deduped.append(doc)

    print(f"  Total unique documents: {len(deduped)}")
    return deduped


def classify_document(doc):
    """Classify a Federal Register document as NOI, DEIS, FEIS, ROD, or None."""
    title = (doc.get("title") or "").lower()
    abstract = (doc.get("abstract") or "").lower()
    action = (doc.get("action") or "").lower()
    combined = f"{title} {abstract} {action}"

    # ROD detection
    if "record of decision" in combined:
        return "ROD"

    # Final EIS
    if ("final environmental impact statement" in combined or
            "final eis" in combined or
            ("feis" in combined and "environmental" in combined)):
        return "FEIS"

    # Draft EIS
    if ("draft environmental impact statement" in combined or
            "draft eis" in combined or
            ("deis" in combined and "environmental" in combined)):
        return "DEIS"

    # NOI (Notice of Intent to prepare an EIS)
    if ("notice of intent" in combined and
            ("environmental impact statement" in combined or "eis" in combined or
             "prepare an" in combined)):
        return "NOI"

    return None


def extract_agency(doc):
    """Extract abbreviated agency name from document."""
    agencies = doc.get("agencies") or []
    for agency in agencies:
        slug = agency.get("slug", "")
        if slug in AGENCY_MAP:
            return AGENCY_MAP[slug]
        # Try raw_name
        raw = agency.get("raw_name", "")
        if raw:
            return raw
    # Fallback: use first agency name
    if agencies:
        name = agencies[0].get("name", agencies[0].get("raw_name", ""))
        return name
    return "Unknown"


def classify_category(title):
    """Classify EIS category from title text."""
    lower = title.lower()
    for pattern, category in CATEGORY_RULES:
        if re.search(pattern, lower):
            return category
    return "Other"


def extract_states(title):
    """Extract state codes from title."""
    states = set()
    lower = title.lower()

    # Check for full state names
    for name, code in STATE_ABBREVS.items():
        if name in lower:
            states.add(code)

    # Check for state abbreviations (2-letter codes in title)
    for m in STATE_CODE_RE.finditer(title):
        states.add(m.group(1))

    if not states:
        # Check if it's a national/programmatic EIS
        if any(w in lower for w in ["programmatic", "nationwide", "national"]):
            return "NAT"
        return ""

    return " - ".join(sorted(states))


def normalize_name(name):
    """Normalize project name for fuzzy matching."""
    # Remove common suffixes, extra whitespace, punctuation
    n = re.sub(r'\s+', ' ', name).strip()
    # Truncate at 200 chars like existing data
    return n[:200]


def fuzzy_match(name, candidates, threshold=0.6):
    """Simple fuzzy matching: check if significant words overlap."""
    def words(s):
        return set(re.findall(r'[a-z]{3,}', s.lower())) - {
            'the', 'and', 'for', 'environmental', 'impact', 'statement',
            'draft', 'final', 'supplemental', 'proposed', 'project',
            'notice', 'intent', 'record', 'decision', 'prepare',
        }

    name_words = words(name)
    if not name_words:
        return None

    best_score = 0
    best_match = None

    for candidate in candidates:
        cand_words = words(candidate)
        if not cand_words:
            continue
        overlap = len(name_words & cand_words)
        union = len(name_words | cand_words)
        score = overlap / union if union else 0
        if score > best_score and score >= threshold:
            best_score = score
            best_match = candidate

    return best_match


def parse_data_js(content):
    """Parse data.js into Python data structures.

    data.js has a simple structure: 3 comment lines followed by 3 const lines,
    each on a single line. We parse line-by-line to avoid offset issues.
    """
    lines = content.split('\n')
    completed = None
    underway = None

    for line in lines:
        if line.startswith('const DATA_COMPLETED'):
            # Extract the JSON array between = and ;
            json_str = line.split('=', 1)[1].strip().rstrip(';').strip()
            completed = json.loads(json_str)
        elif line.startswith('const DATA_UNDERWAY'):
            json_str = line.split('=', 1)[1].strip().rstrip(';').strip()
            underway = json.loads(json_str)

    if completed is None:
        raise ValueError("Could not find DATA_COMPLETED in data.js")
    if underway is None:
        raise ValueError("Could not find DATA_UNDERWAY in data.js")

    return completed, underway


def build_data_js(content, completed, underway):
    """Rebuild data.js with updated arrays, preserving all other lines exactly."""
    completed.sort(key=lambda r: r.get("feis", "") or "")
    underway.sort(key=lambda r: r.get("noi", "") or "", reverse=True)

    completed_json = json.dumps(completed, ensure_ascii=False, separators=(",", ":"))
    underway_json = json.dumps(underway, ensure_ascii=False, separators=(",", ":"))

    lines = content.split('\n')
    new_lines = []
    for line in lines:
        if line.startswith('const DATA_COMPLETED'):
            new_lines.append(f'const DATA_COMPLETED = {completed_json};')
        elif line.startswith('const DATA_UNDERWAY'):
            new_lines.append(f'const DATA_UNDERWAY = {underway_json};')
        else:
            new_lines.append(line)

    return '\n'.join(new_lines)


def update_header_comment(content, n_completed, n_underway):
    """Update the counts in the header comment of data.js."""
    total = n_completed + n_underway
    new_comment = (
        f"// CEQ EIS Timeline Data (2010-2024) + EPA 2025-2026 Supplement + "
        f"Federal Register NOI Audit (March 2026) + Daily Auto-Update\n"
        f"// {n_completed} completed + {n_underway} underway = {total} total\n"
        f"// CEQ: ceq.doe.gov | EPA: cdxapps.epa.gov | ROD: federalregister.gov | "
        f"Updated: {date.today().isoformat()}"
    )
    # Replace first 3 lines (comments)
    lines = content.split('\n')
    # Find how many comment lines at the top
    comment_end = 0
    for i, line in enumerate(lines):
        if line.startswith('//'):
            comment_end = i + 1
        else:
            break
    lines = new_comment.split('\n') + lines[comment_end:]
    return '\n'.join(lines)


def main():
    print("=== NEPA EIS Dashboard Daily Update ===")
    print(f"Date: {date.today().isoformat()}")

    # Read current data
    print("\n1. Reading current data.js...")
    content = DATA_JS.read_text(encoding="utf-8")
    completed, underway = parse_data_js(content)
    print(f"   Completed: {len(completed)}, Underway: {len(underway)}")

    # Build lookup maps
    completed_names = {r["n"] for r in completed}
    underway_names = {r["n"] for r in underway}
    underway_by_name = {r["n"]: r for r in underway}

    # Query Federal Register
    print("\n2. Querying Federal Register API...")
    # Look back 3 days to catch weekends/holidays
    docs = query_federal_register(days_back=3)

    if not docs:
        print("   No documents found or API error. Skipping document processing.")
    else:
        # Process each document
        print(f"\n3. Processing {len(docs)} documents...")
        new_nois = 0
        updates = 0
        completions = 0

        for doc in docs:
            doc_type = classify_document(doc)
            if not doc_type:
                continue

            title = doc.get("title", "")
            pub_date = doc.get("publication_date", "")
            agency = extract_agency(doc)

            print(f"   [{doc_type}] {agency}: {title[:80]}...")

            if doc_type == "NOI":
                # Check if this project is already tracked
                if title in completed_names or title in underway_names:
                    continue
                # Fuzzy match against existing
                match = fuzzy_match(title, completed_names | underway_names)
                if match:
                    print(f"     -> Fuzzy match to existing: {match[:60]}...")
                    continue

                # New NOI - add to underway
                cat = classify_category(title)
                states = extract_states(title)
                new_record = {
                    "n": normalize_name(title),
                    "a": agency,
                    "c": cat,
                    "s": states,
                    "noi": pub_date,
                    "l": pub_date,
                    "e": 0,
                }
                underway.append(new_record)
                underway_names.add(new_record["n"])
                underway_by_name[new_record["n"]] = new_record
                new_nois += 1
                print(f"     -> NEW underway project added")

            elif doc_type == "DEIS":
                # Update latest status for matching underway project
                match = fuzzy_match(title, underway_names, threshold=0.5)
                if match and match in underway_by_name:
                    rec = underway_by_name[match]
                    if pub_date > (rec.get("l") or ""):
                        rec["l"] = pub_date
                        updates += 1
                        print(f"     -> Updated latest date for: {match[:60]}...")

            elif doc_type == "FEIS":
                # Update latest status for matching underway project
                match = fuzzy_match(title, underway_names, threshold=0.5)
                if match and match in underway_by_name:
                    rec = underway_by_name[match]
                    if pub_date > (rec.get("l") or ""):
                        rec["l"] = pub_date
                        updates += 1
                        print(f"     -> Updated FEIS date for: {match[:60]}...")

            elif doc_type == "ROD":
                # Check if this matches an underway project - if so, complete it
                match = fuzzy_match(title, underway_names, threshold=0.5)
                if match and match in underway_by_name:
                    rec = underway_by_name[match]
                    noi_date = rec.get("noi", "")

                    if noi_date:
                        # Calculate durations
                        noi_dt = datetime.strptime(noi_date, "%Y-%m-%d")
                        rod_dt = datetime.strptime(pub_date, "%Y-%m-%d")
                        dur_years = round((rod_dt - noi_dt).days / 365.25, 1)

                        # We don't have FEIS date from the underway record,
                        # so use latest status date as approximation if it looks like FEIS
                        latest = rec.get("l", "")
                        if latest and latest != noi_date and latest < pub_date:
                            feis_date = latest
                            feis_dt = datetime.strptime(feis_date, "%Y-%m-%d")
                            dur_feis = round((feis_dt - noi_dt).days / 365.25, 1)
                        else:
                            feis_date = pub_date
                            dur_feis = dur_years

                        completed_record = {
                            "n": rec["n"],
                            "a": rec["a"],
                            "c": rec["c"],
                            "s": rec["s"],
                            "noi": noi_date,
                            "feis": feis_date,
                            "rod": pub_date,
                            "df": dur_feis,
                            "d": dur_years,
                        }
                        completed.append(completed_record)
                        completed_names.add(rec["n"])

                        # Remove from underway
                        underway = [r for r in underway if r["n"] != match]
                        del underway_by_name[match]
                        underway_names.discard(match)

                        completions += 1
                        print(f"     -> COMPLETED: {match[:60]}... ({dur_years} yrs)")
                    else:
                        # No NOI date, just update latest
                        rec["l"] = pub_date
                        updates += 1

                # Also check if ROD matches a completed project missing its ROD
                match_c = fuzzy_match(title, {r["n"] for r in completed if not r.get("rod")}, threshold=0.5)
                if match_c:
                    for r in completed:
                        if r["n"] == match_c and not r.get("rod"):
                            r["rod"] = pub_date
                            if r.get("noi"):
                                noi_dt = datetime.strptime(r["noi"], "%Y-%m-%d")
                                rod_dt = datetime.strptime(pub_date, "%Y-%m-%d")
                                r["d"] = round((rod_dt - noi_dt).days / 365.25, 1)
                            updates += 1
                            print(f"     -> Filled ROD for completed: {match_c[:60]}...")
                            break

        print(f"\n   Summary: {new_nois} new NOIs, {updates} updates, {completions} completions")

    # Update elapsed times for all underway projects
    print("\n4. Updating elapsed times...")
    today = date.today()
    for r in underway:
        noi = r.get("noi", "")
        if noi:
            try:
                noi_dt = datetime.strptime(noi, "%Y-%m-%d").date()
                r["e"] = round((today - noi_dt).days / 365.25, 1)
            except ValueError:
                pass

    # Rebuild data.js
    print("\n5. Writing updated data.js...")
    new_content = build_data_js(content, completed, underway)
    new_content = update_header_comment(new_content, len(completed), len(underway))
    DATA_JS.write_text(new_content, encoding="utf-8")
    print(f"   Done. Completed: {len(completed)}, Underway: {len(underway)}")

    # Report if anything changed
    changed = new_content != content
    if changed:
        print("\n   DATA CHANGED - commit needed")
    else:
        print("\n   No substantive changes")

    return 0 if changed else 0  # Always succeed


if __name__ == "__main__":
    sys.exit(main())
