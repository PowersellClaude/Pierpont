"""
Pierpont Money Printer — SC Lowcountry Construction Lead Intelligence
Streamlit Cloud Dashboard with Built-in Python Scraper
"""

import streamlit as st
import sqlite3
import pandas as pd
import requests
import os
import re
import math
import json
import base64
import time
import logging
from datetime import datetime, timedelta
from urllib.parse import quote, urlparse
from html import escape as html_escape

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("pierpont")

# ─── Page Config ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Pierpont Money Printer",
    page_icon="🏗️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─── Constants ──────────────────────────────────────────────────────────────
APP_PASSWORD = "Bulleit"
DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "db", "permits.db"))

# Logo embedded as base64
LOGO_B64 = ""
_logo_path = os.path.join(os.path.dirname(__file__), "public", "logo.png")
if os.path.exists(_logo_path):
    with open(_logo_path, "rb") as f:
        LOGO_B64 = base64.b64encode(f.read()).decode()

DRIVE_TIMES = {
    "Town of Mount Pleasant": 0, "Sullivan's Island": 15, "City of Charleston": 20,
    "Isle of Palms": 20, "Charleston County": 20, "City of North Charleston": 25,
    "City of Hanahan": 25, "Kiawah Island": 35, "Seabrook Island": 35,
    "City of Folly Beach": 35, "Town of Summerville": 35, "City of Goose Creek": 35,
    "Berkeley County": 45, "Dorchester County": 45, "Town of Moncks Corner": 50,
    "Georgetown County": 70, "Colleton County": 70, "Town of Bluffton": 75,
    "City of Beaufort": 80, "Town of Hilton Head Island": 80, "City of Hardeeville": 85,
    "Williamsburg County": 85, "Orangeburg County": 85,
}

FOIA_BODY = """To Whom It May Concern,

Pursuant to the South Carolina Freedom of Information Act, I am a taxpaying citizen requesting the following records for research purposes only:

A list of all strapping inspections (also known as strap/banding inspections) that received a passing status within the last 90 days, including permit number, property address, contractor/builder name, inspection date, and status.

Thank you for your time."""

FOIA_MUNICIPALITIES = [
    {"name": "City of Folly Beach", "type": "email", "email": "permits@follybeach.gov"},
    {"name": "City of Hanahan", "type": "portal", "url": "https://cityofhanahansc.nextrequest.com/requests/new", "portal_name": "NextRequest Portal"},
    {"name": "Town of Moncks Corner", "type": "email", "email": "info@monckscornersc.gov"},
    {"name": "Georgetown County", "type": "email", "email": "cityfoiarequest@georgetownsc.gov"},
    {"name": "Colleton County", "type": "email", "email": "foia@colletoncounty.org"},
    {"name": "City of Beaufort", "type": "portal", "url": "https://beaufortcountysc.justfoia.com/publicportal/home/newrequest", "portal_name": "JustFOIA Portal"},
    {"name": "Williamsburg County", "type": "email", "email": "FOIA-Request@wc.sc.gov"},
    {"name": "Orangeburg County", "type": "email", "email": "foia@orangeburgcounty.org"},
]

# ─── EnerGov API Config ────────────────────────────────────────────────────
ENERGOV_BASE = "https://egcss.charleston-sc.gov/EnerGov_Prod/selfservice"
SEARCH_API = f"{ENERGOV_BASE}/api/energov/search/search"
CONTACTS_API = f"{ENERGOV_BASE}/api/energov/entity/contacts/search/search"
PERMIT_DETAIL_API = f"{ENERGOV_BASE}/api/energov/permits/permitdetail"

TENANT_HEADERS = {
    "Content-Type": "application/json;charset=UTF-8",
    "Accept": "application/json, text/plain, */*",
    "tenantid": "1",
    "tenantname": "CharlestonSC",
    "tyler-tenant-culture": "en-US",
    "tyler-tenanturl": "CharlestonSC",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Origin": ENERGOV_BASE,
    "Referer": f"{ENERGOV_BASE}/",
}

STRAPPING_TYPE_ID = "2a1af85e-dcdc-4a9e-b17a-e95e3d92d918"
PASSED_STATUS_ID = "c0351663-6112-4181-a943-cf58f67c6c9d"


# ─── Opportunity Score (matches Node.js version) ───────────────────────────
def calculate_opportunity_score(project_value, inspection_date, municipality):
    val_score = 0
    if project_value and project_value > 100000:
        log_min = math.log(100000)
        log_max = math.log(2000000)
        val_score = min(100, max(0, ((math.log(project_value) - log_min) / (log_max - log_min)) * 100))
    recent_score = 0
    if inspection_date:
        try:
            d = datetime.strptime(str(inspection_date)[:10], "%Y-%m-%d")
            days_diff = (datetime.now() - d).days
            recent_score = min(100, max(0, (1 - days_diff / 30) * 100))
        except Exception:
            pass
    dist_score = 50
    if municipality and municipality in DRIVE_TIMES:
        dist_score = min(100, max(0, (1 - DRIVE_TIMES[municipality] / 90) * 100))
    return min(100, max(0, round(val_score * 0.4 + recent_score * 0.3 + dist_score * 0.3)))


# ─── EnerGov Scraper (Python port) ─────────────────────────────────────────
def build_search_body(type_id, status_id, page_number=1, page_size=100):
    now = datetime.utcnow()
    from_date = now - timedelta(days=30)
    empty = {"PageNumber": 0, "PageSize": 0, "SortBy": None, "SortAscending": False}
    return {
        "Keyword": "", "ExactMatch": False, "SearchModule": 4, "FilterModule": 4,
        "SearchMainAddress": False,
        "PlanCriteria": {**empty}, "PermitCriteria": {**empty},
        "InspectionCriteria": {
            "Keyword": None, "ExactMatch": False, "Complete": None,
            "InspectionNumber": None, "InspectionTypeId": type_id, "InspectionStatusId": status_id,
            "RequestDateFrom": None, "RequestDateTo": None,
            "ScheduleDateFrom": from_date.isoformat(), "ScheduleDateTo": now.isoformat(),
            "Address": None, "SearchMainAddress": False, "ContactId": None,
            "TypeId": [], "WorkClassIds": [], "ParcelNumber": None,
            "DisplayCodeInspections": False, "ExcludeCases": [], "ExcludeFilterModules": [],
            "HiddenInspectionTypeIDs": None,
            "PageNumber": page_number, "PageSize": page_size,
            "SortBy": "ScheduledDate", "SortAscending": False,
        },
        "CodeCaseCriteria": {**empty}, "RequestCriteria": {**empty},
        "BusinessLicenseCriteria": {**empty}, "ProfessionalLicenseCriteria": {**empty},
        "LicenseCriteria": {**empty}, "ProjectCriteria": {**empty},
        "ExcludeCases": None, "HiddenInspectionTypeIDs": None,
        "PageNumber": 0, "PageSize": 0, "SortBy": "ScheduledDate", "SortAscending": False,
    }


def build_permit_search_body(address):
    p = {"PageNumber": 1, "PageSize": 100, "SortBy": None, "SortAscending": False}
    return {
        "Keyword": address, "ExactMatch": True, "SearchModule": 1, "FilterModule": 1,
        "SearchMainAddress": False,
        "PlanCriteria": {**p}, "PermitCriteria": {
            "PermitNumber": None, "PermitTypeId": None, "PermitWorkclassId": None, "PermitStatusId": None,
            "ProjectName": None, "IssueDateFrom": None, "IssueDateTo": None,
            "Address": None, "Description": None,
            "ExpireDateFrom": None, "ExpireDateTo": None, "FinalDateFrom": None, "FinalDateTo": None,
            "ApplyDateFrom": None, "ApplyDateTo": None,
            "SearchMainAddress": False, "ContactId": None, "TypeId": None, "WorkClassIds": None,
            "ParcelNumber": None, "ExcludeCases": None, "EnableDescriptionSearch": False,
            "PageNumber": 1, "PageSize": 100, "SortBy": "IssueDate", "SortAscending": False,
        },
        "InspectionCriteria": {**p}, "CodeCaseCriteria": {**p}, "RequestCriteria": {**p},
        "BusinessLicenseCriteria": {**p}, "ProfessionalLicenseCriteria": {**p},
        "LicenseCriteria": {**p}, "ProjectCriteria": {**p},
        "ExcludeCases": None, "HiddenInspectionTypeIDs": None,
        "PageNumber": 1, "PageSize": 100, "SortBy": "IssueDate", "SortAscending": False,
    }


def parse_energov_date(val):
    if not val:
        return None
    m = re.search(r'/Date\((\d+)\)/', str(val))
    if m:
        return datetime.utcfromtimestamp(int(m.group(1)) / 1000).strftime("%Y-%m-%d")
    try:
        return datetime.fromisoformat(str(val).replace("Z", "")).strftime("%Y-%m-%d")
    except Exception:
        return str(val)[:10] if len(str(val)) >= 10 else None


def map_entity(entity):
    if not entity:
        return None
    addr = ""
    if isinstance(entity.get("Address"), dict):
        addr = entity["Address"].get("FullAddress") or entity["Address"].get("AddressLine1") or ""
    elif entity.get("AddressDisplay"):
        addr = entity["AddressDisplay"]
    elif isinstance(entity.get("MainAddress"), str):
        addr = entity["MainAddress"]
    case_num = entity.get("CaseNumber") or entity.get("InspectionNumber") or entity.get("PermitNumber")
    if not case_num and not addr:
        return None
    return {
        "permit_number": case_num,
        "address": addr,
        "municipality": "City of Charleston",
        "builder_name": entity.get("ContractorName") or None,
        "builder_company": entity.get("CompanyName") or entity.get("HolderCompanyName") or None,
        "project_value": float(entity.get("ProjectValue") or entity.get("EstimatedValue") or 0) or None,
        "permit_type": entity.get("CaseType") or entity.get("CaseWorkclass") or None,
        "inspection_type": entity.get("CaseType") or None,
        "inspection_date": parse_energov_date(entity.get("ScheduleDate") or entity.get("RequestDate")),
        "inspection_status": entity.get("CaseStatus") or entity.get("Status") or None,
        "permit_issue_date": parse_energov_date(entity.get("IssueDate") or entity.get("ApplyDate")),
        "source_url": f"{ENERGOV_BASE}#/inspectionDetail/inspection/{entity.get('CaseId')}" if entity.get("CaseId") else None,
        "raw_data": json.dumps(entity),
        "owner_name": entity.get("OwnerName") or None,
        "applicant_name": entity.get("ApplicantName") or None,
    }


def enrich_permit(session, permit):
    """Search for building permit by address to get value + contacts."""
    address = permit.get("address")
    if not address:
        return permit
    enriched = {**permit}
    try:
        body = build_permit_search_body(address)
        resp = session.post(SEARCH_API, json=body, headers=TENANT_HEADERS, timeout=30)
        result = resp.json()
        if not result.get("Success") or not result.get("Result", {}).get("EntityResults"):
            return enriched
        entities = result["Result"]["EntityResults"]
        building_permits = [
            e for e in entities
            if "permit" in (e.get("CaseType") or "").lower()
            and "building" in (e.get("CaseType") or "").lower()
        ]
        if not building_permits:
            return enriched
        best = max(building_permits, key=lambda e: float(e.get("ProjectValue") or e.get("EstimatedValue") or 0))
        case_id = best.get("CaseId")
        if not case_id:
            return enriched
        # Get permit detail for value
        detail_resp = session.post(
            PERMIT_DETAIL_API,
            json={"EntityId": case_id, "ModuleId": 1},
            headers=TENANT_HEADERS, timeout=30
        )
        detail = detail_resp.json()
        p = detail.get("Result", {}) if detail.get("Success") else {}
        raw_val = p.get("Value") or best.get("ProjectValue") or best.get("EstimatedValue")
        if raw_val:
            try:
                enriched["project_value"] = float(str(raw_val).replace(",", "").replace("$", ""))
            except Exception:
                pass
        enriched["permit_type"] = p.get("WorkClassName") or best.get("CaseWorkclass") or enriched.get("permit_type")
        enriched["permit_issue_date"] = parse_energov_date(
            p.get("IssueDate") or p.get("ApplyDate")
        ) or enriched.get("permit_issue_date")
        linked_num = p.get("PermitNumber") or best.get("CaseNumber")

        # Get contacts from building permit
        contacts_resp = session.post(
            CONTACTS_API,
            json={
                "EntityId": case_id, "ModuleId": 1,
                "PageNumber": 1, "PageSize": 100,
                "SortBy": "Type", "SortAscending": True,
            },
            headers=TENANT_HEADERS, timeout=30,
        )
        contacts_data = contacts_resp.json()
        contacts = contacts_data.get("Result", []) if isinstance(contacts_data.get("Result"), list) else []
        all_contacts = []
        for c in contacts:
            ctype = (c.get("ContactTypeName") or "").lower()
            name = " ".join(filter(None, [c.get("FirstName"), c.get("LastName")]))
            company = c.get("GlobalEntityName")
            phone = c.get("Phone") or c.get("CellPhone") or c.get("HomePhone") or ""
            email = c.get("Email") or ""
            all_contacts.append({
                "name": name,
                "company": company or "",
                "type": c.get("ContactTypeName") or "Unknown",
                "phone": phone,
                "email": email,
            })
            if "applicant" in ctype or "contractor" in ctype or "builder" in ctype:
                if not enriched.get("builder_name") and name:
                    enriched["builder_name"] = name
                if not enriched.get("builder_company") and company:
                    enriched["builder_company"] = company
                if not enriched.get("builder_phone") and phone:
                    enriched["builder_phone"] = phone
                if not enriched.get("builder_email") and email:
                    enriched["builder_email"] = email
            elif "owner" in ctype:
                if not enriched.get("owner_name") and name:
                    enriched["owner_name"] = name

        # Store linked permit info and contacts in raw_data
        try:
            raw = json.loads(enriched.get("raw_data") or "{}")
            raw["_linkedPermit"] = linked_num
            raw["_linkedPermitUrl"] = f"{ENERGOV_BASE}#/permitDetail/permit/{case_id}"
            raw["_allContacts"] = all_contacts
            raw["_dataSource"] = "building_permit"
            enriched["raw_data"] = json.dumps(raw)
        except Exception:
            pass
    except Exception as e:
        log.warning(f"Enrich error for {address}: {e}")
    return enriched


# ─── Builder Web Lookup (Python port of builderLookup.js) ─────────────────
SKIP_DOMAINS = [
    "yelp.com", "bbb.org", "facebook.com", "instagram.com", "twitter.com",
    "linkedin.com", "yellowpages.com", "angi.com", "angieslist.com",
    "homeadvisor.com", "thumbtack.com", "houzz.com", "buildzoom.com",
    "manta.com", "mapquest.com", "google.com", "bing.com", "youtube.com",
    "pinterest.com", "nextdoor.com", "porch.com", "chamberofcommerce.com",
    "dnb.com", "buzzfile.com", "bloomberg.com", "zoominfo.com",
    "tiktok.com", "reddit.com", "wikipedia.org", "amazon.com",
    "duckduckgo.com", "apple.com", "x.com", "bizapedia.com",
    "opencorporates.com", "sec.gov", "companieslist.co",
    "newhomesource.com", "newhomeguide.com", "zillow.com",
    "realtor.com", "redfin.com", "trulia.com",
]

JUNK_EMAIL_PATTERNS = [
    "example.com", "sentry.io", "wixpress", "wix.com", "squarespace",
    "wordpress.com", "w3.org", "schema.org", "googleapis.com", "gstatic.com",
    "gravatar.com", "cloudflare", ".png", ".jpg", ".svg", ".gif", ".webp",
    "noreply", "no-reply", "mailer-daemon", "postmaster@", "user@domain",
    "test@", "admin@", "webmaster@", "hostmaster@", "abuse@",
]

PHONE_RE = re.compile(r'(?:\+1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}')
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')

WEB_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"


def _is_valid_phone(p):
    cleaned = re.sub(r'[^\d]', '', p)
    if len(cleaned) == 11 and cleaned.startswith('1'):
        cleaned = cleaned[1:]
    if len(cleaned) != 10:
        return False
    if re.match(r'^(\d)\1{9}$', cleaned):
        return False
    if cleaned[:3] in ('000', '111', '555'):
        return False
    return True


def _is_valid_email(e):
    lower = e.lower()
    if any(pat in lower for pat in JUNK_EMAIL_PATTERNS):
        return False
    if len(lower) > 50:
        return False
    if not re.match(r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$', lower):
        return False
    local = lower.split('@')[0]
    digits = re.sub(r'[^\d]', '', local)
    if len(digits) >= 7:
        return False
    return True


def _extract_contacts_from_html(html_text):
    """Extract phones and emails from HTML using BeautifulSoup."""
    phones = set()
    emails = set()

    if not HAS_BS4 or not html_text:
        # Fallback: regex only
        for m in PHONE_RE.findall(html_text or ""):
            if _is_valid_phone(m):
                phones.add(m.strip())
        for m in EMAIL_RE.findall(html_text or ""):
            if _is_valid_email(m):
                emails.add(m.lower())
        return list(phones), list(emails)

    soup = BeautifulSoup(html_text, "html.parser")

    # 1. tel: links (highest confidence)
    for a in soup.find_all("a", href=re.compile(r'^tel:', re.I)):
        tel = re.sub(r'^tel:\s*', '', a.get("href", "")).replace(" ", "")
        if _is_valid_phone(tel):
            phones.add(tel)

    # 2. mailto: links (highest confidence)
    for a in soup.find_all("a", href=re.compile(r'^mailto:', re.I)):
        mail = re.sub(r'^mailto:\s*', '', a.get("href", "")).split("?")[0].strip().lower()
        if _is_valid_email(mail):
            emails.add(mail)

    # 3. Scan footer, header, contact sections
    for tag in soup.find_all(["script", "style", "noscript", "svg"]):
        tag.decompose()

    selectors = [
        "footer", "header", "nav",
        {"class_": re.compile(r"contact|footer|header|phone|email|info|widget", re.I)},
        {"id": re.compile(r"contact|footer", re.I)},
    ]
    for sel in selectors:
        if isinstance(sel, str):
            elements = soup.find_all(sel)
        else:
            elements = soup.find_all(True, sel)
        for el in elements:
            text = el.get_text(" ", strip=True)
            for m in PHONE_RE.findall(text):
                if _is_valid_phone(m):
                    phones.add(m.strip())
            for m in EMAIL_RE.findall(text):
                if _is_valid_email(m):
                    emails.add(m.lower())

    # 4. Full body fallback if still empty
    if not phones or not emails:
        body = soup.find("body")
        if body:
            text = body.get_text(" ", strip=True)
            if not phones:
                for m in PHONE_RE.findall(text):
                    if _is_valid_phone(m):
                        phones.add(m.strip())
            if not emails:
                for m in EMAIL_RE.findall(text):
                    if _is_valid_email(m):
                        emails.add(m.lower())

    # 5. Raw HTML fallback for tel:/mailto: regex
    for m in re.findall(r'href=["\']tel:([^"\']+)["\']', html_text, re.I):
        if _is_valid_phone(m):
            phones.add(m.strip())
    for m in re.findall(r'href=["\']mailto:([^"\'?]+)', html_text, re.I):
        if _is_valid_email(m.strip()):
            emails.add(m.strip().lower())

    return list(phones), list(emails)


def find_company_website(session, company_name):
    """Search DuckDuckGo HTML for a builder company website."""
    if not company_name:
        return None
    query = f"{company_name} South Carolina"
    try:
        resp = session.get(
            "https://html.duckduckgo.com/html/",
            params={"q": query},
            headers={"User-Agent": WEB_UA},
            timeout=15,
        )
        if resp.status_code != 200:
            return None
        # Parse DuckDuckGo HTML results
        if HAS_BS4:
            soup = BeautifulSoup(resp.text, "html.parser")
            for a in soup.select("a.result__a"):
                href = a.get("href", "")
                # DDG wraps links — extract real URL
                if "uddg=" in href:
                    from urllib.parse import parse_qs, urlparse as _up
                    qs = parse_qs(_up(href).query)
                    href = qs.get("uddg", [href])[0]
                try:
                    hostname = urlparse(href).hostname
                    if not hostname:
                        continue
                    hostname = hostname.lower()
                    if any(hostname == d or hostname.endswith("." + d) for d in SKIP_DOMAINS):
                        continue
                    if "duckduckgo" in hostname:
                        continue
                    return href
                except Exception:
                    continue
        else:
            # Regex fallback if no bs4
            for href in re.findall(r'uddg=([^&"]+)', resp.text):
                from urllib.parse import unquote
                url = unquote(href)
                try:
                    hostname = urlparse(url).hostname
                    if not hostname:
                        continue
                    hostname = hostname.lower()
                    if any(hostname == d or hostname.endswith("." + d) for d in SKIP_DOMAINS):
                        continue
                    return url
                except Exception:
                    continue
    except Exception as e:
        log.warning(f"DuckDuckGo search failed for '{company_name}': {e}")
    return None


def scrape_website_contacts(session, website_url):
    """Scrape a builder website for phone/email across multiple pages."""
    if not website_url:
        return [], []
    all_phones = set()
    all_emails = set()
    try:
        base = urlparse(website_url)
        base_url = f"{base.scheme}://{base.netloc}"
    except Exception:
        return [], []

    pages_to_try = [
        website_url,
        f"{base_url}/contact",
        f"{base_url}/contact-us",
        f"{base_url}/contact.html",
        f"{base_url}/about",
        f"{base_url}/about-us",
    ]

    for page_url in pages_to_try:
        try:
            resp = session.get(
                page_url,
                headers={"User-Agent": WEB_UA, "Accept": "text/html,application/xhtml+xml"},
                timeout=10,
                allow_redirects=True,
            )
            if resp.status_code >= 400:
                continue
            phones, emails = _extract_contacts_from_html(resp.text)
            all_phones.update(phones)
            all_emails.update(emails)
            # If we found both, no need to keep scraping
            if all_phones and all_emails:
                break
        except Exception:
            continue

    return list(all_phones), list(all_emails)


def lookup_builder_web(session, company_name):
    """Full builder lookup: search web for company → scrape contact info."""
    log.info(f"Builder lookup: '{company_name}'")
    website = find_company_website(session, company_name)
    if not website:
        log.info(f"  No website found for '{company_name}'")
        return {"website": None, "phone": None, "email": None}

    log.info(f"  Found website: {website}")
    phones, emails = scrape_website_contacts(session, website)
    log.info(f"  Found {len(phones)} phone(s), {len(emails)} email(s)")
    return {
        "website": website,
        "phone": phones[0] if phones else None,
        "email": emails[0] if emails else None,
    }


def run_scraper(status_placeholder):
    """Run the EnerGov scraper and save results to SQLite."""
    session = requests.Session()
    status_placeholder.info("Establishing session with EnerGov portal...")
    try:
        session.get(f"{ENERGOV_BASE}/", headers={"User-Agent": TENANT_HEADERS["User-Agent"]}, timeout=30)
    except Exception:
        pass
    time.sleep(1)

    status_placeholder.info("Searching for strapping inspections (last 30 days)...")
    all_permits = []
    page_number = 1
    while page_number <= 10:
        body = build_search_body(STRAPPING_TYPE_ID, PASSED_STATUS_ID, page_number, 100)
        try:
            resp = session.post(SEARCH_API, json=body, headers=TENANT_HEADERS, timeout=30)
            result = resp.json()
        except Exception as e:
            status_placeholder.error(f"API error: {e}")
            break
        if not result.get("Success"):
            status_placeholder.error(f"API failed: {result.get('ErrorMessage', 'Unknown')}")
            break
        entities = result.get("Result", {}).get("EntityResults", [])
        total = result.get("Result", {}).get("TotalResults", 0)
        if not entities:
            break
        status_placeholder.info(f"Page {page_number}: {len(entities)} results (total: {total})")
        for entity in entities:
            permit = map_entity(entity)
            if permit:
                all_permits.append(permit)
        if len(entities) < 100:
            break
        page_number += 1
        time.sleep(1)

    if not all_permits:
        status_placeholder.warning("No permits found. The EnerGov API may be temporarily unavailable.")
        return 0, 0

    status_placeholder.info(f"Found {len(all_permits)} permits. Enriching with contact data...")

    # Phase 1: Enrich from EnerGov building permits (contacts + values)
    enriched_count = 0
    for i, permit in enumerate(all_permits[:100]):
        if i % 5 == 0:
            status_placeholder.info(
                f"Enriching permit {i + 1}/{min(len(all_permits), 100)} with contacts & values..."
            )
        all_permits[i] = enrich_permit(session, permit)
        enriched_count += 1
        time.sleep(0.5)

    # Phase 2: Builder web lookup — search DuckDuckGo for company website, scrape phone/email
    companies_searched = set()
    company_results = {}  # cache: company_name -> {website, phone, email}
    builders_with_company = [
        (i, p) for i, p in enumerate(all_permits)
        if p.get("builder_company") and not p.get("builder_website")
    ]
    if builders_with_company:
        status_placeholder.info(
            f"Phase 2: Looking up {len(builders_with_company)} builder websites..."
        )
        web_session = requests.Session()
        for idx, (i, permit) in enumerate(builders_with_company):
            company = (permit.get("builder_company") or "").strip()
            if not company:
                continue
            if idx % 3 == 0:
                status_placeholder.info(
                    f"Builder lookup {idx + 1}/{len(builders_with_company)}: {company[:40]}..."
                )
            # Deduplicate: only search each company once
            if company in company_results:
                result = company_results[company]
            elif company not in companies_searched:
                companies_searched.add(company)
                result = lookup_builder_web(web_session, company)
                company_results[company] = result
                time.sleep(2)  # Rate limit
            else:
                continue

            if result.get("website"):
                all_permits[i]["builder_website"] = result["website"]
            if result.get("phone") and not all_permits[i].get("builder_phone"):
                all_permits[i]["builder_phone"] = result["phone"]
            if result.get("email") and not all_permits[i].get("builder_email"):
                all_permits[i]["builder_email"] = result["email"]

    status_placeholder.info(f"Saving {len(all_permits)} permits to database...")
    conn = get_db()
    new_count = 0
    for p in all_permits:
        score = calculate_opportunity_score(
            p.get("project_value"), p.get("inspection_date"), p.get("municipality")
        )
        try:
            conn.execute("""
                INSERT OR REPLACE INTO permits (
                    permit_number, address, municipality, builder_name, builder_company,
                    builder_phone, builder_email, applicant_name, owner_name,
                    project_value, permit_type, inspection_type, inspection_date,
                    inspection_status, permit_issue_date, source_url, raw_data,
                    opportunity_score, builder_website, scraped_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """, (
                p.get("permit_number"), p.get("address"), p.get("municipality"),
                p.get("builder_name"), p.get("builder_company"),
                p.get("builder_phone"), p.get("builder_email"),
                p.get("applicant_name"), p.get("owner_name"),
                p.get("project_value"), p.get("permit_type"), p.get("inspection_type"),
                p.get("inspection_date"), p.get("inspection_status"),
                p.get("permit_issue_date"), p.get("source_url"), p.get("raw_data"),
                score, p.get("builder_website"),
            ))
            new_count += 1
        except Exception as e:
            log.warning(f"DB insert error: {e}")
    conn.commit()

    web_found = sum(1 for r in company_results.values() if r.get("website"))
    status_placeholder.success(
        f"Scrape complete! {len(all_permits)} permits found, {new_count} saved. "
        f"Builder websites: {web_found}/{len(companies_searched)} found."
    )
    return len(all_permits), new_count


# ─── Custom CSS (Liquid Glass) ──────────────────────────────────────────────
def inject_css():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Fira+Code:wght@400;500;600;700&family=Fira+Sans:wght@300;400;500;600;700&display=swap');

    /* ── Base ── */
    .stApp {
        font-family: 'Fira Sans', system-ui, sans-serif;
        background: linear-gradient(135deg, #0F172A 0%, #1E293B 30%, #0F172A 60%, #1a1a3e 100%) !important;
    }
    .main .block-container { padding-top: 0.5rem; max-width: 100%; }
    #MainMenu {visibility:hidden;} footer {visibility:hidden;} .stDeployButton {display:none;}
    header[data-testid="stHeader"] {background:transparent !important; height:0 !important; min-height:0 !important; padding:0 !important;}

    /* ── Ambient glow behind everything ── */
    .stApp::before {
        content: '';
        position: fixed; top: -50%; left: -50%; width: 200%; height: 200%;
        background: radial-gradient(ellipse at 20% 50%, rgba(43,108,176,0.08) 0%, transparent 50%),
                    radial-gradient(ellipse at 80% 20%, rgba(107,123,141,0.06) 0%, transparent 50%),
                    radial-gradient(ellipse at 50% 80%, rgba(43,108,176,0.04) 0%, transparent 50%);
        pointer-events: none; z-index: 0;
    }

    /* ── Header ── */
    .header-bar {
        background: rgba(15,23,42,0.7);
        backdrop-filter: blur(30px); -webkit-backdrop-filter: blur(30px);
        border: 1px solid rgba(255,255,255,0.1);
        border-top: 3px solid #2B6CB0;
        border-radius: 0 0 16px 16px;
        padding: 16px 24px; margin-bottom: 20px;
        box-shadow: 0 8px 32px rgba(0,0,0,0.4), inset 0 1px 0 rgba(255,255,255,0.05);
        display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 12px;
    }
    .header-left { display:flex; align-items:center; gap:12px; }
    .header-logo { height:48px; width:auto; border-radius:6px; }
    .header-title {
        font-family: 'Fira Code', monospace; font-weight: 700; font-size: 1.5rem;
        background: linear-gradient(135deg, #3B82C4, #2B6CB0, #6B7B8D);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin: 0; line-height: 1.2;
    }
    .header-sub { font-size: .75rem; color: #94A3B8; font-weight: 300; letter-spacing: 0.05em; margin: 0; }

    /* ── Glass Cards ── */
    .glass-card {
        background: rgba(255,255,255,0.04);
        backdrop-filter: blur(16px); -webkit-backdrop-filter: blur(16px);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 16px;
        transition: all 0.3s cubic-bezier(0.4,0,0.2,1);
    }
    .glass-card:hover {
        background: rgba(255,255,255,0.07);
        border-color: rgba(255,255,255,0.15);
        box-shadow: 0 12px 40px rgba(0,0,0,0.3);
    }

    /* ── Stat Cards ── */
    .stat-card {
        background: rgba(255,255,255,0.04);
        backdrop-filter: blur(16px); -webkit-backdrop-filter: blur(16px);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 16px; padding: 20px;
        transition: all 0.3s ease;
    }
    .stat-card:hover {
        background: rgba(255,255,255,0.07);
        border-color: rgba(255,255,255,0.15);
        transform: translateY(-2px);
        box-shadow: 0 12px 40px rgba(0,0,0,0.3);
    }
    .stat-label { font-size:.65rem; font-weight:600; text-transform:uppercase; letter-spacing:.08em; color:#94A3B8; margin-bottom:8px; }
    .stat-value {
        font-family:'Fira Code',monospace; font-size:1.75rem; font-weight:700;
        background:linear-gradient(135deg,#F8FAFC,#E2E8F0);
        -webkit-background-clip:text; -webkit-text-fill-color:transparent;
    }
    .stat-value-blue {
        font-family:'Fira Code',monospace; font-size:1.75rem; font-weight:700;
        background:linear-gradient(135deg,#2B6CB0,#3B82C4);
        -webkit-background-clip:text; -webkit-text-fill-color:transparent;
    }

    /* ── Badges ── */
    .badge { display:inline-flex;align-items:center;padding:3px 10px;border-radius:9999px;font-size:.7rem;font-weight:600;font-family:'Fira Code',monospace; }
    .badge-green { background:rgba(34,197,94,0.15);color:#4ADE80;border:1px solid rgba(34,197,94,0.2); }
    .badge-yellow { background:rgba(234,179,8,0.15);color:#FDE047;border:1px solid rgba(234,179,8,0.2); }
    .badge-red { background:rgba(239,68,68,0.15);color:#FCA5A5;border:1px solid rgba(239,68,68,0.2); }
    .badge-gray { background:rgba(148,163,184,0.1);color:#94A3B8;border:1px solid rgba(148,163,184,0.15); }
    .badge-blue { background:rgba(59,130,246,0.15);color:#93C5FD;border:1px solid rgba(59,130,246,0.2); }

    /* ── Filter chips ── */
    .filter-chip {
        display:inline-flex; align-items:center; gap:6px; padding:4px 12px;
        border-radius:9999px; font-size:.72rem; font-weight:500;
        background:rgba(43,108,176,0.12); color:#93C5FD;
        border:1px solid rgba(43,108,176,0.25); margin-right:6px; margin-bottom:4px;
    }

    /* ── Login Card ── */
    .login-card {
        background:rgba(255,255,255,0.06); backdrop-filter:blur(20px); -webkit-backdrop-filter:blur(20px);
        border:1px solid rgba(255,255,255,0.1); border-radius:20px; padding:40px;
        max-width:380px; margin:10vh auto 20px auto;
        box-shadow:0 8px 32px rgba(0,0,0,0.4), inset 0 1px 0 rgba(255,255,255,0.05);
        text-align:center;
    }
    .login-logo { height:64px; margin-bottom:16px; }
    .login-title {
        font-family:'Fira Code',monospace; font-size:1.3rem; font-weight:700;
        background:linear-gradient(135deg,#3B82C4,#2B6CB0,#6B7B8D);
        -webkit-background-clip:text; -webkit-text-fill-color:transparent; margin-bottom:4px;
    }
    .login-sub { font-size:.75rem; color:#94A3B8; margin-bottom:28px; letter-spacing:.04em; }

    /* ── FOIA Cards ── */
    .foia-card {
        background:rgba(255,255,255,0.04); border:1px solid rgba(255,255,255,0.08);
        border-radius:12px; padding:12px 16px;
        display:flex; align-items:center; justify-content:space-between;
        transition: all 0.2s ease;
    }
    .foia-card:hover { background:rgba(255,255,255,0.07); border-color:rgba(255,255,255,0.15); }
    .foia-name { font-size:.85rem; font-weight:500; color:#F8FAFC; }
    .foia-detail { font-size:.65rem; color:#94A3B8; }
    .foia-link {
        background:rgba(43,108,176,0.1); color:#93C5FD;
        border:1px solid rgba(43,108,176,0.2); padding:6px 14px;
        border-radius:8px; font-size:.75rem; font-weight:600; text-decoration:none;
        transition: all 0.2s ease;
    }
    .foia-link:hover { background:rgba(43,108,176,0.2); border-color:rgba(43,108,176,0.4); }

    /* ── Permit Table ── */
    .permit-table { width:100%; border-collapse:separate; border-spacing:0; font-size:.78rem; }
    .permit-table thead th {
        padding:12px 12px; text-align:left; font-size:.6rem; font-weight:600;
        text-transform:uppercase; letter-spacing:.08em; color:#94A3B8;
        background:rgba(15,23,42,0.5); border-bottom:1px solid rgba(255,255,255,0.06);
        position:sticky; top:0; z-index:10;
    }
    .permit-table tbody tr { border-bottom:1px solid rgba(255,255,255,0.03); }
    .permit-table tbody tr:hover { background:rgba(43,108,176,0.06); }
    .permit-table tbody td { padding:10px 12px; vertical-align:middle; }
    .permit-table .hv { border-left:3px solid #2B6CB0; }
    .empty-cell { color:rgba(148,163,184,0.4); }

    /* ── Detail Expand Row ── */
    details.permit-detail { margin:0; }
    details.permit-detail summary {
        list-style:none; cursor:pointer; display:contents;
    }
    details.permit-detail summary::-webkit-details-marker { display:none; }
    .detail-panel {
        background: rgba(43,108,176,0.04);
        border-top: 1px solid rgba(43,108,176,0.1);
        padding: 16px 20px;
        display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 12px;
    }
    .detail-field {
        padding: 8px 12px;
        background: rgba(255,255,255,0.03);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 8px;
    }
    .detail-label { font-size:.6rem; font-weight:600; text-transform:uppercase; letter-spacing:.06em; color:#94A3B8; margin-bottom:4px; }
    .detail-value { font-size:.8rem; color:#F8FAFC; word-break:break-all; }
    .detail-value a { color:#60A5FA; text-decoration:none; }
    .detail-value a:hover { color:#93C5FD; text-decoration:underline; }

    /* ── Streamlit overrides ── */
    .stExpander { border:1px solid rgba(255,255,255,0.08) !important; border-radius:16px !important; background:rgba(255,255,255,0.04) !important; }
    .stExpander > div:first-child { background:transparent !important; }
    .stTextInput > div > div > input, .stSelectbox > div > div, .stNumberInput > div > div > input {
        background: rgba(255,255,255,0.06) !important; border: 1px solid rgba(255,255,255,0.1) !important;
        border-radius: 10px !important; color: #E2E8F0 !important;
    }
    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #3B82C4, #2B6CB0) !important;
        border: none !important; border-radius: 12px !important;
        box-shadow: 0 4px 15px rgba(43,108,176,0.3) !important;
        font-weight: 600 !important;
    }
    .stButton > button[kind="primary"]:hover {
        box-shadow: 0 6px 20px rgba(43,108,176,0.5) !important;
        transform: translateY(-1px);
    }
    .stButton > button:not([kind="primary"]) {
        background: rgba(255,255,255,0.06) !important;
        border: 1px solid rgba(255,255,255,0.1) !important;
        border-radius: 10px !important; color: #E2E8F0 !important;
    }
    .stButton > button:not([kind="primary"]):hover {
        background: rgba(255,255,255,0.12) !important;
        border-color: rgba(255,255,255,0.2) !important;
    }
    .stDownloadButton > button {
        background: rgba(255,255,255,0.06) !important;
        border: 1px solid rgba(255,255,255,0.1) !important;
        border-radius: 10px !important; color: #E2E8F0 !important;
    }
    </style>
    """, unsafe_allow_html=True)


# ─── Database ───────────────────────────────────────────────────────────────
@st.cache_resource
def get_db():
    db_dir = os.path.dirname(DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS permits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            permit_number TEXT UNIQUE, address TEXT NOT NULL, municipality TEXT NOT NULL,
            builder_name TEXT, builder_company TEXT, builder_phone TEXT, builder_email TEXT,
            applicant_name TEXT, applicant_phone TEXT, applicant_email TEXT, owner_name TEXT,
            project_value REAL, permit_type TEXT, inspection_type TEXT, inspection_date TEXT,
            inspection_status TEXT, permit_issue_date TEXT, source_url TEXT,
            scraped_at TEXT DEFAULT (datetime('now')), raw_data TEXT,
            is_drywall_opportunity INTEGER DEFAULT 0, opportunity_confidence TEXT,
            opportunity_signals TEXT, estimated_drywall_date TEXT,
            opportunity_score INTEGER, builder_website TEXT, personal_phone TEXT, personal_email TEXT
        )
    """)
    conn.commit()
    return conn


def query_permits(conn, filters=None, sort_by="opportunity_score", sort_order="DESC", page=1, per_page=50):
    conditions = []
    values = []
    if filters:
        if filters.get("search"):
            s = f"%{filters['search']}%"
            conditions.append(
                "(address LIKE ? OR municipality LIKE ? OR builder_name LIKE ? "
                "OR builder_company LIKE ? OR owner_name LIKE ? OR permit_number LIKE ? "
                "OR builder_phone LIKE ? OR builder_email LIKE ?)"
            )
            values.extend([s] * 8)
        if filters.get("municipality"):
            conditions.append("municipality = ?")
            values.append(filters["municipality"])
        if filters.get("date_from"):
            conditions.append("inspection_date >= ?")
            values.append(filters["date_from"])
        if filters.get("date_to"):
            conditions.append("inspection_date <= ?")
            values.append(filters["date_to"])
        if filters.get("min_value"):
            conditions.append("project_value >= ?")
            values.append(float(filters["min_value"]))
        if filters.get("max_value"):
            conditions.append("project_value <= ?")
            values.append(float(filters["max_value"]))
        if filters.get("max_drive_time"):
            in_range = [n for n, m in DRIVE_TIMES.items() if m <= int(filters["max_drive_time"])]
            if in_range:
                placeholders = ",".join(["?"] * len(in_range))
                conditions.append(f"municipality IN ({placeholders})")
                values.extend(in_range)
            else:
                conditions.append("1=0")
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    allowed = ["municipality", "address", "builder_name", "project_value", "inspection_date", "opportunity_score", "permit_number"]
    if sort_by not in allowed:
        sort_by = "opportunity_score"
    total_row = conn.execute(f"SELECT COUNT(*) FROM permits {where}", values).fetchone()
    total = total_row[0] if total_row else 0
    rows = conn.execute(
        f"SELECT * FROM permits {where} ORDER BY {sort_by} {sort_order} LIMIT ? OFFSET ?",
        values + [per_page, (page - 1) * per_page]
    ).fetchall()
    return {
        "data": [dict(r) for r in rows],
        "total": total, "page": page, "per_page": per_page,
        "total_pages": max(1, math.ceil(total / per_page)),
    }


def get_stats(conn):
    row = conn.execute(
        "SELECT COUNT(*) as t, COALESCE(AVG(project_value),0) as a, "
        "MIN(inspection_date) as mi, MAX(inspection_date) as ma FROM permits"
    ).fetchone()
    hv = conn.execute("SELECT COUNT(*) FROM permits WHERE project_value >= 300000").fetchone()
    return {
        "total": row[0], "avg": row[1],
        "earliest": row[2], "latest": row[3],
        "hv": hv[0] if hv else 0,
    }


def get_municipalities(conn):
    return [
        r[0] for r in conn.execute(
            "SELECT DISTINCT municipality FROM permits "
            "WHERE municipality IS NOT NULL AND municipality != '' ORDER BY municipality"
        ).fetchall()
    ]


# ─── Helpers ────────────────────────────────────────────────────────────────
def score_badge(score):
    if score is None:
        return '<span class="badge badge-gray">--</span>'
    s = int(score)
    if s >= 70:
        cls = "badge-green"
    elif s >= 40:
        cls = "badge-yellow"
    elif s >= 1:
        cls = "badge-red"
    else:
        cls = "badge-gray"
    return f'<span class="badge {cls}">{s}</span>'


def status_badge(status):
    if not status:
        return '<span class="badge badge-gray">&mdash;</span>'
    low = status.lower()
    if "pass" in low or "approved" in low:
        cls = "badge-green"
    elif "pending" in low or "scheduled" in low:
        cls = "badge-yellow"
    else:
        cls = "badge-gray"
    return f'<span class="badge {cls}">{esc(status)}</span>'


def fmt_money(v):
    if v is None or v == 0:
        return "&mdash;"
    return f"${int(v):,}"


def fmt_date(d):
    if not d:
        return "&mdash;"
    try:
        return datetime.strptime(d[:10], "%Y-%m-%d").strftime("%b %d, %Y")
    except Exception:
        return d


def esc(s):
    if s is None:
        return ""
    return html_escape(str(s), quote=True)


def build_detail_panel(p):
    """Build the expandable detail HTML for a permit row."""
    fields = []

    # Builder info
    bn = p.get("builder_name") or ""
    bc = p.get("builder_company") or ""
    if bn or bc:
        val = esc(bn)
        if bc:
            val += f' <span style="color:#94A3B8">@ {esc(bc)}</span>' if bn else esc(bc)
        fields.append(("Builder", val))

    # Phone / Email
    bphone = p.get("builder_phone") or ""
    bemail = p.get("builder_email") or ""
    if bphone:
        fields.append(("Biz Phone", f'<a href="tel:{esc(bphone)}">{esc(bphone)}</a>'))
    if bemail:
        fields.append(("Biz Email", f'<a href="mailto:{esc(bemail)}">{esc(bemail)}</a>'))
    pphone = p.get("personal_phone") or ""
    pemail = p.get("personal_email") or ""
    if pphone:
        fields.append(("Personal Phone", f'<a href="tel:{esc(pphone)}">{esc(pphone)}</a>'))
    if pemail:
        fields.append(("Personal Email", f'<a href="mailto:{esc(pemail)}">{esc(pemail)}</a>'))

    # Owner
    owner = p.get("owner_name") or ""
    if owner:
        fields.append(("Owner", esc(owner)))

    # Applicant
    applicant = p.get("applicant_name") or ""
    if applicant:
        fields.append(("Applicant", esc(applicant)))

    # Value
    val = p.get("project_value")
    if val and val > 0:
        fields.append(("Project Value", f'<span style="color:#2B6CB0;font-family:Fira Code,monospace">${int(val):,}</span>'))

    # Permit type & inspection
    ptype = p.get("permit_type") or ""
    itype = p.get("inspection_type") or ""
    if ptype:
        fields.append(("Permit Type", esc(ptype)))
    if itype and itype != ptype:
        fields.append(("Inspection Type", esc(itype)))

    # Dates
    idate = p.get("inspection_date") or ""
    pdate = p.get("permit_issue_date") or ""
    if idate:
        fields.append(("Inspection Date", fmt_date(idate)))
    if pdate:
        fields.append(("Permit Issue Date", fmt_date(pdate)))

    # Status & Score
    istatus = p.get("inspection_status") or ""
    if istatus:
        fields.append(("Status", status_badge(istatus)))
    score = p.get("opportunity_score")
    if score is not None:
        fields.append(("Opportunity Score", score_badge(score)))

    # Linked permit from raw_data
    try:
        raw = json.loads(p.get("raw_data") or "{}")
        linked = raw.get("_linkedPermit")
        linked_url = raw.get("_linkedPermitUrl")
        if linked:
            if linked_url:
                fields.append(("Linked Permit", f'<a href="{esc(linked_url)}" target="_blank">{esc(linked)}</a>'))
            else:
                fields.append(("Linked Permit", esc(linked)))

        # All contacts from enrichment
        all_contacts = raw.get("_allContacts", [])
        if all_contacts:
            contacts_html = ""
            for c in all_contacts:
                cname = c.get("name") or "Unknown"
                ctype = c.get("type") or ""
                ccompany = c.get("company") or ""
                cphone = c.get("phone") or ""
                cemail = c.get("email") or ""
                line = f'<div style="margin-bottom:4px"><span style="font-weight:500;color:#F8FAFC">{esc(cname)}</span>'
                if ctype:
                    line += f' <span class="badge badge-blue" style="font-size:.55rem;padding:1px 6px">{esc(ctype)}</span>'
                if ccompany:
                    line += f' <span style="color:#94A3B8;font-size:.7rem">@ {esc(ccompany)}</span>'
                if cphone:
                    line += f' &bull; <a href="tel:{esc(cphone)}" style="font-size:.7rem">{esc(cphone)}</a>'
                if cemail:
                    line += f' &bull; <a href="mailto:{esc(cemail)}" style="font-size:.7rem">{esc(cemail)}</a>'
                line += "</div>"
                contacts_html += line
            if contacts_html:
                fields.append(("All Contacts", contacts_html))

        # Data source
        ds = raw.get("_dataSource")
        if ds:
            src_label = "Building Permit" if ds == "building_permit" else "Inspection Contacts"
            src_cls = "badge-green" if ds == "building_permit" else "badge-blue"
            fields.append(("Data Source", f'<span class="badge {src_cls}">{src_label}</span>'))
    except Exception:
        pass

    # Website
    bw = p.get("builder_website") or ""
    if bw:
        try:
            from urllib.parse import urlparse
            host = urlparse(bw).hostname or bw
            host = host.replace("www.", "")
        except Exception:
            host = bw
        fields.append(("Website", f'<a href="{esc(bw)}" target="_blank">{esc(host)}</a>'))

    # Source URL
    src_url = p.get("source_url") or ""
    if src_url:
        fields.append(("EnerGov Link", f'<a href="{esc(src_url)}" target="_blank">View on EnerGov</a>'))

    # Permit #
    pnum = p.get("permit_number") or ""
    if pnum:
        fields.append(("Permit #", f'<span style="font-family:Fira Code,monospace">{esc(pnum)}</span>'))

    # Municipality
    muni = p.get("municipality") or ""
    if muni:
        drive = DRIVE_TIMES.get(muni)
        drive_str = f" ({drive} min)" if drive is not None else ""
        fields.append(("Municipality", f'{esc(muni)}{drive_str}'))

    # Build HTML
    html = '<div class="detail-panel">'
    for label, value in fields:
        html += f'<div class="detail-field"><div class="detail-label">{label}</div><div class="detail-value">{value}</div></div>'
    if not fields:
        html += '<div style="color:#94A3B8;font-size:.8rem;grid-column:1/-1">No additional details available.</div>'
    html += '</div>'
    return html


# ─── Login ──────────────────────────────────────────────────────────────────
def check_auth():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if st.session_state.authenticated:
        return True
    inject_css()
    logo_img = f'<img src="data:image/png;base64,{LOGO_B64}" class="login-logo">' if LOGO_B64 else ""
    st.markdown(
        f'<div class="login-card">{logo_img}'
        f'<div class="login-title">Pierpont Money Printer</div>'
        f'<div class="login-sub">SC Lowcountry Construction Lead Intelligence</div>'
        f'</div>',
        unsafe_allow_html=True,
    )
    col1, col2, col3 = st.columns([1.2, 1, 1.2])
    with col2:
        password = st.text_input("Password", type="password", placeholder="Enter password", label_visibility="collapsed")
        if st.button("Sign In", use_container_width=True, type="primary"):
            if password == APP_PASSWORD:
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.error("Incorrect password")
    return False


# ─── Main Dashboard ────────────────────────────────────────────────────────
def main():
    if not check_auth():
        return
    inject_css()
    conn = get_db()

    # ── Header ──
    logo_html = f'<img src="data:image/png;base64,{LOGO_B64}" class="header-logo">' if LOGO_B64 else ""
    st.markdown(
        f'<div class="header-bar"><div class="header-left">{logo_html}<div>'
        f'<div class="header-title">Pierpont Money Printer</div>'
        f'<div class="header-sub">SC Lowcountry Construction Lead Intelligence</div>'
        f'</div></div></div>',
        unsafe_allow_html=True,
    )

    # ── Action Buttons ──
    btn_cols = st.columns([1, 1, 1, 1, 1, 1])
    with btn_cols[0]:
        run_scraper_btn = st.button("Run Scraper", type="primary", use_container_width=True)
    with btn_cols[1]:
        export_csv = st.button("Export CSV", use_container_width=True)
    with btn_cols[2]:
        lookup_btn = st.button("Lookup Builders", use_container_width=True)
    with btn_cols[4]:
        clear_btn = st.button("Clear All Data", use_container_width=True)
    with btn_cols[5]:
        if st.button("Logout", use_container_width=True):
            st.session_state.authenticated = False
            st.rerun()

    # ── Clear All Data (confirmation) ──
    if clear_btn:
        if "clear_confirm" not in st.session_state:
            st.session_state.clear_confirm = 1
        else:
            st.session_state.clear_confirm += 1

    if st.session_state.get("clear_confirm", 0) >= 1 and st.session_state.get("clear_confirm", 0) < 3:
        remaining = 3 - st.session_state.clear_confirm
        st.warning(f"Click 'Clear All Data' {remaining} more time(s) to confirm deletion of ALL permit data.")
    elif st.session_state.get("clear_confirm", 0) >= 3:
        conn.execute("DELETE FROM permits")
        conn.commit()
        st.session_state.clear_confirm = 0
        st.success("All data cleared.")
        st.rerun()

    # ── Scraper Execution ──
    if run_scraper_btn:
        st.session_state.clear_confirm = 0
        status_box = st.empty()
        with st.spinner("Scraping EnerGov API..."):
            found, saved = run_scraper(status_box)
        if found > 0:
            st.rerun()

    # ── Builder Lookup (bulk — for permits missing website/phone) ──
    if lookup_btn:
        st.session_state.clear_confirm = 0
        status_box = st.empty()
        with st.spinner("Looking up builder websites..."):
            web_session = requests.Session()
            rows = conn.execute(
                "SELECT id, builder_company, builder_phone, builder_email, builder_website "
                "FROM permits WHERE builder_company IS NOT NULL AND builder_company != '' "
                "AND (builder_website IS NULL OR builder_website = '')"
            ).fetchall()
            if not rows:
                status_box.info("All builders already have website data.")
            else:
                companies_searched = set()
                company_results = {}
                found_count = 0
                for idx, row in enumerate(rows):
                    company = row[1].strip()
                    if not company or company in companies_searched:
                        continue
                    companies_searched.add(company)
                    if idx % 3 == 0:
                        status_box.info(f"Looking up {idx + 1}/{len(rows)}: {company[:40]}...")
                    result = lookup_builder_web(web_session, company)
                    company_results[company] = result
                    if result.get("website"):
                        found_count += 1
                        updates = []
                        vals = []
                        updates.append("builder_website = ?")
                        vals.append(result["website"])
                        if result.get("phone"):
                            updates.append("builder_phone = COALESCE(NULLIF(builder_phone, ''), ?)")
                            vals.append(result["phone"])
                        if result.get("email"):
                            updates.append("builder_email = COALESCE(NULLIF(builder_email, ''), ?)")
                            vals.append(result["email"])
                        conn.execute(
                            f"UPDATE permits SET {', '.join(updates)} "
                            f"WHERE builder_company = ? AND (builder_website IS NULL OR builder_website = '')",
                            vals + [company],
                        )
                    time.sleep(2)
                conn.commit()
                status_box.success(f"Builder lookup complete! {found_count}/{len(companies_searched)} companies found.")
                st.rerun()

    # ── FOIA Section (TOP — first thing after header/buttons) ──
    with st.expander("FOIA Requests — Municipalities Without Public Portals", expanded=False):
        st.markdown(
            '<p style="font-size:.8rem;color:#94A3B8;margin-bottom:12px">'
            "These municipalities require a SC FOIA request to obtain permit data.</p>",
            unsafe_allow_html=True,
        )
        foia_html = '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:8px">'
        for m in FOIA_MUNICIPALITIES:
            if m["type"] == "email":
                subject = quote("FOIA REQUEST — Strapping Inspections")
                body = quote(FOIA_BODY)
                mailto = f"mailto:{m['email']}?subject={subject}&body={body}"
                foia_html += (
                    f'<div class="foia-card"><div>'
                    f'<div class="foia-name">{m["name"]}</div>'
                    f'<div class="foia-detail">{m["email"]}</div>'
                    f'</div><a href="{mailto}" class="foia-link" target="_blank">Send Request</a></div>'
                )
            else:
                foia_html += (
                    f'<div class="foia-card"><div>'
                    f'<div class="foia-name">{m["name"]}</div>'
                    f'<div class="foia-detail">{m["portal_name"]}</div>'
                    f'</div><a href="{m["url"]}" class="foia-link" target="_blank">Open Portal</a></div>'
                )
        foia_html += "</div>"
        st.markdown(foia_html, unsafe_allow_html=True)
        st.code(FOIA_BODY, language=None)
        st.caption("Copy the text above and paste it into the FOIA portal request form.")

    st.markdown("")

    # ── Stats ──
    stats = get_stats(conn)
    sc = st.columns(4)
    with sc[0]:
        st.markdown(
            f'<div class="stat-card"><div class="stat-label">Total Permits</div>'
            f'<div class="stat-value">{stats["total"]:,}</div></div>',
            unsafe_allow_html=True,
        )
    with sc[1]:
        st.markdown(
            f'<div class="stat-card"><div class="stat-label">Avg Value</div>'
            f'<div class="stat-value">{fmt_money(stats["avg"])}</div></div>',
            unsafe_allow_html=True,
        )
    with sc[2]:
        st.markdown(
            f'<div class="stat-card"><div class="stat-label">$300K+ Projects</div>'
            f'<div class="stat-value-blue">{stats["hv"]:,}</div></div>',
            unsafe_allow_html=True,
        )
    with sc[3]:
        dr = f'{fmt_date(stats["earliest"])} &mdash; {fmt_date(stats["latest"])}' if stats["earliest"] else "&mdash;"
        st.markdown(
            f'<div class="stat-card"><div class="stat-label">Date Range</div>'
            f'<div style="font-family:Fira Code,monospace;font-size:.85rem;color:#E2E8F0;margin-top:8px">{dr}</div></div>',
            unsafe_allow_html=True,
        )

    st.markdown("")

    # ── Filters (stackable — all combine with AND) ──
    municipalities = get_municipalities(conn)
    with st.expander("Filters", expanded=False):
        fc1, fc2 = st.columns([3, 1])
        with fc1:
            search = st.text_input(
                "Search", placeholder="Address, builder, phone, email, permit #...",
                label_visibility="collapsed", key="search",
            )
        with fc2:
            dist_opts = {"All distances": "", "15 min": "15", "30 min": "30", "45 min": "45", "60 min": "60", "90 min": "90"}
            max_drive = st.selectbox("Distance", list(dist_opts.keys()), label_visibility="collapsed")
        fc3, fc4, fc5, fc6, fc7 = st.columns(5)
        with fc3:
            muni = st.selectbox("Municipality", ["All"] + municipalities, label_visibility="collapsed")
        with fc4:
            date_from = st.date_input("From", value=None, label_visibility="collapsed")
        with fc5:
            date_to = st.date_input("To", value=None, label_visibility="collapsed")
        with fc6:
            min_val = st.number_input("Min $", min_value=0, value=0, step=50000, label_visibility="collapsed", format="%d")
        with fc7:
            max_val = st.number_input("Max $", min_value=0, value=0, step=50000, label_visibility="collapsed", format="%d")

    # Build filters dict — all filters stack (AND logic)
    filters = {}
    if search:
        filters["search"] = search
    if dist_opts[max_drive]:
        filters["max_drive_time"] = dist_opts[max_drive]
    if muni != "All":
        filters["municipality"] = muni
    if date_from:
        filters["date_from"] = date_from.strftime("%Y-%m-%d")
    if date_to:
        filters["date_to"] = date_to.strftime("%Y-%m-%d")
    if min_val > 0:
        filters["min_value"] = min_val
    if max_val > 0:
        filters["max_value"] = max_val

    # Show active filter chips
    if filters:
        chips_html = '<div style="margin-bottom:8px">'
        if filters.get("search"):
            chips_html += f'<span class="filter-chip">Search: &ldquo;{esc(filters["search"])}&rdquo;</span>'
        if filters.get("max_drive_time"):
            chips_html += f'<span class="filter-chip">Within {esc(filters["max_drive_time"])} min</span>'
        if filters.get("municipality"):
            chips_html += f'<span class="filter-chip">{esc(filters["municipality"])}</span>'
        if filters.get("date_from"):
            chips_html += f'<span class="filter-chip">From: {esc(filters["date_from"])}</span>'
        if filters.get("date_to"):
            chips_html += f'<span class="filter-chip">To: {esc(filters["date_to"])}</span>'
        if filters.get("min_value"):
            chips_html += f'<span class="filter-chip">Min: ${int(filters["min_value"]):,}</span>'
        if filters.get("max_value"):
            chips_html += f'<span class="filter-chip">Max: ${int(filters["max_value"]):,}</span>'
        chips_html += '</div>'
        st.markdown(chips_html, unsafe_allow_html=True)

    # ── Sort & Pagination ──
    scol1, scol2, scol3 = st.columns([1, 1, 4])
    with scol1:
        sort_map = {
            "Score ↓": ("opportunity_score", "DESC"),
            "Score ↑": ("opportunity_score", "ASC"),
            "Date ↓": ("inspection_date", "DESC"),
            "Date ↑": ("inspection_date", "ASC"),
            "Value ↓": ("project_value", "DESC"),
            "Value ↑": ("project_value", "ASC"),
        }
        sort_choice = st.selectbox("Sort", list(sort_map.keys()), index=0, label_visibility="collapsed")
        sort_by, sort_order = sort_map[sort_choice]
    with scol2:
        per_page = st.selectbox("Per page", [25, 50, 100], index=1, label_visibility="collapsed")

    if "page" not in st.session_state:
        st.session_state.page = 1
    result = query_permits(
        conn, filters=filters, sort_by=sort_by, sort_order=sort_order,
        page=st.session_state.page, per_page=per_page,
    )

    # ── CSV Export ──
    if export_csv and result["total"] > 0:
        df = pd.read_sql_query("SELECT * FROM permits ORDER BY opportunity_score DESC", conn)
        st.download_button(
            "Download CSV", df.to_csv(index=False),
            f"pierpont-{datetime.now().strftime('%Y-%m-%d')}.csv", "text/csv",
        )

    # ── Permits Table with Expandable Detail Rows ──
    if not result["data"]:
        st.markdown(
            '<div style="text-align:center;padding:80px 20px">'
            '<div style="font-size:2.5rem;margin-bottom:12px;opacity:0.2">&#x1F50D;</div>'
            '<div style="font-size:1.1rem;font-weight:500;color:#F8FAFC;margin-bottom:8px">No permits loaded</div>'
            '<div style="color:#94A3B8">Click <strong style="color:#2B6CB0">Run Scraper</strong> to fetch permits from Charleston EnerGov.</div>'
            '</div>',
            unsafe_allow_html=True,
        )
    else:
        start = (result["page"] - 1) * result["per_page"] + 1
        end = min(result["page"] * result["per_page"], result["total"])
        st.markdown(
            f"<div style='font-size:.75rem;color:#94A3B8;font-family:Fira Code,monospace;margin-bottom:4px'>"
            f"{start}&ndash;{end} of {result['total']:,} permits &nbsp;&bull;&nbsp; Click any row to expand details</div>",
            unsafe_allow_html=True,
        )

        # Build table rows with expandable detail sections
        rows_html = ""
        for p in result["data"]:
            is_hv = p.get("project_value") and p["project_value"] >= 300000
            hv_cls = ' class="hv"' if is_hv else ""
            val_style = "color:#2B6CB0;" if is_hv else ""

            bn = p.get("builder_name") or ""
            bc = p.get("builder_company") or ""
            if bn and bc:
                builder = (
                    f'<div style="font-weight:500;color:#F8FAFC">{esc(bn)}</div>'
                    f'<div style="font-size:.65rem;color:#94A3B8">{esc(bc)}</div>'
                )
            elif bn or bc:
                builder = f'<span style="font-weight:500;color:#F8FAFC">{esc(bn or bc)}</span>'
            else:
                builder = '<span class="empty-cell">&mdash;</span>'

            def _phone_link(num):
                if not num:
                    return '<span class="empty-cell">&mdash;</span>'
                return (
                    f'<a href="tel:{esc(num)}" style="color:#60A5FA;text-decoration:none;'
                    f'font-family:Fira Code,monospace;font-size:.7rem;white-space:nowrap">{esc(num)}</a>'
                )

            def _email_link(addr):
                if not addr:
                    return '<span class="empty-cell">&mdash;</span>'
                return f'<a href="mailto:{esc(addr)}" style="color:#60A5FA;text-decoration:none;font-size:.7rem">{esc(addr)}</a>'

            biz_ph = _phone_link(p.get("builder_phone"))
            biz_em = _email_link(p.get("builder_email"))
            per_ph = _phone_link(p.get("personal_phone"))
            per_em = _email_link(p.get("personal_email"))

            addr_esc = esc(p.get("address") or "") or "&mdash;"
            muni_esc = esc(p.get("municipality") or "") or "&mdash;"
            owner_esc = esc(p.get("owner_name") or "") or "&mdash;"
            value_html = fmt_money(p.get("project_value"))
            date_html = fmt_date(p.get("inspection_date"))
            score_html = score_badge(p.get("opportunity_score"))
            status_html = status_badge(p.get("inspection_status"))
            pnum_esc = esc(p.get("permit_number") or "") or "&mdash;"

            # Build detail panel
            detail_html = build_detail_panel(p)

            rows_html += (
                f"<tr{hv_cls} style='cursor:pointer' onclick=\""
                f"var d=this.nextElementSibling;d.style.display=d.style.display==='none'?'table-row':'none'\">"
                f"<td style='color:#F8FAFC;font-weight:500;max-width:180px'>"
                f"<div style='overflow:hidden;text-overflow:ellipsis;white-space:nowrap' title='{addr_esc}'>{addr_esc}</div></td>"
                f"<td style='color:#94A3B8;font-size:.7rem'>{muni_esc}</td>"
                f"<td>{builder}</td>"
                f"<td>{biz_ph}</td>"
                f"<td>{biz_em}</td>"
                f"<td>{per_ph}</td>"
                f"<td>{per_em}</td>"
                f"<td style='color:#E2E8F0'>{owner_esc}</td>"
                f"<td style='text-align:right;font-family:Fira Code,monospace;font-size:.75rem;{val_style}'>{value_html}</td>"
                f"<td style='font-family:Fira Code,monospace;font-size:.7rem;color:#E2E8F0'>{date_html}</td>"
                f"<td style='text-align:center'>{status_html}</td>"
                f"<td style='text-align:center'>{score_html}</td>"
                f"<td style='font-family:Fira Code,monospace;font-size:.65rem;color:#94A3B8'>{pnum_esc}</td>"
                f"</tr>"
                f"<tr style='display:none'><td colspan='13' style='padding:0;border:none'>{detail_html}</td></tr>"
            )

        watermark = ""
        if LOGO_B64:
            watermark = (
                f"background:url('data:image/png;base64,{LOGO_B64}') no-repeat center center;"
                f"background-size:contain;opacity:0.06;"
            )

        table_html = (
            f'<div style="overflow-x:auto;border-radius:16px;border:1px solid rgba(255,255,255,0.08);'
            f'background:rgba(255,255,255,0.02);position:relative">'
            f'<div style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);'
            f'width:400px;height:400px;{watermark}pointer-events:none;z-index:0"></div>'
            f'<table class="permit-table" style="position:relative;z-index:1"><thead><tr>'
            f'<th>Address</th><th>Municipality</th><th>Builder</th>'
            f'<th>Biz Phone</th><th>Biz Email</th>'
            f'<th>Personal Phone</th><th>Personal Email</th>'
            f'<th>Owner</th>'
            f'<th style="text-align:right">Value</th><th>Date</th>'
            f'<th style="text-align:center">Status</th><th style="text-align:center">Score</th>'
            f'<th>Permit #</th>'
            f'</tr></thead><tbody>{rows_html}</tbody></table></div>'
        )
        st.markdown(table_html, unsafe_allow_html=True)

        # Pagination
        if result["total_pages"] > 1:
            pc = st.columns([2, 1, 1, 1, 2])
            with pc[1]:
                if st.button("Prev", disabled=st.session_state.page <= 1):
                    st.session_state.page -= 1
                    st.rerun()
            with pc[2]:
                st.markdown(
                    f"<div style='text-align:center;padding:8px;color:#94A3B8;"
                    f"font-family:Fira Code,monospace;font-size:.8rem'>"
                    f"Page {result['page']}/{result['total_pages']}</div>",
                    unsafe_allow_html=True,
                )
            with pc[3]:
                if st.button("Next", disabled=st.session_state.page >= result["total_pages"]):
                    st.session_state.page += 1
                    st.rerun()

    # ── Footer ──
    st.markdown(
        '<div style="text-align:center;padding:20px;font-size:.7rem;color:rgba(148,163,184,0.4)">'
        'Pierpont Money Printer &mdash; SC Lowcountry Construction Lead Intelligence</div>',
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
