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
import threading
from datetime import datetime, timedelta, time as dtime
from urllib.parse import quote, urlparse
from html import escape as html_escape

import pytz

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

# Google Places API key — set as Streamlit secret or env var for best results
GOOGLE_PLACES_KEY = os.environ.get("GOOGLE_PLACES_API_KEY", "")

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


# ─── Drywall Profit Opportunity Calculator ────────────────────────────────
def calculate_drywall_profit(project_value, margin_pct=15.0):
    """
    Dynamic logarithmic scaling for drywall revenue estimation.
    DrywallPercent = 0.12 - ((log10(PermitValue) - 4.7) * 0.022)
    Clamped between 3% and 12%.
    """
    if not project_value or project_value <= 0:
        return {"drywall_pct": 0, "drywall_revenue": 0, "profit_opportunity": 0}
    # Clamp permit value to valid range for calculation
    clamped = max(50000, min(15000000, project_value))
    drywall_pct = 0.12 - ((math.log10(clamped) - 4.7) * 0.022)
    drywall_pct = max(0.03, min(0.12, drywall_pct))
    drywall_revenue = project_value * drywall_pct
    margin = max(0, min(100, margin_pct))
    profit_opportunity = drywall_revenue * (margin / 100.0)
    return {
        "drywall_pct": drywall_pct,
        "drywall_revenue": drywall_revenue,
        "profit_opportunity": profit_opportunity,
    }


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


def _is_building_permit(entity):
    """
    Filter to find the actual HOME BUILDING permit, not plumbing/electric/mechanical.
    Matches the original Node.js logic from charleston.js lines 348-358.
    """
    case_type = (entity.get("CaseType") or "").lower()
    case_num = (entity.get("CaseNumber") or "").upper()
    # Must be an actual permit (CaseType contains "permit"), not an inspection
    if "permit" not in case_type:
        return False
    # Must be a BUILDING permit — not plumbing, electrical, mechanical, etc.
    if "building" not in case_type:
        return False
    # Skip sub-trade permits that happen to have "building" in a weird way
    for skip in ("plumbing", "electrical", "mechanical", "hvac", "heating",
                 "fire", "sprinkler", "roofing", "demolition", "sign",
                 "grading", "zoning", "fence"):
        if skip in case_type:
            return False
    # Skip if case number looks like an inspection
    if case_num.startswith("INS-") or case_num.startswith("INSP-"):
        return False
    return True


def enrich_permit(session, permit):
    """
    Search for the BUILDING permit by address to get value + builder contacts.

    Flow (matches original Node.js charleston.js):
    1. Search EnerGov by address for ALL permits at that address
    2. Filter to find the actual residential BUILDING permit (not plumbing/electric/etc.)
    3. Get permit detail (project value)
    4. Get contacts from building permit (the Applicant = the home builder)
    5. Store all contacts + linked permit info in raw_data for the detail view
    """
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

        # Log all permit types found at this address for debugging
        all_types = [
            f"{e.get('CaseType')}/{e.get('CaseNumber')}"
            for e in entities[:10]
        ]
        log.info(f"  Address '{address}': {len(entities)} results, types: {', '.join(all_types)}")

        # Filter to ONLY actual building permits (not plumbing, electrical, etc.)
        building_permits = [e for e in entities if _is_building_permit(e)]

        if not building_permits:
            log.info(f"  No building permits at {address} (found {len(entities)} other permits)")
            # Store all permit types for the detail view even if no building permit
            try:
                raw = json.loads(enriched.get("raw_data") or "{}")
                raw["_allPermitTypes"] = [
                    {"type": e.get("CaseType"), "number": e.get("CaseNumber"),
                     "value": e.get("ProjectValue") or e.get("EstimatedValue")}
                    for e in entities[:20]
                ]
                raw["_dataSource"] = "no_building_permit"
                enriched["raw_data"] = json.dumps(raw)
            except Exception:
                pass
            return enriched

        log.info(
            f"  Found {len(building_permits)} building permit(s): "
            + ", ".join(
                f"{e.get('CaseType')}/{e.get('CaseWorkclass')} [{e.get('CaseNumber')}] "
                f"val={e.get('ProjectValue') or e.get('EstimatedValue') or '?'}"
                for e in building_permits
            )
        )

        # Pick the building permit with the highest value
        best = max(
            building_permits,
            key=lambda e: float(e.get("ProjectValue") or e.get("EstimatedValue") or 0),
        )
        case_id = best.get("CaseId")
        if not case_id:
            return enriched

        # Get permit detail for value
        detail_resp = session.post(
            PERMIT_DETAIL_API,
            json={"EntityId": case_id, "ModuleId": 1},
            headers=TENANT_HEADERS, timeout=30,
        )
        detail = detail_resp.json()
        p = detail.get("Result", {}) if detail.get("Success") else {}
        raw_val = p.get("Value") or best.get("ProjectValue") or best.get("EstimatedValue")
        if raw_val:
            try:
                enriched["project_value"] = float(str(raw_val).replace(",", "").replace("$", ""))
            except Exception:
                pass
        enriched["permit_type"] = (
            p.get("WorkClassName") or best.get("CaseWorkclass") or enriched.get("permit_type")
        )
        enriched["permit_issue_date"] = parse_energov_date(
            p.get("IssueDate") or p.get("ApplyDate")
        ) or enriched.get("permit_issue_date")
        linked_num = p.get("PermitNumber") or best.get("CaseNumber")

        log.info(
            f"  Building permit {linked_num}: Value=${enriched.get('project_value', '?'):,.0f}, "
            f"Type={enriched.get('permit_type')}"
        )

        # Get contacts from the BUILDING permit (ModuleId=1 = permits)
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
        contacts = (
            contacts_data.get("Result", [])
            if isinstance(contacts_data.get("Result"), list)
            else []
        )

        all_contacts = []
        applicant_name = None
        applicant_company = None
        contractors = []
        owner = None

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

            # Applicant on the BUILDING permit = the home builder (not plumber/electrician)
            if "applicant" in ctype:
                if not applicant_name and name:
                    applicant_name = name
                if not applicant_company and company:
                    applicant_company = company
                # Grab their phone/email
                if not enriched.get("builder_phone") and phone:
                    enriched["builder_phone"] = phone
                if not enriched.get("builder_email") and email:
                    enriched["builder_email"] = email
            elif "contractor" in ctype or "builder" in ctype:
                contractors.append({"name": name, "company": company})
                if not enriched.get("builder_phone") and phone:
                    enriched["builder_phone"] = phone
                if not enriched.get("builder_email") and email:
                    enriched["builder_email"] = email
            elif "owner" in ctype:
                if not owner and name:
                    owner = name

        # Set builder = applicant first, then contractor fallback
        enriched["builder_name"] = applicant_name or (contractors[0]["name"] if contractors else None) or enriched.get("builder_name")
        enriched["builder_company"] = applicant_company or (contractors[0]["company"] if contractors else None) or enriched.get("builder_company")
        if owner:
            enriched["owner_name"] = owner

        log.info(
            f"  Contacts: builder={enriched.get('builder_name', '?')} "
            f"@ {enriched.get('builder_company', '?')}, "
            f"phone={enriched.get('builder_phone', '?')}, "
            f"email={enriched.get('builder_email', '?')}, "
            f"owner={enriched.get('owner_name', '?')}"
        )

        # Store linked permit info and ALL contacts in raw_data for detail view
        try:
            raw = json.loads(enriched.get("raw_data") or "{}")
            raw["_linkedPermit"] = linked_num
            raw["_linkedPermitUrl"] = f"{ENERGOV_BASE}#/permitDetail/permit/{case_id}"
            raw["_allContacts"] = all_contacts
            raw["_contractors"] = contractors
            raw["_dataSource"] = "building_permit"
            raw["_allPermitTypes"] = [
                {"type": e.get("CaseType"), "number": e.get("CaseNumber"),
                 "value": e.get("ProjectValue") or e.get("EstimatedValue")}
                for e in entities[:20]
            ]
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

WEB_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"


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


# ─── Playwright Browser Manager ─────────────────────────────────────────────
# Railway runs Docker with Chromium installed — Playwright gives us a real browser
# just like the original Node.js Puppeteer version. No CAPTCHAs, no JS issues.

_pw_browser = None

def _get_browser():
    """Get or create a shared Playwright browser instance."""
    global _pw_browser
    if _pw_browser and _pw_browser.is_connected():
        return _pw_browser
    try:
        from playwright.sync_api import sync_playwright
        pw = sync_playwright().start()
        _pw_browser = pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        log.info("Playwright browser launched")
        return _pw_browser
    except Exception as e:
        log.warning(f"Playwright unavailable: {e}")
        return None


def _domain_matches_company(hostname, company_name):
    """Check if a domain name plausibly belongs to the company."""
    if not hostname or not company_name:
        return False
    words = re.findall(r'[a-z]+', company_name.lower())
    filler = {"the", "of", "and", "inc", "llc", "co", "company", "corp", "group", "sc", "charleston"}
    words = [w for w in words if w not in filler and len(w) > 2]
    if not words:
        return False
    hostname_lower = hostname.lower().replace("-", "").replace(".", "")
    return sum(1 for w in words if w in hostname_lower) >= 1


# ─── Google Places API (optional — best if you have a key) ──────────────────

def lookup_google_places(session, company_name, city=None):
    """Look up a builder on Google Places API — returns phone + website directly."""
    if not GOOGLE_PLACES_KEY:
        return None
    location = city or "South Carolina"
    location = re.sub(r'^(City of|Town of|County of)\s+', '', location, flags=re.I)
    query = f"{company_name} {location} SC builder"
    try:
        resp = session.post(
            "https://places.googleapis.com/v1/places:searchText",
            json={"textQuery": query, "maxResultCount": 3},
            headers={
                "Content-Type": "application/json",
                "X-Goog-Api-Key": GOOGLE_PLACES_KEY,
                "X-Goog-FieldMask": "places.displayName,places.nationalPhoneNumber,places.websiteUri",
            },
            timeout=15,
        )
        if resp.status_code != 200:
            log.warning(f"  Google Places error {resp.status_code}")
            return None
        places = resp.json().get("places", [])
        if not places:
            return None
        company_words = set(re.findall(r'[a-z]+', company_name.lower()))
        company_words -= {"the", "of", "and", "inc", "llc", "co", "company", "corp", "group"}
        best = places[0]
        for place in places:
            name = (place.get("displayName", {}).get("text") or "").lower()
            if any(w in name for w in company_words if len(w) > 2):
                best = place
                break
        phone = best.get("nationalPhoneNumber")
        website = best.get("websiteUri")
        name = best.get("displayName", {}).get("text", "")
        log.info(f"  Google Places: '{name}' → phone={phone}, website={website}")
        return {"phone": phone, "website": website}
    except Exception as e:
        log.warning(f"  Google Places failed: {e}")
        return None


# ─── Playwright-Powered Search (DuckDuckGo via real browser) ────────────────

def find_company_website_browser(company_name, city=None):
    """Search DuckDuckGo using a real browser — bypasses CAPTCHAs.

    This is the same approach the original Node.js Puppeteer version used.
    Works on Railway because Docker has Chromium installed.
    """
    browser = _get_browser()
    if not browser:
        return None
    if not company_name:
        return None
    location = city or "South Carolina"
    location = re.sub(r'^(City of|Town of|County of)\s+', '', location, flags=re.I)
    query = f"{company_name} {location} SC builder"

    page = None
    try:
        page = browser.new_page(
            user_agent=WEB_UA,
            viewport={"width": 1280, "height": 720},
        )
        page.set_default_timeout(20000)

        # Navigate to DuckDuckGo
        page.goto(f"https://duckduckgo.com/?q={quote(query)}", wait_until="domcontentloaded")
        time.sleep(2)

        # Wait for results to load
        try:
            page.wait_for_selector("a[data-testid='result-title-a'], article a, .result__a", timeout=10000)
        except Exception:
            pass

        # Extract result links from page
        links = page.evaluate("""() => {
            const results = [];
            // Try new DDG layout
            document.querySelectorAll('a[data-testid="result-title-a"]').forEach(a => {
                if (a.href) results.push(a.href);
            });
            // Try classic DDG layout
            if (results.length === 0) {
                document.querySelectorAll('a.result__a').forEach(a => {
                    if (a.href) results.push(a.href);
                });
            }
            // Fallback: any article links
            if (results.length === 0) {
                document.querySelectorAll('article a[href^="http"]').forEach(a => {
                    results.push(a.href);
                });
            }
            return results;
        }""")

        log.info(f"  Browser search: {len(links)} results for '{company_name}'")

        # Filter and pick best result
        valid_results = []
        for href in links:
            try:
                hostname = urlparse(href).hostname
                if not hostname:
                    continue
                hostname = hostname.lower()
                if any(hostname == d or hostname.endswith("." + d) for d in SKIP_DOMAINS):
                    continue
                if "duckduckgo" in hostname:
                    continue
                valid_results.append((href, hostname))
            except Exception:
                continue

        # Pass 1: prefer domains matching company name
        for href, hostname in valid_results:
            if _domain_matches_company(hostname, company_name):
                log.info(f"  Browser: matched '{hostname}' to '{company_name}'")
                return href

        # Pass 2: first valid result
        if valid_results:
            log.info(f"  Browser: first result '{valid_results[0][1]}'")
            return valid_results[0][0]

        log.info(f"  Browser: no valid results for '{company_name}'")
    except Exception as e:
        log.warning(f"  Browser search failed for '{company_name}': {e}")
    finally:
        if page:
            try:
                page.close()
            except Exception:
                pass
    return None


# ─── Playwright-Powered Website Scraper ─────────────────────────────────────

def scrape_website_browser(website_url):
    """Scrape a builder website using a real browser — handles JS-rendered sites.

    Visits homepage → discovers contact pages → scrapes them all.
    Returns (phones, emails).
    """
    browser = _get_browser()
    if not browser or not website_url:
        return [], []

    all_phones = set()
    all_emails = set()
    visited = set()

    try:
        parsed = urlparse(website_url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        return [], []

    def _visit_and_extract(url):
        """Visit a page with the browser and extract contacts from rendered HTML."""
        normalized = url.rstrip("/").lower()
        if normalized in visited:
            return ""
        visited.add(normalized)

        page = None
        try:
            page = browser.new_page(
                user_agent=WEB_UA,
                viewport={"width": 1280, "height": 720},
            )
            page.set_default_timeout(15000)
            page.goto(url, wait_until="domcontentloaded")
            time.sleep(1)

            # Scroll down to trigger lazy loading
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(0.5)

            # Get fully rendered HTML
            html = page.content()
            phones, emails = _extract_contacts_from_html(html)
            all_phones.update(phones)
            all_emails.update(emails)
            return html
        except Exception as e:
            log.debug(f"  Browser scrape failed for {url}: {e}")
            return ""
        finally:
            if page:
                try:
                    page.close()
                except Exception:
                    pass

    static_paths = [
        "/contact", "/contact-us", "/contact.html", "/contactus",
        "/about", "/about-us", "/about.html",
        "/our-team", "/team", "/locations", "/get-in-touch",
    ]

    # Phase 1: Homepage
    log.info(f"  Browser scrape: {website_url}")
    homepage_html = _visit_and_extract(website_url)

    # Phase 2: Discover contact links from nav
    if HAS_BS4 and homepage_html:
        soup = BeautifulSoup(homepage_html, "html.parser")
        contact_re = re.compile(r'contact|about|team|staff|our-team|get-in-touch|location|office', re.I)
        base_host = (parsed.hostname or "").lower()
        discovered = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            text = a.get_text(" ", strip=True).lower()
            if contact_re.search(text) or contact_re.search(href):
                if href.startswith("/"):
                    href = f"{base_url}{href}"
                elif href.startswith("http"):
                    try:
                        if (urlparse(href).hostname or "").lower() != base_host:
                            continue
                    except Exception:
                        continue
                elif href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
                    continue
                else:
                    href = f"{base_url}/{href}"
                discovered.add(href.rstrip("/"))
        # Discovered links go first (higher priority)
        static_paths = list(discovered) + [f"{base_url}{p}" for p in static_paths]

    # Phase 3: Scrape additional pages (up to 8 more)
    pages_scraped = 1
    for page_url in static_paths:
        if pages_scraped >= 8:
            break
        if all_phones and all_emails:
            log.info(f"  Browser scrape: found phone + email, stopping early")
            break
        html = _visit_and_extract(page_url)
        if html:
            pages_scraped += 1

    log.info(f"  Browser scrape: {pages_scraped} pages → {len(all_phones)} phone(s), {len(all_emails)} email(s)")
    return list(all_phones), list(all_emails)


# ─── Requests-Based Scraper (fallback if no browser) ────────────────────────

def _discover_contact_links(html_text, base_url):
    """Discover contact/about page links from navigation."""
    links = set()
    if not HAS_BS4 or not html_text:
        return links
    soup = BeautifulSoup(html_text, "html.parser")
    contact_patterns = re.compile(
        r'contact|about|team|staff|our-team|get-in-touch|reach-us|connect|location|office', re.I,
    )
    parsed_base = urlparse(base_url)
    base_host = parsed_base.hostname or ""
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = a.get_text(" ", strip=True).lower()
        if contact_patterns.search(text) or contact_patterns.search(href):
            if href.startswith("/"):
                href = f"{parsed_base.scheme}://{parsed_base.netloc}{href}"
            elif href.startswith("http"):
                try:
                    if (urlparse(href).hostname or "").lower() != base_host.lower():
                        continue
                except Exception:
                    continue
            elif href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
                continue
            else:
                href = f"{parsed_base.scheme}://{parsed_base.netloc}/{href}"
            links.add(href.rstrip("/"))
    return links


def scrape_website_requests(session, website_url):
    """Scrape a builder website using plain requests (fallback if no browser)."""
    if not website_url:
        return [], []
    all_phones = set()
    all_emails = set()
    visited = set()
    try:
        parsed = urlparse(website_url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        return [], []

    static_paths = [
        "/contact", "/contact-us", "/contact.html", "/contactus",
        "/about", "/about-us", "/about.html",
        "/our-team", "/team", "/locations", "/get-in-touch",
    ]

    def _fetch(url):
        normalized = url.rstrip("/").lower()
        if normalized in visited:
            return False, ""
        visited.add(normalized)
        try:
            resp = session.get(
                url, headers={"User-Agent": WEB_UA, "Accept": "text/html,*/*"},
                timeout=12, allow_redirects=True,
            )
            if resp.status_code >= 400:
                return False, ""
            phones, emails = _extract_contacts_from_html(resp.text)
            all_phones.update(phones)
            all_emails.update(emails)
            return True, resp.text
        except Exception:
            return False, ""

    log.info(f"  Requests scrape: {website_url}")
    homepage_html = ""
    try:
        ok, homepage_html = _fetch(website_url)
    except Exception:
        pass

    discovered = _discover_contact_links(homepage_html, base_url)
    pages_to_try = list(discovered) + [f"{base_url}{p}" for p in static_paths]
    pages_scraped = 1
    for page_url in pages_to_try:
        if pages_scraped >= 12 or (all_phones and all_emails):
            break
        try:
            ok, _ = _fetch(page_url)
            if ok:
                pages_scraped += 1
        except Exception:
            continue

    log.info(f"  Requests scrape: {pages_scraped} pages → {len(all_phones)} phone(s), {len(all_emails)} email(s)")
    return list(all_phones), list(all_emails)


# ─── Master Builder Lookup ──────────────────────────────────────────────────

def lookup_builder_web(session, company_name, city=None):
    """Look up builder contact info. Priority:
    1. Google Places API (if key set — returns phone + website directly)
    2. Playwright browser search + scrape (Railway/Docker — like original Puppeteer)
    3. Requests-based DDG search + scrape (fallback)

    Returns: {"website": str|None, "phone": str|None, "email": str|None, "source": str}
    """
    log.info(f"Builder lookup: '{company_name}' in {city or 'SC'}")

    # Method 1: Google Places API
    if GOOGLE_PLACES_KEY:
        gp = lookup_google_places(session, company_name, city=city)
        if gp and (gp.get("phone") or gp.get("website")):
            result = {"website": gp.get("website"), "phone": gp.get("phone"), "email": None, "source": "google_places"}
            # Scrape website for email if we got a URL
            if result["website"] and not result["email"]:
                _, emails = scrape_website_browser(result["website"]) if _get_browser() else scrape_website_requests(session, result["website"])
                if emails:
                    result["email"] = emails[0]
            log.info(f"  Result (Google): phone={result['phone']}, email={result['email']}, web={result['website']}")
            return result

    # Method 2: Browser search + browser scrape (Railway/Docker with Playwright)
    if _get_browser():
        website = find_company_website_browser(company_name, city=city)
        if website:
            phones, emails = scrape_website_browser(website)
            result = {
                "website": website,
                "phone": phones[0] if phones else None,
                "email": emails[0] if emails else None,
                "source": "browser",
            }
            log.info(f"  Result (Browser): phone={result['phone']}, email={result['email']}, web={result['website']}")
            return result
        log.info(f"  Browser search found no website for '{company_name}'")

    # Method 3: Requests-based fallback (DDG HTML + requests scrape)
    log.info(f"  Falling back to requests-based search for '{company_name}'")
    location = city or "South Carolina"
    location = re.sub(r'^(City of|Town of|County of)\s+', '', location, flags=re.I)
    query = f"{company_name} {location} SC builder"
    website = None
    try:
        resp = session.get(
            "https://html.duckduckgo.com/html/",
            params={"q": query},
            headers={"User-Agent": WEB_UA, "Accept": "text/html,*/*"},
            timeout=15,
        )
        if HAS_BS4 and resp.status_code in (200, 202):
            soup = BeautifulSoup(resp.text, "html.parser")
            for a in soup.select("a.result__a"):
                href = a.get("href", "")
                if "uddg=" in href:
                    from urllib.parse import parse_qs
                    qs = parse_qs(urlparse(href).query)
                    href = qs.get("uddg", [href])[0]
                if "duckduckgo.com" in href or "y.js?" in href:
                    continue
                try:
                    hostname = urlparse(href).hostname
                    if not hostname:
                        continue
                    hostname = hostname.lower()
                    if any(hostname == d or hostname.endswith("." + d) for d in SKIP_DOMAINS):
                        continue
                    if _domain_matches_company(hostname, company_name):
                        website = href
                        break
                    if not website:
                        website = href
                except Exception:
                    continue
    except Exception as e:
        log.warning(f"  DDG search failed: {e}")

    if website:
        phones, emails = scrape_website_requests(session, website)
        result = {
            "website": website,
            "phone": phones[0] if phones else None,
            "email": emails[0] if emails else None,
            "source": "requests",
        }
        log.info(f"  Result (Requests): phone={result['phone']}, email={result['email']}, web={result['website']}")
        return result

    log.info(f"  No results found for '{company_name}'")
    return {"website": None, "phone": None, "email": None, "source": "none"}


# ─── Auto-Scheduler ──────────────────────────────────────────────────────
SCHEDULE_TIMES = [dtime(7, 0), dtime(13, 0), dtime(17, 0)]  # ET
TIMEZONE = pytz.timezone("America/New_York")

# Global flag so only one scheduler thread runs across all Streamlit sessions
_scheduler_started = False
_scheduler_lock = threading.Lock()


def run_scraper_background(trigger="manual"):
    """Headless version of run_scraper() for scheduler — no Streamlit UI required."""
    log.info(f"Background scrape started (trigger={trigger})")
    conn = get_db()
    started_at = datetime.now(TIMEZONE).isoformat()

    try:
        session = requests.Session()
        try:
            session.get(f"{ENERGOV_BASE}/", headers={"User-Agent": TENANT_HEADERS["User-Agent"]}, timeout=30)
        except Exception:
            pass
        time.sleep(1)

        # Search EnerGov
        all_permits = []
        page_number = 1
        while page_number <= 10:
            body = build_search_body(STRAPPING_TYPE_ID, PASSED_STATUS_ID, page_number, 100)
            try:
                resp = session.post(SEARCH_API, json=body, headers=TENANT_HEADERS, timeout=30)
                result = resp.json()
            except Exception as e:
                log.error(f"Background scrape API error: {e}")
                break
            if not result.get("Success"):
                break
            entities = result.get("Result", {}).get("EntityResults", [])
            if not entities:
                break
            for entity in entities:
                permit = map_entity(entity)
                if permit:
                    all_permits.append(permit)
            if len(entities) < 100:
                break
            page_number += 1
            time.sleep(1)

        if not all_permits:
            conn.execute(
                "INSERT INTO scrape_log (started_at, completed_at, trigger, permits_found, new_permits, builders_cached, status) "
                "VALUES (?, ?, ?, 0, 0, 0, 'no_results')",
                (started_at, datetime.now(TIMEZONE).isoformat(), trigger),
            )
            conn.commit()
            log.info("Background scrape: no permits found")
            return

        # Filter to new permits only
        existing = set(
            row[0] for row in conn.execute("SELECT permit_number FROM permits").fetchall()
        )
        new_permits = [p for p in all_permits if p.get("permit_number") not in existing]

        if not new_permits:
            conn.execute(
                "INSERT INTO scrape_log (started_at, completed_at, trigger, permits_found, new_permits, builders_cached, status) "
                "VALUES (?, ?, ?, ?, 0, 0, 'success')",
                (started_at, datetime.now(TIMEZONE).isoformat(), trigger, len(all_permits)),
            )
            conn.commit()
            log.info(f"Background scrape: {len(all_permits)} permits found, 0 new")
            return

        # Enrich new permits
        for i, permit in enumerate(new_permits[:100]):
            new_permits[i] = enrich_permit(session, permit)
            time.sleep(0.5)

        # Load builder cache
        cached_builders = {}
        for row in conn.execute("SELECT * FROM builder_cache").fetchall():
            cached_builders[row["company_name"]] = dict(row)

        # Builder web lookup
        companies_searched = set()
        company_results = {}
        builders_cached_count = 0
        web_session = requests.Session()

        for i, permit in enumerate(new_permits):
            company = (permit.get("builder_company") or "").strip()
            if not company or permit.get("builder_website"):
                continue

            if company in cached_builders:
                cached = cached_builders[company]
                result = {"website": cached.get("website"), "phone": cached.get("phone"),
                          "email": cached.get("email"), "source": "cache"}
            elif company in company_results:
                result = company_results[company]
            elif company not in companies_searched:
                companies_searched.add(company)
                city = permit.get("municipality")
                result = lookup_builder_web(web_session, company, city=city)
                company_results[company] = result
                try:
                    conn.execute(
                        "INSERT OR REPLACE INTO builder_cache "
                        "(company_name, website, phone, email, source) VALUES (?, ?, ?, ?, ?)",
                        (company, result.get("website"), result.get("phone"),
                         result.get("email"), result.get("source")),
                    )
                    conn.commit()
                    builders_cached_count += 1
                except Exception:
                    pass
                time.sleep(2)
            else:
                continue

            if result.get("website"):
                new_permits[i]["builder_website"] = result["website"]
            if result.get("phone") and not new_permits[i].get("builder_phone"):
                new_permits[i]["builder_phone"] = result["phone"]
            if result.get("email") and not new_permits[i].get("builder_email"):
                new_permits[i]["builder_email"] = result["email"]

        # Save to DB
        new_count = 0
        for p in new_permits:
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
                log.warning(f"Background DB insert error: {e}")

        conn.execute(
            "INSERT INTO scrape_log (started_at, completed_at, trigger, permits_found, new_permits, builders_cached, status) "
            "VALUES (?, ?, ?, ?, ?, ?, 'success')",
            (started_at, datetime.now(TIMEZONE).isoformat(), trigger,
             len(all_permits), new_count, builders_cached_count),
        )
        conn.commit()
        log.info(
            f"Background scrape complete: {len(all_permits)} found, {new_count} new, "
            f"{builders_cached_count} builders cached"
        )
    except Exception as e:
        log.error(f"Background scrape failed: {e}")
        try:
            conn.execute(
                "INSERT INTO scrape_log (started_at, completed_at, trigger, permits_found, new_permits, builders_cached, status) "
                "VALUES (?, ?, ?, 0, 0, 0, ?)",
                (started_at, datetime.now(TIMEZONE).isoformat(), trigger, f"error: {e}"),
            )
            conn.commit()
        except Exception:
            pass


def scheduler_loop():
    """Background thread that triggers scraper at scheduled times."""
    log.info("Scheduler started — will run at 7:00, 13:00, 17:00 ET")
    last_run_date_hour = None
    while True:
        now = datetime.now(TIMEZONE)
        date_hour_key = now.strftime("%Y-%m-%d-%H")

        for sched_time in SCHEDULE_TIMES:
            if (now.hour == sched_time.hour
                    and now.minute < 2
                    and date_hour_key != last_run_date_hour):
                last_run_date_hour = date_hour_key
                log.info(f"Scheduler triggering scrape at {now.strftime('%H:%M')} ET")
                try:
                    run_scraper_background(trigger=f"scheduled_{sched_time.strftime('%H:%M')}")
                except Exception as e:
                    log.error(f"Scheduled scrape failed: {e}")
                break

        time.sleep(30)


def start_scheduler():
    """Start the scheduler thread if not already running (process-wide singleton)."""
    global _scheduler_started
    with _scheduler_lock:
        if _scheduler_started:
            return
        _scheduler_started = True
    t = threading.Thread(target=scheduler_loop, daemon=True, name="pierpont-scheduler")
    t.start()
    log.info("Scheduler thread launched")


def get_next_scheduled_run():
    """Return the next scheduled run time as a datetime."""
    now = datetime.now(TIMEZONE)
    for sched_time in sorted(SCHEDULE_TIMES, key=lambda t: t.hour):
        candidate = now.replace(hour=sched_time.hour, minute=0, second=0, microsecond=0)
        if candidate > now:
            return candidate
    # All times have passed today — next is tomorrow's first time
    first = min(SCHEDULE_TIMES, key=lambda t: t.hour)
    tomorrow = now + timedelta(days=1)
    return tomorrow.replace(hour=first.hour, minute=0, second=0, microsecond=0)


def run_scraper(status_placeholder):
    """Run the EnerGov scraper and save results to SQLite."""
    scrape_started_at = datetime.now(TIMEZONE).isoformat()
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

    # Filter to only NEW permits not already in the database
    conn = get_db()
    existing = set(
        row[0] for row in conn.execute("SELECT permit_number FROM permits").fetchall()
    )
    new_permits = [p for p in all_permits if p.get("permit_number") not in existing]

    status_placeholder.info(
        f"Found {len(all_permits)} permits total, {len(new_permits)} new. "
        f"Enriching new permits with contact data..."
    )

    if not new_permits:
        status_placeholder.success(
            f"Scrape complete! {len(all_permits)} permits found, 0 new (all already in database)."
        )
        return len(all_permits), 0

    # Phase 1: Enrich NEW permits from EnerGov building permits (contacts + values)
    enriched_count = 0
    for i, permit in enumerate(new_permits[:100]):
        if i % 5 == 0:
            status_placeholder.info(
                f"Enriching permit {i + 1}/{min(len(new_permits), 100)} with contacts & values..."
            )
        new_permits[i] = enrich_permit(session, permit)
        enriched_count += 1
        time.sleep(0.5)

    # Load cached builder info from DB
    cached_builders = {}
    for row in conn.execute("SELECT * FROM builder_cache").fetchall():
        cached_builders[row["company_name"]] = dict(row)

    # Phase 2: Builder web lookup — use cache first, then search web
    companies_searched = set()
    company_results = {}  # session cache: company_name -> {website, phone, email}
    builders_cached_count = 0
    builders_with_company = [
        (i, p) for i, p in enumerate(new_permits)
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

            # 1. Check builder_cache (DB) first — instant, no web request
            if company in cached_builders:
                cached = cached_builders[company]
                result = {
                    "website": cached.get("website"),
                    "phone": cached.get("phone"),
                    "email": cached.get("email"),
                    "source": "cache",
                }
                log.info(f"  Cache hit for '{company}': web={result['website']}")
            # 2. Check session-level dedup dict
            elif company in company_results:
                result = company_results[company]
            # 3. Full web lookup
            elif company not in companies_searched:
                companies_searched.add(company)
                city = permit.get("municipality")
                result = lookup_builder_web(web_session, company, city=city)
                company_results[company] = result
                # Save to builder_cache for future runs
                try:
                    conn.execute(
                        "INSERT OR REPLACE INTO builder_cache "
                        "(company_name, website, phone, email, source) VALUES (?, ?, ?, ?, ?)",
                        (company, result.get("website"), result.get("phone"),
                         result.get("email"), result.get("source")),
                    )
                    conn.commit()
                    builders_cached_count += 1
                except Exception as e:
                    log.warning(f"Builder cache insert error: {e}")
                time.sleep(2)  # Rate limit
            else:
                continue

            if result.get("website"):
                new_permits[i]["builder_website"] = result["website"]
            if result.get("phone") and not new_permits[i].get("builder_phone"):
                new_permits[i]["builder_phone"] = result["phone"]
            if result.get("email") and not new_permits[i].get("builder_email"):
                new_permits[i]["builder_email"] = result["email"]

    status_placeholder.info(f"Saving {len(new_permits)} new permits to database...")
    new_count = 0
    for p in new_permits:
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

    # Log to scrape_log
    try:
        conn.execute(
            "INSERT INTO scrape_log (started_at, completed_at, trigger, permits_found, new_permits, builders_cached, status) "
            "VALUES (?, ?, 'manual', ?, ?, ?, 'success')",
            (scrape_started_at, datetime.now(TIMEZONE).isoformat(),
             len(all_permits), new_count, builders_cached_count),
        )
        conn.commit()
    except Exception:
        pass

    status_placeholder.success(
        f"Scrape complete! {len(all_permits)} permits found, {new_count} new saved. "
        f"Builder websites: {web_found}/{len(companies_searched)} found. "
        f"Cache hits: {len(builders_with_company) - len(companies_searched)}."
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
    details.permit-detail { margin:0; border-bottom:1px solid rgba(255,255,255,0.04); }
    details.permit-detail summary {
        list-style:none; cursor:pointer;
    }
    details.permit-detail summary::-webkit-details-marker { display:none; }
    details.permit-detail summary::marker { display:none; content:''; }
    details.permit-detail:hover { background:rgba(43,108,176,0.04); }
    details.permit-detail[open] { background:rgba(43,108,176,0.03); }
    details.permit-detail[open] summary { border-bottom:1px solid rgba(43,108,176,0.1); }
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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS builder_cache (
            company_name TEXT PRIMARY KEY,
            website TEXT,
            phone TEXT,
            email TEXT,
            source TEXT,
            cached_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scrape_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT,
            completed_at TEXT,
            trigger TEXT,
            permits_found INTEGER,
            new_permits INTEGER,
            builders_cached INTEGER,
            status TEXT
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
    allowed = ["municipality", "address", "builder_name", "builder_company", "project_value", "inspection_date", "opportunity_score", "permit_number", "builder_website", "builder_phone", "builder_email"]
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

    # ── Start Auto-Scheduler ──
    if "auto_scrape" not in st.session_state:
        st.session_state.auto_scrape = True
    if st.session_state.auto_scrape:
        start_scheduler()

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
    with btn_cols[3]:
        _has_browser = _get_browser() is not None
        if GOOGLE_PLACES_KEY:
            method_label = "Google Places API"
            method_color = "#4ADE80"
        elif _has_browser:
            method_label = "Browser Search (Playwright)"
            method_color = "#60A5FA"
        else:
            method_label = "Basic Search (limited)"
            method_color = "#F59E0B"
        st.markdown(
            f'<div style="text-align:center;padding:6px;font-size:.55rem;color:{method_color}">'
            f'Lookup: {method_label}</div>',
            unsafe_allow_html=True,
        )
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
        if GOOGLE_PLACES_KEY:
            method = "Google Places API"
        elif _get_browser():
            method = "Browser Search + Scrape"
        else:
            method = "Web Search + Scrape"
        with st.spinner(f"Looking up builders via {method}..."):
            web_session = requests.Session()
            rows = conn.execute(
                "SELECT id, builder_company, builder_phone, builder_email, builder_website, municipality "
                "FROM permits WHERE builder_company IS NOT NULL AND builder_company != '' "
                "AND (builder_website IS NULL OR builder_website = '')"
            ).fetchall()
            if not rows:
                status_box.info("All builders already have website data.")
            else:
                # Deduplicate: only look up each company once
                companies_searched = set()
                company_results = {}
                found_count = 0
                unique_companies = []
                for row in rows:
                    company = row[1].strip()
                    if company and company not in companies_searched:
                        companies_searched.add(company)
                        unique_companies.append((company, row[5] if len(row) > 5 else None))

                status_box.info(
                    f"Found {len(unique_companies)} unique builders to look up via {method}..."
                )
                for idx, (company, city) in enumerate(unique_companies):
                    status_box.info(
                        f"[{idx + 1}/{len(unique_companies)}] Looking up: {company[:40]}..."
                    )
                    result = lookup_builder_web(web_session, company, city=city)
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
                source_note = f" (via {method})" if GOOGLE_PLACES_KEY else " (set GOOGLE_PLACES_API_KEY for better results)"
                status_box.success(
                    f"Builder lookup complete! {found_count}/{len(unique_companies)} websites found{source_note}"
                )
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

    # ── Auto-Scrape Scheduler Status ──
    with st.expander("Auto-Scrape Scheduler", expanded=False):
        sched_col1, sched_col2 = st.columns([1, 3])
        with sched_col1:
            auto_scrape_on = st.toggle("Auto-scrape enabled", value=st.session_state.auto_scrape, key="auto_scrape_toggle")
            if auto_scrape_on != st.session_state.auto_scrape:
                st.session_state.auto_scrape = auto_scrape_on
                st.rerun()
        with sched_col2:
            if st.session_state.auto_scrape:
                next_run = get_next_scheduled_run()
                next_str = next_run.strftime("%b %d, %Y %I:%M %p ET")
                st.markdown(
                    f'<div style="padding:8px;font-size:.8rem;color:#4ADE80">'
                    f'Next scheduled run: <strong>{next_str}</strong> &nbsp;'
                    f'<span style="color:#94A3B8">(runs at 7:00 AM, 1:00 PM, 5:00 PM ET)</span></div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    '<div style="padding:8px;font-size:.8rem;color:#F59E0B">'
                    'Auto-scrape is disabled. Toggle on to run at 7 AM, 1 PM, 5 PM ET daily.</div>',
                    unsafe_allow_html=True,
                )

        # Show recent scrape log
        log_rows = conn.execute(
            "SELECT * FROM scrape_log ORDER BY id DESC LIMIT 10"
        ).fetchall()
        if log_rows:
            st.markdown(
                '<div style="font-size:.7rem;font-weight:600;color:#94A3B8;text-transform:uppercase;'
                'letter-spacing:.06em;margin-top:8px;margin-bottom:4px">Recent Scrape History</div>',
                unsafe_allow_html=True,
            )
            log_html = '<div style="font-size:.75rem">'
            for row in log_rows:
                r = dict(row)
                trigger = r.get("trigger") or "?"
                status = r.get("status") or "?"
                started = r.get("started_at") or ""
                found = r.get("permits_found") or 0
                new = r.get("new_permits") or 0
                cached = r.get("builders_cached") or 0

                # Format timestamp
                try:
                    dt = datetime.fromisoformat(started)
                    time_str = dt.strftime("%b %d %I:%M %p")
                except Exception:
                    time_str = started[:16] if started else "?"

                status_color = "#4ADE80" if "success" in status else "#FCA5A5"
                trigger_icon = "&#x23F0;" if "scheduled" in trigger else "&#x1F446;"

                log_html += (
                    f'<div style="padding:4px 0;border-bottom:1px solid rgba(255,255,255,0.04)">'
                    f'{trigger_icon} <span style="color:#E2E8F0">{time_str}</span> &nbsp;'
                    f'<span class="badge badge-blue" style="font-size:.55rem;padding:1px 6px">{trigger}</span> &nbsp;'
                    f'<span style="color:{status_color}">{status}</span> &nbsp;'
                    f'<span style="color:#94A3B8">Found: {found}, New: {new}, Cached: {cached}</span>'
                    f'</div>'
                )
            log_html += '</div>'
            st.markdown(log_html, unsafe_allow_html=True)
        else:
            st.markdown(
                '<div style="font-size:.78rem;color:#94A3B8;padding:8px 0">No scrape history yet. '
                'Run the scraper manually or wait for a scheduled run.</div>',
                unsafe_allow_html=True,
            )

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

    # ── Profit Margin Input ──
    margin_col1, margin_col2 = st.columns([1, 5])
    with margin_col1:
        margin_pct = st.number_input(
            "Margin %", min_value=0, max_value=100, value=15,
            step=1, key="margin_pct", label_visibility="collapsed",
        )
    with margin_col2:
        st.markdown(
            f'<div style="padding:8px 0;font-size:.78rem;color:#94A3B8">'
            f'Profit Margin: <strong style="color:#4ADE80">{margin_pct}%</strong> '
            f'&mdash; Profit estimates update live in the table</div>',
            unsafe_allow_html=True,
        )

    # ── Sort & Pagination ──
    scol1, scol2, scol3 = st.columns([1, 1, 4])
    with scol1:
        sort_map = {
            "Score ↓": ("opportunity_score", "DESC"),
            "Score ↑": ("opportunity_score", "ASC"),
            "Value ↓": ("project_value", "DESC"),
            "Value ↑": ("project_value", "ASC"),
            "Date ↓": ("inspection_date", "DESC"),
            "Date ↑": ("inspection_date", "ASC"),
            "Address A-Z": ("address", "ASC"),
            "Address Z-A": ("address", "DESC"),
            "Builder A-Z": ("builder_name", "ASC"),
            "Builder Z-A": ("builder_name", "DESC"),
            "Municipality": ("municipality", "ASC"),
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

    # ── Helper functions ──
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

    def _website_link(url):
        if not url:
            return '<span class="empty-cell">&mdash;</span>'
        try:
            host = urlparse(url).hostname or url
            host = host.replace("www.", "")
        except Exception:
            host = url
        return f'<a href="{esc(url)}" target="_blank" style="color:#60A5FA;text-decoration:none;font-size:.7rem">{esc(host)}</a>'

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

        # Build permit cards
        cards_html = ""
        for p in result["data"]:
            pv = p.get("project_value") or 0
            is_hv = pv >= 300000
            hv_border = "border-left:3px solid #2B6CB0;" if is_hv else ""
            val_color = "color:#2B6CB0;" if is_hv else "color:#E2E8F0;"

            # Profit estimate (live based on margin input)
            profit = calculate_drywall_profit(pv, margin_pct)
            profit_html = fmt_money(profit["profit_opportunity"]) if pv else "&mdash;"

            bn = p.get("builder_name") or ""
            bc = p.get("builder_company") or ""
            if bn and bc:
                builder_text = f'{esc(bn)} <span style="color:#94A3B8;font-size:.65rem">@ {esc(bc)}</span>'
            elif bn or bc:
                builder_text = esc(bn or bc)
            else:
                builder_text = '<span class="empty-cell">&mdash;</span>'

            addr_esc = esc(p.get("address") or "") or "&mdash;"
            muni_esc = esc(p.get("municipality") or "") or "&mdash;"
            score_html = score_badge(p.get("opportunity_score"))
            date_html = fmt_date(p.get("inspection_date"))

            bw = p.get("builder_website") or ""
            web_link = _website_link(bw)
            web_email = _email_link(p.get("builder_email"))
            web_phone = _phone_link(p.get("builder_phone"))

            # Summary row columns: Address | City | Builder | Website | Email | Phone | Value | Profit Est | Score
            summary_html = (
                f'<div style="display:grid;'
                f'grid-template-columns:2fr 0.8fr 1.4fr 1fr 1fr 0.9fr 0.7fr 0.7fr 0.5fr;'
                f'gap:6px;align-items:center;padding:10px 14px;font-size:.76rem">'
                f'<div style="color:#F8FAFC;font-weight:500;overflow:hidden;text-overflow:ellipsis;'
                f'white-space:nowrap" title="{addr_esc}">{addr_esc}</div>'
                f'<div style="color:#94A3B8;font-size:.68rem">{muni_esc}</div>'
                f'<div style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{builder_text}</div>'
                f'<div style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{web_link}</div>'
                f'<div style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{web_email}</div>'
                f'<div>{web_phone}</div>'
                f'<div style="text-align:right;font-family:Fira Code,monospace;font-size:.72rem;{val_color}">{fmt_money(pv)}</div>'
                f'<div style="text-align:right;font-family:Fira Code,monospace;font-size:.72rem;color:#4ADE80">{profit_html}</div>'
                f'<div style="text-align:center">{score_html}</div>'
                f'</div>'
            )

            # Expanded detail panel with ALL data
            detail_fields = build_detail_panel(p)

            # Profit section in expanded view — just value + profit estimate
            profit_detail = (
                f'<div class="detail-panel" style="border-top:1px solid rgba(43,108,176,0.15);'
                f'background:rgba(34,197,94,0.03)">'
                f'<div class="detail-field"><div class="detail-label">Permit Value</div>'
                f'<div class="detail-value" style="font-family:Fira Code,monospace">{fmt_money(pv)}</div></div>'
                f'<div class="detail-field"><div class="detail-label">Profit Estimate ({margin_pct}%)</div>'
                f'<div class="detail-value" style="font-family:Fira Code,monospace;color:#4ADE80;font-weight:700">'
                f'{fmt_money(profit["profit_opportunity"])}</div></div>'
            )
            if bw:
                profit_detail += (
                    f'<div class="detail-field"><div class="detail-label">Builder Website</div>'
                    f'<div class="detail-value"><a href="{esc(bw)}" target="_blank" '
                    f'style="color:#60A5FA">{esc(bw)}</a></div></div>'
                )
            per_ph = p.get("personal_phone") or ""
            per_em = p.get("personal_email") or ""
            if per_ph:
                profit_detail += (
                    f'<div class="detail-field"><div class="detail-label">Personal Phone</div>'
                    f'<div class="detail-value">{_phone_link(per_ph)}</div></div>'
                )
            if per_em:
                profit_detail += (
                    f'<div class="detail-field"><div class="detail-label">Personal Email</div>'
                    f'<div class="detail-value">{_email_link(per_em)}</div></div>'
                )
            profit_detail += '</div>'

            cards_html += (
                f'<details class="permit-detail" style="{hv_border}">'
                f'<summary style="cursor:pointer;list-style:none">{summary_html}</summary>'
                f'{detail_fields}{profit_detail}'
                f'</details>'
            )

        watermark = ""
        if LOGO_B64:
            watermark = (
                f"background:url('data:image/png;base64,{LOGO_B64}') no-repeat center center;"
                f"background-size:contain;opacity:0.06;"
            )

        # Column headers matching summary grid
        header_html = (
            f'<div style="display:grid;'
            f'grid-template-columns:2fr 0.8fr 1.4fr 1fr 1fr 0.9fr 0.7fr 0.7fr 0.5fr;'
            f'gap:6px;padding:10px 14px;font-size:.55rem;font-weight:600;text-transform:uppercase;'
            f'letter-spacing:.07em;color:#94A3B8;background:rgba(15,23,42,0.5);'
            f'border-bottom:1px solid rgba(255,255,255,0.06)">'
            f'<div>Address</div><div>City</div><div>Builder</div>'
            f'<div>Website</div><div>Email</div><div>Phone</div>'
            f'<div style="text-align:right">Value</div>'
            f'<div style="text-align:right">Profit Est</div>'
            f'<div style="text-align:center">Score</div>'
            f'</div>'
        )

        full_html = (
            f'<div style="border-radius:16px;border:1px solid rgba(255,255,255,0.08);'
            f'background:rgba(255,255,255,0.02);position:relative;overflow:hidden">'
            f'<div style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);'
            f'width:400px;height:400px;{watermark}pointer-events:none;z-index:0"></div>'
            f'<div style="position:relative;z-index:1">'
            f'{header_html}{cards_html}</div></div>'
        )
        st.markdown(full_html, unsafe_allow_html=True)

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
