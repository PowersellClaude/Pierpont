"""
Pierpont Money Printer — SC Lowcountry Construction Lead Intelligence
Streamlit Cloud Dashboard
"""

import streamlit as st
import sqlite3
import pandas as pd
import os
import math
import json
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote

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
ENERGOV_BASE = "https://egcss.charleston-sc.gov/EnerGov_Prod/selfservice"

DRIVE_TIMES = {
    "Town of Mount Pleasant": 0,
    "Sullivan's Island": 15,
    "City of Charleston": 20,
    "Isle of Palms": 20,
    "Charleston County": 20,
    "City of North Charleston": 25,
    "City of Hanahan": 25,
    "Kiawah Island": 35,
    "Seabrook Island": 35,
    "City of Folly Beach": 35,
    "Town of Summerville": 35,
    "City of Goose Creek": 35,
    "Berkeley County": 45,
    "Dorchester County": 45,
    "Town of Moncks Corner": 50,
    "Georgetown County": 70,
    "Colleton County": 70,
    "Town of Bluffton": 75,
    "City of Beaufort": 80,
    "Town of Hilton Head Island": 80,
    "City of Hardeeville": 85,
    "Williamsburg County": 85,
    "Orangeburg County": 85,
}

FOIA_BODY = """To Whom It May Concern,

Pursuant to the South Carolina Freedom of Information Act, I am a taxpaying citizen requesting the following records for research purposes only:

A list of all strapping inspections (also known as strap/banding inspections) that received a passing status within the last 90 days, including permit number, property address, contractor/builder name, inspection date, and status.

Thank you for your time."""

FOIA_MUNICIPALITIES = [
    {"name": "City of Folly Beach", "type": "email", "email": "permits@follybeach.gov", "drive": 35},
    {"name": "City of Hanahan", "type": "portal", "url": "https://cityofhanahansc.nextrequest.com/requests/new", "portal_name": "NextRequest Portal", "drive": 25},
    {"name": "Town of Moncks Corner", "type": "email", "email": "info@monckscornersc.gov", "drive": 50},
    {"name": "Georgetown County", "type": "email", "email": "cityfoiarequest@georgetownsc.gov", "drive": 70},
    {"name": "Colleton County", "type": "email", "email": "foia@colletoncounty.org", "drive": 70},
    {"name": "City of Beaufort", "type": "portal", "url": "https://beaufortcountysc.justfoia.com/publicportal/home/newrequest", "portal_name": "JustFOIA Portal", "drive": 80},
    {"name": "Williamsburg County", "type": "email", "email": "FOIA-Request@wc.sc.gov", "drive": 85},
    {"name": "Orangeburg County", "type": "email", "email": "foia@orangeburgcounty.org", "drive": 85},
]


# ─── Custom CSS ─────────────────────────────────────────────────────────────
def inject_css():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Fira+Code:wght@400;500;600;700&family=Fira+Sans:wght@300;400;500;600;700&display=swap');

    .stApp {
        font-family: 'Fira Sans', system-ui, sans-serif;
    }
    .main .block-container { padding-top: 1rem; max-width: 1400px; }

    /* Header */
    .pierpont-header {
        background: rgba(15,23,42,0.7);
        backdrop-filter: blur(30px);
        border: 1px solid rgba(255,255,255,0.1);
        border-top: 3px solid #2B6CB0;
        border-radius: 0 0 16px 16px;
        padding: 20px 28px;
        margin-bottom: 20px;
        box-shadow: 0 8px 32px rgba(0,0,0,0.4);
    }
    .pierpont-title {
        font-family: 'Fira Code', monospace;
        font-weight: 700; font-size: 1.6rem;
        background: linear-gradient(135deg, #3B82C4, #2B6CB0, #6B7B8D);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
        margin: 0;
    }
    .pierpont-sub {
        font-size: .8rem; color: #94A3B8; font-weight: 300;
        letter-spacing: 0.05em; margin: 0;
    }

    /* Glass cards */
    .glass-card {
        background: rgba(255,255,255,0.04);
        backdrop-filter: blur(16px);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 16px;
        padding: 20px;
        transition: all 0.3s;
    }
    .glass-card:hover {
        background: rgba(255,255,255,0.07);
        border-color: rgba(255,255,255,0.15);
    }

    /* Stat cards */
    .stat-card {
        background: rgba(255,255,255,0.04);
        backdrop-filter: blur(16px);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 16px;
        padding: 20px;
        text-align: center;
    }
    .stat-label {
        font-size: .7rem; font-weight: 600; text-transform: uppercase;
        letter-spacing: 0.08em; color: #94A3B8; margin-bottom: 8px;
    }
    .stat-value {
        font-family: 'Fira Code', monospace;
        font-size: 1.6rem; font-weight: 700; color: #F8FAFC;
    }
    .stat-value-blue {
        font-family: 'Fira Code', monospace;
        font-size: 1.6rem; font-weight: 700; color: #2B6CB0;
    }

    /* Badges */
    .badge { display:inline-flex;align-items:center;padding:3px 10px;border-radius:9999px;font-size:.7rem;font-weight:600;font-family:'Fira Code',monospace; }
    .badge-green { background:rgba(34,197,94,0.15);color:#4ADE80;border:1px solid rgba(34,197,94,0.2); }
    .badge-yellow { background:rgba(234,179,8,0.15);color:#FDE047;border:1px solid rgba(234,179,8,0.2); }
    .badge-red { background:rgba(239,68,68,0.15);color:#FCA5A5;border:1px solid rgba(239,68,68,0.2); }
    .badge-gray { background:rgba(148,163,184,0.1);color:#94A3B8;border:1px solid rgba(148,163,184,0.15); }
    .badge-blue { background:rgba(59,130,246,0.15);color:#93C5FD;border:1px solid rgba(59,130,246,0.2); }

    /* FOIA card */
    .foia-card {
        background: rgba(255,255,255,0.04);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 12px;
        padding: 12px 16px;
        display: flex; align-items: center; justify-content: space-between;
    }
    .foia-name { font-size: .85rem; font-weight: 500; color: #F8FAFC; }
    .foia-detail { font-size: .65rem; color: #94A3B8; }
    .foia-link {
        background: rgba(43,108,176,0.1); color: #93C5FD;
        border: 1px solid rgba(43,108,176,0.2);
        padding: 6px 14px; border-radius: 8px;
        font-size: .75rem; font-weight: 600; text-decoration: none;
    }
    .foia-link:hover { background: rgba(43,108,176,0.2); }

    /* Login card */
    .login-card {
        background: rgba(255,255,255,0.06);
        backdrop-filter: blur(20px);
        border: 1px solid rgba(255,255,255,0.1);
        border-radius: 20px;
        padding: 40px;
        max-width: 380px;
        margin: 15vh auto;
        box-shadow: 0 8px 32px rgba(0,0,0,0.4);
        text-align: center;
    }
    .login-title {
        font-family: 'Fira Code', monospace;
        font-size: 1.3rem; font-weight: 700;
        background: linear-gradient(135deg, #3B82C4, #2B6CB0, #6B7B8D);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }

    /* Hide Streamlit defaults */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none;}
    header[data-testid="stHeader"] {background: transparent;}

    /* Table styling */
    .dataframe { font-size: .8rem !important; }
    .dataframe th {
        font-size: .65rem !important; text-transform: uppercase;
        letter-spacing: 0.08em; color: #94A3B8 !important;
    }
    </style>
    """, unsafe_allow_html=True)


# ─── Database ───────────────────────────────────────────────────────────────
def get_db():
    """Get SQLite connection, creating schema if needed."""
    db_dir = os.path.dirname(DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Create schema if needed
    conn.execute("""
        CREATE TABLE IF NOT EXISTS permits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            permit_number TEXT UNIQUE,
            address TEXT NOT NULL,
            municipality TEXT NOT NULL,
            builder_name TEXT,
            builder_company TEXT,
            builder_phone TEXT,
            builder_email TEXT,
            applicant_name TEXT,
            applicant_phone TEXT,
            applicant_email TEXT,
            owner_name TEXT,
            project_value REAL,
            permit_type TEXT,
            inspection_type TEXT,
            inspection_date TEXT,
            inspection_status TEXT,
            permit_issue_date TEXT,
            source_url TEXT,
            scraped_at TEXT DEFAULT (datetime('now')),
            raw_data TEXT,
            is_drywall_opportunity INTEGER DEFAULT 0,
            opportunity_confidence TEXT,
            opportunity_signals TEXT,
            estimated_drywall_date TEXT,
            opportunity_score INTEGER,
            builder_website TEXT,
            personal_phone TEXT,
            personal_email TEXT
        )
    """)
    conn.execute("CREATE TABLE IF NOT EXISTS scrape_runs (id INTEGER PRIMARY KEY AUTOINCREMENT, started_at TEXT, completed_at TEXT, status TEXT, permits_found INTEGER, permits_new INTEGER)")
    conn.commit()
    return conn


def query_permits(conn, filters=None, sort_by="opportunity_score", sort_order="DESC", page=1, per_page=50):
    """Query permits with filters, sorting, pagination."""
    conditions = []
    values = []

    if filters:
        if filters.get("search"):
            s = f"%{filters['search']}%"
            conditions.append("(address LIKE ? OR municipality LIKE ? OR builder_name LIKE ? OR builder_company LIKE ? OR owner_name LIKE ? OR permit_number LIKE ? OR builder_phone LIKE ? OR builder_email LIKE ?)")
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
            max_min = int(filters["max_drive_time"])
            in_range = [name for name, mins in DRIVE_TIMES.items() if mins <= max_min]
            if in_range:
                placeholders = ",".join(["?"] * len(in_range))
                conditions.append(f"municipality IN ({placeholders})")
                values.extend(in_range)
            else:
                conditions.append("1=0")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    allowed_sorts = ["municipality", "address", "builder_name", "project_value", "inspection_date", "opportunity_score", "permit_number"]
    if sort_by not in allowed_sorts:
        sort_by = "opportunity_score"

    count_row = conn.execute(f"SELECT COUNT(*) as total FROM permits {where}", values).fetchone()
    total = count_row[0] if count_row else 0

    offset = (page - 1) * per_page
    rows = conn.execute(
        f"SELECT * FROM permits {where} ORDER BY {sort_by} {sort_order} LIMIT ? OFFSET ?",
        values + [per_page, offset]
    ).fetchall()

    return {
        "data": [dict(r) for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": max(1, math.ceil(total / per_page)),
    }


def get_stats(conn):
    """Get aggregate stats."""
    row = conn.execute("""
        SELECT COUNT(*) as total_permits,
               COALESCE(AVG(project_value), 0) as avg_value,
               MIN(inspection_date) as earliest_date,
               MAX(inspection_date) as latest_date
        FROM permits
    """).fetchone()

    hv = conn.execute("SELECT COUNT(*) FROM permits WHERE project_value >= 300000").fetchone()

    return {
        "total_permits": row[0] if row else 0,
        "avg_value": row[1] if row else 0,
        "earliest_date": row[2] if row else None,
        "latest_date": row[3] if row else None,
        "high_value_count": hv[0] if hv else 0,
    }


def get_municipalities(conn):
    """Get distinct municipalities."""
    rows = conn.execute("SELECT DISTINCT municipality FROM permits WHERE municipality IS NOT NULL AND municipality != '' ORDER BY municipality").fetchall()
    return [r[0] for r in rows]


def get_csv_export(conn, filters=None):
    """Export permits as CSV."""
    conditions = []
    values = []
    if filters:
        if filters.get("search"):
            s = f"%{filters['search']}%"
            conditions.append("(address LIKE ? OR builder_name LIKE ? OR permit_number LIKE ?)")
            values.extend([s, s, s])
        if filters.get("municipality"):
            conditions.append("municipality = ?")
            values.append(filters["municipality"])
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    df = pd.read_sql_query(f"SELECT * FROM permits {where} ORDER BY inspection_date DESC", conn, params=values)
    return df


# ─── Score Badge ────────────────────────────────────────────────────────────
def score_badge_html(score):
    if score is None:
        return '<span class="badge badge-gray">--</span>'
    s = int(score)
    if s >= 70:
        return f'<span class="badge badge-green">{s}</span>'
    elif s >= 40:
        return f'<span class="badge badge-yellow">{s}</span>'
    elif s >= 1:
        return f'<span class="badge badge-red">{s}</span>'
    return f'<span class="badge badge-gray">{s}</span>'


def status_badge_html(status):
    if not status:
        return '<span class="badge badge-gray">—</span>'
    lower = status.lower()
    if "pass" in lower or "approved" in lower:
        return f'<span class="badge badge-green">{status}</span>'
    elif "pending" in lower or "scheduled" in lower:
        return f'<span class="badge badge-yellow">{status}</span>'
    return f'<span class="badge badge-gray">{status}</span>'


def fmt_currency(val):
    if val is None or val == 0:
        return "—"
    return f"${int(val):,}"


def fmt_date(d):
    if not d:
        return "—"
    try:
        dt = datetime.strptime(d[:10], "%Y-%m-%d")
        return dt.strftime("%b %d, %Y")
    except:
        return d


# ─── Auth ───────────────────────────────────────────────────────────────────
def check_auth():
    """Handle login gate."""
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if st.session_state.authenticated:
        return True

    inject_css()

    # Login form
    st.markdown("""
    <div class="login-card">
        <h1 class="login-title">Pierpont Money Printer</h1>
        <p style="font-size:.75rem;color:#94A3B8;margin-bottom:24px;letter-spacing:.04em">SC Lowcountry Construction Lead Intelligence</p>
    </div>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 1, 1])
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
    logo_path = os.path.join(os.path.dirname(__file__), "public", "logo.png")
    header_cols = st.columns([0.06, 0.5, 0.44])
    with header_cols[0]:
        if os.path.exists(logo_path):
            st.image(logo_path, width=48)
    with header_cols[1]:
        st.markdown("""
        <div>
            <h1 class="pierpont-title">Pierpont Money Printer</h1>
            <p class="pierpont-sub">SC Lowcountry Construction Lead Intelligence</p>
        </div>
        """, unsafe_allow_html=True)
    with header_cols[2]:
        logout_col1, logout_col2 = st.columns([3, 1])
        with logout_col2:
            if st.button("Logout", type="secondary"):
                st.session_state.authenticated = False
                st.rerun()

    st.markdown("---")

    # ── Stats ──
    stats = get_stats(conn)
    stat_cols = st.columns(4)
    with stat_cols[0]:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-label">Total Permits</div>
            <div class="stat-value">{stats['total_permits']:,}</div>
        </div>""", unsafe_allow_html=True)
    with stat_cols[1]:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-label">Avg Value</div>
            <div class="stat-value">{fmt_currency(stats['avg_value'])}</div>
        </div>""", unsafe_allow_html=True)
    with stat_cols[2]:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-label">$300K+ Projects</div>
            <div class="stat-value-blue">{stats['high_value_count']:,}</div>
        </div>""", unsafe_allow_html=True)
    with stat_cols[3]:
        date_range = "—"
        if stats["earliest_date"] and stats["latest_date"]:
            date_range = f"{fmt_date(stats['earliest_date'])} – {fmt_date(stats['latest_date'])}"
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-label">Date Range</div>
            <div style="font-family:'Fira Code',monospace;font-size:.85rem;color:#E2E8F0;margin-top:8px">{date_range}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("")

    # ── Filters ──
    municipalities = get_municipalities(conn)

    with st.expander("🔍 Filters", expanded=True):
        fc1, fc2 = st.columns([3, 1])
        with fc1:
            search = st.text_input("Search", placeholder="Address, builder, phone, email, permit #...", label_visibility="collapsed")
        with fc2:
            distance_options = {"All distances": "", "Within 15 min": "15", "Within 30 min": "30", "Within 45 min": "45", "Within 60 min": "60", "Within 90 min": "90"}
            max_drive = st.selectbox("Max Distance", options=list(distance_options.keys()), label_visibility="collapsed")

        fc3, fc4, fc5, fc6, fc7 = st.columns(5)
        with fc3:
            muni = st.selectbox("Municipality", ["All municipalities"] + municipalities, label_visibility="collapsed")
        with fc4:
            date_from = st.date_input("Date From", value=None, label_visibility="collapsed")
        with fc5:
            date_to = st.date_input("Date To", value=None, label_visibility="collapsed")
        with fc6:
            min_val = st.number_input("Min Value ($)", min_value=0, value=0, step=50000, label_visibility="collapsed", format="%d")
        with fc7:
            max_val = st.number_input("Max Value ($)", min_value=0, value=0, step=50000, label_visibility="collapsed", format="%d")

    # Build filters dict
    filters = {}
    if search:
        filters["search"] = search
    if distance_options[max_drive]:
        filters["max_drive_time"] = distance_options[max_drive]
    if muni != "All municipalities":
        filters["municipality"] = muni
    if date_from:
        filters["date_from"] = date_from.strftime("%Y-%m-%d")
    if date_to:
        filters["date_to"] = date_to.strftime("%Y-%m-%d")
    if min_val > 0:
        filters["min_value"] = min_val
    if max_val > 0:
        filters["max_value"] = max_val

    # ── Sort & Pagination ──
    sort_cols = st.columns([1, 1, 4])
    with sort_cols[0]:
        sort_options = {
            "Score (High→Low)": ("opportunity_score", "DESC"),
            "Score (Low→High)": ("opportunity_score", "ASC"),
            "Date (Newest)": ("inspection_date", "DESC"),
            "Date (Oldest)": ("inspection_date", "ASC"),
            "Value (High→Low)": ("project_value", "DESC"),
            "Value (Low→High)": ("project_value", "ASC"),
            "Municipality A-Z": ("municipality", "ASC"),
            "Address A-Z": ("address", "ASC"),
        }
        sort_choice = st.selectbox("Sort by", list(sort_options.keys()), index=0)
        sort_by, sort_order = sort_options[sort_choice]

    with sort_cols[1]:
        per_page = st.selectbox("Per page", [25, 50, 100, 200], index=1)

    # Page state
    if "page" not in st.session_state:
        st.session_state.page = 1

    result = query_permits(conn, filters=filters, sort_by=sort_by, sort_order=sort_order, page=st.session_state.page, per_page=per_page)

    # ── Results Table ──
    if not result["data"]:
        st.markdown("""
        <div style="text-align:center;padding:60px 20px;color:#94A3B8">
            <div style="font-size:1.1rem;font-weight:500;color:#E2E8F0;margin-bottom:8px">No permits loaded</div>
            <div>Run the Node.js scraper locally first, then the data will appear here.</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        # Show results count
        start = (result["page"] - 1) * result["per_page"] + 1
        end = min(result["page"] * result["per_page"], result["total"])
        st.markdown(f"<div style='font-size:.75rem;color:#94A3B8;font-family:Fira Code,monospace;margin-bottom:8px'>{start}–{end} of {result['total']:,} permits</div>", unsafe_allow_html=True)

        # Build table HTML
        table_html = """
        <div style="overflow-x:auto;border-radius:12px;border:1px solid rgba(255,255,255,0.08);background:rgba(255,255,255,0.02)">
        <table style="width:100%;border-collapse:collapse;font-size:.78rem">
        <thead>
        <tr style="background:rgba(15,23,42,0.6)">
            <th style="padding:10px 12px;text-align:left;font-size:.6rem;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:#94A3B8;border-bottom:1px solid rgba(255,255,255,0.06)">Address</th>
            <th style="padding:10px 12px;text-align:left;font-size:.6rem;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:#94A3B8;border-bottom:1px solid rgba(255,255,255,0.06)">Municipality</th>
            <th style="padding:10px 12px;text-align:left;font-size:.6rem;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:#94A3B8;border-bottom:1px solid rgba(255,255,255,0.06)">Builder</th>
            <th style="padding:10px 12px;text-align:left;font-size:.6rem;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:#94A3B8;border-bottom:1px solid rgba(255,255,255,0.06)">Phone</th>
            <th style="padding:10px 12px;text-align:left;font-size:.6rem;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:#94A3B8;border-bottom:1px solid rgba(255,255,255,0.06)">Email</th>
            <th style="padding:10px 12px;text-align:left;font-size:.6rem;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:#94A3B8;border-bottom:1px solid rgba(255,255,255,0.06)">Owner</th>
            <th style="padding:10px 12px;text-align:right;font-size:.6rem;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:#94A3B8;border-bottom:1px solid rgba(255,255,255,0.06)">Value</th>
            <th style="padding:10px 12px;text-align:left;font-size:.6rem;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:#94A3B8;border-bottom:1px solid rgba(255,255,255,0.06)">Date</th>
            <th style="padding:10px 12px;text-align:center;font-size:.6rem;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:#94A3B8;border-bottom:1px solid rgba(255,255,255,0.06)">Status</th>
            <th style="padding:10px 12px;text-align:center;font-size:.6rem;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:#94A3B8;border-bottom:1px solid rgba(255,255,255,0.06)">Score</th>
            <th style="padding:10px 12px;text-align:left;font-size:.6rem;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:#94A3B8;border-bottom:1px solid rgba(255,255,255,0.06)">Permit #</th>
        </tr>
        </thead>
        <tbody>
        """

        for p in result["data"]:
            is_hv = p.get("project_value") and p["project_value"] >= 300000
            border_style = "border-left:3px solid #2B6CB0;" if is_hv else ""
            val_color = "color:#2B6CB0;" if is_hv else ""

            builder = ""
            if p.get("builder_name") and p.get("builder_company"):
                builder = f"<div style='font-weight:500;color:#F8FAFC'>{p['builder_name']}</div><div style='font-size:.65rem;color:#94A3B8'>{p['builder_company']}</div>"
            elif p.get("builder_name") or p.get("builder_company"):
                builder = f"<span style='font-weight:500;color:#F8FAFC'>{p.get('builder_name') or p.get('builder_company')}</span>"
            else:
                builder = "<span style='color:rgba(148,163,184,0.4)'>—</span>"

            phone = f"<a href='tel:{p['builder_phone']}' style='color:#60A5FA;text-decoration:none;font-family:Fira Code,monospace;font-size:.7rem'>{p['builder_phone']}</a>" if p.get("builder_phone") else "<span style='color:rgba(148,163,184,0.4)'>—</span>"
            email = f"<a href='mailto:{p['builder_email']}' style='color:#60A5FA;text-decoration:none;font-size:.7rem'>{p['builder_email']}</a>" if p.get("builder_email") else "<span style='color:rgba(148,163,184,0.4)'>—</span>"

            table_html += f"""
            <tr style="border-bottom:1px solid rgba(255,255,255,0.03);{border_style}">
                <td style="padding:8px 12px;color:#F8FAFC;font-weight:500;max-width:200px"><div style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="{p.get('address', '')}">{p.get('address') or '—'}</div></td>
                <td style="padding:8px 12px;color:#94A3B8;font-size:.7rem">{p.get('municipality') or '—'}</td>
                <td style="padding:8px 12px">{builder}</td>
                <td style="padding:8px 12px">{phone}</td>
                <td style="padding:8px 12px">{email}</td>
                <td style="padding:8px 12px;color:#E2E8F0">{p.get('owner_name') or '—'}</td>
                <td style="padding:8px 12px;text-align:right;font-family:Fira Code,monospace;font-size:.75rem;{val_color}">{fmt_currency(p.get('project_value'))}</td>
                <td style="padding:8px 12px;font-family:Fira Code,monospace;font-size:.7rem;color:#E2E8F0">{fmt_date(p.get('inspection_date'))}</td>
                <td style="padding:8px 12px;text-align:center">{status_badge_html(p.get('inspection_status'))}</td>
                <td style="padding:8px 12px;text-align:center">{score_badge_html(p.get('opportunity_score'))}</td>
                <td style="padding:8px 12px;font-family:Fira Code,monospace;font-size:.65rem;color:#94A3B8">{p.get('permit_number') or '—'}</td>
            </tr>
            """

        table_html += "</tbody></table></div>"
        st.markdown(table_html, unsafe_allow_html=True)

        # Pagination
        if result["total_pages"] > 1:
            pg_cols = st.columns([1, 1, 1, 1, 1])
            with pg_cols[1]:
                if st.button("⬅ Previous", disabled=st.session_state.page <= 1):
                    st.session_state.page -= 1
                    st.rerun()
            with pg_cols[2]:
                st.markdown(f"<div style='text-align:center;padding:8px;color:#94A3B8;font-family:Fira Code,monospace;font-size:.8rem'>Page {result['page']} of {result['total_pages']}</div>", unsafe_allow_html=True)
            with pg_cols[3]:
                if st.button("Next ➡", disabled=st.session_state.page >= result["total_pages"]):
                    st.session_state.page += 1
                    st.rerun()

    # ── CSV Export ──
    st.markdown("")
    exp_cols = st.columns([1, 1, 4])
    with exp_cols[0]:
        if st.button("📥 Export CSV"):
            df = get_csv_export(conn, filters)
            csv = df.to_csv(index=False)
            st.download_button("Download CSV", csv, f"pierpont-permits-{datetime.now().strftime('%Y-%m-%d')}.csv", "text/csv")

    # ── FOIA Section ──
    st.markdown("")
    with st.expander("📋 FOIA Requests — Municipalities Without Public Portals"):
        st.markdown("<p style='font-size:.8rem;color:#94A3B8;margin-bottom:12px'>These municipalities require a SC FOIA request to obtain permit data.</p>", unsafe_allow_html=True)

        foia_html = '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:8px">'
        for m in FOIA_MUNICIPALITIES:
            if m["type"] == "email":
                subject = quote("FOIA REQUEST — Strapping Inspections")
                body = quote(FOIA_BODY)
                mailto = f"mailto:{m['email']}?subject={subject}&body={body}"
                foia_html += f"""
                <div class="foia-card">
                    <div>
                        <div class="foia-name">{m['name']}</div>
                        <div class="foia-detail">{m['email']}</div>
                    </div>
                    <a href="{mailto}" class="foia-link" target="_blank">Send Request</a>
                </div>"""
            else:
                foia_html += f"""
                <div class="foia-card">
                    <div>
                        <div class="foia-name">{m['name']}</div>
                        <div class="foia-detail">{m['portal_name']}</div>
                    </div>
                    <a href="{m['url']}" class="foia-link" target="_blank">Open Portal</a>
                </div>"""
        foia_html += "</div>"
        st.markdown(foia_html, unsafe_allow_html=True)

        st.markdown("")
        st.code(FOIA_BODY, language=None)
        st.caption("Copy the text above and paste it into the FOIA portal request form.")

    # ── Info Section ──
    st.markdown("")
    with st.expander("ℹ️ About This Dashboard"):
        st.markdown("""
        **Pierpont Money Printer** tracks construction permits across the SC Lowcountry.

        **How it works:**
        - The **Node.js scraper** runs locally (or on a server) to collect permit data from Charleston's EnerGov portal and other municipal sources
        - Data is stored in SQLite and displayed in this dashboard
        - **Opportunity scores** (0-100) rank permits by value (40%), recency (30%), and distance from 29464 (30%)
        - **Builder lookup** automatically finds contractor websites and extracts contact info

        **Note:** The scraper must be run from the Node.js server (`node server.js`). This Streamlit dashboard is read-only and displays the data collected by the scraper.
        """)

    conn.close()


if __name__ == "__main__":
    main()
