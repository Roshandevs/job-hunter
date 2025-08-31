#!/usr/bin/env python3
"""
Job Hunter
- Uses RapidAPI JSearch (jsearch.p.rapidapi.com) by default to fetch jobs.
- Filters for fresher / 0-6 months, recent postings (<= MAX_POST_AGE_HOURS),
  roles and skills you want.
- Stores jobs into SQLite and avoids duplicates.
- Emails new matches via SMTP (non-Gmail supported).
- Runs immediately and then every RUN_EVERY_HOURS using APScheduler.
"""

import os
import re
import time
import json
import sqlite3
import smtplib
import logging
import traceback
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta, timezone

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv

# ---------------------------
# Load config
# ---------------------------
load_dotenv()

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "").strip()
RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST", "jsearch.p.rapidapi.com").strip()
JSEARCH_ENDPOINT = os.getenv("JSEARCH_ENDPOINT", "https://jsearch.p.rapidapi.com/search").strip()

SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASS = os.getenv("SMTP_PASS", "").strip()
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "true").lower() == "true"
EMAIL_FROM = os.getenv("EMAIL_FROM", SMTP_USER or "jobs@yourdomain.com")
EMAIL_TO = os.getenv("EMAIL_TO", "roshanpanda01@gmail.com")

DEFAULT_COUNTRY = os.getenv("DEFAULT_COUNTRY", "India")
INCLUDE_REMOTE = os.getenv("INCLUDE_REMOTE", "true").lower() == "true"
RUN_EVERY_HOURS = float(os.getenv("RUN_EVERY_HOURS", "2.0"))

DB_PATH = os.getenv("DB_PATH", "jobs.db")
TEMPLATE_PATH = os.getenv("TEMPLATE_PATH", "templates/email_template.html")

MAX_POST_AGE_HOURS = float(os.getenv("MAX_POST_AGE_HOURS", "6"))
# JSON arrays in env (edit in .env)
ROLE_KEYWORDS = json.loads(os.getenv("ROLE_KEYWORDS", '["full stack","frontend","front-end","backend","back-end","software developer","software engineer","web developer"]'))
SKILL_KEYWORDS = json.loads(os.getenv("SKILL_KEYWORDS", '["python","html","css","javascript","bootstrap","jquery","ajax","laravel","php","vue","vuejs","angular","react","reactjs"]'))
EXP_KEYWORDS = json.loads(os.getenv("EXP_KEYWORDS", '["fresher","0-6 months","0 to 6 months","0-1 year","0 to 1 year","no experience","entry level","junior","trainee"]'))
LOCATIONS = json.loads(os.getenv("LOCATIONS", '["India","Remote","Bengaluru","Bangalore","Hyderabad","Pune","Mumbai","Navi Mumbai","Delhi","Noida","Gurgaon","Chennai","Kolkata","Ahmedabad","Indore","Jaipur","Surat","Nagpur","Chandigarh"]'))

MAX_RESULTS_PER_RUN = int(os.getenv("MAX_RESULTS_PER_RUN", "80"))
PAGES_PER_QUERY = int(os.getenv("PAGES_PER_QUERY", "2"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s")

# ---------------------------
# DB: sqlite
# ---------------------------
def init_db(path: str):
    conn = sqlite3.connect(path, check_same_thread=False)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            external_id TEXT,
            title TEXT,
            company TEXT,
            location TEXT,
            url TEXT,
            posted_at_utc INTEGER,
            description TEXT,
            created_at_utc INTEGER
        );
    """)
    # unique constraints to reduce duplicates (external + fallback)
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_jobs_source_external ON jobs(source, external_id)")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_jobs_fallback ON jobs(title, company, location, url)")
    conn.commit()
    return conn

# ---------------------------
# RapidAPI JSearch provider
# ---------------------------
def jsearch_headers():
    return {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": RAPIDAPI_HOST,
    }

def jsearch_query(role_kw: str, location: str, page: int = 1):
    params = {
        "query": f"{role_kw} in {location}",
        "page": str(page),
        "num_pages": "1",  # we'll loop pages externally
        "date_posted": "today"
    }
    resp = requests.get(JSEARCH_ENDPOINT, headers=jsearch_headers(), params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def parse_jsearch_item(item: dict) -> dict:
    # robust mapping with fallbacks
    job_id = item.get("job_id") or item.get("job_posting_id") or item.get("id")
    title = (item.get("job_title") or item.get("title") or "").strip()
    company = (item.get("employer_name") or item.get("company_name") or "").strip()
    location = (item.get("job_city") or item.get("job_location") or item.get("job_country") or item.get("job_state") or ("Remote" if item.get("job_is_remote") else "")).strip()
    desc = (item.get("job_description") or item.get("description") or "").strip()
    url = item.get("job_apply_link") or (item.get("job_google_link") or "")
    # posted timestamp (JSearch often gives unix epoch seconds)
    ts = 0
    if item.get("job_posted_at_timestamp"):
        try:
            ts = int(item.get("job_posted_at_timestamp"))
        except Exception:
            ts = 0
    return {
        "source": "jsearch",
        "external_id": str(job_id) if job_id else None,
        "title": title,
        "company": company,
        "location": location,
        "description": desc,
        "url": url,
        "posted_at_utc": ts
    }

# ---------------------------
# Filtering helpers
# ---------------------------
def contains_any(text: str, keywords):
    text_low = (text or "").lower()
    return any(kw.lower() in text_low for kw in keywords)

def experience_ok(text: str) -> bool:
    # quick keyword check
    if contains_any(text, EXP_KEYWORDS):
        return True
    # regex patterns for "0-6 months", "6 months", "no experience", "fresher", "entry-level"
    patterns = [
        r"\b0\s*[-–]\s*6\s*months?\b",
        r"\b0\s*to\s*6\s*months?\b",
        r"\b0\s*[-–]\s*1\s*years?\b",
        r"\b0\s*to\s*1\s*years?\b",
        r"\b(up\s*to\s*)?6\s*months?\s*(of)?\s*experience\b",
        r"\bfresher(s)?\b",
        r"\bentry[-\s]?level\b",
        r"\bno\s+experience\b",
        r"\b0\+?\s*years?\b"
    ]
    txt = (text or "").lower()
    for p in patterns:
        if re.search(p, txt):
            return True
    return False

def role_ok(text: str) -> bool:
    return contains_any(text, ROLE_KEYWORDS)

def skills_ok(text: str) -> bool:
    return contains_any(text, SKILL_KEYWORDS)

def posted_recent(posted_ts: int, max_age_hours: float) -> bool:
    if not posted_ts:
        return False
    now = datetime.now(timezone.utc).timestamp()
    age_hours = (now - posted_ts) / 3600.0
    return age_hours <= max_age_hours

# ---------------------------
# Persistence helpers
# ---------------------------
def save_jobs(conn, jobs):
    cur = conn.cursor()
    inserted = 0
    for j in jobs:
        try:
            cur.execute("""
                INSERT OR IGNORE INTO jobs(source, external_id, title, company, location, url, posted_at_utc, description, created_at_utc)
                VALUES(?,?,?,?,?,?,?,?,?)
            """, (
                j["source"], j.get("external_id"), j["title"], j["company"], j["location"], j["url"],
                j.get("posted_at_utc", 0), j.get("description", ""), int(datetime.now(timezone.utc).timestamp())
            ))
            if cur.rowcount > 0:
                inserted += 1
        except sqlite3.IntegrityError:
            # duplicate or constraint error: ignore
            pass
        except Exception as e:
            logging.error("DB insert error: %s", e)
    conn.commit()
    return inserted

def fetch_unsent_jobs_since(conn, since_ts: int):
    cur = conn.cursor()
    cur.execute("""
        SELECT title, company, location, url, posted_at_utc
        FROM jobs
        WHERE created_at_utc >= ?
        ORDER BY posted_at_utc DESC
        LIMIT ?
    """, (since_ts, MAX_RESULTS_PER_RUN))
    return cur.fetchall()

# ---------------------------
# Email
# ---------------------------
def render_email_html(rows):
    # Uses simple HTML template file with a {{ROWS}} placeholder
    tpl = ""
    try:
        with open(TEMPLATE_PATH, "r", encoding="utf-8") as fh:
            tpl = fh.read()
    except Exception:
        # fallback minimal template
        tpl = """<html><body><h3>New jobs</h3><table border="1" cellpadding="6"><thead><tr><th>Title</th><th>Company</th><th>Location</th><th>Apply</th><th>Posted</th></tr></thead><tbody>{{ROWS}}</tbody></table></body></html>"""

    def fmt_time(ts):
        if not ts:
            return ""
        dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
        # convert to IST for readability
        ist = dt.astimezone(timezone(timedelta(hours=5, minutes=30)))
        return ist.strftime("%d %b %Y, %I:%M %p IST")

    rows_html = []
    for (title, company, location, url, ts) in rows:
        rows_html.append(f"<tr><td>{title}</td><td>{company}</td><td>{location}</td><td><a href='{url}' target='_blank' rel='noopener'>Apply</a></td><td>{fmt_time(ts)}</td></tr>")
    return tpl.replace("{{ROWS}}", "\n".join(rows_html))

def send_email(subject: str, html: str):
    if not SMTP_HOST:
        logging.error("SMTP_HOST not configured, skipping email.")
        return
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO
    msg.attach(MIMEText(html, "html", "utf-8"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        if SMTP_USE_TLS:
            server.starttls()
        if SMTP_USER and SMTP_PASS:
            server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(EMAIL_FROM, [EMAIL_TO], msg.as_string())

# ---------------------------
# Main search & notify
# ---------------------------
def search_and_notify():
    try:
        conn = init_db(DB_PATH)
        collected = []

        roles = ROLE_KEYWORDS
        locations = LOCATIONS[:]  # copy
        if INCLUDE_REMOTE and "Remote" not in locations:
            locations.append("Remote")

        for role in roles:
            for loc in locations:
                for page in range(1, PAGES_PER_QUERY + 1):
                    try:
                        data = jsearch_query(role, loc, page)
                        items = data.get("data") or data.get("jobs") or []
                        for item in items:
                            job = parse_jsearch_item(item)
                            text_blob = " ".join([job.get("title", ""), job.get("description", ""), job.get("company", ""), job.get("location", "")])
                            if not role_ok(text_blob):
                                continue
                            if not skills_ok(text_blob):
                                continue
                            if not experience_ok(text_blob):
                                continue
                            if not posted_recent(job.get("posted_at_utc", 0), MAX_POST_AGE_HOURS):
                                continue
                            collected.append(job)
                    except requests.HTTPError as he:
                        logging.warning("HTTP error for %s/%s page %s: %s", role, loc, page, he)
                    except Exception as e:
                        logging.warning("Error for %s/%s page %s: %s", role, loc, page, e)
                time.sleep(0.6)  # polite pause

        # de-duplicate in-memory by external_id or (title,company,location,url)
        unique = {}
        for j in collected:
            key = j.get("external_id") or (j.get("title"), j.get("company"), j.get("location"), j.get("url"))
            unique[key] = j
        unique_jobs = list(unique.values())

        inserted_count = save_jobs(conn, unique_jobs)
        logging.info("Inserted %d new jobs into DB.", inserted_count)

        # email jobs created in last RUN_EVERY_HOURS hours
        since_ts = int((datetime.now(timezone.utc) - timedelta(hours=RUN_EVERY_HOURS)).timestamp())
        rows = fetch_unsent_jobs_since(conn, since_ts)

        if rows:
            html = render_email_html(rows)
            subject = f"{len(rows)} Fresher/0-6mo {DEFAULT_COUNTRY} Tech Jobs (last {int(RUN_EVERY_HOURS)}h)"
            send_email(subject, html)
            logging.info("Emailed %d jobs to %s", len(rows), EMAIL_TO)
        else:
            logging.info("No matching jobs found this cycle.")
    except Exception as e:
        logging.error("Fatal error in search_and_notify: %s", e)
        traceback.print_exc()

def main():
    logging.info("Job Hunter starting — first run now, then every %.2f hours", RUN_EVERY_HOURS)
    # first run
    search_and_notify()

    # scheduled runs
    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.start()
    scheduler.add_job(search_and_notify, IntervalTrigger(hours=RUN_EVERY_HOURS))

    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logging.info("Shutting down.")

if __name__ == "__main__":
    main()
