from flask import Flask, request, jsonify
from flask_mail import Mail, Message
from flask_limiter import Limiter
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
from psycopg2 import connect, OperationalError
from datetime import date
from dotenv import load_dotenv
from werkzeug.middleware.proxy_fix import ProxyFix
from collections import namedtuple
import os, json, csv, time, hmac, hashlib, requests, logging, io

# ==============================
# ENV + LOGGING
# ==============================
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ==============================
# APP SETUP
# ==============================
app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

CORS(
    app,
    resources={r"/submit": {"origins": r"https://(.*\.)?mysqft\.in"}},
    supports_credentials=False
)

# ==============================
# MAIL
# ==============================
app.config.update(
    MAIL_SERVER=os.getenv("MAIL_SERVER"),
    MAIL_PORT=int(os.getenv("MAIL_PORT", 587)),
    MAIL_USE_TLS=True,
    MAIL_USERNAME=os.getenv("MAIL_USERNAME"),
    MAIL_PASSWORD=os.getenv("MAIL_PASSWORD"),
    MAIL_DEFAULT_SENDER=("MySqft", os.getenv("MAIL_DEFAULT_SENDER")),
)
mail = Mail(app)

# ==============================
# RATE LIMIT
# ==============================
def limiter_key():
    return (
        request.headers.get("CF-Connecting-IP")
        or request.headers.get("X-Forwarded-For", "").split(",")[0]
        or request.remote_addr
    )

limiter = Limiter(app=app, key_func=limiter_key)

# ==============================
# DATABASE (Neon-safe)
# ==============================
def get_db():
    try:
        return connect(
            os.getenv("NEON_DATABASE_URL"),
            connect_timeout=5,
            sslmode="require",
            application_name="mysqft-leads",
        )
    except OperationalError as e:
        logger.error("DB connection failed: %s", e)
        return None

# ==============================
# COMPANY CACHE
# ==============================
Company = namedtuple(
    "Company",
    "id name email discord webhook_url webhook_secret plan expiry fields"
)

COMPANY_CACHE = {}
CACHE_TTL = 300
LAST_LOAD = 0

def load_companies(force=False):
    global LAST_LOAD
    if not force and time.time() - LAST_LOAD < CACHE_TTL:
        return

    conn = get_db()
    if not conn:
        return

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT subdomain, id, company_name, email,
                       discord_webhook, webhook_url, webhook_secret,
                       plan, plan_expiry, lead_fields
                FROM companies
                WHERE is_active=true
            """)
            COMPANY_CACHE.clear()
            for row in cur.fetchall():
                COMPANY_CACHE[row[0]] = Company(*row[1:])
        LAST_LOAD = time.time()
        logger.info("Loaded %d companies", len(COMPANY_CACHE))
    finally:
        conn.close()

# ==============================
# HELPERS
# ==============================
def resolve_subdomain():
    host = request.headers.get("X-Forwarded-Host", request.host).split(":")[0]
    if host.endswith(".mysqft.in"):
        return host.replace(".mysqft.in", "")
    return "mysqft"

def days_left(expiry):
    return (expiry - date.today()).days

# ==============================
# LEADS
# ==============================
def save_lead(company_id, data):
    conn = get_db()
    if not conn:
        return False

    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO company_leads (company_id, lead_data) VALUES (%s,%s)",
                (company_id, json.dumps(data)),
            )
        conn.commit()
        return True
    except Exception as e:
        logger.error("save_lead error: %s", e)
        conn.rollback()
        return False
    finally:
        conn.close()

# ==============================
# NOTIFICATIONS
# ==============================
def send_email(to, csv_content, expiry):
    dleft = days_left(expiry)

    body = "Attached is today's lead report."

    if 0 <= dleft < 3:
        body += (
            f"\n\n⚠️ IMPORTANT:\n"
            f"Your plan expires in {dleft} day(s).\n"
            f"Please renew your plan to avoid service interruption."
        )

    with app.app_context():
        msg = Message(
            subject="Daily Lead Report",
            recipients=[to],
            body=body,
        )
        msg.attach("leads.csv", "text/csv", csv_content)
        mail.send(msg)

def send_discord(webhook, content):
    if webhook:
        requests.post(webhook, json={"content": content}, timeout=5)

def send_webhook(url, secret, payload):
    if not url or not secret:
        return

    body = json.dumps(payload)
    timestamp = str(int(time.time()))
    signature = hmac.new(
        secret.encode(),
        (timestamp + body).encode(),
        hashlib.sha256,
    ).hexdigest()

    requests.post(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "X-Signature": signature,
            "X-Timestamp": timestamp,
        },
        timeout=5,
    )

# ==============================
# DAILY REPORT
# ==============================
def daily_report():
    logger.info("Running daily report")
    conn = get_db()
    if not conn:
        return

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, company_name, email, plan, plan_expiry
                FROM companies
                WHERE is_active=true AND plan_expiry >= CURRENT_DATE
            """)
            for cid, name, email, plan, expiry in cur.fetchall():
                cur.execute("""
                    SELECT lead_data, created_at
                    FROM company_leads
                    WHERE company_id=%s AND created_at::date=CURRENT_DATE
                """, (cid,))
                rows = cur.fetchall()
                if not rows:
                    continue

                headers = sorted({k for r, _ in rows for k in r})
                buf = io.StringIO()
                writer = csv.writer(buf)
                writer.writerow(headers + ["created_at"])
                for data, ts in rows:
                    writer.writerow([data.get(h, "") for h in headers] + [ts])

                if plan in ("email", "all"):
                    send_email(email, buf.getvalue(), expiry)

                cur.execute(
                    "DELETE FROM company_leads WHERE company_id=%s AND created_at::date=CURRENT_DATE",
                    (cid,),
                )
                conn.commit()
    finally:
        conn.close()

# ==============================
# SCHEDULER (single instance only)
# ==============================
if os.getenv("RUN_SCHEDULER") == "1":
    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(daily_report, "cron", hour=6)
    scheduler.add_job(load_companies, "cron", hour=6, minute=30)
    scheduler.start()

# ==============================
# ROUTE
# ==============================
@app.route("/submit", methods=["POST"])
@limiter.limit("5 per 10 minutes")
def submit():
    sub = resolve_subdomain()
    company = COMPANY_CACHE.get(sub)

    if not company:
        load_companies(force=True)
        company = COMPANY_CACHE.get(sub)

    if not company or company.expiry < date.today():
        return jsonify(error="Unauthorized"), 403

    lead = {f: request.form.get(f) for f in company.fields if request.form.get(f)}
    if not lead:
        return jsonify(error="No valid data"), 400

    if not save_lead(company.id, lead):
        return jsonify(error="Failed to save lead"), 500

    if company.plan in ("discord", "all"):
        dleft = days_left(company.expiry)

        msg = (
            f"📩 **New Lead for {company.name}**\n"
            + "\n".join(f"**{k}**: {v}" for k, v in lead.items())
        )

        if 0 <= dleft < 3:
            msg += f"\n\n⚠️ **Plan expires in {dleft} day(s)** — please renew."

        send_discord(company.discord, msg)

    if company.plan in ("webhook", "all"):
        send_webhook(company.webhook_url, company.webhook_secret, {
            "event": "lead.created",
            "company": company.name,
            "lead": lead
        })

    return jsonify(message="Lead stored"), 200



