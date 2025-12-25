from flask import Flask, request, jsonify
from flask_mail import Mail, Message
from flask_limiter import Limiter
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
from psycopg2 import connect, OperationalError
from datetime import date
from dotenv import load_dotenv
from werkzeug.middleware.proxy_fix import ProxyFix
import os, json, csv, time, hmac, hashlib, requests, logging

# ==============================
# ENV + LOGGING
# ==============================
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==============================
# APP SETUP
# ==============================
app = Flask(__name__)

app.wsgi_app = ProxyFix(
    app.wsgi_app,
    x_for=1,
    x_proto=1,
    x_host=1
)

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
    MAIL_PORT=int(os.getenv("MAIL_PORT")),
    MAIL_USE_TLS=True,
    MAIL_USERNAME=os.getenv("MAIL_USERNAME"),
    MAIL_PASSWORD=os.getenv("MAIL_PASSWORD"),
    MAIL_DEFAULT_SENDER=("MySqft", os.getenv("MAIL_DEFAULT_SENDER"))
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

limiter = Limiter(
    app=app,
    key_func=limiter_key,
    default_limits=[]
)

@app.errorhandler(429)
def ratelimit_handler(e):
    return jsonify({
        "error": "Too many requests",
        "message": "Only 5 submissions allowed per 10 minutes."
    }), 429

# ==============================
# DATABASE
# ==============================
def get_db():
    try:
        return connect(os.getenv("NEON_DATABASE_URL"))
    except OperationalError as e:
        logger.error(f"DB connection failed: {e}")
        return None

# ==============================
# COMPANY CACHE
# ==============================
COMPANY_CACHE = {}

def load_companies():
    conn = get_db()
    if not conn:
        return

    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                subdomain,
                id, company_name, email, discord_webhook,
                webhook_url, webhook_secret,
                plan, plan_expiry, lead_fields
            FROM companies
            WHERE is_active=true
        """)

        COMPANY_CACHE.clear()

        for row in cur.fetchall():
            COMPANY_CACHE[row[0]] = row[1:]

        logger.info(
            "Company cache loaded (%d): %s",
            len(COMPANY_CACHE),
            list(COMPANY_CACHE.keys())
        )

    except Exception as e:
        logger.error(f"load_companies error: {e}")
    finally:
        cur.close()
        conn.close()

# ==============================
# DAILY REPORT
# ==============================
def daily_report():
    logger.info("Running daily report")

    conn = get_db()
    if not conn:
        return

    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, company_name, email, plan
            FROM companies
            WHERE is_active=true
              AND plan_expiry >= CURRENT_DATE
        """)
        companies = cur.fetchall()

        for company_id, company_name, email, plan in companies:
            cur.execute("""
                SELECT lead_data, created_at
                FROM company_leads
                WHERE company_id=%s
                  AND created_at::date = CURRENT_DATE
            """, (company_id,))
            rows = cur.fetchall()

            if not rows:
                continue

            headers = set()
            for data, _ in rows:
                headers.update(data.keys())

            csv_file = f"{company_id}.csv"
            with open(csv_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(list(headers) + ["created_at"])
                for data, created_at in rows:
                    writer.writerow(
                        [data.get(h, "") for h in headers] + [created_at]
                    )

            if plan in ("email", "all"):
                send_email(email, csv_file)

            cur.execute("""
                DELETE FROM company_leads
                WHERE company_id=%s
                  AND created_at::date = CURRENT_DATE
            """, (company_id,))
            conn.commit()

            os.remove(csv_file)

    except Exception as e:
        logger.error(f"Daily report error: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

# ==============================
# LOAD CACHE AT GUNICORN STARTUP
# ==============================
with app.app_context():
    load_companies()

# ==============================
# SCHEDULER (ONE INSTANCE ONLY)
# ==============================
scheduler = BackgroundScheduler(timezone="UTC")

if not scheduler.running:
    scheduler.add_job(daily_report, "cron", hour=6)
    scheduler.add_job(load_companies, "cron", hour=6, minute=30)
    scheduler.start()
    logger.info("Scheduler started (daily_report + load_companies)")

# ==============================
# SUBDOMAIN RESOLUTION
# ==============================
def resolve_subdomain():
    host = request.headers.get("X-Forwarded-Host", request.host)
    host = host.split(":")[0]

    if host in ("mysqft.in", "www.mysqft.in"):
        return "mysqft"

    if host.endswith(".mysqft.in"):
        return host.replace(".mysqft.in", "")

    return "mysqft"

# ==============================
# LEADS
# ==============================
def save_lead(company_id, lead_data):
    conn = get_db()
    if not conn:
        return False

    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO company_leads (company_id, lead_data)
            VALUES (%s, %s)
        """, (company_id, json.dumps(lead_data)))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"save_lead error: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()

# ==============================
# ROUTE
# ==============================
@app.route("/submit", methods=["POST"])
@limiter.limit("5 per 10 minutes")
def submit():
    try:
        subdomain = resolve_subdomain()

        company = COMPANY_CACHE.get(subdomain)

        if not company:
            load_companies()
            company = COMPANY_CACHE.get(subdomain)

        if not company:
            return jsonify({"error": "Company not found"}), 403

        (
            company_id, company_name, email, discord,
            webhook_url, webhook_secret,
            plan, expiry, fields
        ) = company

        if expiry < date.today():
            return jsonify({"error": "Plan expired"}), 403

        lead_data = {
            field: request.form.get(field)
            for field in fields
            if request.form.get(field)
        }

        if not lead_data:
            return jsonify({"error": "No valid data"}), 400

        if not save_lead(company_id, lead_data):
            return jsonify({"error": "Failed to save lead"}), 500

        return jsonify({"message": "Send Success!"}), 200

    except Exception:
        logger.exception("Unhandled submit error")
        return jsonify({"error": "Internal server error"}), 500
