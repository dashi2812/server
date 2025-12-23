from flask import Flask, request, jsonify
from flask_mail import Mail, Message
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
from psycopg2 import connect, OperationalError
from datetime import date
from dotenv import load_dotenv
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

CORS(
    app,
    resources={r"/*": {"origins": r"https://(.*\.)?mysqft\.in"}}
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
limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=[]
)

@app.errorhandler(429)
def ratelimit_handler(e):
    return jsonify({
        "error": "Too many requests",
        "message": "Only 5 submissions allowed per 10 minutes per IP."
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
# COMPANY CACHE (OPTIMAL)
# ==============================
COMPANY_CACHE = {}

def load_companies():
    """Load all active companies into memory"""
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
            subdomain = row[0]
            COMPANY_CACHE[subdomain] = row[1:]

        logger.info(f"Company cache loaded: {len(COMPANY_CACHE)} companies")

    except Exception as e:
        logger.error(f"load_companies error: {e}")
    finally:
        cur.close()
        conn.close()

# ==============================
# SUBDOMAIN RESOLUTION
# ==============================
def resolve_subdomain():
    host = request.host.split(":")[0]
    parts = host.split(".")

    if host in ("mysqft.in", "www.mysqft.in"):
        return "mysqft"

    if len(parts) > 2:
        return parts[0]

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
# NOTIFICATIONS
# ==============================
def send_email(to_email, csv_file):
    try:
        with app.app_context():
            msg = Message(
                subject="Daily Lead Report",
                recipients=[to_email],
                body="Attached is today's lead report."
            )
            with open(csv_file, "r", encoding="utf-8") as f:
                msg.attach("leads.csv", "text/csv", f.read())
            mail.send(msg)
    except Exception as e:
        logger.error(f"Email failed: {e}")

def send_discord(webhook, content):
    if webhook:
        try:
            requests.post(webhook, json={"content": content}, timeout=5)
        except Exception as e:
            logger.error(f"Discord webhook failed: {e}")

def send_webhook(url, secret, payload):
    if not url:
        return

    try:
        body = json.dumps(payload)
        timestamp = str(int(time.time()))
        signature = hmac.new(
            secret.encode(),
            msg=(timestamp + body).encode(),
            digestmod=hashlib.sha256
        ).hexdigest()

        headers = {
            "Content-Type": "application/json",
            "X-Signature": signature,
            "X-Timestamp": timestamp
        }

        requests.post(url, data=body, headers=headers, timeout=5)
    except Exception as e:
        logger.error(f"Webhook failed: {e}")

# ==============================
# DAILY REPORT + CLEANUP
# ==============================
def daily_report():
    conn = get_db()
    if not conn:
        return

    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, company_name, email, plan
            FROM companies
            WHERE is_active=true AND plan_expiry >= CURRENT_DATE
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
                    writer.writerow([data.get(h, "") for h in headers] + [created_at])

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
# ROUTE
# ==============================
@app.route("/submit", methods=["POST"])
@limiter.limit("5 per 10 minutes")
def submit():
    try:
        subdomain = resolve_subdomain()
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

        if plan in ("discord", "all"):
            text = "\n".join(f"**{k}**: {v}" for k, v in lead_data.items())
            send_discord(discord, f"📩 New Lead for {company_name}\n{text}")

        if plan in ("webhook", "all"):
            send_webhook(webhook_url, webhook_secret, {
                "event": "lead.created",
                "company": company_name,
                "lead": lead_data
            })

        return jsonify({"message": "Lead stored"}), 200

    except Exception:
        logger.exception("Unhandled submit error")
        return jsonify({"error": "Internal server error"}), 500

# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    with app.app_context():
        load_companies()

    scheduler = BackgroundScheduler()
    scheduler.add_job(daily_report, "cron", hour=6)
    scheduler.add_job(load_companies, "cron", hour=6, minute=30)
    scheduler.start()

    app.run(host="0.0.0.0", port=5000)
