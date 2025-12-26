from server import (
    get_db,
    send_email,
    load_companies,
    app
)
from datetime import date
import csv, io, logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mysqft-cron")

def run_daily_report():
    load_companies(force=True)
    conn = get_db()
    if not conn:
        return

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, company_name, email, plan, plan_expiry
                FROM companies
                WHERE is_active=true
                  AND plan_expiry >= CURRENT_DATE
            """)
            companies = cur.fetchall()

            for cid, name, email, plan, expiry in companies:
                cur.execute("""
                    SELECT lead_data, created_at
                    FROM company_leads
                    WHERE company_id=%s
                      AND created_at::date=CURRENT_DATE
                """, (cid,))
                rows = cur.fetchall()
                if not rows:
                    continue

                headers = sorted({k for d, _ in rows for k in d})
                buf = io.StringIO()
                writer = csv.writer(buf)
                writer.writerow(headers + ["created_at"])

                for data, ts in rows:
                    writer.writerow([data.get(h, "") for h in headers] + [ts])

                if plan in ("email", "all"):
                    with app.app_context():
                        send_email(email, buf.getvalue(), expiry)

                cur.execute("""
                    DELETE FROM company_leads
                    WHERE company_id=%s
                      AND created_at::date=CURRENT_DATE
                """, (cid,))
                conn.commit()

                logger.info("Daily report sent for %s", name)
    finally:
        conn.close()

if __name__ == "__main__":
    run_daily_report()
