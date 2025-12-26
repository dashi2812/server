import logging
from server import daily_report, load_companies

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CRON] %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)

def main():
    logger.info("Cron started")

    load_companies(force=True)
    daily_report()

    logger.info("Cron finished")

if __name__ == "__main__":
    main()
