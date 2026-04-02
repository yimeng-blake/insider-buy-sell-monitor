"""Scheduled ingestion script.

Runs daily to pull new Form 4 filings for all active watchlist tickers.
Can be executed as a standalone cron job or via APScheduler.

Usage:
    python -m ingestion.scheduled_ingest          # Run once
    python -m ingestion.scheduled_ingest --daemon  # Run as daemon with APScheduler
"""

import argparse
import logging
import sys
from datetime import datetime, timezone

from apscheduler.schedulers.blocking import BlockingScheduler

# Add project root to path
sys.path.insert(0, ".")

from api.services import snowflake as sf
from api.services import edgar
from api.services import anomaly

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def ingest_all_tickers():
    """Pull new Form 4 filings for every active watchlist ticker."""
    logger.info("Starting scheduled ingestion run")
    watchlist = sf.get_watchlist(active_only=True)

    if not watchlist:
        logger.info("Watchlist is empty, nothing to ingest")
        return

    for item in watchlist:
        ticker = item["TICKER"]
        cik = item["CIK"]
        run_id = sf.create_ingestion_log(ticker)

        try:
            last_date = sf.get_last_ingestion_date(ticker)
            logger.info(
                f"[{ticker}] Fetching filings after {last_date or 'beginning'}"
            )

            filings = edgar.fetch_form4_filings(cik, after_date=last_date)
            logger.info(f"[{ticker}] Found {len(filings)} new filings")

            ref_price = sf.get_recent_median_price(ticker)
            total_inserted = 0
            insiders_seen = {}
            for i, filing in enumerate(filings):
                if (i + 1) % 50 == 0 or i == 0:
                    logger.info(
                        f"[{ticker}] Processing filing {i + 1}/{len(filings)}"
                    )
                transactions = edgar.parse_form4_xml(
                    cik=cik,
                    accession_number=filing["accession_number"],
                    filing_date=filing["filing_date"],
                    ticker=ticker,
                    primary_doc=filing.get("primary_doc"),
                )
                edgar.sanitize_transactions(transactions, ref_price)
                inserted = sf.insert_transactions(transactions)
                total_inserted += inserted

                for txn in transactions:
                    insiders_seen[txn["insider_cik"]] = (
                        txn["insider_name"], txn["insider_title"]
                    )

            for insider_cik, (name, title) in insiders_seen.items():
                sf.upsert_insider(insider_cik, name, title)

            # Anomaly detection
            alerts = anomaly.run_anomaly_detection(ticker)
            for alert in alerts:
                sf.insert_alert(**alert)

            sf.complete_ingestion_log(run_id, len(filings), total_inserted)
            logger.info(
                f"[{ticker}] Done: {len(filings)} filings, "
                f"{total_inserted} transactions, {len(alerts)} alerts"
            )

        except Exception as e:
            logger.error(f"[{ticker}] Ingestion failed: {e}")
            sf.complete_ingestion_log(run_id, 0, 0, status="FAILED", error=str(e))

    logger.info("Scheduled ingestion run complete")


def main():
    parser = argparse.ArgumentParser(description="Insider trading data ingestion")
    parser.add_argument(
        "--daemon", action="store_true",
        help="Run as a daemon with daily scheduled execution",
    )
    parser.add_argument(
        "--hour", type=int, default=6,
        help="Hour (UTC) to run daily ingestion (default: 6)",
    )
    args = parser.parse_args()

    if args.daemon:
        logger.info(f"Starting daemon mode, will run daily at {args.hour}:00 UTC")
        scheduler = BlockingScheduler()
        scheduler.add_job(ingest_all_tickers, "cron", hour=args.hour, minute=0)
        # Run once immediately on startup
        ingest_all_tickers()
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Scheduler stopped")
    else:
        ingest_all_tickers()


if __name__ == "__main__":
    main()
