"""On-demand ingestion endpoint.

Triggers Form 4 data pull from SEC EDGAR for a given ticker.

Two modes:
  - Initial ingestion (no prior data): fetches ALL historical Form 4 filings,
    including paginated older filings. This is the heavy one-time cost.
  - Incremental ingestion: only fetches filings newer than the last ingestion.
    Fast because most data is already in Snowflake.
"""

import logging

from fastapi import APIRouter, HTTPException

from api.models.schemas import IngestionResult
from api.services import anomaly, edgar, snowflake as sf

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ingest", tags=["ingestion"])


@router.post("/{ticker}", response_model=IngestionResult)
def ingest_ticker(ticker: str):
    """Pull Form 4 filings for a ticker and run anomaly detection.

    On first run for a ticker, performs a full historical fetch.
    Subsequent runs are incremental (only new filings since last ingestion).
    """
    ticker = ticker.upper()

    watchlist_item = sf.get_watchlist_item(ticker)
    if not watchlist_item:
        raise HTTPException(404, f"{ticker} is not on the watchlist. Add it first.")

    cik = watchlist_item["CIK"]
    run_id = sf.create_ingestion_log(ticker)

    try:
        last_date = sf.get_last_ingestion_date(ticker)
        is_initial = last_date is None

        if is_initial:
            logger.info(f"Initial full ingestion for {ticker} (CIK {cik})")
        else:
            logger.info(f"Incremental ingestion for {ticker} since {last_date}")

        filings = edgar.fetch_form4_filings(cik, after_date=last_date)

        if not filings:
            sf.complete_ingestion_log(run_id, 0, 0)
            return IngestionResult(
                ticker=ticker,
                filings_processed=0,
                transactions_inserted=0,
                alerts_generated=0,
                status="SUCCESS",
                message="No new filings found.",
            )

        total_inserted = 0
        insiders_seen = {}  # insider_cik -> (name, title) for batch upsert

        for i, filing in enumerate(filings):
            if (i + 1) % 50 == 0 or i == 0:
                logger.info(
                    f"Processing filing {i + 1}/{len(filings)} for {ticker}"
                )

            transactions = edgar.parse_form4_xml(
                cik=cik,
                accession_number=filing["accession_number"],
                filing_date=filing["filing_date"],
                ticker=ticker,
                primary_doc=filing.get("primary_doc"),
            )
            inserted = sf.insert_transactions(transactions)
            total_inserted += inserted

            # Collect unique insiders for batch upsert
            for txn in transactions:
                insiders_seen[txn["insider_cik"]] = (
                    txn["insider_name"],
                    txn["insider_title"],
                )

        # Batch upsert all insiders at the end
        for insider_cik, (name, title) in insiders_seen.items():
            sf.upsert_insider(insider_cik, name, title)

        # Run anomaly detection on the newly loaded data
        alerts = anomaly.run_anomaly_detection(ticker)
        alerts_generated = 0
        for alert in alerts:
            sf.insert_alert(**alert)
            alerts_generated += 1

        sf.complete_ingestion_log(run_id, len(filings), total_inserted)

        mode = "initial bulk load" if is_initial else "incremental update"
        logger.info(
            f"Ingestion complete for {ticker} ({mode}): "
            f"{len(filings)} filings, {total_inserted} transactions, "
            f"{alerts_generated} alerts"
        )

        return IngestionResult(
            ticker=ticker,
            filings_processed=len(filings),
            transactions_inserted=total_inserted,
            alerts_generated=alerts_generated,
            status="SUCCESS",
            message=f"Completed {mode}.",
        )

    except Exception as e:
        logger.error(f"Ingestion failed for {ticker}: {e}", exc_info=True)
        sf.complete_ingestion_log(run_id, 0, 0, status="FAILED", error=str(e))
        raise HTTPException(500, f"Ingestion failed: {e}")
