"""Snowflake connector data access layer.

Provides CRUD operations for watchlist, transactions, insiders, alerts,
and ingestion log tables in the INSIDER_MONITOR database.
"""

import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import snowflake.connector
from snowflake.connector import DictCursor

from api.config import settings


_conn = None


def _get_streamlit_secrets() -> Optional[dict]:
    """Try to read Snowflake config from Streamlit secrets (for Streamlit Cloud).

    Returns a connector config dict if st.secrets has the required keys, else None.
    """
    try:
        import streamlit as st
        sf_secrets = st.secrets.get("snowflake")
        if sf_secrets and sf_secrets.get("password"):
            return {
                "account": sf_secrets["account"],
                "user": sf_secrets["user"],
                "password": sf_secrets["password"],
                "warehouse": sf_secrets.get("warehouse", "COMPUTE_WH"),
                "database": sf_secrets.get("database", "INSIDER_MONITOR"),
                "schema": sf_secrets.get("schema", "PUBLIC"),
                "role": sf_secrets.get("role", ""),
            }
    except Exception:
        pass
    return None


def get_session():
    """Get or create a Snowflake connection.

    Priority: st.secrets (Streamlit Cloud) > env vars > connections.toml (local OAuth).
    """
    global _conn
    if _conn is None or _conn.is_closed():
        # 1. Streamlit Cloud secrets
        st_config = _get_streamlit_secrets()
        if st_config:
            role = st_config.pop("role", "")
            if role:
                st_config["role"] = role
            _conn = snowflake.connector.connect(**st_config)
        elif settings.SNOWFLAKE_PASSWORD:
            # 2. Env vars / .env file
            _conn = snowflake.connector.connect(
                account=settings.SNOWFLAKE_ACCOUNT,
                user=settings.SNOWFLAKE_USER,
                password=settings.SNOWFLAKE_PASSWORD,
                warehouse=settings.SNOWFLAKE_WAREHOUSE,
                database=settings.SNOWFLAKE_DATABASE,
                schema=settings.SNOWFLAKE_SCHEMA,
                role=settings.SNOWFLAKE_ROLE,
            )
        else:
            # 3. Fall back to connections.toml (supports OAuth, browser-based SSO, etc.)
            import toml
            from pathlib import Path

            toml_path = Path.home() / ".snowflake" / "connections.toml"
            conn_name = None
            if toml_path.exists():
                toml_data = toml.load(toml_path)
                conn_name = toml_data.get("default_connection_name")

            kwargs = {
                "database": settings.SNOWFLAKE_DATABASE,
                "schema": settings.SNOWFLAKE_SCHEMA,
                "warehouse": settings.SNOWFLAKE_WAREHOUSE,
            }
            if conn_name:
                kwargs["connection_name"] = conn_name
            if settings.SNOWFLAKE_ROLE:
                kwargs["role"] = settings.SNOWFLAKE_ROLE
            _conn = snowflake.connector.connect(**kwargs)
    return _conn


def close_session():
    """Close the Snowflake connection."""
    global _conn
    if _conn is not None:
        _conn.close()
        _conn = None


def _execute(sql: str, params=None) -> list[dict]:
    """Execute SQL and return results as a list of dicts."""
    conn = get_session()
    cur = conn.cursor(DictCursor)
    try:
        cur.execute(sql, params)
        return cur.fetchall()
    finally:
        cur.close()


def _execute_no_fetch(sql: str, params=None):
    """Execute SQL without fetching results (for INSERT/UPDATE/MERGE)."""
    conn = get_session()
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
    finally:
        cur.close()


# --- Watchlist operations ---


def get_watchlist(active_only: bool = True) -> list[dict]:
    """Get all watchlist items."""
    if active_only:
        return _execute(
            "SELECT * FROM WATCHLIST WHERE ACTIVE = TRUE ORDER BY ADDED_AT"
        )
    return _execute("SELECT * FROM WATCHLIST ORDER BY ADDED_AT")


def add_to_watchlist(
    ticker: str, company_name: str, cik: str,
    exchange: Optional[str] = None, sic_code: Optional[str] = None,
) -> dict:
    """Add a company to the watchlist. Returns the inserted row."""
    now = datetime.now(timezone.utc)
    _execute_no_fetch(
        "INSERT INTO WATCHLIST (TICKER, COMPANY_NAME, CIK, EXCHANGE, SIC_CODE, ADDED_AT, ACTIVE) "
        "SELECT %s, %s, %s, %s, %s, %s, %s "
        "WHERE NOT EXISTS (SELECT 1 FROM WATCHLIST WHERE TICKER = %s)",
        (ticker.upper(), company_name, cik, exchange, sic_code, now, True, ticker.upper()),
    )
    return {
        "ticker": ticker.upper(),
        "company_name": company_name,
        "cik": cik,
        "exchange": exchange,
        "sic_code": sic_code,
        "added_at": now,
        "active": True,
    }


def remove_from_watchlist(ticker: str) -> bool:
    """Soft-delete a company from the watchlist."""
    _execute_no_fetch(
        "UPDATE WATCHLIST SET ACTIVE = FALSE WHERE TICKER = %s",
        (ticker.upper(),),
    )
    return True


def get_watchlist_item(ticker: str) -> Optional[dict]:
    """Get a single watchlist item by ticker."""
    rows = _execute(
        "SELECT * FROM WATCHLIST WHERE TICKER = %s", (ticker.upper(),)
    )
    if rows:
        return rows[0]
    return None


# --- Transaction operations ---


def insert_transactions(transactions: list[dict]) -> int:
    """Insert transactions, skipping duplicates by TRANSACTION_ID.

    Returns number of rows inserted.
    """
    if not transactions:
        return 0

    inserted = 0

    for txn in transactions:
        try:
            _execute_no_fetch(
                "INSERT INTO TRANSACTIONS "
                "(TRANSACTION_ID, ACCESSION_NUMBER, FILING_DATE, COMPANY_CIK, "
                "TICKER, INSIDER_CIK, INSIDER_NAME, INSIDER_TITLE, "
                "TRANSACTION_DATE, TRANSACTION_CODE, SHARES, PRICE_PER_SHARE, "
                "TOTAL_VALUE, SHARES_OWNED_AFTER, DIRECT_OR_INDIRECT) "
                "SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s "
                "WHERE NOT EXISTS (SELECT 1 FROM TRANSACTIONS WHERE TRANSACTION_ID = %s)",
                (
                    txn["transaction_id"],
                    txn["accession_number"],
                    txn["filing_date"],
                    txn["company_cik"],
                    txn["ticker"],
                    txn["insider_cik"],
                    txn["insider_name"],
                    txn["insider_title"],
                    txn["transaction_date"],
                    txn["transaction_code"],
                    txn["shares"],
                    txn["price_per_share"],
                    txn["total_value"],
                    txn["shares_owned_after"],
                    txn["direct_or_indirect"],
                    txn["transaction_id"],
                ),
            )
            inserted += 1
        except Exception:
            # Duplicate key -- skip
            pass

    return inserted


def get_transactions(
    ticker: Optional[str] = None,
    days: int = 90,
    limit: int = 500,
) -> list[dict]:
    """Get transactions with optional ticker filter and date window."""
    cutoff = date.today() - timedelta(days=days)
    if ticker:
        return _execute(
            "SELECT * FROM TRANSACTIONS "
            "WHERE TICKER = %s AND FILING_DATE >= %s "
            "ORDER BY TRANSACTION_DATE DESC, FILING_DATE DESC LIMIT %s",
            (ticker.upper(), cutoff, limit),
        )
    return _execute(
        "SELECT * FROM TRANSACTIONS "
        "WHERE FILING_DATE >= %s "
        "ORDER BY TRANSACTION_DATE DESC, FILING_DATE DESC LIMIT %s",
        (cutoff, limit),
    )


def get_transaction_summary(ticker: str, days: int = 90) -> dict:
    """Get aggregated buy/sell summary for a ticker."""
    cutoff = date.today() - timedelta(days=days)
    rows = _execute(
        "SELECT "
        "  COUNT(CASE WHEN TRANSACTION_CODE = 'P' THEN 1 END) AS TOTAL_BUYS, "
        "  COUNT(CASE WHEN TRANSACTION_CODE = 'S' THEN 1 END) AS TOTAL_SELLS, "
        "  COALESCE(SUM(CASE WHEN TRANSACTION_CODE = 'P' THEN TOTAL_VALUE END), 0) AS TOTAL_BUY_VALUE, "
        "  COALESCE(SUM(CASE WHEN TRANSACTION_CODE = 'S' THEN TOTAL_VALUE END), 0) AS TOTAL_SELL_VALUE, "
        "  COUNT(DISTINCT INSIDER_CIK) AS UNIQUE_INSIDERS, "
        "  MAX(TRANSACTION_DATE) AS LATEST_TRANSACTION_DATE "
        "FROM TRANSACTIONS "
        "WHERE TICKER = %s AND FILING_DATE >= %s",
        (ticker.upper(), cutoff),
    )

    if not rows:
        return {
            "ticker": ticker.upper(),
            "total_buys": 0, "total_sells": 0,
            "total_buy_value": 0.0, "total_sell_value": 0.0,
            "unique_insiders": 0, "latest_transaction_date": None,
            "net_sentiment": "neutral",
        }

    row = rows[0]
    buys = row.get("TOTAL_BUYS", 0) or 0
    sells = row.get("TOTAL_SELLS", 0) or 0
    buy_val = float(row.get("TOTAL_BUY_VALUE", 0) or 0)
    sell_val = float(row.get("TOTAL_SELL_VALUE", 0) or 0)

    if buy_val > sell_val * 1.5:
        sentiment = "bullish"
    elif sell_val > buy_val * 1.5:
        sentiment = "bearish"
    else:
        sentiment = "neutral"

    return {
        "ticker": ticker.upper(),
        "total_buys": buys,
        "total_sells": sells,
        "total_buy_value": buy_val,
        "total_sell_value": sell_val,
        "unique_insiders": row.get("UNIQUE_INSIDERS", 0) or 0,
        "latest_transaction_date": row.get("LATEST_TRANSACTION_DATE"),
        "net_sentiment": sentiment,
    }


# --- Insider operations ---


def upsert_insider(insider_cik: str, name: str, title: str):
    """Insert or update an insider record."""
    now = datetime.now(timezone.utc)
    _execute_no_fetch(
        "MERGE INTO INSIDERS t USING (SELECT %s AS CIK, %s AS NAME, %s AS TITLE, %s AS NOW) s "
        "ON t.INSIDER_CIK = s.CIK "
        "WHEN MATCHED THEN UPDATE SET "
        "  MOST_RECENT_TITLE = s.TITLE, LAST_SEEN = s.NOW, NAME = s.NAME "
        "WHEN NOT MATCHED THEN INSERT "
        "  (INSIDER_CIK, NAME, MOST_RECENT_TITLE, FIRST_SEEN, LAST_SEEN) "
        "  VALUES (s.CIK, s.NAME, s.TITLE, s.NOW, s.NOW)",
        (insider_cik, name, title, now),
    )


# --- Alert operations ---


def insert_alert(
    ticker: str, insider_name: str, alert_type: str,
    description: str, severity: str, transaction_ids: Optional[str] = None,
) -> str:
    """Insert an alert and return its ID."""
    alert_id = str(uuid.uuid4())
    _execute_no_fetch(
        "INSERT INTO ALERTS "
        "(ALERT_ID, TICKER, INSIDER_NAME, ALERT_TYPE, DESCRIPTION, SEVERITY, TRANSACTION_IDS) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (alert_id, ticker, insider_name, alert_type, description, severity, transaction_ids),
    )
    return alert_id


def get_alerts(
    ticker: Optional[str] = None,
    acknowledged: Optional[bool] = None,
    limit: int = 100,
) -> list[dict]:
    """Get alerts with optional filters."""
    conditions = []
    params = []

    if ticker:
        conditions.append("TICKER = %s")
        params.append(ticker.upper())
    if acknowledged is not None:
        conditions.append("ACKNOWLEDGED = %s")
        params.append(acknowledged)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params.append(limit)
    return _execute(
        f"SELECT * FROM ALERTS {where} ORDER BY DETECTED_AT DESC LIMIT %s",
        tuple(params),
    )


def acknowledge_alert(alert_id: str) -> bool:
    """Mark an alert as acknowledged."""
    _execute_no_fetch(
        "UPDATE ALERTS SET ACKNOWLEDGED = TRUE WHERE ALERT_ID = %s",
        (alert_id,),
    )
    return True


# --- Ingestion log ---


def create_ingestion_log(ticker: str) -> str:
    """Create a new ingestion log entry and return its run_id."""
    run_id = str(uuid.uuid4())
    _execute_no_fetch(
        "INSERT INTO INGESTION_LOG (RUN_ID, TICKER) VALUES (%s, %s)",
        (run_id, ticker.upper()),
    )
    return run_id


def complete_ingestion_log(
    run_id: str, filings: int, transactions: int,
    status: str = "SUCCESS", error: Optional[str] = None,
):
    """Update ingestion log on completion."""
    now = datetime.now(timezone.utc)
    _execute_no_fetch(
        "UPDATE INGESTION_LOG SET "
        "COMPLETED_AT = %s, FILINGS_PROCESSED = %s, "
        "TRANSACTIONS_INSERTED = %s, STATUS = %s, ERROR_MESSAGE = %s "
        "WHERE RUN_ID = %s",
        (now, filings, transactions, status, error, run_id),
    )


def get_last_ingestion_date(ticker: str) -> Optional[date]:
    """Get the filing date of the most recent successful ingestion for a ticker."""
    rows = _execute(
        "SELECT MAX(FILING_DATE) AS LAST_DATE FROM TRANSACTIONS WHERE TICKER = %s",
        (ticker.upper(),),
    )
    if rows and rows[0]["LAST_DATE"]:
        return rows[0]["LAST_DATE"]
    return None
