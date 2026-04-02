"""Anomaly detection for insider trading patterns.

Detects:
- Large transactions (single transaction > 2x insider's historical average)
- Cluster buying/selling (3+ insiders same direction within 7 days)
- Unusual frequency (insider trading significantly more than baseline)
"""

from datetime import date, timedelta
from typing import Optional

from api.services import snowflake as sf


def run_anomaly_detection(ticker: str, days: int = 90) -> list[dict]:
    """Run all anomaly detectors for a ticker. Returns list of alert dicts."""
    alerts = []
    alerts.extend(_detect_large_transactions(ticker, days))
    alerts.extend(_detect_cluster_activity(ticker, days))
    alerts.extend(_detect_unusual_frequency(ticker, days))
    return alerts


def _detect_large_transactions(ticker: str, days: int) -> list[dict]:
    """Flag transactions where value > 2x the insider's historical average."""
    cutoff = date.today() - timedelta(days=days)

    rows = sf._execute(
        "WITH insider_avg AS ( "
        "  SELECT INSIDER_CIK, AVG(ABS(TOTAL_VALUE)) AS AVG_VALUE "
        "  FROM TRANSACTIONS "
        "  WHERE TICKER = %s AND TOTAL_VALUE IS NOT NULL "
        "  GROUP BY INSIDER_CIK "
        "  HAVING COUNT(*) >= 2 "
        "), "
        "recent_txns AS ( "
        "  SELECT * FROM TRANSACTIONS "
        "  WHERE TICKER = %s AND FILING_DATE >= %s AND TOTAL_VALUE IS NOT NULL "
        ") "
        "SELECT r.*, a.AVG_VALUE "
        "FROM recent_txns r "
        "JOIN insider_avg a ON r.INSIDER_CIK = a.INSIDER_CIK "
        "WHERE ABS(r.TOTAL_VALUE) > a.AVG_VALUE * 2 "
        "ORDER BY r.FILING_DATE DESC",
        (ticker.upper(), ticker.upper(), cutoff),
    )

    alerts = []
    for r in rows:
        code = r["TRANSACTION_CODE"]
        action = "purchase" if code == "P" else "sale" if code == "S" else f"transaction ({code})"
        value = abs(r["TOTAL_VALUE"])
        avg = r["AVG_VALUE"]

        alerts.append({
            "ticker": ticker.upper(),
            "insider_name": r["INSIDER_NAME"],
            "alert_type": "LARGE_TRANSACTION",
            "description": (
                f"{r['INSIDER_NAME']} ({r['INSIDER_TITLE']}) made a {action} "
                f"of ${value:,.0f} on {r['TRANSACTION_DATE']}, "
                f"which is {value / avg:.1f}x their historical average of ${avg:,.0f}."
            ),
            "severity": "HIGH" if value > avg * 5 else "MEDIUM",
            "transaction_ids": r["TRANSACTION_ID"],
        })

    return alerts


def _detect_cluster_activity(ticker: str, days: int) -> list[dict]:
    """Flag when 3+ insiders trade in the same direction within 7 days."""
    cutoff = date.today() - timedelta(days=days)

    rows = sf._execute(
        "WITH directional AS ( "
        "  SELECT INSIDER_CIK, INSIDER_NAME, TRANSACTION_DATE, TRANSACTION_CODE, "
        "    TOTAL_VALUE, TRANSACTION_ID "
        "  FROM TRANSACTIONS "
        "  WHERE TICKER = %s AND FILING_DATE >= %s "
        "    AND TRANSACTION_CODE IN ('P', 'S') "
        ") "
        "SELECT a.TRANSACTION_CODE AS DIRECTION, "
        "  MIN(a.TRANSACTION_DATE) AS WINDOW_START, "
        "  MAX(a.TRANSACTION_DATE) AS WINDOW_END, "
        "  COUNT(DISTINCT a.INSIDER_CIK) AS INSIDER_COUNT, "
        "  LISTAGG(DISTINCT a.INSIDER_NAME, ', ') AS INSIDERS, "
        "  SUM(ABS(a.TOTAL_VALUE)) AS TOTAL_ACTIVITY, "
        "  LISTAGG(a.TRANSACTION_ID, ',') AS TXN_IDS "
        "FROM directional a "
        "JOIN directional b "
        "  ON a.TRANSACTION_CODE = b.TRANSACTION_CODE "
        "  AND a.INSIDER_CIK != b.INSIDER_CIK "
        "  AND ABS(DATEDIFF('day', a.TRANSACTION_DATE, b.TRANSACTION_DATE)) <= 7 "
        "GROUP BY a.TRANSACTION_CODE, "
        "  DATE_TRUNC('week', a.TRANSACTION_DATE) "
        "HAVING COUNT(DISTINCT a.INSIDER_CIK) >= 3 "
        "ORDER BY WINDOW_START DESC",
        (ticker.upper(), cutoff),
    )

    alerts = []
    seen_windows = set()
    for r in rows:
        direction = "buying" if r["DIRECTION"] == "P" else "selling"
        key = f"{r['DIRECTION']}_{r['WINDOW_START']}"
        if key in seen_windows:
            continue
        seen_windows.add(key)

        alerts.append({
            "ticker": ticker.upper(),
            "insider_name": r["INSIDERS"],
            "alert_type": "CLUSTER_ACTIVITY",
            "description": (
                f"Cluster {direction} detected: {r['INSIDER_COUNT']} insiders "
                f"({r['INSIDERS']}) all {direction} between {r['WINDOW_START']} "
                f"and {r['WINDOW_END']}, totaling ${r['TOTAL_ACTIVITY']:,.0f}."
            ),
            "severity": "HIGH",
            "transaction_ids": r.get("TXN_IDS", ""),
        })

    return alerts


def _detect_unusual_frequency(ticker: str, days: int) -> list[dict]:
    """Flag insiders trading significantly more often than their baseline."""
    cutoff = date.today() - timedelta(days=days)

    rows = sf._execute(
        "WITH historical AS ( "
        "  SELECT INSIDER_CIK, INSIDER_NAME, "
        "    COUNT(*) AS TOTAL_TXNS, "
        "    DATEDIFF('month', MIN(TRANSACTION_DATE), MAX(TRANSACTION_DATE)) + 1 AS MONTHS_ACTIVE, "
        "    COUNT(*) / NULLIF(DATEDIFF('month', MIN(TRANSACTION_DATE), MAX(TRANSACTION_DATE)) + 1, 0) AS MONTHLY_AVG "
        "  FROM TRANSACTIONS "
        "  WHERE TICKER = %s "
        "  GROUP BY INSIDER_CIK, INSIDER_NAME "
        "  HAVING DATEDIFF('month', MIN(TRANSACTION_DATE), MAX(TRANSACTION_DATE)) >= 3 "
        "), "
        "recent AS ( "
        "  SELECT INSIDER_CIK, COUNT(*) AS RECENT_COUNT "
        "  FROM TRANSACTIONS "
        "  WHERE TICKER = %s AND FILING_DATE >= %s "
        "  GROUP BY INSIDER_CIK "
        ") "
        "SELECT h.INSIDER_CIK, h.INSIDER_NAME, h.MONTHLY_AVG, "
        "  r.RECENT_COUNT, "
        "  r.RECENT_COUNT / NULLIF(h.MONTHLY_AVG * (%s / 30.0), 0) AS FREQUENCY_RATIO "
        "FROM historical h "
        "JOIN recent r ON h.INSIDER_CIK = r.INSIDER_CIK "
        "WHERE r.RECENT_COUNT > h.MONTHLY_AVG * (%s / 30.0) * 2 "
        "ORDER BY FREQUENCY_RATIO DESC",
        (ticker.upper(), ticker.upper(), cutoff, days, days),
    )

    alerts = []
    for r in rows:
        ratio = r.get("FREQUENCY_RATIO", 0) or 0

        alerts.append({
            "ticker": ticker.upper(),
            "insider_name": r["INSIDER_NAME"],
            "alert_type": "UNUSUAL_FREQUENCY",
            "description": (
                f"{r['INSIDER_NAME']} has made {r['RECENT_COUNT']} transactions "
                f"in the last {days} days, which is {ratio:.1f}x their historical "
                f"average of {r['MONTHLY_AVG']:.1f} transactions per month."
            ),
            "severity": "MEDIUM" if ratio < 4 else "HIGH",
            "transaction_ids": None,
        })

    return alerts
