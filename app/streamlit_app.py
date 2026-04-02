"""Streamlit frontend for Insider Buy/Sell Monitor.

Communicates with the FastAPI backend for all data operations.
"""

import requests
import streamlit as st
import pandas as pd
from datetime import datetime

API_BASE = "http://localhost:8000"


def api_get(path: str, params: dict = None):
    """Make a GET request to the FastAPI backend."""
    try:
        resp = requests.get(f"{API_BASE}{path}", params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.ConnectionError:
        st.error("Cannot connect to API. Make sure the FastAPI server is running on port 8000.")
        return None
    except requests.exceptions.HTTPError as e:
        st.error(f"API error: {e.response.text}")
        return None


def api_post(path: str, json: dict = None):
    """Make a POST request to the FastAPI backend."""
    try:
        resp = requests.post(f"{API_BASE}{path}", json=json, timeout=120)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.ConnectionError:
        st.error("Cannot connect to API. Make sure the FastAPI server is running on port 8000.")
        return None
    except requests.exceptions.HTTPError as e:
        st.error(f"API error: {e.response.text}")
        return None


def api_delete(path: str):
    """Make a DELETE request to the FastAPI backend."""
    try:
        resp = requests.delete(f"{API_BASE}{path}", timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.ConnectionError:
        st.error("Cannot connect to API. Make sure the FastAPI server is running on port 8000.")
        return None
    except requests.exceptions.HTTPError as e:
        st.error(f"API error: {e.response.text}")
        return None


# --- Page configuration ---

st.set_page_config(
    page_title="Insider Monitor",
    page_icon="📊",
    layout="wide",
)

st.title("Insider Buy/Sell Monitor")

page = st.sidebar.radio(
    "Navigation",
    ["Watchlist", "Dashboard", "Alerts", "Analytics"],
)


# ============================================================
# WATCHLIST PAGE
# ============================================================

if page == "Watchlist":
    st.header("Watchlist Management")

    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("Active Watchlist")
        watchlist = api_get("/watchlist")
        if watchlist:
            df = pd.DataFrame(watchlist)
            if not df.empty:
                display_cols = ["ticker", "company_name", "cik", "exchange", "added_at"]
                available = [c for c in display_cols if c in df.columns]
                df_display = df[available].copy()
                col_labels = {
                    "ticker": "Ticker", "company_name": "Company",
                    "cik": "CIK", "exchange": "Exchange", "added_at": "Added",
                }
                df_display.rename(columns=col_labels, inplace=True)
                if "Added" in df_display.columns:
                    df_display["Added"] = pd.to_datetime(df_display["Added"]).dt.strftime("%Y-%m-%d")
                st.dataframe(df_display, use_container_width=True, hide_index=True)
            else:
                st.info("Watchlist is empty. Add a ticker to get started.")
        else:
            st.info("Watchlist is empty or API unavailable.")

    with col2:
        st.subheader("Add Ticker")
        with st.form("add_ticker"):
            ticker_input = st.text_input(
                "Ticker Symbol", placeholder="AAPL", max_chars=10
            ).upper()
            submitted = st.form_submit_button("Add to Watchlist")
            if submitted and ticker_input:
                with st.spinner(f"Resolving {ticker_input} via SEC EDGAR..."):
                    result = api_post("/watchlist", json={"ticker": ticker_input})
                if result:
                    st.success(f"Added {ticker_input}")
                    st.rerun()

        st.subheader("Remove Ticker")
        if watchlist:
            tickers = [item.get("ticker", "") for item in watchlist]
            if tickers:
                remove_ticker = st.selectbox("Select ticker to remove", tickers)
                if st.button("Remove"):
                    result = api_delete(f"/watchlist/{remove_ticker}")
                    if result:
                        st.success(f"Removed {remove_ticker}")
                        st.rerun()

    st.divider()
    st.subheader("Ingest Data")
    if watchlist:
        ingest_ticker = st.selectbox(
            "Select ticker to ingest",
            [item.get("ticker", "") for item in watchlist],
            key="ingest_select",
        )
        if st.button("Ingest Now"):
            with st.spinner(f"Pulling Form 4 filings for {ingest_ticker}..."):
                result = api_post(f"/ingest/{ingest_ticker}")
            if result:
                st.success(
                    f"Ingested {result.get('filings_processed', 0)} filings, "
                    f"{result.get('transactions_inserted', 0)} transactions, "
                    f"{result.get('alerts_generated', 0)} alerts generated."
                )


# ============================================================
# DASHBOARD PAGE
# ============================================================

elif page == "Dashboard":
    st.header("Insider Trading Dashboard")

    watchlist = api_get("/watchlist")
    if not watchlist:
        st.info("Add tickers to your watchlist first.")
    else:
        tickers = [item.get("ticker", "") for item in watchlist]

        col1, col2 = st.columns([1, 1])
        with col1:
            selected_ticker = st.selectbox("Company", tickers)
        with col2:
            days = st.slider("Lookback (days)", min_value=7, max_value=365, value=90)

        if selected_ticker:
            # Summary metrics
            summary = api_get(f"/transactions/{selected_ticker}/summary", {"days": days})
            if summary:
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Total Buys", summary.get("total_buys", 0))
                m2.metric("Total Sells", summary.get("total_sells", 0))
                m3.metric("Buy Value", f"${summary.get('total_buy_value', 0):,.0f}")
                m4.metric("Sell Value", f"${summary.get('total_sell_value', 0):,.0f}")
                sentiment = summary.get("net_sentiment", "neutral")
                sentiment_icon = {"bullish": "🟢 Bullish", "bearish": "🔴 Bearish", "neutral": "⚪ Neutral"}.get(sentiment, "⚪ Neutral")
                st.caption(f"Net Insider Sentiment: **{sentiment_icon}**")

            st.divider()

            # Transaction table
            st.subheader("Recent Transactions")
            txns = api_get("/transactions", {"ticker": selected_ticker, "days": days, "limit": 500})
            if txns:
                df = pd.DataFrame(txns)
                if not df.empty:
                    display_cols = [
                        "TRANSACTION_DATE", "INSIDER_NAME", "INSIDER_TITLE",
                        "TRANSACTION_CODE", "SHARES", "PRICE_PER_SHARE",
                        "TOTAL_VALUE", "SHARES_OWNED_AFTER",
                    ]
                    available = [c for c in display_cols if c in df.columns]
                    df_display = df[available].copy()

                    # Human-readable transaction codes
                    code_map = {
                        "P": "Purchase", "S": "Sale", "A": "Grant",
                        "D": "Disposition", "M": "Exercise", "G": "Gift",
                        "F": "Tax Withholding", "C": "Conversion",
                    }
                    if "TRANSACTION_CODE" in df_display.columns:
                        df_display["TRANSACTION_CODE"] = df_display["TRANSACTION_CODE"].map(
                            lambda x: code_map.get(x, x)
                        )

                    col_labels = {
                        "TRANSACTION_DATE": "Date", "INSIDER_NAME": "Insider",
                        "INSIDER_TITLE": "Title", "TRANSACTION_CODE": "Type",
                        "SHARES": "Shares", "PRICE_PER_SHARE": "Price",
                        "TOTAL_VALUE": "Value", "SHARES_OWNED_AFTER": "Owned After",
                    }
                    df_display.rename(columns=col_labels, inplace=True)
                    df_display.fillna("-", inplace=True)

                    st.dataframe(df_display, use_container_width=True, hide_index=True)

                    # --- Charts ---
                    st.subheader("Activity Over Time")

                    if "TRANSACTION_DATE" in df.columns and "TRANSACTION_CODE" in df.columns:
                        chart_df = df[df["TRANSACTION_CODE"].isin(["P", "S"])].copy()
                        if not chart_df.empty:
                            chart_df["TRANSACTION_DATE"] = pd.to_datetime(chart_df["TRANSACTION_DATE"])
                            chart_df["Direction"] = chart_df["TRANSACTION_CODE"].map(
                                {"P": "Buy", "S": "Sell"}
                            )

                            # Daily aggregated value
                            if "TOTAL_VALUE" in chart_df.columns:
                                daily = (
                                    chart_df.groupby(
                                        [pd.Grouper(key="TRANSACTION_DATE", freq="W"), "Direction"]
                                    )["TOTAL_VALUE"]
                                    .sum()
                                    .reset_index()
                                )
                                daily_pivot = daily.pivot(
                                    index="TRANSACTION_DATE", columns="Direction", values="TOTAL_VALUE"
                                ).fillna(0)
                                st.bar_chart(daily_pivot)

                            # Transaction count by insider
                            st.subheader("Transactions by Insider")
                            insider_counts = (
                                chart_df.groupby(["INSIDER_NAME", "Direction"])
                                .size()
                                .reset_index(name="Count")
                            )
                            insider_pivot = insider_counts.pivot(
                                index="INSIDER_NAME", columns="Direction", values="Count"
                            ).fillna(0)
                            st.bar_chart(insider_pivot)
                else:
                    st.info(f"No transactions found for {selected_ticker} in the last {days} days.")
            else:
                st.info("No transaction data available. Try ingesting data first.")


# ============================================================
# ALERTS PAGE
# ============================================================

elif page == "Alerts":
    st.header("Anomaly Alerts")

    col1, col2 = st.columns([1, 1])
    with col1:
        show_acknowledged = st.checkbox("Show acknowledged alerts", value=False)
    with col2:
        watchlist = api_get("/watchlist")
        filter_ticker = st.selectbox(
            "Filter by ticker",
            ["All"] + [item.get("ticker", "") for item in (watchlist or [])],
        )

    params = {}
    if filter_ticker != "All":
        params["ticker"] = filter_ticker
    if not show_acknowledged:
        params["acknowledged"] = False

    alerts = api_get("/alerts", params)

    if alerts:
        for alert in alerts:
            severity = alert.get("SEVERITY", "MEDIUM")
            severity_colors = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}
            icon = severity_colors.get(severity, "⚪")

            with st.expander(
                f"{icon} [{alert.get('ALERT_TYPE', '')}] {alert.get('TICKER', '')} - {alert.get('INSIDER_NAME', '')}",
                expanded=severity == "HIGH",
            ):
                st.write(alert.get("DESCRIPTION", ""))
                st.caption(f"Detected: {alert.get('DETECTED_AT', 'Unknown')}")

                if not alert.get("ACKNOWLEDGED"):
                    if st.button("Acknowledge", key=alert.get("ALERT_ID")):
                        api_post(f"/alerts/{alert['ALERT_ID']}/acknowledge")
                        st.rerun()
                else:
                    st.caption("Acknowledged")
    else:
        st.info("No alerts to display.")


# ============================================================
# ANALYTICS PAGE
# ============================================================

elif page == "Analytics":
    st.header("Cross-Company Analytics")

    watchlist = api_get("/watchlist")
    if not watchlist:
        st.info("Add tickers to your watchlist to see analytics.")
    else:
        days = st.slider("Lookback (days)", min_value=7, max_value=365, value=90, key="analytics_days")

        # Collect summaries for all tickers
        summaries = []
        for item in watchlist:
            ticker = item.get("ticker", "")
            summary = api_get(f"/transactions/{ticker}/summary", {"days": days})
            if summary:
                summaries.append(summary)

        if summaries:
            df = pd.DataFrame(summaries)

            st.subheader("Buy vs Sell Activity Across Watchlist")
            if not df.empty and "ticker" in df.columns:
                chart_df = df.set_index("ticker")[["total_buy_value", "total_sell_value"]]
                chart_df.columns = ["Buy Value ($)", "Sell Value ($)"]
                st.bar_chart(chart_df)

                st.subheader("Insider Activity Summary")
                display_df = df[["ticker", "total_buys", "total_sells", "total_buy_value", "total_sell_value", "unique_insiders", "net_sentiment"]].copy()
                display_df.columns = ["Ticker", "Buys", "Sells", "Buy Value", "Sell Value", "Unique Insiders", "Sentiment"]
                display_df["Buy Value"] = display_df["Buy Value"].apply(lambda x: f"${x:,.0f}")
                display_df["Sell Value"] = display_df["Sell Value"].apply(lambda x: f"${x:,.0f}")
                st.dataframe(display_df, use_container_width=True, hide_index=True)

                # Net sentiment overview
                st.subheader("Sentiment Overview")
                sentiment_counts = df["net_sentiment"].value_counts()
                col1, col2, col3 = st.columns(3)
                col1.metric("Bullish", sentiment_counts.get("bullish", 0))
                col2.metric("Neutral", sentiment_counts.get("neutral", 0))
                col3.metric("Bearish", sentiment_counts.get("bearish", 0))
        else:
            st.info("No transaction data available. Ingest data first.")
