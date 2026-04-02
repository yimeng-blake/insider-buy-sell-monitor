"""Streamlit frontend for Insider Buy/Sell Monitor.

Calls Snowflake, EDGAR, and anomaly services directly — no FastAPI backend needed.
Deployable to Streamlit Community Cloud.
"""

import sys
import os

# Ensure project root is on the path so `api.*` imports work
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import streamlit as st
import pandas as pd
from datetime import datetime

from api.services import snowflake as sf
from api.services import edgar
from api.services import anomaly


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
        try:
            watchlist = sf.get_watchlist(active_only=True)
        except Exception as e:
            st.error(f"Snowflake error: {e}")
            watchlist = []

        if watchlist:
            df = pd.DataFrame(watchlist)
            if not df.empty:
                display_cols = ["TICKER", "COMPANY_NAME", "CIK", "EXCHANGE", "ADDED_AT"]
                available = [c for c in display_cols if c in df.columns]
                df_display = df[available].copy()
                col_labels = {
                    "TICKER": "Ticker", "COMPANY_NAME": "Company",
                    "CIK": "CIK", "EXCHANGE": "Exchange", "ADDED_AT": "Added",
                }
                df_display.rename(columns=col_labels, inplace=True)
                if "Added" in df_display.columns:
                    df_display["Added"] = pd.to_datetime(df_display["Added"], utc=True).dt.strftime("%Y-%m-%d")
                st.dataframe(df_display, width="stretch", hide_index=True)
            else:
                st.info("Watchlist is empty. Add a ticker to get started.")
        else:
            st.info("Watchlist is empty or Snowflake unavailable.")

    with col2:
        st.subheader("Add Ticker")
        with st.form("add_ticker"):
            ticker_input = st.text_input(
                "Ticker Symbol", placeholder="AAPL", max_chars=10
            ).upper()
            submitted = st.form_submit_button("Add to Watchlist")
            if submitted and ticker_input:
                existing = sf.get_watchlist_item(ticker_input)
                if existing and existing.get("ACTIVE"):
                    st.warning(f"{ticker_input} is already on the watchlist.")
                else:
                    with st.spinner(f"Resolving {ticker_input} via SEC EDGAR..."):
                        company = edgar.resolve_ticker_to_cik(ticker_input)
                    if not company:
                        st.error(f"Could not resolve ticker '{ticker_input}' via SEC EDGAR.")
                    else:
                        sf.add_to_watchlist(
                            ticker=ticker_input,
                            company_name=company["name"],
                            cik=company["cik"],
                            exchange=company.get("exchange"),
                            sic_code=company.get("sic"),
                        )
                        st.success(f"Added {ticker_input}")
                        st.rerun()

        st.subheader("Remove Ticker")
        if watchlist:
            tickers = [item.get("TICKER", "") for item in watchlist]
            if tickers:
                remove_ticker = st.selectbox("Select ticker to remove", tickers)
                if st.button("Remove"):
                    sf.remove_from_watchlist(remove_ticker)
                    st.success(f"Removed {remove_ticker}")
                    st.rerun()

    st.divider()
    st.subheader("Ingest Data")
    if watchlist:
        ingest_ticker = st.selectbox(
            "Select ticker to ingest",
            [item.get("TICKER", "") for item in watchlist],
            key="ingest_select",
        )
        if st.button("Ingest Now"):
            with st.spinner(f"Pulling Form 4 filings for {ingest_ticker}..."):
                watchlist_item = sf.get_watchlist_item(ingest_ticker)
                if not watchlist_item:
                    st.error(f"{ingest_ticker} not found in watchlist.")
                else:
                    cik = watchlist_item["CIK"]
                    run_id = sf.create_ingestion_log(ingest_ticker)
                    try:
                        last_date = sf.get_last_ingestion_date(ingest_ticker)
                        filings = edgar.fetch_form4_filings(cik, after_date=last_date)

                        total_inserted = 0
                        insiders_seen = {}
                        for filing in filings:
                            transactions = edgar.parse_form4_xml(
                                cik=cik,
                                accession_number=filing["accession_number"],
                                filing_date=filing["filing_date"],
                                ticker=ingest_ticker,
                                primary_doc=filing.get("primary_doc"),
                            )
                            total_inserted += sf.insert_transactions(transactions)
                            for txn in transactions:
                                insiders_seen[txn["insider_cik"]] = (
                                    txn["insider_name"], txn["insider_title"],
                                )

                        for insider_cik, (name, title) in insiders_seen.items():
                            sf.upsert_insider(insider_cik, name, title)

                        alerts_list = anomaly.run_anomaly_detection(ingest_ticker)
                        alerts_generated = 0
                        for alert in alerts_list:
                            sf.insert_alert(**alert)
                            alerts_generated += 1

                        sf.complete_ingestion_log(run_id, len(filings), total_inserted)
                        if len(filings) == 0 and total_inserted == 0:
                            st.info(
                                f"No new filings found for {ingest_ticker}. "
                                f"Data is already up to date."
                                + (f" {alerts_generated} alerts generated from existing data." if alerts_generated else "")
                            )
                        else:
                            st.success(
                                f"Ingested {len(filings)} filings, "
                                f"{total_inserted} new transactions."
                                + (f" {alerts_generated} alerts generated." if alerts_generated else "")
                            )
                    except Exception as e:
                        sf.complete_ingestion_log(run_id, 0, 0, status="FAILED", error=str(e))
                        st.error(f"Ingestion failed: {e}")


# ============================================================
# DASHBOARD PAGE
# ============================================================

elif page == "Dashboard":
    st.header("Insider Trading Dashboard")

    try:
        watchlist = sf.get_watchlist(active_only=True)
    except Exception as e:
        st.error(f"Snowflake error: {e}")
        watchlist = []

    if not watchlist:
        st.info("Add tickers to your watchlist first.")
    else:
        tickers = [item.get("TICKER", "") for item in watchlist]

        col1, col2 = st.columns([1, 1])
        with col1:
            selected_ticker = st.selectbox("Company", tickers)
        with col2:
            days = st.slider("Lookback (days)", min_value=7, max_value=365, value=90)

        if selected_ticker:
            # Summary metrics
            summary = sf.get_transaction_summary(selected_ticker, days=days)
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
            txns = sf.get_transactions(ticker=selected_ticker, days=days, limit=500)
            if txns:
                df = pd.DataFrame(txns)
                if not df.empty:
                    # Transaction type filter
                    code_labels = {
                        "P": "Purchase", "S": "Sale", "A": "Grant",
                        "D": "Disposition", "M": "Exercise", "G": "Gift",
                        "F": "Tax Withholding", "C": "Conversion", "J": "Other",
                    }
                    available_codes = df["TRANSACTION_CODE"].unique().tolist() if "TRANSACTION_CODE" in df.columns else []
                    filter_options = {code_labels.get(c, c): c for c in available_codes}

                    selected_types = st.multiselect(
                        "Filter by transaction type",
                        options=list(filter_options.keys()),
                        default=list(filter_options.keys()),
                        key="txn_type_filter",
                    )
                    selected_codes = [filter_options[t] for t in selected_types]

                    if selected_codes:
                        df_filtered = df[df["TRANSACTION_CODE"].isin(selected_codes)].copy()
                    else:
                        df_filtered = df.copy()

                    display_cols = [
                        "TRANSACTION_DATE", "INSIDER_NAME", "INSIDER_TITLE",
                        "TRANSACTION_CODE", "SHARES", "PRICE_PER_SHARE",
                        "TOTAL_VALUE", "SHARES_OWNED_AFTER",
                    ]
                    available = [c for c in display_cols if c in df_filtered.columns]
                    df_display = df_filtered[available].copy()

                    # Keep raw codes for coloring before mapping labels
                    raw_codes = df_display["TRANSACTION_CODE"].copy() if "TRANSACTION_CODE" in df_display.columns else pd.Series()

                    # Human-readable transaction codes with emoji indicators
                    code_map = {
                        "P": "\U0001f7e2 Purchase", "S": "\U0001f534 Sale",
                        "A": "\U0001f535 Grant", "D": "\U0001f534 Disposition",
                        "M": "\u26aa Exercise", "G": "\U0001f7e3 Gift",
                        "F": "\U0001f7e0 Tax Withholding", "C": "\u26aa Conversion",
                        "J": "\u26aa Other",
                    }
                    if "TRANSACTION_CODE" in df_display.columns:
                        df_display["TRANSACTION_CODE"] = df_display["TRANSACTION_CODE"].map(
                            lambda x: code_map.get(x, x)
                        )

                    # Format numeric columns cleanly
                    def fmt_int(v):
                        try:
                            n = float(v)
                            return f"{n:,.0f}" if pd.notna(n) else "-"
                        except (ValueError, TypeError):
                            return "-"

                    def fmt_currency(v):
                        try:
                            n = float(v)
                            return f"${n:,.2f}" if pd.notna(n) and n != 0 else "-"
                        except (ValueError, TypeError):
                            return "-"

                    for col in ["SHARES", "SHARES_OWNED_AFTER"]:
                        if col in df_display.columns:
                            df_display[col] = df_display[col].apply(fmt_int)
                    if "PRICE_PER_SHARE" in df_display.columns:
                        df_display["PRICE_PER_SHARE"] = df_display["PRICE_PER_SHARE"].apply(fmt_currency)
                    if "TOTAL_VALUE" in df_display.columns:
                        df_display["TOTAL_VALUE"] = df_display["TOTAL_VALUE"].apply(fmt_currency)

                    col_labels = {
                        "TRANSACTION_DATE": "Date", "INSIDER_NAME": "Insider",
                        "INSIDER_TITLE": "Title", "TRANSACTION_CODE": "Type",
                        "SHARES": "Shares", "PRICE_PER_SHARE": "Price",
                        "TOTAL_VALUE": "Value", "SHARES_OWNED_AFTER": "Owned After",
                    }
                    df_display.rename(columns=col_labels, inplace=True)

                    # Row background coloring by transaction type
                    row_colors = {
                        "P": "background-color: rgba(76, 175, 80, 0.12)",   # green
                        "S": "background-color: rgba(244, 67, 54, 0.12)",   # red
                        "A": "background-color: rgba(33, 150, 243, 0.10)",  # blue
                        "D": "background-color: rgba(244, 67, 54, 0.12)",   # red
                        "M": "background-color: rgba(158, 158, 158, 0.10)", # gray
                        "G": "background-color: rgba(156, 39, 176, 0.10)",  # purple
                        "F": "background-color: rgba(255, 152, 0, 0.12)",   # orange
                        "C": "background-color: rgba(158, 158, 158, 0.10)", # gray
                    }

                    def color_rows(row_idx):
                        code = raw_codes.iloc[row_idx] if row_idx < len(raw_codes) else ""
                        style = row_colors.get(code, "")
                        return [style] * len(df_display.columns)

                    styled = df_display.style.apply(
                        lambda x: color_rows(x.name), axis=1
                    )

                    st.dataframe(styled, width="stretch", hide_index=True)

                    # --- Charts ---
                    st.subheader("Activity Over Time")

                    if "TRANSACTION_DATE" in df_filtered.columns and "TRANSACTION_CODE" in df_filtered.columns:
                        # Include all meaningful transaction types
                        type_labels = {
                            "P": "Purchase", "S": "Sale", "A": "Grant",
                            "F": "Tax Withholding", "G": "Gift",
                            "M": "Exercise", "D": "Disposition", "C": "Conversion",
                            "J": "Other",
                        }
                        chart_df = df_filtered[df_filtered["TRANSACTION_CODE"].isin(type_labels.keys())].copy()
                        if not chart_df.empty:
                            chart_df["TRANSACTION_DATE"] = pd.to_datetime(chart_df["TRANSACTION_DATE"])
                            chart_df["Type"] = chart_df["TRANSACTION_CODE"].map(type_labels)

                            # Weekly aggregated count by type
                            weekly = (
                                chart_df.groupby(
                                    [pd.Grouper(key="TRANSACTION_DATE", freq="W"), "Type"]
                                )
                                .size()
                                .reset_index(name="Count")
                            )
                            weekly_pivot = weekly.pivot(
                                index="TRANSACTION_DATE", columns="Type", values="Count"
                            ).fillna(0)
                            st.bar_chart(weekly_pivot)

                            # Buy vs Sell value comparison (if both exist)
                            buy_sell = chart_df[chart_df["TRANSACTION_CODE"].isin(["P", "S"])].copy()
                            if not buy_sell.empty and "TOTAL_VALUE" in buy_sell.columns:
                                buy_sell["Direction"] = buy_sell["TRANSACTION_CODE"].map(
                                    {"P": "Buy", "S": "Sell"}
                                )
                                daily_val = (
                                    buy_sell.groupby(
                                        [pd.Grouper(key="TRANSACTION_DATE", freq="W"), "Direction"]
                                    )["TOTAL_VALUE"]
                                    .sum()
                                    .reset_index()
                                )
                                val_pivot = daily_val.pivot(
                                    index="TRANSACTION_DATE", columns="Direction", values="TOTAL_VALUE"
                                ).fillna(0)
                                st.caption("Buy vs Sell Value ($)")
                                st.bar_chart(val_pivot)

                            # Transaction count by insider
                            st.subheader("Transactions by Insider")
                            insider_counts = (
                                chart_df.groupby(["INSIDER_NAME", "Type"])
                                .size()
                                .reset_index(name="Count")
                            )
                            insider_pivot = insider_counts.pivot(
                                index="INSIDER_NAME", columns="Type", values="Count"
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
        try:
            watchlist = sf.get_watchlist(active_only=True)
        except Exception:
            watchlist = []
        filter_ticker = st.selectbox(
            "Filter by ticker",
            ["All"] + [item.get("TICKER", "") for item in watchlist],
        )

    params = {}
    if filter_ticker != "All":
        params["ticker"] = filter_ticker
    if not show_acknowledged:
        params["acknowledged"] = False

    try:
        alerts = sf.get_alerts(**params)
    except Exception as e:
        st.error(f"Snowflake error: {e}")
        alerts = []

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
                        sf.acknowledge_alert(alert["ALERT_ID"])
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

    try:
        watchlist = sf.get_watchlist(active_only=True)
    except Exception:
        watchlist = []

    if not watchlist:
        st.info("Add tickers to your watchlist to see analytics.")
    else:
        days = st.slider("Lookback (days)", min_value=7, max_value=365, value=90, key="analytics_days")

        # Collect summaries for all tickers
        summaries = []
        for item in watchlist:
            ticker = item.get("TICKER", "")
            summary = sf.get_transaction_summary(ticker, days=days)
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
                st.dataframe(display_df, width="stretch", hide_index=True)

                # Net sentiment overview
                st.subheader("Sentiment Overview")
                sentiment_counts = df["net_sentiment"].value_counts()
                col1, col2, col3 = st.columns(3)
                col1.metric("Bullish", sentiment_counts.get("bullish", 0))
                col2.metric("Neutral", sentiment_counts.get("neutral", 0))
                col3.metric("Bearish", sentiment_counts.get("bearish", 0))
        else:
            st.info("No transaction data available. Ingest data first.")
