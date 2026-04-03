"""Streamlit frontend for Insider Buy/Sell Monitor.

Queries Snowflake directly via the service layer (works on Streamlit Cloud
and locally without requiring a FastAPI backend).
"""

import streamlit as st
import pandas as pd

from api.services import snowflake as sf
from api.services.edgar import resolve_ticker_to_cik


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

    # --- Today's Insider Activity Brief ---
    st.subheader("Today's Insider Activity")
    today_txns = sf.get_transactions(days=0, limit=500)
    if today_txns:
        tdf = pd.DataFrame(today_txns)
        if not tdf.empty:
            buys = tdf[tdf["TRANSACTION_CODE"] == "P"]
            sells = tdf[tdf["TRANSACTION_CODE"] == "S"]
            m1, m2, m3 = st.columns(3)
            m1.metric("Filings Today", len(tdf))
            m2.metric("Total Buy Value", f"${buys['TOTAL_VALUE'].sum():,.0f}" if not buys.empty else "$0")
            m3.metric("Total Sell Value", f"${sells['TOTAL_VALUE'].sum():,.0f}" if not sells.empty else "$0")

            code_map = {
                "P": "Purchase", "S": "Sale", "A": "Grant",
                "D": "Disposition", "M": "Exercise", "G": "Gift",
                "F": "Tax Withholding", "C": "Conversion",
            }
            display_cols = [
                "TICKER", "INSIDER_NAME", "INSIDER_TITLE",
                "TRANSACTION_CODE", "SHARES", "PRICE_PER_SHARE",
                "TOTAL_VALUE", "TRANSACTION_DATE",
            ]
            available = [c for c in display_cols if c in tdf.columns]
            brief_df = tdf[available].copy()
            if "TRANSACTION_CODE" in brief_df.columns:
                brief_df["TRANSACTION_CODE"] = brief_df["TRANSACTION_CODE"].map(
                    lambda x: code_map.get(x, x)
                )
            brief_df.rename(columns={
                "TICKER": "Ticker", "INSIDER_NAME": "Insider",
                "INSIDER_TITLE": "Title", "TRANSACTION_CODE": "Type",
                "SHARES": "Shares", "PRICE_PER_SHARE": "Price",
                "TOTAL_VALUE": "Value", "TRANSACTION_DATE": "Date",
            }, inplace=True)
            brief_df.fillna("-", inplace=True)
            st.dataframe(brief_df, use_container_width=True, hide_index=True)
        else:
            st.info("No insider filings today.")
    else:
        st.info("No insider filings today.")
    st.divider()


    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("Active Watchlist")
        watchlist = sf.get_watchlist(active_only=True)
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
                    df_display["Added"] = pd.to_datetime(df_display["Added"]).dt.strftime("%Y-%m-%d")
                st.dataframe(df_display, use_container_width=True, hide_index=True)
            else:
                st.info("Watchlist is empty. Add a ticker to get started.")
        else:
            st.info("Watchlist is empty. Add a ticker to get started.")

    with col2:
        st.subheader("Add Ticker")
        with st.form("add_ticker"):
            ticker_input = st.text_input(
                "Ticker Symbol", placeholder="AAPL", max_chars=10
            ).upper()
            submitted = st.form_submit_button("Add to Watchlist")
            if submitted and ticker_input:
                with st.spinner(f"Resolving {ticker_input} via SEC EDGAR..."):
                    try:
                        company = resolve_ticker_to_cik(ticker_input)
                        result = sf.add_to_watchlist(
                            ticker=ticker_input,
                            company_name=company["name"],
                            cik=company["cik"],
                            exchange=company.get("exchange"),
                            sic_code=company.get("sic"),
                        )
                        if result:
                            st.success(f"Added {ticker_input}")
                            st.rerun()
                    except Exception as e:
                        st.error(f"Failed to add {ticker_input}: {e}")

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
            try:
                import requests as _req
                with st.spinner(f"Pulling Form 4 filings for {ingest_ticker}..."):
                    resp = _req.post(f"http://localhost:8000/ingest/{ingest_ticker}", timeout=120)
                    resp.raise_for_status()
                    result = resp.json()
                st.success(
                    f"Ingested {result.get('filings_processed', 0)} filings, "
                    f"{result.get('transactions_inserted', 0)} transactions, "
                    f"{result.get('alerts_generated', 0)} alerts generated."
                )
            except Exception:
                st.warning(
                    "Ingestion requires the local FastAPI server (port 8000). "
                    "Run `uvicorn api.main:app` locally, or use the CLI: "
                    "`python -m ingestion.scheduled_ingest`."
                )


# ============================================================
# DASHBOARD PAGE
# ============================================================

elif page == "Dashboard":
    st.header("Insider Trading Dashboard")

    watchlist = sf.get_watchlist(active_only=True)
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
        watchlist = sf.get_watchlist(active_only=True)
        filter_ticker = st.selectbox(
            "Filter by ticker",
            ["All"] + [item.get("TICKER", "") for item in (watchlist or [])],
        )

    alert_ticker = filter_ticker if filter_ticker != "All" else None
    alert_ack = None if show_acknowledged else False

    alerts = sf.get_alerts(ticker=alert_ticker, acknowledged=alert_ack, limit=100)

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

    watchlist = sf.get_watchlist(active_only=True)
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
