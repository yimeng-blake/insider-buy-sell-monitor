"""Watchlist management endpoints."""

from fastapi import APIRouter, HTTPException

from api.models.schemas import WatchlistAdd, WatchlistItem
from api.services import snowflake as sf
from api.services.edgar import resolve_ticker_to_cik

router = APIRouter(prefix="/watchlist", tags=["watchlist"])


@router.get("", response_model=list[WatchlistItem])
def list_watchlist():
    """Get all active watchlist items."""
    rows = sf.get_watchlist(active_only=True)
    return [WatchlistItem.from_sf_row(r) for r in rows]


@router.post("", response_model=WatchlistItem)
def add_ticker(payload: WatchlistAdd):
    """Add a ticker to the watchlist.

    Resolves the ticker to CIK via SEC EDGAR and adds it to Snowflake.
    """
    ticker = payload.ticker.upper()

    existing = sf.get_watchlist_item(ticker)
    if existing and existing.get("ACTIVE"):
        raise HTTPException(400, f"{ticker} is already on the watchlist")

    company = resolve_ticker_to_cik(ticker)
    if not company:
        raise HTTPException(404, f"Could not resolve ticker '{ticker}' via SEC EDGAR")

    result = sf.add_to_watchlist(
        ticker=ticker,
        company_name=company["name"],
        cik=company["cik"],
        exchange=company.get("exchange"),
        sic_code=company.get("sic"),
    )
    return result


@router.delete("/{ticker}")
def remove_ticker(ticker: str):
    """Remove a ticker from the watchlist (soft delete)."""
    existing = sf.get_watchlist_item(ticker.upper())
    if not existing:
        raise HTTPException(404, f"{ticker.upper()} not found in watchlist")
    sf.remove_from_watchlist(ticker)
    return {"status": "removed", "ticker": ticker.upper()}
