"""Transaction query endpoints."""

from typing import Optional

from fastapi import APIRouter, Query

from api.models.schemas import InsiderTransaction, TransactionSummary
from api.services import snowflake as sf

router = APIRouter(prefix="/transactions", tags=["transactions"])


@router.get("", response_model=list[dict])
def list_transactions(
    ticker: Optional[str] = Query(None, description="Filter by ticker"),
    days: int = Query(90, description="Lookback window in days"),
    limit: int = Query(500, description="Max rows"),
):
    """Get insider transactions with optional filters."""
    return sf.get_transactions(ticker=ticker, days=days, limit=limit)


@router.get("/{ticker}/summary", response_model=TransactionSummary)
def transaction_summary(
    ticker: str,
    days: int = Query(90, description="Lookback window in days"),
):
    """Get aggregated buy/sell summary for a ticker."""
    return sf.get_transaction_summary(ticker=ticker, days=days)
