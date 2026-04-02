from pydantic import BaseModel, ConfigDict
from datetime import datetime, date
from typing import Optional


class SnowflakeModel(BaseModel):
    """Base model that accepts both lowercase and UPPERCASE keys from Snowflake."""
    model_config = ConfigDict(populate_by_name=True)

    @classmethod
    def from_sf_row(cls, row: dict):
        """Create from a Snowflake row dict with UPPERCASE keys."""
        lowered = {k.lower(): v for k, v in row.items()}
        return cls(**lowered)


class WatchlistItem(SnowflakeModel):
    ticker: str
    company_name: str
    cik: str
    exchange: Optional[str] = None
    sic_code: Optional[str] = None
    added_at: Optional[datetime] = None
    active: bool = True


class WatchlistAdd(BaseModel):
    ticker: str


class InsiderTransaction(BaseModel):
    transaction_id: str
    accession_number: str
    filing_date: date
    company_cik: str
    ticker: str
    insider_cik: str
    insider_name: str
    insider_title: str
    transaction_date: date
    transaction_code: str
    shares: float
    price_per_share: Optional[float] = None
    total_value: Optional[float] = None
    shares_owned_after: Optional[float] = None
    direct_or_indirect: str = "D"
    created_at: Optional[datetime] = None


class Alert(BaseModel):
    alert_id: str
    ticker: str
    insider_name: str
    alert_type: str
    description: str
    severity: str
    transaction_ids: Optional[str] = None
    detected_at: Optional[datetime] = None
    acknowledged: bool = False


class IngestionResult(BaseModel):
    ticker: str
    filings_processed: int
    transactions_inserted: int
    alerts_generated: int
    status: str
    message: Optional[str] = None
    error_message: Optional[str] = None


class TransactionSummary(BaseModel):
    ticker: str
    total_buys: int
    total_sells: int
    total_buy_value: float
    total_sell_value: float
    unique_insiders: int
    latest_transaction_date: Optional[date] = None
    net_sentiment: str  # "bullish", "bearish", "neutral"
