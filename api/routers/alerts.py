"""Alert management endpoints."""

from typing import Optional

from fastapi import APIRouter, Query

from api.services import snowflake as sf

router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.get("")
def list_alerts(
    ticker: Optional[str] = Query(None, description="Filter by ticker"),
    acknowledged: Optional[bool] = Query(None, description="Filter by acknowledged status"),
    limit: int = Query(100, description="Max alerts"),
):
    """Get alerts with optional filters."""
    return sf.get_alerts(ticker=ticker, acknowledged=acknowledged, limit=limit)


@router.post("/{alert_id}/acknowledge")
def acknowledge_alert(alert_id: str):
    """Mark an alert as acknowledged."""
    sf.acknowledge_alert(alert_id)
    return {"status": "acknowledged", "alert_id": alert_id}
