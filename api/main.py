"""FastAPI application entry point."""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.routers import watchlist, transactions, alerts, ingest
from api.services.snowflake import close_session


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    close_session()


app = FastAPI(
    title="Insider Buy/Sell Monitor",
    description="Monitor SEC Form 4 insider trading activity for watchlisted companies.",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(watchlist.router)
app.include_router(transactions.router)
app.include_router(alerts.router)
app.include_router(ingest.router)


@app.get("/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    from api.config import settings

    uvicorn.run(app, host=settings.API_HOST, port=settings.API_PORT)
