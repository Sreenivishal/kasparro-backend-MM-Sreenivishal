from fastapi import APIRouter
from uuid import uuid4

from services.db import fetch_all, ping
from services.checkpoints import get_checkpoint

router = APIRouter()


@router.get("/health")
def health():
    return {
        "db_connected": ping(),
        "coingecko_last_run": str(get_checkpoint("coingecko")),
        "coinpaprika_last_run": str(get_checkpoint("coinpaprika")),
    }


@router.get("/data")
def get_data(limit: int = 50, offset: int = 0):
    rows = fetch_all(
        """
        SELECT *
        FROM prices
        ORDER BY timestamp DESC
        LIMIT %s OFFSET %s
        """,
        (limit, offset),
    )

    return {
        "request_id": str(uuid4()),
        "count": len(rows),
        "data": rows,
    }


@router.get("/stats")
def stats():
    return fetch_all(
        "SELECT source, last_run FROM checkpoints ORDER BY source"
    )
