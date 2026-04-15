"""Tests for utils.query_router.QueryRouter."""

import asyncio

from utils.query_router import DatabaseType, QueryRouter


def test_route_prefers_yelp_and_reviews_for_yelp_question() -> None:
    async def _run():
        router = QueryRouter(schema_introspector=None)
        return await router.route("Yelp business reviews and star ratings")

    routes = asyncio.run(_run())
    assert len(routes) >= 1
    assert DatabaseType.DUCKDB in routes or DatabaseType.MONGODB in routes


def test_needs_cross_db_join_detects_multiple_routes() -> None:
    router = QueryRouter(None)
    assert router.needs_cross_db_join(
        "customers and tickets",
        [DatabaseType.POSTGRESQL, DatabaseType.MONGODB],
    ) is True
