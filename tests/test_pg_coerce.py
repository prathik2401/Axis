"""Unit tests for PostgreSQL value coercion used during replica replay."""

from datetime import datetime, timezone
from decimal import Decimal

from axis.plugins.postgres import _coerce_pg_value, _coerce_row


def test_coerce_iso_timestamp():
    value = _coerce_pg_value("2026-08-25T03:54:04.097052+00:00")
    assert isinstance(value, datetime)
    assert value.year == 2026
    assert value.tzinfo is not None


def test_coerce_leaves_email_and_phone():
    assert _coerce_pg_value("user@example.com") == "user@example.com"
    assert _coerce_pg_value("555-1234") == "555-1234"


def test_coerce_numeric_string():
    assert _coerce_pg_value("1299.99") == Decimal("1299.99")


def test_coerce_row_none():
    assert _coerce_row(None) is None


def test_coerce_row_mixed():
    row = _coerce_row(
        {
            "id": 1,
            "email": "a@b.com",
            "created_at": "2026-01-02T03:04:05+00:00",
            "price": 10.5,
        }
    )
    assert row["id"] == 1
    assert row["email"] == "a@b.com"
    assert isinstance(row["created_at"], datetime)
    assert row["price"] == 10.5
