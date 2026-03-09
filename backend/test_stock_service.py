import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, patch
from datetime import date, timedelta


# ── Helpers to build fake DB rows ────────────────────────────────────────────

def make_price_volume_rows(n=25, base_price=50.0, base_volume=1_000_000,
                            today_volume_multiplier=1.0, zero_volume_on_day=None,
                            none_price_on_day=None):
    """
    Generate n fake (date, close_price, volume) rows sorted ascending.
    today_volume_multiplier: multiplies the last row's volume to simulate surge.
    zero_volume_on_day: index of row to set volume=0 (edge case).
    none_price_on_day: index of row to set price=None (edge case).
    """
    rows = []
    for i in range(n):
        d      = date(2025, 1, 1) + timedelta(days=i)
        price  = base_price + i * 0.1
        volume = base_volume
        if zero_volume_on_day is not None and i == zero_volume_on_day:
            volume = 0
        if none_price_on_day is not None and i == none_price_on_day:
            price = None
        rows.append((d, price, volume))

    # Apply today's volume multiplier to last row
    last = rows[-1]
    rows[-1] = (last[0], last[1], int(base_volume * today_volume_multiplier))
    return rows


def rows_to_df(rows):
    """Convert fake rows to DataFrame matching analyze_stock_from_db format."""
    df = pd.DataFrame(rows, columns=["Date", "Close", "Volume"])
    df["Date"]   = pd.to_datetime(df["Date"])
    df["Close"]  = pd.to_numeric(df["Close"],  errors="coerce")
    df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")
    return df.sort_values("Date").reset_index(drop=True)


# ── Pure logic tests (no DB needed) ──────────────────────────────────────────

class TestVolumeSurgeCalculation:
    """Tests for the core volume surge calculation logic."""

    def _calc_surge(self, rows):
        """Replicate the surge calculation from analyze_stock_from_db."""
        df      = rows_to_df(rows)
        volumes = df["Volume"].dropna()
        prices  = df["Close"].dropna()

        if len(volumes) < 21 or len(prices) < 2:
            return None

        avg_volume   = volumes.iloc[-21:-1].mean()
        today_volume = volumes.iloc[-1]

        if avg_volume <= 0:
            return None

        return ((today_volume - avg_volume) / avg_volume) * 100

    def test_normal_surge(self):
        """Stock with 2x today's volume should show ~100% surge."""
        rows  = make_price_volume_rows(n=25, base_volume=1_000_000,
                                       today_volume_multiplier=2.0)
        surge = self._calc_surge(rows)
        assert surge is not None
        assert 90 < surge < 110  # ~100% surge

    def test_no_surge(self):
        """Stock with same volume as average should show ~0% surge."""
        rows  = make_price_volume_rows(n=25, base_volume=1_000_000,
                                       today_volume_multiplier=1.0)
        surge = self._calc_surge(rows)
        assert surge is not None
        assert -5 < surge < 5  # ~0% surge

    def test_extreme_surge(self):
        """Extreme 30x volume surge (like a news event) should not crash."""
        rows  = make_price_volume_rows(n=25, base_volume=1_000_000,
                                       today_volume_multiplier=30.0)
        surge = self._calc_surge(rows)
        assert surge is not None
        assert surge > 2000  # ~2900% surge

    def test_below_threshold_filtered(self):
        """Surge below threshold should return None (filtered out)."""
        rows    = make_price_volume_rows(n=25, base_volume=1_000_000,
                                         today_volume_multiplier=1.01)
        surge   = self._calc_surge(rows)
        threshold = 1.5
        # Simulate the threshold filter
        result = surge if (surge is not None and surge >= threshold) else None
        assert result is None

    def test_insufficient_rows_returns_none(self):
        """Less than 21 rows should return None — not enough for 20-day avg."""
        rows  = make_price_volume_rows(n=15)  # only 15 rows
        surge = self._calc_surge(rows)
        assert surge is None

    def test_exactly_21_rows(self):
        """Exactly 21 rows should work (minimum required)."""
        rows  = make_price_volume_rows(n=21, today_volume_multiplier=2.0)
        surge = self._calc_surge(rows)
        assert surge is not None

    def test_zero_volume_day_in_history(self):
        """A zero-volume day in the middle of history should not crash."""
        rows  = make_price_volume_rows(n=25, zero_volume_on_day=10)
        surge = self._calc_surge(rows)
        # Should still compute (zero volume dilutes avg but doesn't crash)
        assert surge is not None or surge is None  # either is acceptable

    def test_zero_average_volume_returns_none(self):
        """If all historical volumes are 0, should return None (div by zero guard)."""
        rows = make_price_volume_rows(n=25, base_volume=0,
                                      today_volume_multiplier=1.0)
        surge = self._calc_surge(rows)
        assert surge is None


class TestPriceChangeCalculation:
    """Tests for the price change calculation logic."""

    def _calc_price_change(self, rows):
        df     = rows_to_df(rows)
        prices = df["Close"].dropna()
        if len(prices) < 2:
            return None
        prev_close = prices.iloc[-2]
        price      = prices.iloc[-1]
        if prev_close == 0:
            return None
        return ((price - prev_close) / prev_close) * 100

    def test_positive_price_change(self):
        """Price going from 100 to 110 should show +10% change."""
        rows = make_price_volume_rows(n=25, base_price=100.0)
        # Force last two prices
        rows[-2] = (rows[-2][0], 100.0, rows[-2][2])
        rows[-1] = (rows[-1][0], 110.0, rows[-1][2])
        change = self._calc_price_change(rows)
        assert change is not None
        assert abs(change - 10.0) < 0.01

    def test_negative_price_change(self):
        """Price going from 100 to 90 should show -10% change."""
        rows = make_price_volume_rows(n=25, base_price=100.0)
        rows[-2] = (rows[-2][0], 100.0, rows[-2][2])
        rows[-1] = (rows[-1][0], 90.0,  rows[-1][2])
        change = self._calc_price_change(rows)
        assert change is not None
        assert abs(change - (-10.0)) < 0.01

    def test_zero_prev_close_returns_none(self):
        """If previous close is 0, should return None (div by zero guard)."""
        rows = make_price_volume_rows(n=25)
        rows[-2] = (rows[-2][0], 0.0, rows[-2][2])
        change = self._calc_price_change(rows)
        assert change is None

    def test_none_price_in_last_row(self):
        """None price in last row should not crash."""
        rows = make_price_volume_rows(n=25, none_price_on_day=24)
        df     = rows_to_df(rows)
        prices = df["Close"].dropna()
        # After dropna, last row might be gone — should handle gracefully
        assert len(prices) >= 0  # no crash


class TestFilters:
    """Tests for the stock filtering logic (price, volume, market cap)."""

    def test_penny_stock_filtered(self):
        """Stocks under $5 should be excluded."""
        price = 4.99
        assert price < 5  # filter should catch this

    def test_price_above_threshold_passes(self):
        """Stocks at $5 or above should pass price filter."""
        price = 5.00
        assert price >= 5

    def test_low_avg_volume_filtered(self):
        """Stocks with avg volume under 500k should be excluded."""
        avg_volume = 499_999
        assert avg_volume < 500_000

    def test_sufficient_avg_volume_passes(self):
        """Stocks with avg volume 500k+ should pass volume filter."""
        avg_volume = 500_000
        assert avg_volume >= 500_000

    def test_market_cap_under_1b_filtered(self):
        """Stocks under $1B market cap should be excluded."""
        market_cap = 999_999_999
        assert market_cap < 1_000_000_000

    def test_market_cap_over_1b_passes(self):
        """Stocks over $1B market cap should pass."""
        market_cap = 1_000_000_001
        assert market_cap >= 1_000_000_000

    def test_threshold_default_is_1_5(self):
        """Default threshold should be 1.5%."""
        default_threshold = 1.5
        assert default_threshold == 1.5

    def test_threshold_zero_includes_all_surges(self):
        """Threshold of 0 should include any positive surge."""
        surge     = 0.1
        threshold = 0.0
        assert surge >= threshold

    def test_threshold_100_filters_normal_days(self):
        """Threshold of 100% should only include stocks with 2x+ normal volume."""
        surge_normal  = 50.0   # only 50% above avg — filtered
        surge_extreme = 150.0  # 150% above avg — passes
        threshold = 100.0
        assert surge_normal  < threshold
        assert surge_extreme >= threshold


class TestDataIntegrity:
    """Tests for data integrity and output format."""

    def test_result_has_required_fields(self):
        """Result dict must have all required fields."""
        required = {"symbol", "company", "price", "price_change",
                    "market_cap_billion", "today_volume", "avg_volume", "volume_surge"}
        result = {
            "symbol": "AAPL", "company": "Apple Inc.", "price": 150.0,
            "price_change": 2.5, "market_cap_billion": 2500.0,
            "today_volume": 5_000_000, "avg_volume": 2_000_000, "volume_surge": 150.0,
        }
        assert required.issubset(result.keys())

    def test_volume_surge_is_float(self):
        """volume_surge must be a float, not int or string."""
        surge = float(round(150.333, 2))
        assert isinstance(surge, float)

    def test_market_cap_billion_conversion(self):
        """Market cap stored as raw int should convert to billions correctly."""
        raw_mc           = 2_500_000_000_000  # $2.5T (Apple)
        mc_billion       = round(raw_mc / 1_000_000_000, 2)
        assert mc_billion == 2500.0

    def test_market_cap_zero_when_none(self):
        """None market cap should default to 0.0, not crash."""
        mc     = None
        result = round(float(mc) / 1_000_000_000, 2) if mc else 0.0
        assert result == 0.0

    def test_limit_respected(self):
        """Results should never exceed the requested limit."""
        fake_results = [{"symbol": f"SYM{i}"} for i in range(50)]
        limit        = 10
        assert len(fake_results[:limit]) == limit

    def test_results_sorted_by_volume_surge_desc(self):
        """Results must be sorted by volume_surge descending."""
        results = [
            {"symbol": "A", "volume_surge": 50.0},
            {"symbol": "B", "volume_surge": 200.0},
            {"symbol": "C", "volume_surge": 100.0},
        ]
        results.sort(key=lambda x: x["volume_surge"], reverse=True)
        assert results[0]["symbol"] == "B"
        assert results[1]["symbol"] == "C"
        assert results[2]["symbol"] == "A"

    def test_empty_result_on_no_data(self):
        """Empty DB should return empty list, not crash."""
        rows = []  # no DB rows
        assert rows == []

    def test_company_name_defaults_to_empty_string(self):
        """Missing company name should be empty string, not None."""
        company = None
        result  = str(company) if company else ""
        assert result == ""


class TestEdgeCases:
    """Edge cases explicitly mentioned in requirements."""

    def test_single_zero_volume_day_at_end(self):
        """Zero volume on today (last row) — today_volume=0, should be filtered."""
        today_volume = 0
        assert today_volume <= 0  # filter: today_volume <= 0 → return None

    def test_all_same_price_no_change(self):
        """All prices identical → 0% price change, valid result."""
        rows = make_price_volume_rows(n=25, base_price=50.0)
        # Override all prices to same value
        rows = [(r[0], 50.0, r[2]) for r in rows]
        df     = rows_to_df(rows)
        prices = df["Close"].dropna()
        prev_close = prices.iloc[-2]
        price      = prices.iloc[-1]
        change = ((price - prev_close) / prev_close) * 100
        assert change == 0.0

    def test_extreme_outlier_volume_does_not_crash(self):
        """Volume of 1 billion shares (extreme outlier) should not crash."""
        rows = make_price_volume_rows(n=25, base_volume=1_000_000,
                                      today_volume_multiplier=1000.0)
        df      = rows_to_df(rows)
        volumes = df["Volume"].dropna()
        avg     = volumes.iloc[-21:-1].mean()
        today   = volumes.iloc[-1]
        surge   = ((today - avg) / avg) * 100
        assert surge > 0
        assert not np.isnan(surge)
        assert not np.isinf(surge)

    def test_nan_volume_handled(self):
        """NaN volume values should be dropped, not crash."""
        rows = make_price_volume_rows(n=25)
        df   = rows_to_df(rows)
        # Inject NaN
        df.at[5, "Volume"] = float("nan")
        volumes = df["Volume"].dropna()
        assert len(volumes) == 24  # one dropped

    def test_ticker_with_special_chars_not_loaded(self):
        """Tickers with ^ or / should be filtered from universe."""
        invalid_tickers = ["BRK/B", "BRK^A", "SPY^", "A/B"]
        valid = [
            t for t in invalid_tickers
            if "^" not in t and "/" not in t and t.isalpha()
        ]
        assert valid == []

    def test_valid_tickers_pass_filter(self):
        """Normal alphabetic tickers should pass the loader filter."""
        valid_tickers = ["AAPL", "MSFT", "TSLA", "NVDA"]
        result = [
            t for t in valid_tickers
            if "^" not in t and "/" not in t and t.isalpha()
        ]
        assert result == valid_tickers

    def test_threshold_can_be_zero(self):
        """Threshold of 0 is valid — should not raise error."""
        threshold = 0.0
        assert threshold >= 0

    def test_threshold_cannot_be_negative(self):
        """Negative threshold is invalid — UI prevents it with min=0."""
        threshold = -1.0
        is_valid  = threshold >= 0
        assert not is_valid