"""A bar read must not hand back a bar that has not closed yet.

QMT answers a bar read with the session's in-progress bar appended: a daily
read at 10:30 returns today's partial OHLCV, a ``1m`` read returns the minute
that has not finished. Callers that build signals from finished bars used to be
fed those partial values silently, and nothing later corrected them -- the
subscription side only delivers a bar once it closes, and it delivers the
*previous* one (see ``test_subscribe_quote.BarSignatureTest``).

Measured 2026-11-19 on 601288.SH: a strategy warm-up request for the last 65
days came back with today's bar at ``close=7.58`` while the session had not
finished, and that value never got replaced by the close.

The rule lives in ``get_market_data_ex`` because every bar read funnels through
it: the node path, ``data.history``, and the download CLI. Filtering anywhere
above it would leave the other callers wrong.
"""

import datetime as dt
import os
import sys
import unittest

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader import xtquant_compat as compat


CN = dt.timezone(dt.timedelta(hours=8))


def _ms(year, month, day, hour=0, minute=0):
    """Epoch milliseconds for a Beijing wall-clock moment."""
    return int(dt.datetime(year, month, day, hour, minute, tzinfo=CN).timestamp() * 1000)


def _normalized(times):
    """A frame in the shape every live read returns: a ``time`` column."""
    return pd.DataFrame({"time": times, "close": [1.0] * len(times)})


def _stime_indexed(labels):
    """A frame in the other shape: a Beijing wall-clock label index."""
    return pd.DataFrame({"close": [1.0] * len(labels)}, index=labels)


class DropUnclosedBarsTest(unittest.TestCase):
    """Only the last bar can be open, and only while its period runs."""

    def _drop(self, period, frame, now):
        return compat.BigQmtXtData._drop_unclosed_bars({"X": frame}, period, now=now)

    # -- daily ------------------------------------------------------------

    def test_the_daily_bar_of_a_session_in_progress_is_dropped(self):
        frame = _normalized([_ms(2026, 11, 18), _ms(2026, 11, 19)])
        kept = self._drop("1d", frame, dt.datetime(2026, 11, 19, 10, 30))
        self.assertEqual(len(kept["X"]), 1, "today's partial daily bar was served")

    def test_the_daily_bar_is_kept_once_the_session_closes(self):
        frame = _normalized([_ms(2026, 11, 18), _ms(2026, 11, 19)])
        kept = self._drop("1d", frame, dt.datetime(2026, 11, 19, 15, 0))
        self.assertEqual(len(kept["X"]), 2, "a finalized daily bar was dropped")

    def test_the_close_time_itself_counts_as_closed(self):
        """15:00 is the boundary: the bar is final exactly then."""
        frame = _normalized([_ms(2026, 11, 18), _ms(2026, 11, 19)])
        kept = self._drop("1d", frame, dt.datetime(2026, 11, 19, 15, 0))
        self.assertEqual(len(kept["X"]), 2)

    def test_a_friday_read_on_saturday_keeps_everything(self):
        """There is no Saturday bar, so the last one is Friday's, finalized."""
        frame = _normalized([_ms(2026, 11, 19), _ms(2026, 11, 20)])
        kept = self._drop("1d", frame, dt.datetime(2026, 11, 21, 11, 0))
        self.assertEqual(len(kept["X"]), 2)

    def test_a_daily_read_before_the_open_drops_nothing_too_early(self):
        """08:00 on a trading day: yesterday is closed, today has no bar yet."""
        frame = _normalized([_ms(2026, 11, 18), _ms(2026, 11, 19)])
        kept = self._drop("1d", frame, dt.datetime(2026, 11, 19, 8, 0))
        self.assertEqual(len(kept["X"]), 1, "today's not-yet-formed bar was kept")

    # -- intraday ---------------------------------------------------------

    def test_the_current_minute_is_dropped(self):
        frame = _normalized([_ms(2026, 11, 19, 10, 29), _ms(2026, 11, 19, 10, 30)])
        kept = self._drop("1m", frame, dt.datetime(2026, 11, 19, 10, 30, 12))
        self.assertEqual(len(kept["X"]), 1, "the unfinished minute was served")

    def test_the_previous_minute_is_kept(self):
        frame = _normalized([_ms(2026, 11, 19, 10, 29), _ms(2026, 11, 19, 10, 30)])
        kept = self._drop("1m", frame, dt.datetime(2026, 11, 19, 10, 31, 5))
        self.assertEqual(len(kept["X"]), 2)

    def test_the_minute_boundary_counts_as_closed(self):
        """10:31:00 means the 10:30 bar has just finished."""
        frame = _normalized([_ms(2026, 11, 19, 10, 30), _ms(2026, 11, 19, 10, 31)])
        kept = self._drop("1m", frame, dt.datetime(2026, 11, 19, 10, 31, 0))
        self.assertEqual(len(kept["X"]), 1, "the 10:31 bar should still be open")

    def test_a_five_minute_bar_stays_open_for_five_minutes(self):
        frame = _normalized([_ms(2026, 11, 19, 10, 25), _ms(2026, 11, 19, 10, 30)])
        early = self._drop("5m", frame, dt.datetime(2026, 11, 19, 10, 31, 0))
        late = self._drop("5m", frame, dt.datetime(2026, 11, 19, 10, 35, 0))
        self.assertEqual(len(early["X"]), 1)
        self.assertEqual(len(late["X"]), 2)

    # -- the other frame shape --------------------------------------------

    def test_a_minute_label_is_not_truncated_to_its_date(self):
        """stime-indexed frames carry YYYYMMDDHHMM; the minute must survive."""
        frame = _stime_indexed(["202611191029", "202611191030"])
        kept = self._drop("1m", frame, dt.datetime(2026, 11, 19, 10, 30, 12))
        self.assertEqual(len(kept["X"]), 1)

    def test_a_daily_label_index_is_handled(self):
        frame = _stime_indexed(["20261118", "20261119"])
        kept = self._drop("1d", frame, dt.datetime(2026, 11, 19, 10, 30))
        self.assertEqual(len(kept["X"]), 1)

    # -- what must not be touched -----------------------------------------

    def test_weekly_bars_are_left_to_the_resynth(self):
        """``_resynth_ongoing_multiday_bars`` owns the ongoing multi-day bar."""
        frame = _normalized([_ms(2026, 11, 16), _ms(2026, 11, 23)])
        kept = self._drop("1w", frame, dt.datetime(2026, 11, 19, 10, 30))
        self.assertEqual(len(kept["X"]), 2)

    def test_ticks_are_untouched(self):
        frame = _normalized([_ms(2026, 11, 19, 10, 29)])
        kept = self._drop("tick", frame, dt.datetime(2026, 11, 19, 10, 30, 12))
        self.assertEqual(len(kept["X"]), 1)

    def test_an_empty_frame_survives(self):
        kept = self._drop("1d", pd.DataFrame(), dt.datetime(2026, 11, 19, 10, 30))
        self.assertEqual(len(kept["X"]), 0)

    def test_an_unreadable_frame_is_passed_through_not_guessed_at(self):
        frame = pd.DataFrame({"close": [1.0, 2.0]}, index=[0, 1])
        kept = self._drop("1d", frame, dt.datetime(2026, 11, 19, 10, 30))
        self.assertEqual(len(kept["X"]), 2, "a frame with no timestamps was cut")

    def test_a_non_dict_result_is_returned_unchanged(self):
        sentinel = object()
        self.assertIs(
            compat.BigQmtXtData._drop_unclosed_bars(sentinel, "1d"), sentinel)


class LabelToEpochTest(unittest.TestCase):
    """Labels are Beijing wall clock, never the host's zone."""

    def test_a_date_label(self):
        self.assertEqual(
            compat._label_to_epoch_seconds("20261119"), _ms(2026, 11, 19) / 1000.0)

    def test_a_minute_label(self):
        self.assertEqual(
            compat._label_to_epoch_seconds("202611191030"),
            _ms(2026, 11, 19, 10, 30) / 1000.0)

    def test_a_second_label(self):
        self.assertEqual(
            compat._label_to_epoch_seconds("20261119103015"),
            _ms(2026, 11, 19, 10, 30) / 1000.0 + 15)

    def test_garbage_is_not_a_timestamp(self):
        self.assertIsNone(compat._label_to_epoch_seconds("not-a-date"))


if __name__ == "__main__":
    unittest.main()
