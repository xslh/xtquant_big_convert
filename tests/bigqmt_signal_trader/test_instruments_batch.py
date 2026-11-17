"""Batched contract details: ``get_instruments`` on both sides of the bridge.

Contract metadata is the single most-repeated read in a whole-market download:
5000+ codes, every round. It used to be one RPC per code, each of which the
server ran serially on the adjust thread -- so the queue outgrew the client's
own timeout and the tail of every batch came back as ``TimeoutError``.

``get_instruments`` collapses that to one RPC per ``batch_size`` codes. These
tests pin the client's chunking (what reaches the wire) and the server's
routing (which handler answers, and on which thread).
"""

import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader import redis_rpc
from bigqmt_signal_trader import xtquant_compat as compat


class RecordingClient(object):
    """Stands in for ``BigQmtRpcClient``: records every request."""

    def __init__(self, answer=None):
        self.account_id = "acct"
        self.transport_name = "pipe"
        self.calls = []
        self._answer = answer

    def call(self, method, params=None, timeout_seconds=None, **kwargs):
        self.calls.append((method, dict(params or {}), timeout_seconds))
        codes = list((params or {}).get("codes") or [])
        if self._answer is not None:
            return self._answer(codes)
        return {code: {"InstrumentName": code} for code in codes}


class ClientBatchingTest(unittest.TestCase):

    def test_it_chunks_by_batch_size(self):
        client = RecordingClient()
        data = compat.BigQmtXtData(client)

        result = data.get_instruments(
            ["600%03d.SH" % i for i in range(120)], batch_size=50
        )

        self.assertEqual(len(result), 120)
        self.assertEqual([len(c[1]["codes"]) for c in client.calls], [50, 50, 20])
        self.assertTrue(all(c[0] == "get_instruments" for c in client.calls))

    def test_duplicates_and_blanks_are_dropped_before_the_wire(self):
        client = RecordingClient()
        data = compat.BigQmtXtData(client)

        result = data.get_instruments(["600000.SH", "600000.SH", "", None])

        self.assertEqual(result, {"600000.SH": {"InstrumentName": "600000.SH"}})
        self.assertEqual(client.calls[0][1]["codes"], ["600000.SH"])

    def test_an_empty_list_costs_no_request(self):
        client = RecordingClient()
        data = compat.BigQmtXtData(client)

        self.assertEqual(data.get_instruments([]), {})
        self.assertEqual(client.calls, [])

    def test_types_flag_travels_with_the_request(self):
        client = RecordingClient()
        data = compat.BigQmtXtData(client)

        data.get_instruments(["600000.SH"], types=False)
        data.get_instruments(["600000.SH"], types=True)

        self.assertIs(client.calls[0][1]["types"], False)
        self.assertIs(client.calls[1][1]["types"], True)

    def test_timeout_is_forwarded_per_chunk(self):
        client = RecordingClient()
        data = compat.BigQmtXtData(client)

        data.get_instruments(["600000.SH"], timeout_seconds=7.5)

        self.assertEqual(client.calls[0][2], 7.5)

    def test_a_missing_code_simply_does_not_appear(self):
        """The server may answer a subset; the client must not invent entries."""
        client = RecordingClient(answer=lambda codes: {"600000.SH": {}})
        data = compat.BigQmtXtData(client)

        result = data.get_instruments(["600000.SH", "600001.SH"])

        self.assertEqual(result, {"600000.SH": {}})

    def test_non_positive_batch_size_is_rejected(self):
        data = compat.BigQmtXtData(RecordingClient())

        with self.assertRaises(ValueError):
            data.get_instruments(["600000.SH"], batch_size=0)


class ServerRoutingTest(unittest.TestCase):
    """Which handler answers ``get_instruments``, and where it runs."""

    def test_it_is_an_allowed_read_method(self):
        self.assertIn("get_instruments", redis_rpc.READ_METHODS)
        self.assertIn("get_instruments", redis_rpc.MARKET_DATA_METHODS)
        self.assertIn("get_instruments", redis_rpc.MARKET_DATA_METHODS)
        self.assertNotIn("get_instruments", redis_rpc.ORDER_METHODS)

    def test_it_is_offloaded_off_the_adjust_thread(self):
        """A whole batch is never cheap enough to run on the strategy thread."""
        self.assertIn("get_instruments", redis_rpc.HEAVY_READ_METHODS)

    def test_single_code_reads_are_size_gated(self):
        """One code stays milliseconds; the point is only to keep long runs of
        them off the adjust tick, which the code-count gate already handles."""
        self.assertIn("get_instrument", redis_rpc.HEAVY_READ_CODE_METHODS)


class ExceptionDescriptionTest(unittest.TestCase):

    def test_a_message_less_exception_still_names_its_type(self):
        """``TimeoutError()`` stringifies to ``""``; the wire must not get an
        error with no reason in it."""
        self.assertEqual(
            redis_rpc._describe_exception(TimeoutError()), "TimeoutError"
        )

    def test_a_message_is_kept(self):
        self.assertEqual(
            redis_rpc._describe_exception(RuntimeError("合约表没拉全")),
            "RuntimeError: 合约表没拉全",
        )


if __name__ == "__main__":
    unittest.main()
