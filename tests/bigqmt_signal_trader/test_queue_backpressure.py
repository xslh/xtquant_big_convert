# coding: utf-8
"""#357: a full queue refuses the request, it never silently drops another one.

The QMT side used to answer nothing when ``pending`` filled up: it dropped the
OLDEST queued request and kept the newest. The caller of the dropped request
then only ever saw its own deadline expire, which is indistinguishable from a
hung bridge -- nobody could tell backpressure from a failure.

The contract now: answer ``ok: False`` with ``error="RPC_BUSY: ..."`` on the
same ``request_id`` the caller is waiting on. The request was never dispatched,
so the client retries it briefly, and no queued work disappears.
"""

import json
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.redis_rpc import (  # noqa: E402
    BigQmtRpcHandlers,
    RedisPubSubRpcService,
)

from test_redis_rpc import FakeMarketData, FakePositionProvider, FakeRedis  # noqa: E402


def _service(**kwargs):
    redis_client = FakeRedis()
    handlers = BigQmtRpcHandlers(
        account_id="acct",
        market_data=FakeMarketData(),
        position_provider=FakePositionProvider(),
        order_gateway=None,
        allow_order_methods=False,
    )
    # background_threads=True: no drain, no adjust thread -- these tests drive
    # ``enqueue_payload`` directly, which is where the queue lives.
    options = dict(process_in_listener=False, background_threads=True, max_queue_size=2)
    options.update(kwargs)
    service = RedisPubSubRpcService(redis_client, handlers, account_id="acct", **options)
    return redis_client, service


def _request(request_id="r1", method="get_trading_dates", **params):
    return {
        "schema_version": 1,
        "request_id": request_id,
        "account_id": "acct",
        "method": method,
        "params": params,
        "reply_key": "resp:%s" % request_id,
    }


def _answers(redis_client):
    """Decode every published response envelope."""
    out = []
    for _channel, raw in redis_client.published:
        try:
            out.append(json.loads(raw))
        except Exception:
            out.append({"raw": raw})
    return out


def _by_request(answers, request_id):
    return [a for a in answers if a.get("request_id") == request_id]


class PendingQueueBackpressureTest(unittest.TestCase):
    def test_a_full_queue_refuses_instead_of_dropping_the_oldest(self):
        redis_client, service = _service()
        service.enqueue_payload(json.dumps(_request("r1")))
        service.enqueue_payload(json.dumps(_request("r2")))

        service.enqueue_payload(json.dumps(_request("r3")))

        queued = []
        while not service.pending.empty():
            queued.append(service.pending.get_nowait()["request_id"])
        # Nothing was evicted to make room: the two oldest are still queued.
        self.assertEqual(queued, ["r1", "r2"])

        answers = _by_request(_answers(redis_client), "r3")
        self.assertEqual(len(answers), 1, "the refused request must be answered")
        self.assertFalse(answers[0]["ok"])
        self.assertIn("RPC_BUSY", answers[0]["error"])
        self.assertEqual(answers[0]["method"], "get_trading_dates")

    def test_the_refusal_names_the_queue_that_was_full(self):
        redis_client, service = _service(max_queue_size=1)
        service.enqueue_payload(json.dumps(_request("r1")))

        service.enqueue_payload(json.dumps(_request("r2")))

        answers = _by_request(_answers(redis_client), "r2")
        self.assertIn("pending queue full (1)", answers[0]["error"])

    def test_a_full_heavy_queue_refuses_too(self):
        redis_client, service = _service(heavy_queue_maxsize=1)
        service._heavy_thread = _Alive()
        # A financial read is heavy whatever its arguments.
        service.enqueue_payload(
            json.dumps(_request("h1", method="get_financial_data", stock_list=["600000.SH"]))
        )
        service.enqueue_payload(
            json.dumps(_request("h2", method="get_financial_data", stock_list=["600000.SH"]))
        )

        answers = _by_request(_answers(redis_client), "h2")
        self.assertEqual(len(answers), 1)
        self.assertIn("RPC_BUSY", answers[0]["error"])
        self.assertIn("heavy queue full (1)", answers[0]["error"])
        self.assertEqual(service._heavy_queue.qsize(), 1)
        service._heavy_thread = None

    def test_an_accepted_request_is_not_answered_early(self):
        redis_client, service = _service()
        service.enqueue_payload(json.dumps(_request("r1")))

        self.assertEqual(_by_request(_answers(redis_client), "r1"), [])
        self.assertEqual(service.pending.qsize(), 1)


class _Alive(object):
    """Stand-in for a live heavy worker thread."""

    def is_alive(self):
        return True


class InstrumentDetailRoutingTest(unittest.TestCase):
    def test_a_single_contract_read_goes_to_the_heavy_worker(self):
        _redis_client, service = _service()
        self.assertTrue(service.is_heavy_read("get_instrument_detail", {"code": "600000.SH"}))
        self.assertTrue(service.is_heavy_read("get_instrument", {"stock_code": "600000.SH"}))


if __name__ == "__main__":
    unittest.main()
