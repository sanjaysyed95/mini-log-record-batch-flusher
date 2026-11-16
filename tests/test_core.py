import logging
import unittest

from mini_log_record_batch_flusher import BatchFlusher


class FakeClock:
    """Deterministic, manually advanced clock for tests."""

    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class RecordingSink:
    """Sink that records every batch it receives."""

    def __init__(self) -> None:
        self.batches = []

    def __call__(self, records):
        self.batches.append(list(records))


class BatchFlusherTests(unittest.TestCase):
    def make_record(self, msg="hello", level=logging.INFO):
        return logging.LogRecord(
            name="test",
            level=level,
            pathname=__file__,
            lineno=1,
            msg=msg,
            args=(),
            exc_info=None,
        )

    def test_capacity_trigger(self):
        clock = FakeClock()
        sink = RecordingSink()
        handler = BatchFlusher(sink, capacity=3, flush_interval=100.0, clock=clock)
        for i in range(3):
            handler.emit(self.make_record(f"m{i}"))
        self.assertEqual(len(sink.batches), 1)
        self.assertEqual(len(sink.batches[0]), 3)
        self.assertEqual([r.getMessage() for r in sink.batches[0]], ["m0", "m1", "m2"])

    def test_capacity_not_reached_does_not_flush(self):
        clock = FakeClock()
        sink = RecordingSink()
        handler = BatchFlusher(sink, capacity=3, flush_interval=100.0, clock=clock)
        handler.emit(self.make_record())
        handler.emit(self.make_record())
        self.assertEqual(sink.batches, [])

    def test_time_trigger_on_emit(self):
        clock = FakeClock()
        sink = RecordingSink()
        handler = BatchFlusher(sink, capacity=10, flush_interval=5.0, clock=clock)
        handler.emit(self.make_record("old"))
        clock.advance(5.0)
        handler.emit(self.make_record("new"))
        self.assertEqual(len(sink.batches), 1)
        self.assertEqual([r.getMessage() for r in sink.batches[0]], ["old"])
        # The new record is still buffered.
        handler.drain()
        self.assertEqual([r.getMessage() for r in sink.batches[1]], ["new"])

    def test_flush_only_when_expired(self):
        clock = FakeClock()
        sink = RecordingSink()
        handler = BatchFlusher(sink, capacity=10, flush_interval=5.0, clock=clock)
        handler.emit(self.make_record())
        clock.advance(1.0)
        handler.flush()
        self.assertEqual(sink.batches, [])
        clock.advance(4.0)
        handler.flush()
        self.assertEqual(len(sink.batches), 1)

    def test_drain_forces_flush_regardless_of_age(self):
        clock = FakeClock()
        sink = RecordingSink()
        handler = BatchFlusher(sink, capacity=10, flush_interval=100.0, clock=clock)
        handler.emit(self.make_record())
        handler.drain()
        self.assertEqual(len(sink.batches), 1)
        self.assertEqual(len(sink.batches[0]), 1)

    def test_drain_on_empty_buffer_is_noop(self):
        clock = FakeClock()
        sink = RecordingSink()
        handler = BatchFlusher(sink, capacity=10, flush_interval=5.0, clock=clock)
        handler.drain()
        self.assertEqual(sink.batches, [])

    def test_close_flushes_pending_records(self):
        clock = FakeClock()
        sink = RecordingSink()
        handler = BatchFlusher(sink, capacity=10, flush_interval=100.0, clock=clock)
        handler.emit(self.make_record("pending"))
        handler.close()
        self.assertEqual(len(sink.batches), 1)
        self.assertEqual([r.getMessage() for r in sink.batches[0]], ["pending"])

    def test_capacity_of_one_flushes_each_record(self):
        clock = FakeClock()
        sink = RecordingSink()
        handler = BatchFlusher(sink, capacity=1, flush_interval=100.0, clock=clock)
        handler.emit(self.make_record("a"))
        handler.emit(self.make_record("b"))
        self.assertEqual(len(sink.batches), 2)
        self.assertEqual([r.getMessage() for r in sink.batches[0]], ["a"])
        self.assertEqual([r.getMessage() for r in sink.batches[1]], ["b"])

    def test_invalid_capacity_rejected(self):
        sink = RecordingSink()
        with self.assertRaises(ValueError):
            BatchFlusher(sink, capacity=0)
        with self.assertRaises(ValueError):
            BatchFlusher(sink, capacity=-1)

    def test_invalid_interval_rejected(self):
        sink = RecordingSink()
        with self.assertRaises(ValueError):
            BatchFlusher(sink, flush_interval=0.0)
        with self.assertRaises(ValueError):
            BatchFlusher(sink, flush_interval=-1.0)

    def test_non_callable_sink_rejected(self):
        with self.assertRaises(TypeError):
            BatchFlusher(sink=None)

    def test_sink_receives_a_copy(self):
        clock = FakeClock()
        seen = []

        def sink(records):
            seen.append(records)
            records.append(self.make_record("injected"))

        handler = BatchFlusher(sink, capacity=1, flush_interval=100.0, clock=clock)
        handler.emit(self.make_record("original"))
        self.assertEqual(len(seen), 1)
        self.assertEqual([r.getMessage() for r in seen[0]], ["original", "injected"])
        # Mutating the sink's list must not affect future batches.
        handler.emit(self.make_record("next"))
        self.assertEqual(len(seen), 2)
        self.assertEqual([r.getMessage() for r in seen[1]], ["next", "injected"])

    def test_emit_batch_respects_capacity(self):
        clock = FakeClock()
        sink = RecordingSink()
        handler = BatchFlusher(sink, capacity=2, flush_interval=100.0, clock=clock)
        records = [self.make_record(f"r{i}") for i in range(5)]
        handler.emit_batch(records)
        self.assertEqual(len(sink.batches), 2)
        self.assertEqual([r.getMessage() for r in sink.batches[0]], ["r0", "r1"])
        self.assertEqual([r.getMessage() for r in sink.batches[1]], ["r2", "r3"])
        handler.drain()
        self.assertEqual([r.getMessage() for r in sink.batches[2]], ["r4"])

    def test_emit_batch_uses_single_acceptance_time(self):
        clock = FakeClock()
        sink = RecordingSink()
        handler = BatchFlusher(sink, capacity=10, flush_interval=5.0, clock=clock)
        handler.emit_batch([self.make_record("a"), self.make_record("b")])
        # Both records share the acceptance time; advancing by exactly the
        # interval expires the batch.
        clock.advance(5.0)
        handler.flush()
        self.assertEqual(len(sink.batches), 1)
        self.assertEqual(len(sink.batches[0]), 2)

    def test_handler_works_with_logging_logger(self):
        clock = FakeClock()
        sink = RecordingSink()
        handler = BatchFlusher(sink, capacity=2, flush_interval=100.0, clock=clock)
        logger = logging.getLogger("mini-log-record-batch-flusher-test")
        logger.setLevel(logging.INFO)
        logger.propagate = False
        logger.addHandler(handler)
        try:
            logger.info("one")
            logger.info("two")
        finally:
            logger.removeHandler(handler)
        self.assertEqual(len(sink.batches), 1)
        self.assertEqual([r.getMessage() for r in sink.batches[0]], ["one", "two"])


if __name__ == "__main__":
    unittest.main()
