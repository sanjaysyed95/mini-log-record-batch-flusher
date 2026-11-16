# mini-log-record-batch-flusher

A `logging.Handler` that buffers `LogRecord` objects and hands them to a sink in batches, flushing when the buffer reaches a capacity limit or when the oldest buffered record has aged past a time limit.

```python
import logging

from mini_log_record_batch_flusher import BatchFlusher


def write_batch(records):
    # Do the expensive I/O once per batch instead of once per record.
    with open("app.log", "a") as fh:
        for record in records:
            fh.write(record.getMessage() + "\n")


handler = BatchFlusher(sink=write_batch, capacity=100, flush_interval=5.0)
logger = logging.getLogger("app")
logger.addHandler(handler)
logger.setLevel(logging.INFO)

logger.info("one")
logger.info("two")

# Flush whatever is still buffered, for example at shutdown.
handler.drain()
```

## Why this exists

Logging is often the hottest path in an application, and a handler that writes each record individually pays the syscall cost once per line. Buffering amortises that cost across a batch. The trade-off is latency and loss: a record sits in memory until the batch fills or the interval expires, and an unflushed buffer is lost if the process dies. This handler does not start a background thread; time-based flushing is checked on every `emit` and on every explicit `flush` call. That keeps the handler simple and dependency-free, but it means an idle application will not flush on a timer alone. Call `drain()` at shutdown or after quiet periods when that matters.

## API

The package exports one name:

- `BatchFlusher(sink, capacity=100, flush_interval=5.0, clock=None)`

  - `sink`: callable invoked with a non-empty list of `LogRecord` objects. It receives a fresh copy of the buffer, so it may retain or mutate the list without affecting the handler.
  - `capacity`: positive integer. When the buffer reaches this many records, it is flushed immediately.
  - `flush_interval`: positive float. When the oldest buffered record is at least this many seconds old, the buffer is flushed on the next `emit` or `flush`.
  - `clock`: callable returning a monotonically non-decreasing float. Defaults to `time.monotonic`. Inject a fake clock in tests to avoid wall-clock dependence.

  Methods: `emit(record)`, `flush()`, `drain()`, `close()`, `emit_batch(records)`.

## Edge case to know about

The time limit is only checked when a record arrives or when `flush()` is called. If the application goes quiet with records still buffered, they stay buffered until the next log call or an explicit `drain()`. This is the deliberate cost of not running a background timer thread.
