"""Batch-flushing logging handler.

A logging.Handler that accumulates LogRecord objects and hands them to a
sink in batches. The batch is flushed when either the buffer reaches
``capacity`` records or ``flush_interval`` seconds have elapsed since the
oldest record in the buffer was accepted.

Why a clock function: the timer is driven by ``time.monotonic`` by default,
but tests must be deterministic. The constructor accepts a ``clock`` callable
returning a float, so tests can advance time manually without sleeping.

Why a sink callable instead of an emit method: keeping the destination
pluggable keeps the handler focused on buffering and lets callers write to
files, network sockets, or in-memory lists without subclassing.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Callable, Iterable, List, Optional


# A sink receives a non-empty list of logging.LogRecord objects and is
# responsible for doing whatever I/O it needs. It must not retain the list
# beyond the call, because the handler reuses its internal buffer.
Sink = Callable[[List[logging.LogRecord]], None]


class BatchFlusher(logging.Handler):
    """A logging.Handler that buffers records and flushes them in batches.

    Parameters
    ----------
    sink:
        Callable invoked with a list of records when a batch is flushed.
        The list is never empty and is only valid for the duration of the
        call. The sink must be thread-safe if the handler is shared.
    capacity:
        Maximum number of records to buffer before forcing a flush.
        Must be positive.
    flush_interval:
        Maximum age in seconds of the oldest buffered record before a flush
        is forced. Must be positive.
    clock:
        Callable returning a monotonically non-decreasing float. Defaults to
        ``time.monotonic``. Tests pass a fake clock so no wall-clock time is
        observed.

    The handler does not start a background thread. Time-based flushing is
    checked on every ``emit`` and on every explicit ``flush`` call. This is
    a deliberate trade-off: it avoids a thread and a wakeup timer, at the
    cost of not flushing while the application is idle. Call ``flush`` at
    shutdown or after quiet periods if that matters.
    """

    def __init__(
        self,
        sink: Sink,
        capacity: int = 100,
        flush_interval: float = 5.0,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        super().__init__()
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        if flush_interval <= 0:
            raise ValueError("flush_interval must be positive")
        if not callable(sink):
            raise TypeError("sink must be callable")
        self._sink = sink
        self._capacity = capacity
        self._flush_interval = flush_interval
        self._clock = clock if clock is not None else time.monotonic
        self._buffer: List[logging.LogRecord] = []
        self._lock = threading.RLock()

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def flush_interval(self) -> float:
        return self._flush_interval

    def emit(self, record: logging.LogRecord) -> None:
        """Buffer ``record`` and flush if capacity or age requires it."""
        with self._lock:
            # Check age before appending so the oldest record's timestamp is
            # still the one that determines whether the interval elapsed.
            if self._buffer and self._expired():
                self._flush_locked()
            self._buffer.append(record)
            if len(self._buffer) >= self._capacity:
                self._flush_locked()

    def flush(self) -> None:
        """Flush the buffer if its oldest record has aged out.

        This override makes the handler safe to use with ``logging.shutdown``
        and with ``logging.Handler.flush`` conventions. It only flushes on
        age; callers that need an unconditional flush should use ``drain``.
        """
        with self._lock:
            if self._buffer and self._expired():
                self._flush_locked()

    def drain(self) -> None:
        """Flush all buffered records immediately, regardless of age."""
        with self._lock:
            self._flush_locked()

    def close(self) -> None:
        """Flush remaining records and release the handler."""
        try:
            with self._lock:
                self._flush_locked()
        finally:
            super().close()

    def _expired(self) -> bool:
        return (self._clock() - self._record_time(self._buffer[0])) >= self._flush_interval

    def _record_time(self, record: logging.LogRecord) -> float:
        # ``created`` is wall-clock seconds, not comparable to a monotonic
        # clock. Store the acceptance time on the record instead. Use a
        # private attribute so we do not collide with user fields.
        return getattr(record, "_batch_flusher_time", 0.0)

    def _flush_locked(self) -> None:
        if not self._buffer:
            return
        batch = self._buffer
        self._buffer = []
        # Hand a fresh list to the sink so a misbehaving sink cannot mutate
        # our internal state after the lock is released.
        self._sink(list(batch))

    def handle(self, record: logging.LogRecord) -> bool:
        """Stamp the record with the current clock time, then emit it."""
        record._batch_flusher_time = self._clock()
        return super().handle(record)

    def emit_batch(self, records: Iterable[logging.LogRecord]) -> None:
        """Emit several records as one logical unit.

        Provided for callers that already have records in hand and want to
        avoid re-entering the lock per record. Each record is stamped with
        the same acceptance time.
        """
        now = self._clock()
        with self._lock:
            for record in records:
                record._batch_flusher_time = now
                if self._buffer and self._expired():
                    self._flush_locked()
                self._buffer.append(record)
                if len(self._buffer) >= self._capacity:
                    self._flush_locked()
