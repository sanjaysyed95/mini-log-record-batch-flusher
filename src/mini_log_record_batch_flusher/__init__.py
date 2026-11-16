"""Public interface for mini-log-record-batch-flusher.

Exports:
    BatchFlusher: logging.Handler that buffers records and flushes in batches.
"""

from .core import BatchFlusher

__all__ = ["BatchFlusher"]
