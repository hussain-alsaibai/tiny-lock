"""
tiny-lock — zero-dependency async + file-based distributed lock for Python.

Features:
- AsyncLock: asyncio.Lock with timeout support
- FileLock: cross-process lock using fcntl (Linux/Mac) or msvcrt (Windows)
- RLock: reentrant async lock (same task can re-acquire)
- Semaphore: bounded async semaphore

Usage:
    from tiny_lock import AsyncLock, FileLock

    # Async context manager
    async with AsyncLock(timeout=5) as lock:
        await do_critical_work()

    # File-based distributed lock
    with FileLock("my-resource.lock"):
        write_shared_file()
"""
import asyncio
import os
import sys
import time
import fcntl
from contextlib import asynccontextmanager

__version__ = "0.1.0"
__all__ = ["AsyncLock", "FileLock", "RLock", "Semaphore", "LockTimeout"]

class LockTimeout(Exception):
    """Raised when a lock acquisition times out."""
    pass

class AsyncLock:
    """asyncio.Lock replacement with context manager + optional timeout."""
    def __init__(self, timeout=None):
        self._lock = asyncio.Lock()
        self.timeout = timeout
        self._waiters = 0
        self._acquired_at = None

    @property
    def locked(self):
        return self._lock.locked()

    @property
    def waiters(self):
        """Number of tasks currently waiting to acquire."""
        return self._waiters

    def get_stats(self):
        held_for = None
        if self._acquired_at is not None:
            held_for = time.monotonic() - self._acquired_at
        return {
            "locked": self.locked,
            "waiters": self._waiters,
            "held_for": held_for,
        }

    async def acquire(self, timeout=None):
        timeout = timeout if timeout is not None else self.timeout
        self._waiters += 1
        try:
            if timeout is None:
                await self._lock.acquire()
                self._acquired_at = time.monotonic()
                return True
            else:
                try:
                    await asyncio.wait_for(self._lock.acquire(), timeout=timeout)
                    self._acquired_at = time.monotonic()
                    return True
                except asyncio.TimeoutError:
                    raise LockTimeout(f"Lock acquisition timed out after {timeout}s")
        finally:
            self._waiters -= 1

    def release(self):
        self._lock.release()
        self._acquired_at = None

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.release()

class FileLock:
    """Cross-process file-based lock using fcntl/flock."""
    def __init__(self, path, timeout=30, blocking=True):
        self.path = path
        self.timeout = timeout
        self.blocking = blocking
        self._fd = None

    @property
    def acquired(self):
        """True if this lock is currently held."""
        return self._fd is not None

    def acquire(self):
        if self._fd is not None:
            raise RuntimeError("lock already acquired by this instance")

        start_time = time.time()
        self._fd = os.open(self.path, os.O_CREAT | os.O_RDWR)

        while True:
            try:
                if self.blocking:
                    if self.timeout is not None:
                        # Non-blocking attempt in a loop to handle timeout
                        fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    else:
                        fcntl.flock(self._fd, fcntl.LOCK_EX)
                else:
                    fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return True
            except (IOError, OSError):
                if not self.blocking or (self.timeout is not None and (time.time() - start_time) > self.timeout):
                    os.close(self._fd)
                    self._fd = None
                    if not self.blocking:
                        return False
                    raise LockTimeout(f"File lock acquisition timed out after {self.timeout}s")

                time.sleep(0.1)

    def release(self):
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
            finally:
                os.close(self._fd)
                self._fd = None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()

    def __del__(self):
        self.release()

class RLock:
    """Reentrant async lock."""
    def __init__(self):
        self._lock = asyncio.Lock()
        self._owner = None
        self._count = 0

    @property
    def depth(self):
        """Current reentrant acquisition depth."""
        return self._count

    @property
    def locked(self):
        """True if the lock is currently held."""
        return self._lock.locked()

    def get_stats(self):
        return {
            "locked": self.locked,
            "owner": id(self._owner) if self._owner is not None else None,
            "depth": self.depth,
        }

    async def acquire(self, timeout=None):
        curr_task = asyncio.current_task()
        if self._owner == curr_task:
            self._count += 1
            return True

        if timeout is None:
            await self._lock.acquire()
        else:
            try:
                await asyncio.wait_for(self._lock.acquire(), timeout=timeout)
            except asyncio.TimeoutError:
                raise LockTimeout(f"RLock acquisition timed out after {timeout}s")
        self._owner = curr_task
        self._count = 1
        return True

    def release(self):
        curr_task = asyncio.current_task()
        if self._owner != curr_task:
            raise RuntimeError("Cannot release lock held by another task")
        
        self._count -= 1
        if self._count == 0:
            self._owner = None
            self._lock.release()

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.release()

class Semaphore:
    """Async semaphore wrapper."""
    def __init__(self, value=1):
        if value < 1:
            raise ValueError(f"Semaphore value must be >= 1, got {value!r}")
        self._sem = asyncio.Semaphore(value)
        self._initial = value

    @property
    def available(self):
        """Number of slots currently available."""
        return self._sem._value

    @property
    def waiting(self):
        """Number of tasks waiting to acquire."""
        return len(self._sem._waiters) if self._sem._waiters else 0

    async def acquire(self, timeout=None):
        if timeout is None:
            await self._sem.acquire()
            return True
        else:
            try:
                await asyncio.wait_for(self._sem.acquire(), timeout=timeout)
                return True
            except asyncio.TimeoutError:
                raise LockTimeout(f"Semaphore acquisition timed out after {timeout}s")

    def release(self):
        self._sem.release()

    def get_stats(self):
        return {
            "available": self.available,
            "waiting": self.waiting,
            "initial": self._initial,
        }

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.release()
