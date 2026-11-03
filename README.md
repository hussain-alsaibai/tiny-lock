# tiny-lock

Zero-dependency async + file-based distributed lock library for Python.

One file. Zero dependencies. Works everywhere Python 3.8+ runs.

## Features

| Component | Purpose | Use case |
|-----------|---------|----------|
| `AsyncLock` | `asyncio.Lock` with timeout + stats | Single-process async mutual exclusion |
| `FileLock`  | Cross-process lock via `fcntl` / `msvcrt` | Coordinating processes on the same host |
| `RLock`     | Reentrant async lock | Recursive code paths in async contexts |
| `Semaphore` | Bounded async semaphore | Limit concurrent access to a resource |

## Install

```bash
pip install tiny-lock
```

Or just copy `tiny_lock.py` into your project — it's one file, no deps.

## Quick start

```python
import asyncio
from tiny_lock import AsyncLock, FileLock, RLock, Semaphore, LockTimeout
```

### AsyncLock

```python
async def main():
    lock = AsyncLock()

    # Context manager
    async with lock:
        await do_critical_work()

    # Manual acquire with timeout
    try:
        await lock.acquire(timeout=2.0)
    except LockTimeout:
        print("someone else holds the lock")
    else:
        try:
            await do_critical_work()
        finally:
            lock.release()

    # Stats
    print(lock.get_stats())  # {'locked': ..., 'waiters': ..., 'held_for': ...}

asyncio.run(main())
```

### FileLock (cross-process)

```python
from tiny_lock import FileLock

# Blocking: wait up to 30s (default) for the lock
with FileLock("/tmp/my-resource.lock"):
    write_shared_file()

# Non-blocking: returns immediately
lock = FileLock("/tmp/my-resource.lock", blocking=False)
if lock.acquire():
    try:
        write_shared_file()
    finally:
        lock.release()
else:
    print("another process holds the lock")

# Custom timeout
with FileLock("/tmp/my-resource.lock", timeout=5.0):
    write_shared_file()
```

### RLock (reentrant)

```python
async def main():
    lock = RLock()

    async with lock:
        await helper_a(lock)  # same task — re-entrant OK

async def helper_a(lock):
    async with lock:  # re-acquired by the same task
        await helper_b(lock)

async def helper_b(lock):
    async with lock:  # and again
        print(lock.depth)  # 3

asyncio.run(main())
```

### Semaphore

```python
async def main():
    sem = Semaphore(value=3)

    # Allow at most 3 concurrent calls
    async def worker(i):
        async with sem:
            await limited_resource(i)

    await asyncio.gather(*(worker(i) for i in range(10)))

asyncio.run(main())
```

## API reference

### `AsyncLock`

- `acquire(timeout=None)` → `await ...` → `bool`. Raises `LockTimeout` on timeout.
- `release()` — must be held by the current task.
- `async with lock` / `AsyncLock(timeout=5)()` context managers.
- `get_stats()` → `dict` with `locked`, `waiters`, `held_for`.
- Properties: `locked`, `waiters`, `locked_for`.

### `FileLock(path, timeout=30.0, blocking=True, poll_interval=0.05)`

- `acquire()` → `bool`. Returns `False` for non-blocking contention; raises
  `LockTimeout` for blocking timeout.
- `release()` — no-op if not held.
- `with lock:` context manager.
- `acquired` property.

### `RLock`

- `acquire()` → `await ...` → `bool`. Same task may re-enter; depth increments.
- `release()` — depth-decrements; raises if not held by current task.
- `async with lock:` context manager.
- `get_stats()` → `dict` with `locked`, `owner`, `depth`.

### `Semaphore(value=1)`

- `acquire(timeout=None)` → `await ...` → `bool`. Raises `LockTimeout`.
- `release()`.
- `async with sem:` context manager.
- `get_stats()` → `dict` with `available`, `waiters`, `initial`.

### `LockTimeout`

Raised by `AsyncLock.acquire()` and `Semaphore.acquire()` when a timeout elapses.

## Platform support

| OS | Mechanism |
|-----|----------|
| Linux | `fcntl.flock` |
| macOS | `fcntl.flock` |
| Windows | `msvcrt.locking` |
| Other / sandboxed | Best-effort `fcntl.flock` (may raise) |

## License

MIT — see [LICENSE](LICENSE).

## Badges

![Python](https://img.shields.io/badge/python-3.8%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Dependencies](https://img.shields.io/badge/dependencies-zero-brightgreen)
![Status](https://img.shields.io/badge/status-stable-success)
![Coverage](https://img.shields.io/badge/coverage-100%25-success)
![Async](https://img.shields.io/badge/asyncio-supported-blueviolet)
![FileLock](https://img.shields.io/badge/filelock-cross--process-orange)