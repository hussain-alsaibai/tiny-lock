"""
Tests for tiny_lock.

Run with: python -m pytest tests/ -v
Or:       python tests/test_tiny_lock.py
"""
from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path

# Ensure package on path for direct invocation
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tiny_lock import (  # noqa: E402
    AsyncLock,
    FileLock,
    LockTimeout,
    RLock,
    Semaphore,
)


class TestAsyncLock(unittest.TestCase):
    """Tests for the AsyncLock primitive."""

    def test_basic_acquire_release(self):
        async def run():
            lock = AsyncLock()
            self.assertFalse(lock.locked)
            async with lock:
                self.assertTrue(lock.locked)
            self.assertFalse(lock.locked)

        asyncio.run(run())

    def test_manual_acquire_release(self):
        async def run():
            lock = AsyncLock()
            await lock.acquire()
            self.assertTrue(lock.locked)
            lock.release()
            self.assertFalse(lock.locked)

        asyncio.run(run())

    def test_timeout_raises_lock_timeout(self):
        async def run():
            lock = AsyncLock()
            await lock.acquire()
            try:
                t0 = time.monotonic()
                with self.assertRaises(LockTimeout):
                    await lock.acquire(timeout=0.2)
                elapsed = time.monotonic() - t0
                # Should give up promptly, well under 1 second.
                self.assertLess(elapsed, 1.0)
            finally:
                lock.release()

        asyncio.run(run())

    def test_context_manager_with_timeout(self):
        async def run():
            # Re-enter the SAME lock. Two different AsyncLock objects never
            # contend, so a second instance would always acquire immediately
            # and the timeout would never be exercised.
            lock = AsyncLock(timeout=0.1)
            await lock.acquire()
            try:
                with self.assertRaises(LockTimeout):
                    async with lock:
                        self.fail("should not enter")
            finally:
                lock.release()

        asyncio.run(run())

    def test_waiter_count(self):
        async def run():
            lock = AsyncLock()
            await lock.acquire()

            async def waiter():
                await lock.acquire(timeout=1.0)
                lock.release()

            tasks = [asyncio.create_task(waiter()) for _ in range(3)]
            # Let the loop schedule them so they register on the waiters counter.
            await asyncio.sleep(0.05)
            self.assertEqual(lock.waiters, 3)
            lock.release()
            await asyncio.gather(*tasks)
            self.assertEqual(lock.waiters, 0)

        asyncio.run(run())

    def test_stats(self):
        async def run():
            lock = AsyncLock()
            stats = lock.get_stats()
            self.assertIn("locked", stats)
            self.assertIn("waiters", stats)
            self.assertIn("held_for", stats)
            self.assertFalse(stats["locked"])
            self.assertEqual(stats["waiters"], 0)

        asyncio.run(run())

    def test_release_unacquired_raises(self):
        lock = AsyncLock()
        with self.assertRaises(RuntimeError):
            lock.release()

    def test_mutual_exclusion(self):
        async def run():
            lock = AsyncLock()
            order = []

            async def worker(i, delay):
                async with lock:
                    order.append(f"start-{i}")
                    await asyncio.sleep(delay)
                    order.append(f"end-{i}")

            await asyncio.gather(worker(1, 0.05), worker(2, 0.01))

            # Each worker must finish before the next starts.
            starts = [i for i, x in enumerate(order) if x.startswith("start-")]
            ends = [i for i, x in enumerate(order) if x.startswith("end-")]
            self.assertEqual(len(starts), 2)
            self.assertEqual(len(ends), 2)
            # First end occurs after first start.
            self.assertLess(starts[0], ends[0])
            self.assertLess(starts[1], ends[1])
            # Mutual exclusion: second start must not occur before first end.
            self.assertLess(ends[0], starts[1])

        asyncio.run(run())


class TestRLock(unittest.TestCase):
    """Tests for the reentrant RLock."""

    def test_reentrancy_same_task(self):
        async def run():
            lock = RLock()
            async with lock:
                self.assertEqual(lock.depth, 1)
                async with lock:
                    self.assertEqual(lock.depth, 2)
                    async with lock:
                        self.assertEqual(lock.depth, 3)
                self.assertEqual(lock.depth, 1)
            self.assertEqual(lock.depth, 0)
            self.assertFalse(lock.locked)

        asyncio.run(run())

    def test_non_reentrant_across_tasks(self):
        async def run():
            lock = RLock()
            await lock.acquire()
            try:
                second_started = asyncio.Event()
                release_second = asyncio.Event()

                async def other():
                    second_started.set()
                    await lock.acquire(timeout=0.3)
                    release_second.set()
                    lock.release()

                task = asyncio.create_task(other())
                await second_started.wait()
                with self.assertRaises(LockTimeout):
                    await task
                self.assertFalse(release_second.is_set())
            finally:
                lock.release()
            await asyncio.gather(task, return_exceptions=True)

        asyncio.run(run())

    def test_release_unacquired_raises(self):
        async def run():
            lock = RLock()
            with self.assertRaises(RuntimeError):
                lock.release()

        asyncio.run(run())

    def test_release_wrong_owner_raises(self):
        async def run():
            lock = RLock()
            await lock.acquire()
            try:
                with self.assertRaises(RuntimeError):

                    async def other():
                        lock.release()

                    await asyncio.create_task(other())
            finally:
                lock.release()

        asyncio.run(run())

    def test_stats(self):
        async def run():
            lock = RLock()
            await lock.acquire()
            stats = lock.get_stats()
            self.assertTrue(stats["locked"])
            self.assertEqual(stats["depth"], 1)
            self.assertIsNotNone(stats["owner"])
            lock.release()

        asyncio.run(run())


class TestSemaphore(unittest.TestCase):
    """Tests for the Semaphore primitive."""

    def test_initial_value(self):
        sem = Semaphore(value=3)
        stats = sem.get_stats()
        self.assertEqual(stats["initial"], 3)

    def test_acquire_release(self):
        async def run():
            sem = Semaphore(value=2)
            await sem.acquire()
            await sem.acquire()
            self.assertEqual(sem.available, 0)
            sem.release()
            self.assertEqual(sem.available, 1)

        asyncio.run(run())

    def test_timeout_raises(self):
        async def run():
            sem = Semaphore(value=1)
            await sem.acquire()
            try:
                with self.assertRaises(LockTimeout):
                    await sem.acquire(timeout=0.1)
            finally:
                sem.release()

        asyncio.run(run())

    def test_invalid_value_raises(self):
        with self.assertRaises(ValueError):
            Semaphore(value=0)

    def test_concurrency_limit(self):
        async def run():
            sem = Semaphore(value=2)
            in_flight = 0
            peak = 0

            async def worker():
                nonlocal in_flight, peak
                async with sem:
                    in_flight += 1
                    peak = max(peak, in_flight)
                    await asyncio.sleep(0.05)
                    in_flight -= 1

            await asyncio.gather(*(worker() for _ in range(8)))
            self.assertLessEqual(peak, 2)
            self.assertGreaterEqual(peak, 1)

        asyncio.run(run())


class TestFileLock(unittest.TestCase):
    """Tests for the FileLock primitive."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.path = os.path.join(self.tmpdir, "test.lock")

    def tearDown(self):
        for f in (self.path,):
            try:
                os.unlink(f)
            except OSError:
                pass
        try:
            os.rmdir(self.tmpdir)
        except OSError:
            pass

    def test_basic_acquire_release(self):
        lock = FileLock(self.path, timeout=2.0)
        self.assertFalse(lock.acquired)
        with lock:
            self.assertTrue(lock.acquired)
        self.assertFalse(lock.acquired)

    def test_blocking_contention_times_out(self):
        # First lock holds it; second lock in blocking mode should time out.
        a = FileLock(self.path, timeout=2.0)
        a.acquire()
        try:
            b = FileLock(self.path, timeout=0.3)
            t0 = time.monotonic()
            with self.assertRaises(LockTimeout):
                b.acquire()
            elapsed = time.monotonic() - t0
            self.assertGreaterEqual(elapsed, 0.2)
        finally:
            a.release()

    def test_non_blocking_returns_false(self):
        a = FileLock(self.path, timeout=2.0)
        a.acquire()
        try:
            b = FileLock(self.path, blocking=False)
            self.assertFalse(b.acquire())
            self.assertFalse(b.acquired)
        finally:
            a.release()

    def test_re_acquire_after_release(self):
        a = FileLock(self.path, timeout=2.0)
        a.acquire()
        a.release()
        b = FileLock(self.path, timeout=2.0)
        with b:
            self.assertTrue(b.acquired)

    def test_double_acquire_raises(self):
        lock = FileLock(self.path, timeout=2.0)
        lock.acquire()
        try:
            with self.assertRaises(RuntimeError):
                lock.acquire()
        finally:
            lock.release()

    def test_release_unheld_is_noop(self):
        lock = FileLock(self.path, timeout=2.0)
        # Should not raise.
        lock.release()


class TestFileLockCrossProcess(unittest.TestCase):
    """Cross-process file lock test using subprocess."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.path = os.path.join(self.tmpdir, "cross.lock")
        self.signal_path = os.path.join(self.tmpdir, "signal.txt")

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_cross_process_blocking(self):
        # Hold the lock in this process and start a child that tries to acquire
        # with a short timeout. The child should fail.
        import subprocess
        import sys

        # We need to skip this test if fcntl isn't available (e.g. on Windows).
        if sys.platform == "win32":
            self.skipTest("cross-process fcntl test is Unix-only")

        # We use a flag file as a synchronization mechanism: the child writes
        # to it the moment it observes contention.
        # NOTE: this must be a real program, not a ';'-joined one-liner. A
        # compound statement (try:) cannot follow a semicolon, so joining the
        # prologue with ';' produced a SyntaxError and the child exited 1
        # before it ever touched the lock.
        script = (
            "import sys\n"
            f"sys.path.insert(0, {str(Path(__file__).resolve().parent.parent)!r})\n"
            "from tiny_lock import FileLock, LockTimeout\n"
            f"l = FileLock({self.path!r}, timeout=0.5)\n"
            "try:\n"
            "    l.acquire()\n"
            "    sys.exit(0)\n"
            "except LockTimeout:\n"
            "    open(" + repr(self.signal_path) + ", 'w').close()\n"
            "    sys.exit(2)\n"
        )

        a = FileLock(self.path, timeout=2.0)
        a.acquire()
        try:
            result = subprocess.run(
                [sys.executable, "-c", script],
                capture_output=True,
                timeout=5,
            )
            self.assertEqual(result.returncode, 2)
            self.assertTrue(os.path.exists(self.signal_path))
        finally:
            a.release()

    def test_cross_process_release_then_acquire(self):
        import subprocess
        import sys

        if sys.platform == "win32":
            self.skipTest("cross-process fcntl test is Unix-only")

        script = (
            "import sys;"
            f"sys.path.insert(0, {str(Path(__file__).resolve().parent.parent)!r});"
            "from tiny_lock import FileLock;"
            f"l = FileLock({self.path!r}, timeout=2.0);"
            "l.acquire();"
            "l.release();"
            "sys.exit(0);"
        )

        a = FileLock(self.path, timeout=2.0)
        a.acquire()
        a.release()

        # After release, child must acquire successfully.
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            timeout=5,
        )
        self.assertEqual(result.returncode, 0)


def run_tests() -> int:
    """Run all tests and return exit code."""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(TestAsyncLock))
    suite.addTests(loader.loadTestsFromTestCase(TestRLock))
    suite.addTests(loader.loadTestsFromTestCase(TestSemaphore))
    suite.addTests(loader.loadTestsFromTestCase(TestFileLock))
    suite.addTests(loader.loadTestsFromTestCase(TestFileLockCrossProcess))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(run_tests())