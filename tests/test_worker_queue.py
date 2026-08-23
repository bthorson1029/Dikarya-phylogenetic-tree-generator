import unittest
from datetime import datetime, timezone
from unittest.mock import Mock, patch

from rq import Retry

from app.workers.queue import get_job_status


class WorkerQueueStatusTests(unittest.TestCase):
    def test_retry_result_is_omitted_from_status_response(self):
        job = Mock()
        job.get_status.return_value = "scheduled"
        job.result = Retry(max=1, interval=60)
        job.enqueued_at = datetime.now(timezone.utc)
        job.started_at = None
        job.ended_at = None
        job.exc_info = "an error from a prior attempt"

        queue = Mock()
        queue.fetch_job.return_value = job

        with patch("app.workers.queue.get_queue", return_value=queue):
            status = get_job_status("job-id")

        self.assertEqual(status["status"], "queued")
        self.assertNotIn("error", status)
        self.assertNotIn("result", status)

    def test_failed_status_includes_error(self):
        job = Mock()
        job.get_status.return_value = "failed"
        job.result = None
        job.enqueued_at = None
        job.started_at = None
        job.ended_at = datetime.now(timezone.utc)
        job.exc_info = "current failure"

        queue = Mock()
        queue.fetch_job.return_value = job

        with patch("app.workers.queue.get_queue", return_value=queue):
            status = get_job_status("job-id")

        self.assertEqual(status["status"], "failed")
        self.assertEqual(status["error"], "current failure")


if __name__ == "__main__":
    unittest.main()


class VoucherSyncQueueTests(unittest.TestCase):
    def test_voucher_sync_queue_is_valid(self):
        from app.workers.queue import QUEUE_VOUCHER, VALID_QUEUE_NAMES, get_queue

        self.assertIn(QUEUE_VOUCHER, VALID_QUEUE_NAMES)
        self.assertEqual(QUEUE_VOUCHER, "voucher_sync")
        with patch("app.workers.queue.get_redis_connection", return_value=Mock()):
            self.assertEqual(get_queue(QUEUE_VOUCHER).name, "voucher_sync")

    def test_enqueue_voucher_sync_run_carries_only_the_run_id(self):
        from app.workers.queue import enqueue_voucher_sync_run

        queue = Mock()
        queue.enqueue.return_value = Mock(id="run-1")
        with patch("app.workers.queue.get_queue", return_value=queue), \
             patch("app.workers.voucher_sync_tasks.run_voucher_scan_job") as scan:
            self.assertEqual(enqueue_voucher_sync_run("run-1", "scan"), "run-1")
        args, kwargs = queue.enqueue.call_args
        self.assertIs(args[0], scan)
        self.assertEqual(args[1:], ("run-1",))
        self.assertEqual(kwargs["job_id"], "run-1")
        self.assertEqual(kwargs["job_timeout"], "3h")
        self.assertNotIn("token", kwargs["description"].lower())
