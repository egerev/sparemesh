from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest

from sparemesh.analytics import AnalyticsStore
from sparemesh.coordinator import ConflictError, Coordinator, ValidationError


def instant(value):
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp()


class Clock:
    def __init__(self, value="2026-11-10T00:00:00"):
        self.now = instant(value)

    def __call__(self):
        return self.now


class AnalyticsTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "analytics.sqlite3"
        self.clock = Clock()
        self.store = AnalyticsStore(self.path, clock=self.clock, retention_days=1000)
        self.addCleanup(self.store.close)

    def sample(self, sample_id="sample-1", sampled_at="2026-11-09T12:00:00", **changes):
        value = {"schema_version": 1, "sample_id": sample_id, "host_id": "alice-mac",
                 "sampled_at": instant(sampled_at), "cpu_percent": None, "memory_total_mb": None,
                 "memory_available_mb": None, "memory_pressure": "unknown", "swap_used_mb": None,
                 "guest_eligible": None, "active_attempt_id": None}
        value.update(changes)
        self.store.record_sample(value)
        return value

    def attempt(self, attempt_id="attempt-1", **changes):
        value = {"schema_version": 1, "attempt_id": attempt_id, "job_id": "job-" + attempt_id,
                 "host_id": "alice-mac", "job_owner": "alice", "host_owner": "alice",
                 "assigned_at": instant("2026-11-09T12:00:00"), "started_at": None,
                 "finished_at": None, "state": "assigned", "cpu_seconds": None,
                 "peak_rss_mb": None, "usage_sample_count": 0}
        value.update(changes)
        self.store.record_attempt(value)
        return value

    def host(self, date="2026-11-09", zone="UTC"):
        return self.store.report(date, zone)["hosts"][0]

    def test_schema_is_versioned_durable_and_separate_from_coordinator(self):
        sample = self.sample(cpu_percent=0)
        with AnalyticsStore(self.path, clock=self.clock) as reopened:
            reopened.record_sample(sample)
            self.assertEqual(reopened.report("2026-11-09")["hosts"][0]["observation"]["sample_count"], 1)
        connection = sqlite3.connect(self.path)
        self.addCleanup(connection.close)
        self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 1)
        coordinator_path = Path(self.directory.name) / "coordinator.sqlite3"
        with Coordinator(coordinator_path):
            with self.assertRaisesRegex(ValidationError, "dedicated"):
                AnalyticsStore(coordinator_path)

    def test_unknown_schema_is_rejected(self):
        self.store.connection.execute("PRAGMA user_version=2")
        with self.assertRaisesRegex(ValidationError, "unsupported"):
            AnalyticsStore(self.path)

    def test_samples_are_idempotent_and_conflicts_are_rejected(self):
        sample = self.sample(cpu_percent=20)
        self.store.record_sample(sample)
        with self.assertRaises(ConflictError):
            self.store.record_sample({**sample, "cpu_percent": 21})
        self.assertEqual(self.host()["observation"]["sample_count"], 1)

    def test_observation_summaries_distinguish_unknown_and_zero(self):
        self.sample("unknown")
        self.sample("zero", "2026-11-09T12:00:10", cpu_percent=0, memory_total_mb=8192,
                    memory_available_mb=0, memory_pressure="critical", swap_used_mb=0)
        self.sample("normal", "2026-11-09T12:00:20", cpu_percent=60, memory_total_mb=8192,
                    memory_available_mb=1024, memory_pressure="normal", swap_used_mb=40)
        observation = self.host()["observation"]
        self.assertEqual(observation["cpu_sample_count"], 2)
        self.assertEqual(observation["cpu_percent_mean"], 30)
        self.assertEqual(observation["cpu_percent_max"], 60)
        self.assertEqual(observation["memory_available_mb_min"], 0)
        self.assertEqual(observation["memory_pressure_samples"], {"unknown": 1, "normal": 1, "warning": 0, "critical": 1})
        self.assertEqual(observation["swap_used_mb_max"], 40)

    def test_sampling_gaps_boundary_sample_and_current_day_are_clipped(self):
        self.clock.now = instant("2026-11-09T12:00:10")
        self.sample("prior", "2026-11-08T23:59:40", guest_eligible=True, cpu_percent=99)
        self.sample("noon", "2026-11-09T12:00:00", guest_eligible=False, cpu_percent=10)
        self.sample("future", "2026-11-09T12:00:30", guest_eligible=True)
        observation = self.host()["observation"]
        self.assertEqual(observation["sample_count"], 1)
        self.assertEqual(observation["cpu_percent_mean"], 10)
        self.assertEqual(observation["guest_eligible_seconds"], 40)
        self.assertEqual(observation["guest_ineligible_seconds"], 10)
        self.assertEqual(observation["observed_seconds"], 50)
        report = self.store.report("2026-11-09")
        self.assertEqual(report["period"]["observed_until"], self.clock.now)

    def test_gap_cap_does_not_fill_offline_hours(self):
        self.sample("one", "2026-11-09T00:00:00", guest_eligible=True)
        self.sample("two", "2026-11-09T20:00:00", guest_eligible=True)
        self.assertEqual(self.host()["observation"]["observed_seconds"], 120)

    def test_calendar_days_respect_timezone_and_dst(self):
        short = self.store.report("2026-03-08", "America/New_York")["period"]
        long = self.store.report("2026-11-01", "America/New_York")["period"]
        self.assertEqual(short["end"] - short["start"], 23 * 3600)
        self.assertEqual(long["end"] - long["start"], 25 * 3600)
        self.sample("bali", "2026-11-08T16:00:00")
        self.assertEqual(self.host("2026-11-09", "Asia/Makassar")["observation"]["sample_count"], 1)
        for bad_date, bad_zone in (("20261109", "UTC"), ("2026-02-30", "UTC"), ("9999-12-31", "UTC"),
                                   ("2026-11-09", "not/a-zone")):
            with self.assertRaises(ValidationError):
                self.store.report(bad_date, bad_zone)

    def test_attempts_classify_owners_and_unknown_execution_remains_null(self):
        self.attempt()
        self.attempt("guest", job_owner="bob")
        self.attempt("legacy", host_owner=None)
        workload = self.host()["workload"]
        self.assertEqual([workload[key]["attempt_count"] for key in ("own", "guest", "unclassified")], [1, 1, 1])
        self.assertIsNone(workload["own"]["execution_seconds_estimate"])
        self.assertIsNone(workload["own"]["cpu_seconds_estimate"])
        self.assertEqual(workload["own"]["execution_unknown_attempt_count"], 1)

    def test_cross_midnight_execution_and_cpu_allocation(self):
        start = instant("2026-11-08T23:50:00")
        finish = instant("2026-11-09T00:10:00")
        self.attempt(assigned_at=start - 5, started_at=start, finished_at=finish,
                     state="succeeded", cpu_seconds=120, peak_rss_mb=64, usage_sample_count=10)
        previous = self.host("2026-11-08")["workload"]["own"]
        current = self.host()["workload"]["own"]
        self.assertEqual(previous["execution_seconds_estimate"], 600)
        self.assertEqual(current["execution_seconds_estimate"], 600)
        self.assertEqual(previous["cpu_seconds_estimate"], 60)
        self.assertEqual(current["cpu_seconds_estimate"], 60)
        self.assertEqual(previous["started_count"], 1)
        self.assertEqual(previous["completed_count"], 0)
        self.assertEqual(current["started_count"], 0)
        self.assertEqual(current["completed_count"], 1)
        self.assertEqual(current["peak_rss_mb_max"], 64)

    def test_execution_overlap_respects_dst_and_half_open_midnight(self):
        start = instant("2026-03-08T04:30:00")
        finish = instant("2026-03-09T04:00:00")
        self.attempt(assigned_at=start, started_at=start, finished_at=finish, state="succeeded")
        current = self.host("2026-03-08", "America/New_York")["workload"]["own"]
        self.assertEqual(current["execution_seconds_estimate"], 23 * 3600)
        self.assertEqual(current["completed_count"], 0)
        following = self.host("2026-03-09", "America/New_York")["workload"]["own"]
        self.assertEqual(following["completed_count"], 1)
        self.assertEqual(following["execution_seconds_estimate"], 0)

    def test_partial_stale_and_duplicate_attempts_preserve_terminal_metrics(self):
        value = self.attempt(state="running", started_at=instant("2026-11-09T12:00:02"),
                             cpu_seconds=2, peak_rss_mb=12, usage_sample_count=2)
        finished = {**value, "state": "succeeded", "finished_at": instant("2026-11-09T12:01:00"),
                    "cpu_seconds": 4, "peak_rss_mb": 20, "usage_sample_count": 4}
        self.store.record_attempt(finished)
        self.store.record_attempt(finished)
        self.store.record_attempt({**value, "state": "assigned", "started_at": None,
                                   "cpu_seconds": None, "peak_rss_mb": None, "usage_sample_count": 0})
        group = self.host()["workload"]["own"]
        self.assertEqual(group["attempt_count"], 1)
        self.assertEqual(group["state_counts"]["succeeded"], 1)
        self.assertEqual(group["completed_count"], 1)
        self.assertEqual(group["execution_seconds_estimate"], 58)
        self.assertEqual(group["cpu_seconds_estimate"], 4)
        self.assertEqual(group["peak_rss_mb_max"], 20)
        with self.assertRaises(ConflictError):
            self.store.record_attempt({**finished, "state": "failed"})
        with self.assertRaises(ConflictError):
            self.store.record_attempt({**finished, "job_owner": "different"})

    def test_failed_contact_does_not_extend_known_running_duration(self):
        self.clock.now = instant("2026-11-09T12:00:10")
        value = self.attempt(state="running", started_at=instant("2026-11-09T12:00:00"))
        self.clock.now = instant("2026-11-09T23:00:00")
        self.store.record_attempt({**value, "state": "assigned", "started_at": None})
        self.store.record_attempt({**value, "state": "uncertain"})
        self.assertEqual(self.host()["workload"]["own"]["execution_seconds_estimate"], 10)
        self.attempt("unknown", state="uncertain", started_at=instant("2026-11-09T12:00:00"))
        row = self.store.connection.execute("SELECT observed_at FROM task_attempts WHERE attempt_id='unknown'").fetchone()
        self.assertEqual(row[0], instant("2026-11-09T12:00:00"))

    def test_zero_duration_retains_known_zero_cpu(self):
        timestamp = instant("2026-11-09T12:00:00")
        self.attempt(started_at=timestamp, finished_at=timestamp, state="succeeded", cpu_seconds=0)
        group = self.host()["workload"]["own"]
        self.assertEqual(group["execution_seconds_estimate"], 0)
        self.assertEqual(group["cpu_seconds_estimate"], 0)
        self.assertEqual(group["cpu_measured_attempt_count"], 1)

    def test_prune_retains_boundary_samples_and_all_attempts(self):
        self.sample("oldest", "2026-11-06T12:00:00")
        self.sample("boundary", "2026-11-08T23:59:40", guest_eligible=True)
        self.sample("recent", "2026-11-09T00:01:00")
        self.attempt("old-unresolved", assigned_at=instant("2026-11-06T00:00:00"))
        self.assertEqual(self.store.prune(1), 1)
        samples = self.store.connection.execute("SELECT sample_id FROM host_samples ORDER BY sampled_at").fetchall()
        self.assertEqual([row[0] for row in samples], ["boundary", "recent"])
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM task_attempts").fetchone()[0], 1)
        self.assertEqual(self.host()["observation"]["guest_eligible_seconds"], 40)
        self.assertEqual(self.store.report("2026-11-09")["retention"]["samples_pruned_before"], instant("2026-11-09T00:00:00"))

    def test_validation_rejects_nonfinite_boolean_and_unbounded_private_payloads(self):
        for changes in ({"cpu_percent": True}, {"memory_total_mb": float("nan")}, {"swap_used_mb": -1},
                        {"cpu_percent": 101}, {"guest_eligible": 1}, {"memory_pressure": False},
                        {"memory_total_mb": 1, "memory_available_mb": 2}, {"process_names": ["private"]}):
            with self.assertRaises(ValidationError):
                self.sample(**changes)
        for changes in ({"cpu_seconds": float("inf")}, {"usage_sample_count": True}, {"usage_sample_count": -1},
                        {"started_at": instant("2026-11-08T00:00:00")}, {"schema_version": True},
                        {"finished_at": instant("2026-11-09T12:01:00")}):
            with self.assertRaises(ValidationError):
                self.attempt(**changes)

    def test_concurrent_connections_deduplicate_same_attempt(self):
        self.clock.now = instant("2026-11-09T12:00:10")
        value = self.attempt(state="running", started_at=instant("2026-11-09T12:00:00"))
        barrier = threading.Barrier(3)
        failures = []

        def record():
            try:
                with AnalyticsStore(self.path, clock=self.clock) as store:
                    barrier.wait(timeout=5)
                    store.record_attempt(value)
            except Exception as error:
                failures.append(error)

        threads = [threading.Thread(target=record) for _ in range(3)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(failures, [])
        self.assertEqual(self.host()["workload"]["own"]["attempt_count"], 1)


if __name__ == "__main__":
    unittest.main()
