import copy
from datetime import datetime, timezone
import json
import platform
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from sparemesh.coordinator import ValidationError
from sparemesh.pilot import HostConfig, Pilot, PilotConfig, load_config
from sparemesh.source_bundle import SourceBundleError
from sparemesh.transport import RemoteError, TransportConfig, TransportError


class FakeWorker:
    def __init__(self, host_id, owner):
        self.host_id = host_id
        self.worker_id = "worker-" + host_id
        self.sequence = 0
        self.ready = True
        self.requests = []
        self.attempts = {}
        self.starts = 0
        self.offline = False
        self.lose_start_response = False
        self.responses = {}
        self.analytics = None

    def request(self, header, archive=None):
        self.requests.append(copy.deepcopy(header))
        if self.offline:
            raise TransportError("network unavailable")
        operation = header["op"]
        if operation in self.responses:
            return copy.deepcopy(self.responses[operation])
        if operation == "probe":
            self.sequence += 1
            response = {
                "schema_version": 1, "host_id": self.host_id, "worker_id": self.worker_id,
                "snapshot": {"schema_version": 1, "sequence": self.sequence, "os": "darwin", "arch": "arm64",
                             "runtimes": ["python3.15"], "profiles": ["smoke"], "available_memory_mb": 8192,
                             "ready": self.ready and not any(item["state"] == "running" for item in self.attempts.values()),
                             "allow_guests": True, "pressure": False},
            }
            if self.analytics is not None:
                response["analytics"] = copy.deepcopy(self.analytics)
            return response
        attempt_id = header["assignment"]["attempt_id"] if operation == "start" else header["attempt_id"]
        if operation == "start":
            if attempt_id not in self.attempts:
                self.starts += 1
                self.attempts[attempt_id] = {"schema_version": 1, "attempt_id": attempt_id, "state": "running",
                                             "result": None, "stdout_tail": "", "stderr_tail": ""}
            if header["assignment"]["cancel_requested"]:
                self.finish(attempt_id, "canceled")
            if self.lose_start_response:
                self.lose_start_response = False
                raise TransportError("response lost after launch")
        elif attempt_id not in self.attempts:
            raise RemoteError("NotFoundError", "unknown attempt")
        elif operation == "cancel":
            self.finish(attempt_id, "canceled")
        return copy.deepcopy(self.attempts[attempt_id])

    def finish(self, attempt_id, state="succeeded"):
        self.attempts[attempt_id].update(
            state=state, result={"schema_version": 1, "outcome": state, "exit_code": 0 if state == "succeeded" else None,
                                 "summary": "confirmed worker result"}, stdout_tail="test output\n",
        )


class PilotTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.repository = self.directory / "repo"
        self.repository.mkdir()
        for arguments in (("init", "-q"), ("config", "user.email", "test@example.invalid"), ("config", "user.name", "Test")):
            subprocess.run(["git", "-C", str(self.repository), *arguments], check=True, capture_output=True)
        (self.repository / "smoke.py").write_text("print('smoke')\n")
        subprocess.run(["git", "-C", str(self.repository), "add", "."], check=True, capture_output=True)
        subprocess.run(["git", "-C", str(self.repository), "commit", "-qm", "fixture"], check=True, capture_output=True)
        self.revision = subprocess.check_output(["git", "-C", str(self.repository), "rev-parse", "HEAD"], text=True).strip()
        # Deliberately put Bob's host first to verify the all-host probe phase.
        self.workers = {name: FakeWorker(name, name) for name in ("bob", "alice")}
        hosts = tuple(HostConfig(name, name, "worker-" + name, TransportConfig("local", "/python", "/source", name))
                      for name in self.workers)
        self.config = PilotConfig(self.directory / "db.sqlite", self.directory / "bundles", hosts)
        self.pilot = Pilot(self.config, transport_factory=lambda config, timeout: self.workers[config.worker_config])
        self.addCleanup(self.pilot.close)

    def submit(self, job_id="job", owner="alice"):
        return self.pilot.submit(self.repository, self.revision, job_id, owner, "smoke",
                                 {"os": "darwin", "arch": "arm64", "runtime": "python3.15", "memory_mb": 128})

    def test_all_hosts_are_probed_before_own_host_preference(self):
        self.submit()
        self.pilot.tick()
        self.assertEqual(self.pilot.coordinator.status("job")["attempt"]["host_id"], "alice")
        self.assertEqual(self.workers["bob"].starts, 0)

    def test_falls_back_to_available_peer_and_reconciles_terminal_result(self):
        self.workers["alice"].ready = False
        self.submit()
        self.pilot.tick()
        assignment = self.pilot.coordinator.status("job")["attempt"]
        self.assertEqual(assignment["host_id"], "bob")
        self.workers["bob"].finish(assignment["attempt_id"])
        self.pilot.tick()
        result = self.pilot.result("job")
        self.assertEqual(result["job"]["state"], "succeeded")
        self.assertEqual(result["worker"]["stdout_tail"], "test output\n")
        self.assertIsNone(next(host for host in self.pilot.coordinator.hosts() if host["host_id"] == "bob")["active_attempt_id"])

    def test_lost_start_response_retains_slot_and_status_avoids_duplicate_launch(self):
        self.submit()
        self.workers["alice"].lose_start_response = True
        tick = self.pilot.tick()
        assignment = self.pilot.coordinator.status("job")["attempt"]
        self.assertIn("error", tick["hosts"][1])
        self.assertEqual(self.pilot.coordinator.status("job")["state"], "assigned")
        self.pilot.tick()
        self.assertEqual(self.workers["alice"].starts, 1)
        self.assertEqual(self.pilot.coordinator.status("job")["attempt"]["attempt_id"], assignment["attempt_id"])

    def test_offline_worker_retains_assigned_slot(self):
        self.submit()
        self.pilot.tick()
        assignment = self.pilot.coordinator.status("job")["attempt"]
        self.workers["alice"].offline = True
        self.pilot.tick()
        self.assertEqual(self.pilot.coordinator.status("job")["state"], "assigned")
        host = next(host for host in self.pilot.coordinator.hosts() if host["host_id"] == "alice")
        self.assertEqual(host["active_attempt_id"], assignment["attempt_id"])

    def test_cancel_running_and_prelaunch_attempts(self):
        self.submit()
        self.pilot.tick()
        self.pilot.coordinator.cancel("job", "alice")
        self.pilot.tick()
        self.assertEqual(self.pilot.coordinator.status("job")["state"], "canceled")
        self.pilot.tick()
        self.submit("prelaunch")
        self.pilot.coordinator.claim("alice", "worker-alice")
        self.pilot.coordinator.cancel("prelaunch", "alice")
        self.pilot.tick()
        self.assertEqual(self.pilot.coordinator.status("prelaunch")["state"], "canceled")
        self.assertTrue(self.workers["alice"].requests[-1]["assignment"]["cancel_requested"])

    def test_mismatched_response_never_releases_slot(self):
        self.submit()
        self.pilot.tick()
        self.workers["alice"].responses["status"] = {
            "schema_version": 1, "attempt_id": "different-attempt", "state": "succeeded",
            "result": {"schema_version": 1, "outcome": "succeeded", "exit_code": 0, "summary": "wrong"},
            "stdout_tail": "", "stderr_tail": "",
        }
        self.pilot.tick()
        self.assertEqual(self.pilot.coordinator.status("job")["state"], "assigned")

    def test_probe_identity_must_match_configured_worker(self):
        self.submit()
        self.workers["alice"].responses["probe"] = {"schema_version": 1, "host_id": "bob", "worker_id": "worker-bob"}
        tick = self.pilot.tick()
        self.assertIn("error", tick["hosts"][1])
        self.assertEqual(self.workers["alice"].starts, 0)

    def test_submission_reuses_verified_bundle_and_rejects_changed_bytes(self):
        self.submit()
        self.submit("second")
        archive = self.config.artifacts_dir / self.revision / "source.tar"
        data = archive.read_bytes()
        archive.write_bytes(b"X" + data[1:])
        with self.assertRaisesRegex(SourceBundleError, "hash"):
            self.submit("third")

    def test_controller_restart_reconciles_existing_assignment(self):
        self.submit()
        self.pilot.tick()
        self.pilot.close()
        with Pilot(self.config, transport_factory=lambda config, timeout: self.workers[config.worker_config]) as restarted:
            restarted.tick()
            self.assertEqual(restarted.coordinator.status("job")["state"], "assigned")
        self.assertEqual(self.workers["alice"].starts, 1)

    def test_report_separates_own_and_guest_attempts_using_configured_owners(self):
        assigned_at = time.time() - 20
        self.pilot.coordinator.clock = lambda: assigned_at
        self.submit("own", "alice")
        self.submit("guest", "carol")
        self.pilot.tick()
        for job_id in ("own", "guest"):
            assignment = self.pilot.coordinator.status(job_id)["attempt"]
            worker = self.workers[assignment["host_id"]]
            worker.attempts[assignment["attempt_id"]]["usage"] = {
                "started_at": assigned_at + 1, "finished_at": assigned_at + 3,
                "cpu_seconds": 1.5, "peak_rss_mb": 24, "usage_sample_count": 2,
            }
            worker.finish(assignment["attempt_id"])
            worker.analytics = {"attempt": {
                "job_id": job_id, "attempt_id": assignment["attempt_id"],
                "state": "succeeded",
                "job_owner": "forged-owner", "host_owner": "forged-owner",
                **worker.attempts[assignment["attempt_id"]]["usage"],
            }}
        tick = self.pilot.tick()
        self.assertTrue(all("analytics_errors" not in host for host in tick["hosts"]), tick)
        date = datetime.fromtimestamp(assigned_at + 1, timezone.utc).date().isoformat()
        report = self.pilot.analytics.report(date, "UTC")
        hosts = {host["host_id"]: host for host in report["hosts"]}
        self.assertEqual(hosts["alice"]["workload"]["own"]["attempt_count"], 1)
        self.assertEqual(hosts["alice"]["workload"]["guest"]["attempt_count"], 0)
        self.assertEqual(hosts["bob"]["workload"]["guest"]["attempt_count"], 1)
        self.assertEqual(hosts["bob"]["workload"]["own"]["attempt_count"], 0)
        self.assertEqual(hosts["bob"]["workload"]["guest"]["completed_count"], 1)
        self.assertEqual(hosts["bob"]["workload"]["guest"]["cpu_seconds_estimate"], 1.5)

    def test_legacy_worker_does_not_fabricate_execution_usage(self):
        self.submit()
        self.pilot.tick()
        assignment = self.pilot.coordinator.status("job")["attempt"]
        self.workers["alice"].finish(assignment["attempt_id"])
        self.pilot.tick()
        date = datetime.fromtimestamp(assignment["assigned_at"], timezone.utc).date().isoformat()
        host = self.pilot.analytics.report(date, "UTC", "alice")["hosts"][0]
        own = host["workload"]["own"]
        self.assertEqual(own["attempt_count"], 1)
        self.assertEqual(own["started_count"], 0)
        self.assertEqual(own["completed_count"], 0)
        self.assertIsNone(own["execution_seconds_estimate"])
        self.assertIsNone(own["cpu_seconds_estimate"])
        self.assertEqual(own["cpu_measured_attempt_count"], 0)
        self.assertEqual(own["state_counts"]["succeeded"], 1)
        self.assertEqual(own["execution_unknown_attempt_count"], 1)

    def test_analytics_failure_does_not_block_terminal_reconciliation(self):
        self.submit()
        self.pilot.tick()
        assignment = self.pilot.coordinator.status("job")["attempt"]
        self.workers["alice"].finish(assignment["attempt_id"])
        with patch.object(self.pilot.analytics, "record_attempt", side_effect=OSError("analytics disk unavailable")):
            tick = self.pilot.tick()
        alice = next(host for host in tick["hosts"] if host["host_id"] == "alice")
        self.assertEqual(alice["state"], "succeeded")
        self.assertIn("analytics_errors", alice)
        self.assertNotIn("error", alice)
        self.assertEqual(self.pilot.coordinator.status("job")["state"], "succeeded")
        host = next(host for host in self.pilot.coordinator.hosts() if host["host_id"] == "alice")
        self.assertIsNone(host["active_attempt_id"])

    def test_analytics_initialization_failure_does_not_block_dispatch(self):
        self.submit()
        self.pilot.close()
        with patch("sparemesh.pilot.AnalyticsStore", side_effect=OSError("analytics disk unavailable")):
            with Pilot(self.config, transport_factory=lambda config, timeout: self.workers[config.worker_config]) as pilot:
                tick = pilot.tick()
                self.assertEqual(self.workers["alice"].starts, 1)
                self.assertEqual(tick["analytics_errors"][0]["code"], "OSError")

    def test_invalid_probe_analytics_does_not_block_dispatch(self):
        self.submit()
        self.workers["alice"].analytics = {"sample": {"host_id": "different-host"}}
        tick = self.pilot.tick()
        alice = next(host for host in tick["hosts"] if host["host_id"] == "alice")
        self.assertEqual(alice["state"], "running")
        self.assertIn("analytics_errors", alice)
        self.assertNotIn("error", alice)
        self.assertEqual(self.workers["alice"].starts, 1)

    def test_worker_analytics_diagnostics_are_bounded_and_deduplicated(self):
        self.submit()
        self.pilot.tick()
        assignment = self.pilot.coordinator.status("job")["attempt"]
        diagnostics = ["sampler unavailable", "x" * 2048] + [f"detail-{index}" for index in range(30)]
        worker = self.workers["alice"]
        worker.analytics = {"diagnostics": diagnostics}
        worker.attempts[assignment["attempt_id"]]["analytics"] = worker.analytics
        tick = self.pilot.tick()
        alice = next(host for host in tick["hosts"] if host["host_id"] == "alice")
        self.assertEqual(alice["state"], "running")
        self.assertEqual(len(alice["analytics_diagnostics"]), 20)
        self.assertEqual(alice["analytics_diagnostics"].count("sampler unavailable"), 1)
        self.assertTrue(all(len(message) <= 1024 for message in alice["analytics_diagnostics"]))
        report = self.pilot.result("job")
        self.assertEqual(report["analytics_diagnostics"], alice["analytics_diagnostics"])

    def test_probe_sample_replay_and_reconnect_preserve_usage(self):
        assigned_at = time.time() - 20
        self.pilot.coordinator.clock = lambda: assigned_at
        self.submit()
        self.pilot.tick()
        assignment = self.pilot.coordinator.status("job")["attempt"]
        self.workers["alice"].analytics = {"sample": {
            "schema_version": 1, "sample_id": "sample-one", "host_id": "alice",
            "sampled_at": assigned_at + 2, "cpu_percent": 20, "memory_total_mb": 1024,
            "memory_available_mb": 800, "memory_pressure": "normal", "swap_used_mb": 0,
            "guest_eligible": True, "active_attempt_id": assignment["attempt_id"],
        }}
        self.workers["alice"].attempts[assignment["attempt_id"]]["usage"] = {
            "started_at": assigned_at + 1, "finished_at": None,
            "cpu_seconds": 0.5, "peak_rss_mb": 24, "usage_sample_count": 2,
        }
        self.pilot.tick()
        self.pilot.close()
        self.workers["alice"].attempts[assignment["attempt_id"]].pop("usage")
        with Pilot(self.config, transport_factory=lambda config, timeout: self.workers[config.worker_config]) as restarted:
            restarted.tick()
            date = datetime.fromtimestamp(assigned_at + 2, timezone.utc).date().isoformat()
            host = restarted.analytics.report(date, "UTC", "alice")["hosts"][0]
            self.assertEqual(host["observation"]["sample_count"], 1)
            self.assertEqual(host["workload"]["own"]["attempt_count"], 1)
            self.assertEqual(host["workload"]["own"]["started_count"], 1)
            self.assertEqual(host["workload"]["own"]["cpu_seconds_estimate"], 0.5)
        self.assertEqual(self.workers["alice"].starts, 1)

    def test_config_relative_paths_and_timing_validation(self):
        path = self.directory / "pilot.json"
        value = {"schema_version": 1, "database": "state/db.sqlite", "artifacts_dir": "bundles", "hosts": [
            {"host_id": "mac", "owner": "alice", "worker_id": "worker-mac", "transport": {
                "kind": "local", "python": "/python", "package_dir": "source", "worker_config": "worker.json",
            }},
        ]}
        path.write_text(json.dumps(value))
        loaded = load_config(path)
        self.assertEqual(loaded.database, self.directory / "state/db.sqlite")
        self.assertEqual(loaded.analytics_path, self.directory / "state/db.analytics.sqlite3")
        self.assertEqual(loaded.hosts[0].transport.worker_config, str(self.directory / "worker.json"))
        value["analytics_database"] = "reports/usage.sqlite3"
        path.write_text(json.dumps(value))
        self.assertEqual(load_config(path).analytics_path, self.directory / "reports/usage.sqlite3")
        value["timeout_seconds"] = 0
        path.write_text(json.dumps(value))
        with self.assertRaises(ValidationError):
            load_config(path)

    def test_real_local_subprocess_returns_result_and_logs(self):
        runtime = f"python{sys.version_info.major}.{sys.version_info.minor}"
        worker_config = self.directory / "worker.json"
        worker_config.write_text(json.dumps({
            "schema_version": 1, "host_id": "local", "worker_id": "worker-local",
            "state_dir": str(self.directory / "worker-state"), "allow_guests": True,
            "reserve_memory_mb": 0,
            "profiles": {"smoke": {"argv": [sys.executable, "-I", "smoke.py"],
                                     "runtime": runtime, "timeout_seconds": 3}},
        }))
        root = Path(__file__).resolve().parents[1]
        host = HostConfig("local", "alice", "worker-local", TransportConfig(
            "local", sys.executable, str(root), str(worker_config),
        ))
        config = PilotConfig(self.directory / "real.sqlite", self.directory / "real-bundles", (host,))
        with Pilot(config) as pilot:
            pilot.submit(self.repository, self.revision, "local-job", "alice", "smoke", {
                "os": platform.system().lower(), "arch": platform.machine(), "runtime": runtime, "memory_mb": 16,
            })
            try:
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    tick = pilot.tick()
                    status = pilot.coordinator.status("local-job")
                    if status["state"] in {"succeeded", "failed", "canceled"}:
                        break
                    time.sleep(0.05)
                self.assertEqual(status["state"], "succeeded", tick)
                report = pilot.result("local-job")
                self.assertEqual(report["worker"]["stdout_tail"], "smoke\n")
                self.assertEqual(report["worker"]["result"]["exit_code"], 0)
            finally:
                pilot.coordinator.cancel("local-job", "alice")
                pilot.tick()


if __name__ == "__main__":
    unittest.main()
