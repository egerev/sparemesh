import copy
import json
import platform
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

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

    def request(self, header, archive=None):
        self.requests.append(copy.deepcopy(header))
        if self.offline:
            raise TransportError("network unavailable")
        operation = header["op"]
        if operation in self.responses:
            return copy.deepcopy(self.responses[operation])
        if operation == "probe":
            self.sequence += 1
            return {
                "schema_version": 1, "host_id": self.host_id, "worker_id": self.worker_id,
                "snapshot": {"schema_version": 1, "sequence": self.sequence, "os": "darwin", "arch": "arm64",
                             "runtimes": ["python3.15"], "profiles": ["smoke"], "available_memory_mb": 8192,
                             "ready": self.ready and not any(item["state"] == "running" for item in self.attempts.values()),
                             "allow_guests": True, "pressure": False},
            }
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
        self.assertEqual(loaded.hosts[0].transport.worker_config, str(self.directory / "worker.json"))
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
