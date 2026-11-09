from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

from sparemesh.analytics import AnalyticsStore


ROOT = Path(__file__).resolve().parents[1]


class CliTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = str(Path(self.directory.name) / "coordinator.sqlite")

    def cli(self, *arguments, input=None):
        return subprocess.run(
            [sys.executable, "-m", "sparemesh", "--db", self.database, *arguments],
            input=json.dumps(input) if input is not None else None,
            capture_output=True,
            text=True,
            cwd=ROOT,
            timeout=10,
        )

    def test_demo_exercises_the_simulated_lifecycle(self):
        output = self.cli("demo")
        self.assertEqual(output.returncode, 0, output.stderr)
        demo = json.loads(output.stdout)
        self.assertEqual(demo["mode"], "simulated_workers")
        events = {check["event"] for check in demo["checks"]}
        self.assertTrue({"local_run", "peer_alice", "peer_bob", "both_busy", "queued_cancel",
                         "running_cancel", "restart_retains_slots", "stale_snapshot_rejected",
                         "completion_replay", "conflicting_completion_rejected"} <= events)

    def test_cli_roundtrip_across_processes(self):
        registered = self.cli("register-host", "mac", "--owner", "alice", "--worker", "worker-mac")
        self.assertEqual(registered.returncode, 0, registered.stderr)
        snapshot = {
            "schema_version": 1, "sequence": 1, "os": "darwin", "arch": "arm64",
            "runtimes": ["python3.11"], "profiles": ["smoke-tests"],
            "available_memory_mb": 2048, "ready": True, "allow_guests": True, "pressure": False,
        }
        reported = self.cli("report-host", "mac", "--worker", "worker-mac", "--snapshot", "-", input=snapshot)
        self.assertEqual(reported.returncode, 0, reported.stderr)
        submitted = self.cli("submit", "smoke-tests", "--job-id", "job-1", "--owner", "alice",
                             "--ref", "a" * 40, "--os", "darwin", "--arch", "arm64",
                             "--runtime", "python3.11", "--memory-mb", "512")
        self.assertEqual(submitted.returncode, 0, submitted.stderr)
        claimed = self.cli("claim", "mac", "--worker", "worker-mac")
        self.assertEqual(claimed.returncode, 0, claimed.stderr)
        assignment = json.loads(claimed.stdout)
        canceled = self.cli("cancel", "job-1", "--owner", "alice")
        self.assertEqual(json.loads(canceled.stdout)["state"], "cancel_requested")
        finished = self.cli("finish", assignment["attempt_id"], "--host", "mac", "--worker", "worker-mac",
                            "--result", "-", input={"schema_version": 1, "outcome": "canceled", "exit_code": None, "summary": "stopped"})
        self.assertEqual(finished.returncode, 0, finished.stderr)
        status = self.cli("status", "job-1")
        self.assertEqual(json.loads(status.stdout)["state"], "canceled")

    def test_domain_error_is_json_and_nonzero(self):
        output = self.cli("status", "missing")
        self.assertEqual(output.returncode, 2)
        self.assertEqual(json.loads(output.stderr)["error"]["code"], "NotFoundError")

    def test_invalid_json_is_reported_without_traceback(self):
        self.cli("register-host", "mac", "--owner", "alice", "--worker", "worker-mac")
        output = self.cli("report-host", "mac", "--worker", "worker-mac", "--snapshot", "-", input=[])
        self.assertEqual(output.returncode, 2)
        self.assertEqual(json.loads(output.stderr)["error"]["code"], "ValidationError")

    def test_source_bundle_cli_from_another_working_directory(self):
        repo = Path(self.directory.name) / "repo with spaces"
        repo.mkdir()
        for arguments in [("init", "-q"), ("config", "user.name", "Bundle Test"),
                          ("config", "user.email", "bundle@example.invalid")]:
            subprocess.run(["git", "-C", str(repo), *arguments], check=True, capture_output=True)
        (repo / "source.txt").write_text("fixed commit\n")
        subprocess.run(["git", "-C", str(repo), "add", "."], check=True, capture_output=True)
        subprocess.run(["git", "-C", str(repo), "commit", "-qm", "fixture"], check=True, capture_output=True)
        commit = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
        environment = dict(os.environ, PYTHONPATH=str(ROOT))
        output = subprocess.run(
            [sys.executable, "-m", "sparemesh", "--db", self.database, "bundle-source",
             "--repo", "repo with spaces", "--commit", commit, "--output", "bundle"],
            cwd=self.directory.name, env=environment, capture_output=True, text=True, timeout=10,
        )
        self.assertEqual(output.returncode, 0, output.stderr)
        manifest = json.loads(output.stdout)
        self.assertEqual(manifest["commit"], commit)
        self.assertEqual(manifest["schema_version"], 1)
        self.assertTrue((Path(self.directory.name) / "bundle" / "source.tar").is_file())
        self.assertFalse(Path(self.database).exists())

    def test_source_bundle_errors_are_json(self):
        output = self.cli("bundle-source", "--repo", str(ROOT), "--commit", "HEAD",
                          "--output", str(Path(self.directory.name) / "bundle"))
        self.assertEqual(output.returncode, 2)
        self.assertEqual(output.stdout, "")
        error = json.loads(output.stderr)["error"]
        self.assertEqual(error["code"], "SourceBundleError")
        self.assertIn("full", error["message"])
        self.assertFalse(Path(self.database).exists())

    def test_pilot_report_reads_daily_analytics_without_contacting_workers(self):
        directory = Path(self.directory.name)
        assigned_at = time.time() - 60
        date = datetime.fromtimestamp(assigned_at, timezone.utc).date().isoformat()
        config = directory / "pilot.json"
        config.write_text(json.dumps({
            "schema_version": 1, "database": "controller.sqlite3", "artifacts_dir": "bundles",
            "hosts": [{"host_id": "mac", "owner": "alice", "worker_id": "worker-mac", "transport": {
                "kind": "local", "python": "/missing-python", "package_dir": "source", "worker_config": "worker.json",
            }}],
        }))
        with AnalyticsStore(directory / "controller.analytics.sqlite3") as analytics:
            analytics.record_attempt({
                "schema_version": 1, "attempt_id": "attempt-one", "job_id": "job-one", "host_id": "mac",
                "job_owner": "alice", "host_owner": "alice", "assigned_at": assigned_at,
                "started_at": assigned_at + 10, "finished_at": assigned_at + 20, "state": "succeeded",
                "cpu_seconds": 2.5, "peak_rss_mb": 30, "usage_sample_count": 2,
            })
        output = self.cli("pilot-report", "--config", str(config), "--date", date, "--timezone", "UTC", "--host", "mac")
        self.assertEqual(output.returncode, 0, output.stderr)
        report = json.loads(output.stdout)
        self.assertEqual(report["date"], date)
        self.assertEqual(report["timezone"], "UTC")
        self.assertEqual(report["hosts"][0]["host_id"], "mac")
        self.assertEqual(report["hosts"][0]["workload"]["own"]["completed_count"], 1)
        self.assertFalse((directory / "controller.sqlite3").exists())
        self.assertFalse((directory / "bundles").exists())

    def test_pilot_report_invalid_date_is_json_error(self):
        directory = Path(self.directory.name)
        config = directory / "pilot.json"
        config.write_text(json.dumps({
            "schema_version": 1, "database": "controller.sqlite3", "artifacts_dir": "bundles",
            "hosts": [{"host_id": "mac", "owner": "alice", "worker_id": "worker-mac", "transport": {
                "kind": "local", "python": "/python", "package_dir": "source", "worker_config": "worker.json",
            }}],
        }))
        output = self.cli("pilot-report", "--config", str(config), "--date", "not-a-date", "--timezone", "UTC")
        self.assertEqual(output.returncode, 2)
        self.assertEqual(json.loads(output.stderr)["error"]["code"], "ValidationError")


if __name__ == "__main__":
    unittest.main()
