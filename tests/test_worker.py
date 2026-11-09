from concurrent.futures import ThreadPoolExecutor
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
import unittest
from unittest.mock import patch

from sparemesh import ConflictError, IdentityError, NotFoundError, ValidationError
from sparemesh.worker import MAX_HEADER_BYTES, Worker, WorkerError, _extract


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.config_path = self.root / "config.json"
        self.config = {"schema_version": 1, "host_id": "host", "worker_id": "worker",
                       "state_dir": str(self.root / "state"), "reserve_memory_mb": 0,
                       "allow_guests": True, "profiles": {"smoke": {
                           "argv": [sys.executable, "-I", "smoke.py"],
                           "runtime": f"python{sys.version_info.major}.{sys.version_info.minor}",
                           "timeout_seconds": 10}}}
        self.write_config()
        self.worker = Worker(self.config_path)
        self.addCleanup(self.cleanup)

    def write_config(self):
        self.config_path.write_text(json.dumps(self.config))

    def cleanup(self):
        for path in (self.root / "state" / "attempts").glob("*/state.json"):
            state = json.loads(path.read_text())
            if state["state"] == "running":
                self.worker.cancel(state["attempt_id"])
                try:
                    self.wait(state["attempt_id"], timeout=9)
                except AssertionError:
                    pass
            if state.get("process_group") and self.group_exists(state["process_group"]):
                try:
                    os.killpg(state["process_group"], signal.SIGKILL)
                except ProcessLookupError:
                    pass
                deadline = time.monotonic() + 3
                while self.group_exists(state["process_group"]) and time.monotonic() < deadline:
                    time.sleep(0.05)
        self.temporary.cleanup()

    def assignment(self, attempt_id="attempt-one"):
        return {"schema_version": 1, "attempt_id": attempt_id, "host_id": "host", "worker_id": "worker",
                "state": "assigned", "assigned_at": 1.0, "cancel_requested": False,
                "cancel_reason": None, "result": None, "finished_at": None,
                "job": {"schema_version": 1, "job_id": attempt_id + "-job", "owner": "alice",
                        "profile": "smoke", "revision": "a" * 40, "restartable": False,
                        "requirements": {"os": platform.system().lower(), "arch": platform.machine(),
                                         "runtime": self.config["profiles"]["smoke"]["runtime"], "memory_mb": 1}}}

    def bundle(self, code="print('complete')\n", extra=None):
        output = io.BytesIO()
        count = files = total = 0
        with tarfile.open(fileobj=output, mode="w", format=tarfile.PAX_FORMAT) as archive:
            root = tarfile.TarInfo("source/")
            root.type = tarfile.DIRTYPE
            archive.addfile(root)
            data = code.encode()
            info = tarfile.TarInfo("source/smoke.py")
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
            count += 1
            files += 1
            total += len(data)
            if extra is not None:
                info, data = extra
                archive.addfile(info, io.BytesIO(data) if data else None)
                count += 1
                files += int(info.isfile())
                total += len(data)
        archive = output.getvalue()
        return archive, {"schema_version": 1, "commit": "a" * 40, "object_format": "sha1",
                         "archive_name": "source.tar", "archive_format": "tar-pax", "root_prefix": "source/",
                         "archive_sha256": hashlib.sha256(archive).hexdigest(), "archive_size_bytes": len(archive),
                         "entry_count": count, "file_count": files, "total_file_bytes": total}

    def start(self, code="print('complete')\n", attempt_id="attempt-one"):
        archive, manifest = self.bundle(code)
        assignment = self.assignment(attempt_id)
        response = self.worker.start(assignment, manifest, io.BytesIO(archive))
        return response, assignment, manifest, archive

    def wait(self, attempt_id="attempt-one", timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            response = self.worker.status(attempt_id)
            if response["state"] != "running":
                return response
            time.sleep(0.05)
        self.fail("worker did not reach terminal state")

    def group_exists(self, group):
        try:
            os.killpg(group, 0)
            return True
        except ProcessLookupError:
            return False

    def test_probe_persists_sequence_and_validates_actual_runtime(self):
        first = self.worker.probe()
        self.assertTrue(first["snapshot"]["ready"])
        second = Worker(self.config_path).probe()
        self.assertEqual(second["snapshot"]["sequence"], first["snapshot"]["sequence"] + 1)
        self.config["profiles"]["smoke"]["runtime"] = "python3.1"
        self.write_config()
        self.assertFalse(Worker(self.config_path).probe()["snapshot"]["ready"])

    def test_memory_probe_failure_and_headroom_fail_closed(self):
        with patch.object(self.worker, "_memory", side_effect=WorkerError("probe failed")):
            result = self.worker.probe()
            self.assertFalse(result["snapshot"]["ready"])
            self.assertTrue(result["snapshot"]["pressure"])
            self.assertEqual(result["snapshot"]["available_memory_mb"], 0)
            with self.assertRaises(WorkerError):
                self.start()
        self.assertEqual(list(self.worker.attempts.iterdir()), [])
        with patch.object(self.worker, "_memory", return_value=0):
            with self.assertRaisesRegex(WorkerError, "headroom"):
                self.start()

    def test_fixed_profile_environment_and_bounded_logs(self):
        code = "import os\nprint('x' * 40000)\nprint(os.getenv('SECRET_TOKEN', 'absent'))\n"
        with patch.dict(os.environ, {"SECRET_TOKEN": "never-forward"}):
            self.start(code)
        result = self.wait()
        self.assertEqual(result["state"], "succeeded")
        self.assertEqual(len(result["stdout_tail"].encode()), 16384)
        self.assertNotIn("never-forward", result["stdout_tail"])
        self.assertTrue(result["stdout_tail"].endswith("absent\n"))
        self.assertEqual(result["result"]["exit_code"], 0)

    def test_duplicate_start_never_reads_payload_and_conflict_survives_completion(self):
        _, assignment, manifest, archive = self.start("import time\nprint('one', flush=True)\ntime.sleep(0.4)\n")
        class NoRead:
            def read(self, size):
                raise AssertionError("replay must not read source bytes")
        self.assertEqual(self.worker.start(assignment, manifest, NoRead())["state"], "running")
        self.assertEqual(self.wait()["stdout_tail"], "one\n")
        self.assertEqual(self.worker.start(assignment, manifest, NoRead())["state"], "succeeded")
        manifest["archive_sha256"] = "b" * 64
        with self.assertRaises(ConflictError):
            self.worker.start(assignment, manifest, io.BytesIO(archive))

    def test_concurrent_start_and_one_slot(self):
        archive, manifest = self.bundle("import time\ntime.sleep(0.3)\nprint('once')\n")
        assignment = self.assignment()
        with ThreadPoolExecutor(max_workers=2) as executor:
            responses = list(executor.map(lambda _: Worker(self.config_path).start(assignment, manifest, io.BytesIO(archive)), range(2)))
        self.assertTrue(all(result["state"] == "running" for result in responses))
        with self.assertRaises(ConflictError):
            self.worker.start(self.assignment("attempt-two"), manifest, io.BytesIO(archive))
        self.assertFalse(self.worker.probe()["snapshot"]["ready"])
        self.assertEqual(self.wait()["stdout_tail"], "once\n")

    def test_upload_hash_truncation_revision_and_identity(self):
        archive, manifest = self.bundle()
        bad = dict(manifest, archive_sha256="0" * 64)
        result = self.worker.start(self.assignment(), bad, io.BytesIO(archive))
        self.assertEqual(result["state"], "failed")
        self.assertIn("SHA-256", result["result"]["summary"])
        result = self.worker.start(self.assignment("attempt-two"), manifest, io.BytesIO(archive[:100]))
        self.assertEqual(result["state"], "failed")
        with self.assertRaises(ValidationError):
            self.worker.start(self.assignment("attempt-three"), dict(manifest, commit="b" * 40), io.BytesIO(archive))
        assignment = self.assignment("attempt-four")
        assignment["worker_id"] = "other"
        with self.assertRaises(IdentityError):
            self.worker.start(assignment, manifest, io.BytesIO(archive))

    def test_extract_rejects_paths_links_duplicates_and_special_files(self):
        for name, kind in [("source/../escape", tarfile.REGTYPE), ("elsewhere", tarfile.REGTYPE),
                           ("source/link", tarfile.SYMTYPE), ("source/hard", tarfile.LNKTYPE),
                           ("source/pipe", tarfile.FIFOTYPE), ("source/smoke.py", tarfile.REGTYPE),
                           ("source/SMOKE.PY", tarfile.REGTYPE), ("source/.git/config", tarfile.REGTYPE),
                           ("source/dir\\file", tarfile.REGTYPE)]:
            with self.subTest(name=name):
                info = tarfile.TarInfo(name)
                info.type = kind
                info.linkname = "smoke.py" if kind in (tarfile.SYMTYPE, tarfile.LNKTYPE) else ""
                archive, manifest = self.bundle(extra=(info, b""))
                path = self.root / "attack.tar"
                path.write_bytes(archive)
                with tempfile.TemporaryDirectory(dir=self.root) as destination:
                    with self.assertRaises(WorkerError):
                        _extract(path, Path(destination), manifest)
        self.assertFalse((self.root / "escape").exists())

    def test_timeout_and_cancel_remove_stubborn_descendants(self):
        code = ("import os, signal, time\n"
                "if os.fork() == 0:\n"
                " signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
                " print('child', flush=True)\n"
                " while True: time.sleep(1)\n"
                "while True: time.sleep(1)\n")
        self.config["profiles"]["smoke"]["timeout_seconds"] = 0.4
        self.write_config()
        self.worker = Worker(self.config_path)
        self.start(code)
        result = self.wait()
        self.assertEqual(result["state"], "failed")
        self.assertIn("timed out", result["result"]["summary"])
        self.assertFalse(self.group_exists(result["process_group"]))

        self.config["profiles"]["smoke"]["timeout_seconds"] = 20
        self.write_config()
        self.worker = Worker(self.config_path)
        self.start(code, "attempt-two")
        deadline = time.monotonic() + 3
        while "child" not in self.worker.status("attempt-two")["stdout_tail"] and time.monotonic() < deadline:
            time.sleep(0.05)
        self.worker.cancel("attempt-two")
        result = self.wait("attempt-two")
        self.assertEqual(result["state"], "canceled")
        self.assertFalse(self.group_exists(result["process_group"]))

    def test_consecutive_pax_extensions_are_rejected_before_tar_parser(self):
        header = tarfile.TarInfo("PaxHeader")
        header.type = tarfile.XHDTYPE
        archive = header.tobuf() * 1200 + bytes(1024)
        path = self.root / "recursive.tar"
        path.write_bytes(archive)
        with self.assertRaisesRegex(WorkerError, "consecutive"):
            _extract(path, self.root, {"entry_count": 0, "file_count": 0, "total_file_bytes": 0})

    def test_prelaunch_cancel_without_upload_and_unknown_attempt(self):
        archive, manifest = self.bundle("raise Exception('must not run')\n")
        assignment = self.assignment()
        assignment["cancel_requested"] = True
        result = self.worker.start(assignment, manifest, io.BytesIO())
        self.assertEqual(result["state"], "canceled")
        with self.assertRaises(NotFoundError):
            self.worker.status("unknown")
        with self.assertRaises(NotFoundError):
            self.worker.cancel("unknown")

    def test_rpc_disconnect_does_not_stop_supervisor(self):
        archive, manifest = self.bundle("import time\ntime.sleep(0.4)\nprint('survived')\n")
        header = json.dumps({"op": "start", "assignment": self.assignment(), "manifest": manifest}).encode() + b"\n"
        process = subprocess.Popen([sys.executable, "-m", "sparemesh.worker", "--config", str(self.config_path), "rpc"],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.stdin.write(header + archive)
        process.stdin.close()
        response = json.loads(process.stdout.readline())
        process.stdout.close()
        process.stderr.close()
        process.wait(timeout=5)
        self.assertEqual(response["state"], "running")
        result = Worker(self.config_path).status("attempt-one")
        self.assertEqual(result["state"], "running")
        self.assertEqual(self.wait()["stdout_tail"], "survived\n")

    def test_rpc_output_disconnect_before_response_survives(self):
        archive, manifest = self.bundle("import time\ntime.sleep(0.4)\nprint('survived broken pipe')\n")
        header = json.dumps({"op": "start", "assignment": self.assignment(), "manifest": manifest}).encode() + b"\n"
        process = subprocess.Popen([sys.executable, "-m", "sparemesh.worker", "--config", str(self.config_path), "rpc"],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.stdout.close()
        process.stdin.write(header + archive)
        process.stdin.close()
        process.wait(timeout=5)
        process.stderr.close()
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(self.wait()["stdout_tail"], "survived broken pipe\n")

    def test_lost_supervisor_stays_uncertain_and_never_relaunches(self):
        _, assignment, manifest, archive = self.start("import time\ntime.sleep(30)\n")
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            response = self.worker.status("attempt-one")
            if "process_group" in response:
                break
            time.sleep(0.05)
        os.kill(response["supervisor_pid"], signal.SIGKILL)
        deadline = time.monotonic() + 3
        while self.worker.status("attempt-one")["state"] == "running" and time.monotonic() < deadline:
            time.sleep(0.05)
        result = self.worker.start(assignment, manifest, io.BytesIO(archive))
        self.assertEqual(result["state"], "uncertain")
        self.assertIsNone(result["result"])
        self.assertFalse(self.worker.probe()["snapshot"]["ready"])
        with self.assertRaises(ConflictError):
            self.worker.start(self.assignment("attempt-two"), manifest, io.BytesIO(archive))

    def test_rpc_rejects_oversized_header_and_emits_json_error(self):
        process = subprocess.run([sys.executable, "-m", "sparemesh.worker", "--config", str(self.config_path), "rpc"],
                                 input=b"x" * (MAX_HEADER_BYTES + 1), capture_output=True, timeout=5)
        self.assertEqual(process.returncode, 2)
        self.assertEqual(json.loads(process.stdout)["error"]["code"], "ValidationError")


if __name__ == "__main__":
    unittest.main()
