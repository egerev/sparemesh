import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

from sparemesh.coordinator import ValidationError
from sparemesh.transport import (
    MAX_RESPONSE_BYTES, RemoteError, TransportConfig, TransportError, WorkerTransport, parse_transport,
)


class ScriptTransport(WorkerTransport):
    def __init__(self, script, timeout=2):
        super().__init__(TransportConfig("local", sys.executable, ".", "/unused"), timeout)
        self.script = script

    def command(self):
        return [sys.executable, "-c", self.script], None


class TransportTests(unittest.TestCase):
    def test_one_json_header_and_raw_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "source.tar"
            archive.write_bytes(b"archive\nwith\0bytes")
            script = (
                "import json,sys; h=json.loads(sys.stdin.buffer.readline()); "
                "data=sys.stdin.buffer.read(); "
                "print(json.dumps({'op':h['op'],'body':list(data)}))"
            )
            response = ScriptTransport(script).request(
                {"op": "start", "manifest": {"archive_size_bytes": archive.stat().st_size}}, archive,
            )
            self.assertEqual(response, {"op": "start", "body": list(archive.read_bytes())})

    def test_early_response_does_not_require_reading_replayed_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "source.tar"
            archive.write_bytes(b"x" * (1024 * 1024))
            script = "import json,sys; sys.stdin.buffer.readline(); print(json.dumps({'state':'running'}))"
            response = ScriptTransport(script).request(
                {"op": "start", "manifest": {"archive_size_bytes": archive.stat().st_size}}, archive,
            )
            self.assertEqual(response, {"state": "running"})

    def test_remote_errors_preserve_structured_code(self):
        transport = ScriptTransport("import sys; print('{\"error\":{\"code\":\"NotFoundError\",\"message\":\"unknown attempt\"}}'); sys.exit(2)")
        with self.assertRaises(RemoteError) as caught:
            transport.request({"op": "status"})
        self.assertEqual(caught.exception.code, "NotFoundError")

    def test_non_json_and_non_object_responses_are_rejected(self):
        for script in ("print('not json')", "print('[]')", "print('{\"value\":NaN}')"):
            with self.subTest(script=script), self.assertRaises(TransportError):
                ScriptTransport(script).request({"op": "probe"})

    def test_response_is_bounded(self):
        script = f"import sys; sys.stdout.write('x' * {MAX_RESPONSE_BYTES + 1}); sys.stdout.flush()"
        with self.assertRaisesRegex(TransportError, "size limit"):
            ScriptTransport(script).request({"op": "probe"})

    def test_timeout_is_bounded_and_does_not_claim_failure(self):
        start = time.monotonic()
        with self.assertRaisesRegex(TransportError, "unconfirmed"):
            ScriptTransport("import time; time.sleep(10)", timeout=0.05).request({"op": "probe"})
        self.assertLess(time.monotonic() - start, 2)

    def test_local_command_uses_argv_and_fixed_working_directory(self):
        transport = WorkerTransport(TransportConfig("local", "/path with spaces/python", "/source dir", "/config dir/worker.json"))
        command, cwd = transport.command()
        self.assertEqual(cwd, "/source dir")
        self.assertEqual(command, ["/path with spaces/python", "-m", "sparemesh.worker", "--config", "/config dir/worker.json", "rpc"])

    def test_ssh_verifies_host_keys_and_quotes_trusted_paths(self):
        transport = WorkerTransport(TransportConfig(
            "ssh", "/python dir/python", "/source ' dir", "/config dir/worker.json", "alice@host",
            port=2222, identity_file="/key file", known_hosts_file="/known hosts",
        ))
        command, cwd = transport.command()
        self.assertIsNone(cwd)
        for value in ("BatchMode=yes", "StrictHostKeyChecking=yes", "ForwardAgent=no", "ClearAllForwardings=yes"):
            self.assertIn(value, command)
        self.assertEqual(command[-2], "alice@host")
        self.assertIn("exec '/python dir/python' -m sparemesh.worker", command[-1])
        self.assertIn("'/config dir/worker.json'", command[-1])

    def test_transport_config_rejects_ssh_options_in_local_config(self):
        value = {"kind": "local", "python": "python", "package_dir": ".", "worker_config": "worker.json", "target": "host"}
        with self.assertRaises(ValidationError):
            parse_transport(value, Path("/base"))

    def test_transport_config_resolves_local_paths_and_rejects_option_target(self):
        value = {"kind": "local", "python": "python", "package_dir": ".", "worker_config": "worker.json"}
        config = parse_transport(value, Path("/base"))
        self.assertEqual(config.worker_config, "/base/worker.json")
        value.update(kind="ssh", target="-oProxyCommand=bad", python="/python", package_dir="/source", worker_config="/worker.json")
        with self.assertRaises(ValidationError):
            parse_transport(value, Path("/base"))


if __name__ == "__main__":
    unittest.main()
