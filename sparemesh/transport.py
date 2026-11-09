"""Bounded, one-request worker RPC over a local process or verified SSH."""

from dataclasses import dataclass
import json
import os
from pathlib import Path
import selectors
import shlex
import signal
import subprocess
import time
from typing import Any

from .coordinator import ValidationError
from .source_bundle import MAX_ARCHIVE_BYTES

MAX_HEADER_BYTES = 64 * 1024
MAX_RESPONSE_BYTES = 256 * 1024
MAX_ERROR_BYTES = 64 * 1024


class TransportError(Exception):
    """The worker's execution state could not be confirmed."""


class RemoteError(TransportError):
    """A worker rejected an RPC and returned a structured error."""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


@dataclass(frozen=True)
class TransportConfig:
    kind: str
    python: str
    package_dir: str
    worker_config: str
    target: str | None = None
    port: int | None = None
    identity_file: str | None = None
    known_hosts_file: str | None = None


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValidationError(f"{name} must be a nonempty string without control characters")
    return value


def parse_transport(value: Any, base: Path) -> TransportConfig:
    if not isinstance(value, dict):
        raise ValidationError("transport must be an object")
    required = {"kind", "python", "package_dir", "worker_config"}
    optional = {"target", "port", "identity_file", "known_hosts_file"}
    if not required <= value.keys() or value.keys() - required - optional:
        raise ValidationError("transport requires kind, python, package_dir, worker_config and supported SSH options")
    kind = value["kind"]
    if not isinstance(kind, str) or kind not in {"local", "ssh"}:
        raise ValidationError("transport kind must be local or ssh")
    if kind == "local" and value.keys() & optional:
        raise ValidationError("local transport does not accept SSH options")
    paths = {}
    for key in ("python", "package_dir", "worker_config"):
        path = Path(_text(value[key], key))
        if kind == "ssh" and not path.is_absolute():
            raise ValidationError(f"remote {key} must be an absolute path")
        paths[key] = str(path if path.is_absolute() else base / path)
    target = value.get("target")
    if kind == "ssh":
        target = _text(target, "target")
        if target.startswith("-") or any(char.isspace() for char in target):
            raise ValidationError("SSH target must be a hostname or user@hostname")
    port = value.get("port")
    if port is not None and (type(port) is not int or not 1 <= port <= 65535):
        raise ValidationError("SSH port must be an integer from 1 to 65535")
    for key in ("identity_file", "known_hosts_file"):
        if key in value:
            path = Path(_text(value[key], key))
            paths[key] = str(path if path.is_absolute() else base / path)
    return TransportConfig(kind=kind, target=target, port=port, **paths)


def _decode_json(data: bytes) -> dict:
    try:
        value = json.loads(data.decode("utf-8"), parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
    except (ValueError, UnicodeError) as error:
        raise TransportError("worker returned invalid JSON") from error
    if not isinstance(value, dict):
        raise TransportError("worker response must be an object")
    return value


class WorkerTransport:
    """Commands originate exclusively from operator-controlled configuration."""

    def __init__(self, config: TransportConfig, timeout_seconds: float = 30):
        self.config = config
        self.timeout_seconds = timeout_seconds

    def command(self) -> tuple[list[str], str | None]:
        config = self.config
        worker = [config.python, "-m", "sparemesh.worker", "--config", config.worker_config, "rpc"]
        if config.kind == "local":
            return worker, config.package_dir
        command = ["ssh", "-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                   "-o", "ForwardAgent=no", "-o", "ClearAllForwardings=yes",
                   "-o", "ConnectTimeout=" + str(max(1, int(self.timeout_seconds)))]
        if config.port is not None:
            command.extend(["-p", str(config.port)])
        if config.identity_file is not None:
            command.extend(["-i", config.identity_file, "-o", "IdentitiesOnly=yes"])
        if config.known_hosts_file is not None:
            command.extend(["-o", "UserKnownHostsFile=" + config.known_hosts_file])
        remote = "cd " + shlex.quote(config.package_dir) + " && exec " + shlex.join(worker)
        command.extend([config.target, remote])
        return command, None

    def request(self, header: dict, archive: Path | None = None) -> dict:
        encoded = json.dumps(header, separators=(",", ":"), allow_nan=False).encode("utf-8") + b"\n"
        if len(encoded) > MAX_HEADER_BYTES:
            raise TransportError("worker request header exceeds the size limit")
        archive_file = None
        try:
            if archive is not None:
                archive_file = archive.open("rb")
                size = os.fstat(archive_file.fileno()).st_size
                if size > MAX_ARCHIVE_BYTES or size != header.get("manifest", {}).get("archive_size_bytes"):
                    raise TransportError("archive size does not match its manifest")
            command, cwd = self.command()
            try:
                process = subprocess.Popen(command, cwd=cwd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE, start_new_session=True)
            except OSError as error:
                raise TransportError(f"cannot start worker transport: {error}") from error
            output, errors = self._exchange(process, encoded, archive_file)
        finally:
            if archive_file is not None:
                archive_file.close()
        if process.returncode != 0 and not output.strip():
            detail = errors.decode("utf-8", errors="replace").strip()
            raise TransportError(f"worker transport exited {process.returncode}: {detail[:2048]}")
        response = _decode_json(output)
        if "error" in response:
            error = response["error"]
            if not isinstance(error, dict) or not isinstance(error.get("code"), str) or not isinstance(error.get("message"), str):
                raise TransportError("worker returned an invalid error response")
            raise RemoteError(error["code"], error["message"])
        if process.returncode != 0:
            detail = errors.decode("utf-8", errors="replace").strip()
            raise TransportError(f"worker transport exited {process.returncode}: {detail[:2048]}")
        return response

    def _exchange(self, process, header: bytes, archive_file) -> tuple[bytes, bytes]:
        output = bytearray()
        errors = bytearray()
        pending = header
        deadline = time.monotonic() + self.timeout_seconds
        selector = selectors.DefaultSelector()
        try:
            for stream, event in ((process.stdin, selectors.EVENT_WRITE), (process.stdout, selectors.EVENT_READ),
                                  (process.stderr, selectors.EVENT_READ)):
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, event)
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TransportError("worker transport timed out; attempt state remains unconfirmed")
                for key, _ in selector.select(remaining):
                    stream = key.fileobj
                    if stream is process.stdin:
                        if not pending and archive_file is not None:
                            pending = archive_file.read(64 * 1024)
                        if not pending:
                            selector.unregister(stream)
                            stream.close()
                            continue
                        try:
                            written = os.write(stream.fileno(), pending)
                            pending = pending[written:]
                        except BrokenPipeError:
                            # An idempotent replay may respond without reading the archive.
                            selector.unregister(stream)
                            stream.close()
                    else:
                        data = os.read(stream.fileno(), 64 * 1024)
                        if not data:
                            selector.unregister(stream)
                            stream.close()
                            continue
                        destination = output if stream is process.stdout else errors
                        limit = MAX_RESPONSE_BYTES if stream is process.stdout else MAX_ERROR_BYTES
                        if len(destination) + len(data) > limit:
                            raise TransportError("worker transport response exceeds the size limit")
                        destination.extend(data)
            try:
                process.wait(timeout=max(0.001, deadline - time.monotonic()))
            except subprocess.TimeoutExpired as error:
                raise TransportError("worker transport timed out; attempt state remains unconfirmed") from error
            return bytes(output), bytes(errors)
        finally:
            selector.close()
            if process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=0.25)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait()
            for stream in (process.stdin, process.stdout, process.stderr):
                stream.close()
