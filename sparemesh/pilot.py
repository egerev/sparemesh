"""Operator-driven coordinator loop for a small trusted worker fleet."""

from dataclasses import dataclass
import fcntl
import hashlib
import json
from pathlib import Path
import re
import stat
from typing import Any

from .coordinator import Coordinator, CoordinatorError, ValidationError, _identifier, _number
from .source_bundle import MAX_ARCHIVE_BYTES, SourceBundleError, package_source
from .transport import RemoteError, TransportConfig, TransportError, WorkerTransport, parse_transport


@dataclass(frozen=True)
class HostConfig:
    host_id: str
    owner: str
    worker_id: str
    transport: TransportConfig


@dataclass(frozen=True)
class PilotConfig:
    database: Path
    artifacts_dir: Path
    hosts: tuple[HostConfig, ...]
    timeout_seconds: float = 30
    poll_seconds: float = 2
    freshness_seconds: float = 30


def _positive(value: Any, name: str) -> float:
    return _number(value, name, positive=True)


def _path(value: Any, base: Path, name: str) -> Path:
    if not isinstance(value, str) or not value or "\0" in value:
        raise ValidationError(f"{name} must be a nonempty path")
    path = Path(value)
    return path if path.is_absolute() else base / path


def load_config(filename: str | Path) -> PilotConfig:
    path = Path(filename).absolute()
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, UnicodeError) as error:
        raise ValidationError(f"cannot read pilot config: {error}") from error
    required = {"schema_version", "database", "artifacts_dir", "hosts"}
    optional = {"timeout_seconds", "poll_seconds", "freshness_seconds"}
    if not isinstance(value, dict) or not required <= value.keys() or value.keys() - required - optional:
        raise ValidationError("pilot config requires schema_version, database, artifacts_dir, hosts and supported timing options")
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise ValidationError("pilot config schema_version must be the integer 1")
    if not isinstance(value["hosts"], list) or not value["hosts"]:
        raise ValidationError("pilot hosts must be a nonempty array")
    hosts = []
    for item in value["hosts"]:
        if not isinstance(item, dict) or set(item) != {"host_id", "owner", "worker_id", "transport"}:
            raise ValidationError("each pilot host requires host_id, owner, worker_id, transport")
        hosts.append(HostConfig(
            _identifier(item["host_id"], "host_id"), _identifier(item["owner"], "owner"),
            _identifier(item["worker_id"], "worker_id"), parse_transport(item["transport"], path.parent),
        ))
    if len({host.host_id for host in hosts}) != len(hosts) or len({host.worker_id for host in hosts}) != len(hosts):
        raise ValidationError("pilot host and worker identities must be unique")
    return PilotConfig(
        database=_path(value["database"], path.parent, "database"),
        artifacts_dir=_path(value["artifacts_dir"], path.parent, "artifacts_dir"), hosts=tuple(hosts),
        timeout_seconds=_positive(value.get("timeout_seconds", 30), "timeout_seconds"),
        poll_seconds=_positive(value.get("poll_seconds", 2), "poll_seconds"),
        freshness_seconds=_positive(value.get("freshness_seconds", 30), "freshness_seconds"),
    )


def _revision(value: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})", value) is None:
        raise ValidationError("revision must be a full 40- or 64-character hexadecimal commit ID")
    return value.lower()


def _bundle(directory: Path, revision: str) -> tuple[dict, Path]:
    """Recheck immutable bytes before sending an already-published bundle."""
    manifest_path = directory / "manifest.json"
    archive = directory / "source.tar"
    try:
        if manifest_path.stat().st_size > 64 * 1024:
            raise SourceBundleError("bundle manifest exceeds the size limit")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(manifest, dict) or manifest.get("commit") != revision or manifest.get("archive_name") != "source.tar":
            raise SourceBundleError("bundle manifest does not match the submitted revision")
        size = manifest.get("archive_size_bytes")
        if type(size) is not int or not 0 < size <= MAX_ARCHIVE_BYTES:
            raise SourceBundleError("bundle manifest has an invalid archive size")
        info = archive.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_size != size:
            raise SourceBundleError("bundle archive is not a regular file of the declared size")
        with archive.open("rb") as file:
            digest = hashlib.file_digest(file, "sha256").hexdigest()
        if digest != manifest.get("archive_sha256"):
            raise SourceBundleError("bundle archive hash does not match its manifest")
    except (OSError, UnicodeError, ValueError) as error:
        raise SourceBundleError(f"cannot read source bundle: {error}") from error
    return manifest, archive


def _error(error: Exception) -> dict:
    return {"code": error.code if isinstance(error, RemoteError) else type(error).__name__, "message": str(error)}


class Pilot:
    """A tick is restartable. Only a matching worker terminal result releases a slot."""

    def __init__(self, config: PilotConfig, *, transport_factory=WorkerTransport):
        self.config = config
        config.database.parent.mkdir(parents=True, exist_ok=True)
        config.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.coordinator = Coordinator(config.database, freshness_seconds=config.freshness_seconds)
        self.transports = {host.host_id: transport_factory(host.transport, config.timeout_seconds) for host in config.hosts}

    def close(self) -> None:
        self.coordinator.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def submit(self, repository: str | Path, revision: str, job_id: str, owner: str, profile: str,
               requirements: dict, restartable: bool = False) -> dict:
        revision = _revision(revision)
        destination = self.config.artifacts_dir / revision
        # Separate controllers may submit the same revision concurrently.
        with (self.config.artifacts_dir / ".publish.lock").open("a+b") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            if not destination.exists():
                package_source(repository, revision, destination)
            _bundle(destination, revision)
        return self.coordinator.submit(job_id, owner, profile, revision, requirements, restartable)

    def _attempt_response(self, host: HostConfig, assignment: dict, response: dict) -> dict:
        if response.get("schema_version") != 1 or type(response.get("schema_version")) is not int:
            raise TransportError("worker attempt response has an unsupported schema_version")
        if response.get("attempt_id") != assignment["attempt_id"]:
            raise TransportError("worker response does not match the assigned attempt")
        state = response.get("state")
        if not isinstance(state, str) or state not in {"running", "uncertain", "succeeded", "failed", "canceled"}:
            raise TransportError("worker returned an invalid attempt state")
        for key in ("stdout_tail", "stderr_tail"):
            if not isinstance(response.get(key), str) or len(response[key]) > 64 * 1024:
                raise TransportError(f"worker returned invalid {key}")
        result = response.get("result")
        if state in {"succeeded", "failed", "canceled"}:
            if not isinstance(result, dict) or result.get("outcome") != state:
                raise TransportError("worker terminal state does not match its result")
            self.coordinator.finish(host.host_id, host.worker_id, assignment["attempt_id"], result)
        elif result is not None:
            raise TransportError("worker reported a terminal result for a nonterminal attempt")
        return response

    def _dispatch(self, host: HostConfig, assignment: dict) -> dict:
        transport = self.transports[host.host_id]
        operation = "cancel" if assignment["cancel_requested"] else "status"
        try:
            response = transport.request({"op": operation, "attempt_id": assignment["attempt_id"]})
        except RemoteError as error:
            if error.code != "NotFoundError":
                raise
            manifest, archive = _bundle(self.config.artifacts_dir / assignment["job"]["revision"],
                                        assignment["job"]["revision"])
            response = transport.request({"op": "start", "assignment": assignment, "manifest": manifest}, archive)
        return self._attempt_response(host, assignment, response)

    def tick(self) -> dict:
        events = []
        ready = []
        for host in self.config.hosts:
            self.coordinator.register_host(host.host_id, host.owner, host.worker_id)
        # Snapshot the whole fleet before claims so own-host preference has complete input.
        for host in self.config.hosts:
            event = {"host_id": host.host_id}
            events.append(event)
            try:
                probe = self.transports[host.host_id].request({"op": "probe"})
                if type(probe.get("schema_version")) is not int or probe.get("schema_version") != 1:
                    raise TransportError("worker probe has an unsupported schema_version")
                if probe.get("host_id") != host.host_id or probe.get("worker_id") != host.worker_id:
                    raise TransportError("worker probe identity does not match configured host")
                self.coordinator.report_host(host.host_id, host.worker_id, probe.get("snapshot"))
                event["probe"] = "ok"
                ready.append((host, event))
            except (TransportError, CoordinatorError, SourceBundleError, OSError) as error:
                event["error"] = _error(error)
        for host, event in ready:
            try:
                assignment = self.coordinator.claim(host.host_id, host.worker_id)
                if assignment is None:
                    event["state"] = "idle"
                    continue
                event.update(attempt_id=assignment["attempt_id"], job_id=assignment["job"]["job_id"])
                event["worker"] = self._dispatch(host, assignment)
                event["state"] = event["worker"]["state"]
            except (TransportError, CoordinatorError, SourceBundleError, OSError) as error:
                event["error"] = _error(error)
        return {"schema_version": 1, "hosts": events}

    def result(self, job_id: str) -> dict:
        job = self.coordinator.status(job_id)
        report = {"schema_version": 1, "job": job, "worker": None}
        assignment = job["attempt"]
        if assignment is None:
            return report
        host = next((host for host in self.config.hosts if host.host_id == assignment["host_id"]), None)
        if host is None or host.worker_id != assignment["worker_id"]:
            raise ValidationError("assigned worker is absent from this pilot configuration")
        try:
            response = self.transports[host.host_id].request({"op": "status", "attempt_id": assignment["attempt_id"]})
            report["worker"] = self._attempt_response(host, assignment, response)
            report["job"] = self.coordinator.status(job_id)
        except (TransportError, CoordinatorError, OSError) as error:
            report["error"] = _error(error)
        return report
