"""Operator-driven coordinator loop for a small trusted worker fleet."""

from dataclasses import dataclass
import fcntl
import hashlib
import json
from pathlib import Path
import re
import stat
from typing import Any

from .analytics import AnalyticsStore
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
    analytics_database: Path | None = None

    @property
    def analytics_path(self) -> Path:
        return self.analytics_database or self.database.with_name(self.database.stem + ".analytics.sqlite3")


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
    optional = {"timeout_seconds", "poll_seconds", "freshness_seconds", "analytics_database"}
    if not isinstance(value, dict) or not required <= value.keys() or value.keys() - required - optional:
        raise ValidationError("pilot config requires schema_version, database, artifacts_dir, hosts and supported optional settings")
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
        analytics_database=_path(value["analytics_database"], path.parent, "analytics_database")
        if "analytics_database" in value else None,
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
        self.analytics = None
        self.analytics_error = None
        try:
            self.analytics = AnalyticsStore(config.analytics_path, sqlite_timeout=0.1)
        except Exception as error:
            # Telemetry storage must not prevent worker execution or reconciliation.
            self.analytics_error = _error(error)

    def close(self) -> None:
        self.coordinator.close()
        if self.analytics is not None:
            try:
                self.analytics.close()
            except Exception as error:
                self.analytics_error = _error(error)

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

    def _record_analytics(self, operation: str, value: dict, report: dict) -> None:
        if self.analytics is None:
            return
        try:
            getattr(self.analytics, operation)(value)
        except Exception as error:
            report.setdefault("analytics_errors", []).append(_error(error))

    def _record_attempt(self, host: HostConfig, assignment: dict, report: dict,
                        *, state: str | None = None, usage: dict | None = None) -> None:
        value = {
            "schema_version": 1, "attempt_id": assignment["attempt_id"],
            "job_id": assignment["job"]["job_id"], "host_id": host.host_id,
            "job_owner": assignment["job"]["owner"], "host_owner": host.owner,
            "assigned_at": assignment["assigned_at"], "state": state or assignment["state"],
            "started_at": None, "finished_at": None,
            "cpu_seconds": None, "peak_rss_mb": None, "usage_sample_count": 0,
        }
        self._record_analytics("record_attempt", value, report)
        if usage is not None:
            if not isinstance(usage, dict):
                report.setdefault("analytics_errors", []).append(_error(ValidationError("worker usage must be an object")))
                return
            for key in ("started_at", "finished_at", "cpu_seconds", "peak_rss_mb", "usage_sample_count"):
                if key in usage and usage[key] is not None:
                    value[key] = usage[key]
            self._record_analytics("record_attempt", value, report)

    def _record_worker_analytics(self, host: HostConfig, response: dict, report: dict) -> None:
        analytics = response.get("analytics")
        if analytics is None:
            return
        try:
            if not isinstance(analytics, dict):
                raise ValidationError("worker analytics must be an object")
            diagnostics = analytics.get("diagnostics", [])
            if not isinstance(diagnostics, list) or any(not isinstance(item, str) for item in diagnostics):
                raise ValidationError("worker analytics diagnostics must be an array of strings")
            if diagnostics:
                messages = report.get("analytics_diagnostics", []) + [message[:1024] for message in diagnostics[:20]]
                report["analytics_diagnostics"] = list(dict.fromkeys(messages))[:20]
            sample = analytics.get("sample")
            if sample is not None:
                if not isinstance(sample, dict) or sample.get("host_id") != host.host_id:
                    raise ValidationError("worker analytics sample does not match configured host")
                self._record_analytics("record_sample", sample, report)
            attempt = analytics.get("attempt")
            if attempt is not None:
                if not isinstance(attempt, dict):
                    raise ValidationError("worker analytics attempt must be an object")
                assignment = self.coordinator.status(attempt.get("job_id"))["attempt"]
                if (assignment is None or assignment["attempt_id"] != attempt.get("attempt_id")
                        or assignment["host_id"] != host.host_id or assignment["worker_id"] != host.worker_id):
                    raise ValidationError("worker analytics attempt does not match its assignment")
                state = attempt.get("state")
                if not isinstance(state, str) or state not in {
                        "assigned", "running", "uncertain", "cancel_requested", "succeeded", "failed", "canceled"}:
                    raise ValidationError("worker analytics attempt has an invalid state")
                self._record_attempt(host, assignment, report, state=state, usage=attempt)
        except Exception as error:
            report.setdefault("analytics_errors", []).append(_error(error))

    def _attempt_response(self, host: HostConfig, assignment: dict, response: dict, report: dict) -> dict:
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
            assignment = self.coordinator.finish(host.host_id, host.worker_id, assignment["attempt_id"], result)["attempt"]
        elif result is not None:
            raise TransportError("worker reported a terminal result for a nonterminal attempt")
        self._record_attempt(host, assignment, report, state=state, usage=response.get("usage"))
        self._record_worker_analytics(host, response, report)
        return response

    def _dispatch(self, host: HostConfig, assignment: dict, report: dict) -> dict:
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
        return self._attempt_response(host, assignment, response, report)

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
                self._record_worker_analytics(host, probe, event)
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
                self._record_attempt(host, assignment, event)
                event["worker"] = self._dispatch(host, assignment, event)
                event["state"] = event["worker"]["state"]
            except (TransportError, CoordinatorError, SourceBundleError, OSError) as error:
                event["error"] = _error(error)
        report = {"schema_version": 1, "hosts": events}
        if self.analytics_error is not None:
            report["analytics_errors"] = [self.analytics_error]
        return report

    def result(self, job_id: str) -> dict:
        job = self.coordinator.status(job_id)
        report = {"schema_version": 1, "job": job, "worker": None}
        if self.analytics_error is not None:
            report["analytics_errors"] = [self.analytics_error]
        assignment = job["attempt"]
        if assignment is None:
            return report
        host = next((host for host in self.config.hosts if host.host_id == assignment["host_id"]), None)
        if host is None or host.worker_id != assignment["worker_id"]:
            raise ValidationError("assigned worker is absent from this pilot configuration")
        try:
            response = self.transports[host.host_id].request({"op": "status", "attempt_id": assignment["attempt_id"]})
            report["worker"] = self._attempt_response(host, assignment, response, report)
            report["job"] = self.coordinator.status(job_id)
        except (TransportError, CoordinatorError, OSError) as error:
            report["error"] = _error(error)
        return report
