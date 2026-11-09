"""One trusted POSIX worker, reached through bounded JSON-and-tar SSH stdio RPC.

The supervisor outlives the RPC connection. A lost supervisor leaves an uncertain
reservation, which an operator must investigate. Profiles are trusted local
policy. Process groups do not contain code that deliberately creates a session.
"""

import argparse
from contextlib import contextmanager
import ctypes
import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import selectors
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import unicodedata

from .coordinator import (ConflictError, CoordinatorError, IdentityError, NotFoundError,
                          ValidationError, _boolean, _identifier, _number, _object,
                          _requirements, _version)
from .source_bundle import (MAX_ARCHIVE_BYTES, MAX_BLOB_BYTES, MAX_ENTRIES,
                            MAX_SOURCE_BYTES, ROOT_PREFIX, SourceBundleError, _safe_path)


MAX_HEADER_BYTES = 64 * 1024
TAIL_BYTES = 16 * 1024
MANIFEST_FIELDS = {"schema_version", "commit", "object_format", "archive_name", "archive_format",
                   "root_prefix", "archive_sha256", "archive_size_bytes", "entry_count",
                   "file_count", "total_file_bytes"}
TERMINAL = {"succeeded", "failed", "canceled"}


class WorkerError(CoordinatorError):
    """The worker could not safely accept or execute a request."""


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValidationError("duplicate JSON object field")
        result[key] = value
    return result


def _decode(data):
    try:
        return json.loads(data, object_pairs_hook=_pairs,
                          parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
    except (ValueError, UnicodeError) as error:
        raise ValidationError("invalid JSON") from error


def _read_json(path):
    with path.open("rb") as stream:
        data = stream.read(MAX_HEADER_BYTES + 1)
    if len(data) > MAX_HEADER_BYTES:
        raise WorkerError("worker metadata exceeds its size limit")
    return _decode(data)


def _atomic_json(path, value):
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode() + b"\n"
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".state-", delete=False) as stream:
        temporary = Path(stream.name)
        try:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
            os.replace(temporary, path)
            directory = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            temporary.unlink(missing_ok=True)


def _environment(home):
    return {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "HOME": str(home), "TMPDIR": str(home),
            "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "PYTHONNOUSERSITE": "1",
            "PYTHONDONTWRITEBYTECODE": "1"}


def _manifest(value, revision):
    value = dict(_object(value, MANIFEST_FIELDS, "manifest"))
    _version(value["schema_version"])
    if value["commit"] != revision or value["object_format"] != {40: "sha1", 64: "sha256"}[len(revision)]:
        raise ValidationError("manifest commit must match the assigned full revision")
    if (value["archive_name"], value["archive_format"], value["root_prefix"]) != ("source.tar", "tar-pax", ROOT_PREFIX):
        raise ValidationError("unsupported source archive format or root")
    if not isinstance(value["archive_sha256"], str) or not re.fullmatch("[0-9a-f]{64}", value["archive_sha256"]):
        raise ValidationError("manifest archive_sha256 must be lowercase SHA-256")
    limits = {"archive_size_bytes": MAX_ARCHIVE_BYTES, "entry_count": MAX_ENTRIES,
              "file_count": MAX_ENTRIES, "total_file_bytes": MAX_SOURCE_BYTES}
    for key, limit in limits.items():
        if type(value[key]) is not int or not 0 <= value[key] <= limit:
            raise ValidationError(f"manifest {key} exceeds its limit")
    if value["archive_size_bytes"] == 0 or value["archive_size_bytes"] % 512:
        raise ValidationError("archive size must be a positive multiple of 512")
    if value["file_count"] > value["entry_count"]:
        raise ValidationError("manifest file count exceeds entry count")
    return value


def _assignment(value, config):
    fields = {"schema_version", "attempt_id", "host_id", "worker_id", "job", "state", "assigned_at",
              "cancel_requested", "cancel_reason", "result", "finished_at"}
    value = _object(value, fields, "assignment")
    _version(value["schema_version"])
    if value["host_id"] != config["host_id"] or value["worker_id"] != config["worker_id"]:
        raise IdentityError("assignment does not belong to this worker")
    _identifier(value["attempt_id"], "attempt_id")
    _boolean(value["cancel_requested"], "cancel_requested")
    _number(value["assigned_at"], "assigned_at")
    job = dict(_object(value["job"], {"schema_version", "job_id", "owner", "profile", "revision",
                                      "requirements", "restartable"}, "job"))
    _version(job["schema_version"])
    for key in ("job_id", "owner", "profile"):
        _identifier(job[key], key)
    if not isinstance(job["revision"], str) or not re.fullmatch("(?:[0-9a-f]{40}|[0-9a-f]{64})", job["revision"]):
        raise ValidationError("job revision must be a full lowercase commit ID")
    job["requirements"] = _requirements(job["requirements"])
    _boolean(job["restartable"], "restartable")
    identity = {key: value[key] for key in ("attempt_id", "host_id", "worker_id", "assigned_at")}
    identity["job"] = job
    return identity, value["cancel_requested"]


def _preflight_tar(path):
    """Bound extension-header allocations before tarfile interprets PAX metadata."""
    pending_extension = False
    members = 0
    with path.open("rb") as stream:
        while True:
            header = stream.read(512)
            if len(header) != 512:
                raise WorkerError("truncated tar header")
            if header == b"\0" * 512:
                if pending_extension:
                    raise WorkerError("tar extension has no following member")
                # Only padding may follow the end marker.
                while chunk := stream.read(64 * 1024):
                    if any(chunk):
                        raise WorkerError("nonzero bytes after tar end marker")
                return
            try:
                raw_size = header[124:136].strip(b"\0 ")
                size = int(raw_size or b"0", 8)
            except ValueError as error:
                raise WorkerError("unsupported tar size encoding") from error
            kind = header[156:157]
            if kind not in (b"0", b"\0", b"5", b"x"):
                raise WorkerError("tar links and special entries are forbidden")
            if kind == b"x":
                if pending_extension:
                    raise WorkerError("consecutive tar extension headers are forbidden")
                pending_extension = True
            else:
                pending_extension = False
                members += 1
                if members > MAX_ENTRIES + 1:
                    raise WorkerError("tar has too many members")
            limit = MAX_HEADER_BYTES if kind == b"x" else MAX_BLOB_BYTES
            if size < 0 or size > limit or (kind == b"5" and size):
                raise WorkerError("tar member exceeds its size limit")
            offset = stream.tell() + ((size + 511) // 512) * 512
            if offset > path.stat().st_size:
                raise WorkerError("truncated tar member")
            stream.seek(offset)


def _extract(path, destination, manifest):
    _preflight_tar(path)
    seen = set()
    entries = files = total = 0
    root_seen = False
    try:
        with tarfile.open(path, "r:", encoding="utf-8", errors="strict") as archive:
            for member in archive:
                if member.pax_headers.keys() - {"path"} or member.sparse is not None:
                    raise WorkerError("unsupported tar metadata")
                if member.name.rstrip("/") == "source":
                    if root_seen or not member.isdir() or member.size:
                        raise WorkerError("invalid or duplicate source root")
                    root_seen = True
                    (destination / "source").mkdir(mode=0o700)
                    continue
                if not root_seen or not member.name.startswith(ROOT_PREFIX):
                    raise WorkerError("tar member is outside source root")
                relative = member.name[len(ROOT_PREFIX):].rstrip("/") if member.isdir() else member.name[len(ROOT_PREFIX):]
                name = _safe_path(relative.encode("utf-8"))
                key = unicodedata.normalize("NFC", name).casefold()
                if key in seen:
                    raise WorkerError("duplicate or colliding tar member")
                seen.add(key)
                entries += 1
                if entries > MAX_ENTRIES or entries > manifest["entry_count"]:
                    raise WorkerError("tar entry count exceeds manifest or worker limit")
                target = destination / "source" / name
                # Every parent must have appeared as a directory already.
                if not target.parent.is_dir():
                    raise WorkerError("tar member has an undeclared parent")
                if member.isdir() and member.size == 0:
                    target.mkdir(mode=0o700)
                elif member.isfile() and member.size <= MAX_BLOB_BYTES:
                    files += 1
                    total += member.size
                    if total > MAX_SOURCE_BYTES or total > manifest["total_file_bytes"]:
                        raise WorkerError("tar content exceeds manifest or worker limit")
                    with archive.extractfile(member) as source, target.open("xb") as output:
                        shutil.copyfileobj(source, output, 64 * 1024)
                    target.chmod(0o700 if member.mode & 0o111 else 0o600)
                else:
                    raise WorkerError("tar links and special entries are forbidden")
    except (tarfile.TarError, SourceBundleError, UnicodeError, OSError, ValueError, RecursionError) as error:
        raise WorkerError(f"unsafe or invalid source archive: {error}") from error
    if not root_seen or (entries, files, total) != (manifest["entry_count"], manifest["file_count"], manifest["total_file_bytes"]):
        raise WorkerError("tar measurements differ from manifest")


class Worker:
    def __init__(self, config_path):
        self.config_path = Path(config_path)
        if not self.config_path.is_absolute():
            raise ValidationError("worker config path must be absolute")
        info = self.config_path.stat()
        if info.st_uid != os.getuid() or info.st_mode & 0o022:
            raise WorkerError("worker config must be owned by this user and not writable by others")
        self.config = dict(_object(_read_json(self.config_path), {"schema_version", "host_id", "worker_id",
                                 "state_dir", "profiles", "reserve_memory_mb", "allow_guests"}, "worker config"))
        _version(self.config["schema_version"])
        for key in ("host_id", "worker_id"):
            _identifier(self.config[key], key)
        _number(self.config["reserve_memory_mb"], "reserve_memory_mb")
        _boolean(self.config["allow_guests"], "allow_guests")
        self.root = Path(self.config["state_dir"])
        if not self.root.is_absolute() or self.root.is_symlink():
            raise ValidationError("state_dir must be an absolute private directory")
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = self.root.stat()
        if info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise WorkerError("state_dir must be owned by this user with mode 0700")
        profiles = self.config["profiles"]
        if not isinstance(profiles, dict):
            raise ValidationError("profiles must be an object")
        for name, profile in profiles.items():
            _identifier(name, "profile")
            _object(profile, {"argv", "runtime", "timeout_seconds"}, "profile")
            argv = profile["argv"]
            if (not isinstance(argv, list) or not argv or len(argv) > 128
                    or any(not isinstance(arg, str) or "\0" in arg for arg in argv)
                    or not Path(argv[0]).is_absolute()):
                raise ValidationError("profile argv must start with an absolute executable")
            if not isinstance(profile["runtime"], str) or not re.fullmatch(r"python3\.[0-9]+", profile["runtime"]):
                raise ValidationError("profile runtime must be python3.<minor>")
            _number(profile["timeout_seconds"], "timeout_seconds", positive=True)
        self.attempts = self.root / "attempts"
        self.attempts.mkdir(mode=0o700, exist_ok=True)
        with self._lock():
            path = self.root / "identity.json"
            identity = {key: self.config[key] for key in ("host_id", "worker_id")}
            if path.exists() and _read_json(path) != identity:
                raise IdentityError("state directory belongs to another worker identity")
            if not path.exists():
                _atomic_json(path, identity)

    @contextmanager
    def _lock(self):
        with (self.root / "metadata.lock").open("a+b") as stream:
            fcntl.flock(stream, fcntl.LOCK_EX)
            yield

    def _path(self, attempt_id):
        _identifier(attempt_id, "attempt_id")
        return self.attempts / attempt_id

    def _load(self, attempt_id):
        path = self._path(attempt_id) / "state.json"
        if not path.exists():
            raise NotFoundError("attempt has not been accepted by this worker")
        return _read_json(path)

    def _save(self, state):
        _atomic_json(self._path(state["attempt_id"]) / "state.json", state)

    def _reconcile(self, state):
        if state["state"] != "running":
            return state
        with (self._path(state["attempt_id"]) / "supervisor.lock").open("a+b") as stream:
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return state
        state["state"] = "uncertain"
        state["detail"] = "supervisor exited without confirmed process-group cleanup"
        self._save(state)
        return state

    def _unresolved(self):
        unresolved = []
        for path in self.attempts.iterdir():
            if path.is_dir():
                state = self._reconcile(self._load(path.name))
                if state["state"] not in TERMINAL:
                    unresolved.append(state["attempt_id"])
        return unresolved

    def _runtime_profiles(self):
        valid = {}
        for name, profile in self.config["profiles"].items():
            try:
                result = subprocess.run([profile["argv"][0], "-I", "-c",
                                         "import sys; print('python%d.%d' % sys.version_info[:2])"],
                                        capture_output=True, timeout=5, check=True,
                                        env=_environment(self.root), cwd=self.root)
                actual = result.stdout.decode("ascii").strip()
                if actual == profile["runtime"]:
                    valid[name] = profile
            except (OSError, subprocess.SubprocessError, UnicodeError):
                continue
        return valid

    def _memory(self):
        try:
            if sys.platform == "linux":
                values = dict(re.findall(r"^(\w+):\s+(\d+) kB$", Path("/proc/meminfo").read_text(), re.M))
                available = int(values["MemAvailable"]) * 1024
            elif sys.platform == "darwin":
                pressure = subprocess.run(["/usr/sbin/sysctl", "-n", "kern.memorystatus_vm_pressure_level"],
                                          capture_output=True, timeout=5, check=True)
                if int(pressure.stdout) != 1:
                    return 0.0
                result = subprocess.run(["/usr/bin/vm_stat"], capture_output=True, timeout=5, check=True)
                output = result.stdout.decode("ascii")
                page_size = int(re.search(r"page size of (\d+) bytes", output).group(1))
                pages = [int(re.search(rf"^{label}:\s+(\d+)\.", output, re.M).group(1))
                         for label in ("Pages free", "Pages inactive", "Pages speculative")]
                available = sum(pages) * page_size
            else:
                raise WorkerError("worker requires Linux or macOS")
        except (OSError, subprocess.SubprocessError, ValueError, KeyError, AttributeError, UnicodeError) as error:
            raise WorkerError("cannot measure available memory") from error
        return max(0.0, available / 1_000_000 - self.config["reserve_memory_mb"])

    def probe(self):
        profiles = self._runtime_profiles()
        error = None
        try:
            memory = self._memory()
        except WorkerError as failure:
            memory, error = 0.0, str(failure)
        with self._lock():
            sequence_path = self.root / "sequence.json"
            sequence = _read_json(sequence_path) + 1 if sequence_path.exists() else 1
            if sequence > 2**63 - 1:
                raise WorkerError("snapshot sequence exhausted")
            _atomic_json(sequence_path, sequence)
            unresolved = self._unresolved()
        response = {"schema_version": 1, "host_id": self.config["host_id"], "worker_id": self.config["worker_id"],
                    "snapshot": {"schema_version": 1, "sequence": sequence, "os": platform.system().lower(),
                                 "arch": platform.machine(), "runtimes": sorted({p["runtime"] for p in profiles.values()}),
                                 "profiles": sorted(profiles), "available_memory_mb": memory,
                                 "ready": bool(profiles) and memory > 0 and not unresolved,
                                 "allow_guests": self.config["allow_guests"], "pressure": memory <= 0},
                    "unresolved_attempt_ids": unresolved}
        if error:
            response["probe_error"] = error
        return response

    def _response(self, state):
        response = {key: state[key] for key in ("attempt_id", "state", "result")}
        response["schema_version"] = 1
        for name in ("stdout", "stderr"):
            path = self._path(state["attempt_id"]) / (name + ".log")
            try:
                with path.open("rb") as stream:
                    stream.seek(0, os.SEEK_END)
                    stream.seek(max(0, stream.tell() - TAIL_BYTES))
                    response[name + "_tail"] = stream.read(TAIL_BYTES).decode("utf-8", errors="replace")
            except FileNotFoundError:
                response[name + "_tail"] = ""
        for key in ("detail", "supervisor_pid", "process_group"):
            if key in state:
                response[key] = state[key]
        return response

    def status(self, attempt_id):
        with self._lock():
            return self._response(self._reconcile(self._load(attempt_id)))

    def cancel(self, attempt_id):
        with self._lock():
            state = self._reconcile(self._load(attempt_id))
            if state["state"] == "running":
                state["cancel_requested"] = True
                self._save(state)
            return self._response(state)

    def start(self, assignment, manifest, stream):
        identity, canceled = _assignment(assignment, self.config)
        manifest = _manifest(manifest, identity["job"]["revision"])
        attempt_id = identity["attempt_id"]
        with self._lock():
            path = self._path(attempt_id)
            if path.exists():
                state = self._load(attempt_id)
                if state["identity"] != identity or state["manifest"] != manifest:
                    raise ConflictError("attempt was already accepted with different identity or source")
                if canceled and state["state"] == "running":
                    state["cancel_requested"] = True
                    self._save(state)
                return self._response(self._reconcile(state))
            if self._unresolved():
                raise ConflictError("worker has an unresolved reserved attempt")
            slot = (self.root / "slot.lock").open("a+b")
            lease = None
            try:
                try:
                    fcntl.flock(slot, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError as error:
                    raise ConflictError("worker execution slot is occupied") from error
                profile = self._runtime_profiles().get(identity["job"]["profile"]) if not canceled else None
                requirements = identity["job"]["requirements"]
                if not canceled and (profile is None or requirements["runtime"] != profile["runtime"]):
                    raise WorkerError("assigned profile runtime is unavailable")
                if not canceled and (requirements["os"] != platform.system().lower() or requirements["arch"] != platform.machine()):
                    raise WorkerError("assigned platform does not match worker")
                if not canceled and self._memory() < requirements["memory_mb"]:
                    raise WorkerError("insufficient worker memory headroom")
                path.mkdir(mode=0o700)
                state = {"attempt_id": attempt_id, "identity": identity, "manifest": manifest,
                         "profile": profile, "state": "running", "result": None,
                         "cancel_requested": canceled, "phase": "receiving"}
                self._save(state)
                if canceled:
                    self._terminal(state, "canceled", None, "Canceled before execution")
                    return self._response(state)
                archive = path / "source.tar"
                try:
                    digest = hashlib.sha256()
                    remaining = manifest["archive_size_bytes"]
                    with archive.open("xb") as output:
                        while remaining:
                            chunk = stream.read(min(64 * 1024, remaining))
                            if not chunk:
                                raise WorkerError("source upload ended before declared archive size")
                            output.write(chunk)
                            digest.update(chunk)
                            remaining -= len(chunk)
                        output.flush()
                        os.fsync(output.fileno())
                    if digest.hexdigest() != manifest["archive_sha256"]:
                        raise WorkerError("source archive SHA-256 mismatch")
                    _extract(archive, path, manifest)
                    archive.unlink()
                except (WorkerError, OSError) as error:
                    shutil.rmtree(path / "source", ignore_errors=True)
                    archive.unlink(missing_ok=True)
                    self._terminal(state, "failed", None, str(error))
                    return self._response(state)
                lease = (path / "supervisor.lock").open("a+b")
                fcntl.flock(lease, fcntl.LOCK_EX)
                state["phase"] = "prepared"
                self._save(state)
                try:
                    process = subprocess.Popen([sys.executable, "-m", "sparemesh.worker", "--config",
                                                str(self.config_path), "_supervise", attempt_id,
                                                str(slot.fileno()), str(lease.fileno())],
                                               cwd=Path(__file__).resolve().parent.parent,
                                               env=_environment(self.root), stdin=subprocess.DEVNULL,
                                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                               start_new_session=True, close_fds=True,
                                               pass_fds=(slot.fileno(), lease.fileno()))
                    state["supervisor_pid"] = process.pid
                    self._save(state)
                    # Library callers can remain alive after the RPC would exit.
                    # Reap our detached supervisor without tying its lifetime to us.
                    threading.Thread(target=process.wait, daemon=True).start()
                except OSError as error:
                    self._terminal(state, "failed", None, f"cannot launch supervisor: {error}")
                return self._response(state)
            finally:
                if lease is not None:
                    lease.close()
                slot.close()

    def _terminal(self, state, outcome, exit_code, summary):
        state.update(state=outcome, phase="complete",
                     result={"schema_version": 1, "outcome": outcome, "exit_code": exit_code,
                             "summary": summary[:4096]})
        self._save(state)


def _group_exists(group):
    try:
        os.killpg(group, 0)
        return True
    except ProcessLookupError:
        return False


def _subreaper():
    if sys.platform == "linux":
        # Reap orphaned group descendants even on minimal Linux PID 1 systems.
        library = ctypes.CDLL(None, use_errno=True)
        if library.prctl(36, 1, 0, 0, 0) != 0:
            raise WorkerError("cannot enable Linux child subreaper")


def _reap_group(group):
    while True:
        try:
            pid, _ = os.waitpid(-group, os.WNOHANG)
            if pid == 0:
                return
        except ChildProcessError:
            return


def _stop_group(process):
    """Best-effort cleanup on supervisor errors, preserving uncertainty on failure."""
    started = time.monotonic()
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    while time.monotonic() - started < 6:
        returncode = process.poll()
        if returncode is not None:
            _reap_group(process.pid)
        if returncode is not None and not _group_exists(process.pid):
            return True
        if time.monotonic() - started >= 1:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        time.sleep(0.05)
    return False


def _supervise(worker, attempt_id, slot_fd, lease_fd):
    # Inherited open-file-description locks survive the RPC parent's exit.
    os.fstat(slot_fd)
    os.fstat(lease_fd)
    _subreaper()
    path = worker._path(attempt_id)
    process = None
    buffers = {"stdout": bytearray(), "stderr": bytearray()}
    selector = selectors.DefaultSelector()
    reason = None
    group = None
    termination_at = None
    last_flush = 0.0
    try:
        with worker._lock():
            state = worker._load(attempt_id)
            if state.get("supervisor_pid") != os.getpid() or state["state"] != "running":
                return
            if state["cancel_requested"]:
                worker._terminal(state, "canceled", None, "Canceled before execution")
                return
            home = path / "home"
            home.mkdir(mode=0o700)
            process = subprocess.Popen(state["profile"]["argv"], cwd=path / "source",
                                       env=_environment(home), stdin=subprocess.DEVNULL,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       start_new_session=True, close_fds=True)
            group = process.pid
            state.update(phase="executing", process_group=group)
            worker._save(state)
        for name in buffers:
            stream = getattr(process, name)
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ, name)
        deadline = time.monotonic() + state["profile"]["timeout_seconds"]
        while True:
            now = time.monotonic()
            returncode = process.poll()
            if returncode is not None:
                _reap_group(group)
            if reason is None:
                if returncode is not None:
                    reason = "exited"
                else:
                    with worker._lock():
                        canceled = worker._load(attempt_id)["cancel_requested"]
                    if canceled:
                        reason = "canceled"
                    elif now >= deadline:
                        reason = "timeout"
                if reason is not None:
                    termination_at = now
                    try:
                        os.killpg(group, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
            if termination_at is not None and now - termination_at >= 1:
                try:
                    os.killpg(group, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            for key, _ in selector.select(0.05):
                chunk = os.read(key.fd, 64 * 1024)
                if chunk:
                    buffers[key.data].extend(chunk)
                    del buffers[key.data][:-TAIL_BYTES]
                else:
                    selector.unregister(key.fileobj)
                    key.fileobj.close()
            if now - last_flush >= 0.25:
                for name, data in buffers.items():
                    (path / (name + ".log")).write_bytes(data)
                last_flush = now
            if returncode is not None and not _group_exists(group) and not selector.get_map():
                break
            if termination_at is not None and now - termination_at > 6:
                raise WorkerError("process group cleanup could not be confirmed")
        for name, data in buffers.items():
            with (path / (name + ".log")).open("wb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
        with worker._lock():
            state = worker._load(attempt_id)
            if reason == "canceled":
                worker._terminal(state, "canceled", returncode, "Canceled and confirmed process group exited")
            elif reason == "timeout":
                worker._terminal(state, "failed", returncode or None, "Profile timed out and process group exited")
            else:
                outcome = "succeeded" if returncode == 0 else "failed"
                worker._terminal(state, outcome, returncode, f"Profile exited with code {returncode}; process group exited")
    except BaseException as error:
        cleaned = False
        if process is not None:
            try:
                cleaned = _stop_group(process)
            except OSError:
                pass
        with worker._lock():
            state = worker._load(attempt_id)
            if process is None:
                worker._terminal(state, "failed", None, f"Profile launch failed: {error}")
            elif cleaned:
                worker._terminal(state, "failed", process.returncode or None,
                                 f"Supervisor failed and process group exited: {error}")
            else:
                state.update(state="uncertain", detail=f"Supervisor failure: {error}")
                worker._save(state)
    finally:
        if process is not None:
            for name in ("stdout", "stderr"):
                stream = getattr(process, name)
                if stream is not None:
                    stream.close()
        selector.close()
        os.close(lease_fd)
        os.close(slot_fd)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("command", choices=("rpc", "_supervise"))
    parser.add_argument("internal", nargs="*")
    args = parser.parse_args(argv)
    try:
        worker = Worker(args.config)
        if args.command == "_supervise":
            if len(args.internal) != 3:
                raise ValidationError("invalid internal supervisor arguments")
            _supervise(worker, args.internal[0], int(args.internal[1]), int(args.internal[2]))
            return 0
        header = sys.stdin.buffer.readline(MAX_HEADER_BYTES + 1)
        if len(header) > MAX_HEADER_BYTES or not header.endswith(b"\n"):
            raise ValidationError("RPC header must be one bounded JSON line")
        request = _decode(header)
        if not isinstance(request, dict):
            raise ValidationError("RPC request must be an object")
        operation = request.get("op")
        if operation == "probe":
            _object(request, {"op"}, "probe request")
            response = worker.probe()
        elif operation == "start":
            _object(request, {"op", "assignment", "manifest"}, "start request")
            response = worker.start(request["assignment"], request["manifest"], sys.stdin.buffer)
        elif operation in ("status", "cancel"):
            _object(request, {"op", "attempt_id"}, "attempt request")
            response = getattr(worker, operation)(request["attempt_id"])
        else:
            raise ValidationError("unknown worker operation")
        print(json.dumps(response, sort_keys=True, allow_nan=False), flush=True)
        return 0
    except BrokenPipeError:
        return 1
    except (CoordinatorError, OSError, ValueError, TypeError) as error:
        print(json.dumps({"error": {"code": type(error).__name__, "message": str(error)}}), flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
