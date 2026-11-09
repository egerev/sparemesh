"""SQLite scheduling state. Workers own execution and process cleanup."""

from collections.abc import Callable, Mapping
from contextlib import contextmanager
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import time
from typing import Any
import uuid


class CoordinatorError(Exception):
    """Expected caller-visible failure."""


class ValidationError(CoordinatorError):
    """An input does not satisfy the v1 contract."""


class ConflictError(CoordinatorError):
    """An immutable identity or previously accepted report conflicts."""


class IdentityError(CoordinatorError):
    """The supplied principal does not own the requested operation."""


class NotFoundError(CoordinatorError):
    """The requested record does not exist."""


def _identifier(value: Any, name: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", value):
        raise ValidationError(f"{name} must be a 1–128 character identifier")
    return value


def _object(value: Any, keys: set[str], name: str) -> Mapping:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise ValidationError(f"{name} must have exactly these fields: {', '.join(sorted(keys))}")
    return value


def _number(value: Any, name: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{name} must be a finite number")
    try:
        number = float(value)
    except OverflowError as error:
        raise ValidationError(f"{name} must be a finite number") from error
    if not math.isfinite(number) or number < 0 or (positive and number == 0):
        raise ValidationError(f"{name} must be finite and {'positive' if positive else 'nonnegative'}")
    return number


def _boolean(value: Any, name: str) -> bool:
    if type(value) is not bool:
        raise ValidationError(f"{name} must be a boolean")
    return value


def _version(value: Any) -> None:
    if type(value) is not int or value != 1:
        raise ValidationError("schema_version must be the integer 1")


def _labels(value: Any, name: str) -> list[str]:
    if not isinstance(value, list):
        raise ValidationError(f"{name} must be an array of identifiers")
    labels = [_identifier(item, name) for item in value]
    if len(labels) != len(set(labels)):
        raise ValidationError(f"{name} must not contain duplicates")
    return sorted(labels)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _requirements(value: Any) -> dict:
    fields = _object(value, {"os", "arch", "runtime", "memory_mb"}, "requirements")
    return {
        "os": _identifier(fields["os"], "os"),
        "arch": _identifier(fields["arch"], "arch"),
        "runtime": _identifier(fields["runtime"], "runtime"),
        "memory_mb": _number(fields["memory_mb"], "memory_mb", positive=True),
    }


def _snapshot(value: Any) -> dict:
    fields = _object(value, {
        "schema_version", "sequence", "os", "arch", "runtimes", "profiles",
        "available_memory_mb", "ready", "allow_guests", "pressure",
    }, "snapshot")
    _version(fields["schema_version"])
    sequence = fields["sequence"]
    if type(sequence) is not int or not 0 <= sequence <= 2**63 - 1:
        raise ValidationError("sequence must be a nonnegative signed 64-bit integer")
    return {
        "schema_version": 1,
        "sequence": sequence,
        "os": _identifier(fields["os"], "os"),
        "arch": _identifier(fields["arch"], "arch"),
        "runtimes": _labels(fields["runtimes"], "runtimes"),
        "profiles": _labels(fields["profiles"], "profiles"),
        "available_memory_mb": _number(fields["available_memory_mb"], "available_memory_mb"),
        "ready": _boolean(fields["ready"], "ready"),
        "allow_guests": _boolean(fields["allow_guests"], "allow_guests"),
        "pressure": _boolean(fields["pressure"], "pressure"),
    }


def _result(value: Any) -> dict:
    fields = _object(value, {"schema_version", "outcome", "exit_code", "summary"}, "result")
    _version(fields["schema_version"])
    outcome = fields["outcome"]
    if not isinstance(outcome, str) or outcome not in {"succeeded", "failed", "canceled"}:
        raise ValidationError("outcome must be succeeded, failed, or canceled")
    exit_code = fields["exit_code"]
    if exit_code is not None and (type(exit_code) is not int or not -(2**31) <= exit_code < 2**31):
        raise ValidationError("exit_code must be null or a signed 32-bit integer")
    if outcome == "succeeded" and exit_code != 0:
        raise ValidationError("a succeeded result requires exit_code 0")
    if outcome == "failed" and exit_code == 0:
        raise ValidationError("a failed result requires a nonzero or null exit_code")
    if not isinstance(fields["summary"], str) or len(fields["summary"]) > 4096:
        raise ValidationError("summary must be a string of at most 4096 characters")
    return dict(fields)


_SCHEMA = """
BEGIN IMMEDIATE;
CREATE TABLE IF NOT EXISTS hosts (
    host_id TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    worker_id TEXT NOT NULL UNIQUE,
    snapshot_json TEXT,
    snapshot_sequence INTEGER,
    received_at REAL,
    guests_paused INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    owner TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('queued', 'assigned', 'cancel_requested', 'succeeded', 'failed', 'canceled')),
    submitted_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS attempts (
    attempt_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL UNIQUE REFERENCES jobs(job_id),
    host_id TEXT NOT NULL REFERENCES hosts(host_id),
    worker_id TEXT NOT NULL,
    assigned_at REAL NOT NULL,
    cancel_reason TEXT,
    result_json TEXT,
    finished_at REAL
);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_attempt_per_host
    ON attempts(host_id) WHERE result_json IS NULL;
PRAGMA user_version = 1;
COMMIT;
"""


class Coordinator:
    """One connection per thread. Independent instances may share a local DB.

    Identity arguments bind records, but do not authenticate a remote caller.
    A transport must supply authenticated identities as described in the contract.
    """

    def __init__(self, database: str | Path, *, clock: Callable[[], float] = time.time,
                 freshness_seconds: float = 30):
        self.clock = clock
        self.freshness_seconds = _number(freshness_seconds, "freshness_seconds", positive=True)
        self._connection = sqlite3.connect(str(database), isolation_level=None, timeout=10)
        self._connection.row_factory = sqlite3.Row
        try:
            self._connection.execute("PRAGMA foreign_keys = ON")
            self._connection.execute("PRAGMA journal_mode = WAL")
            self._connection.execute("PRAGMA synchronous = FULL")
            version = self._connection.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1):
                raise ConflictError(f"unsupported database schema version {version}")
            self._connection.executescript(_SCHEMA)
        except Exception:
            self._connection.close()
            raise

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    @contextmanager
    def _transaction(self, *, write: bool = False):
        if self._connection is None:
            raise CoordinatorError("coordinator is closed")
        self._connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
        try:
            yield self._connection
            self._connection.execute("COMMIT")
        except BaseException:
            self._connection.execute("ROLLBACK")
            raise

    def _now(self) -> float:
        return _number(self.clock(), "clock")

    def _host(self, host_id: str) -> sqlite3.Row:
        row = self._connection.execute("SELECT * FROM hosts WHERE host_id = ?", (host_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"unknown host {host_id}")
        return row

    def _worker(self, host_id: str, worker_id: str) -> sqlite3.Row:
        host = self._host(host_id)
        if host["worker_id"] != worker_id:
            raise IdentityError("worker identity does not match the registered host")
        return host

    def _job(self, job_id: str) -> sqlite3.Row:
        row = self._connection.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"unknown job {job_id}")
        return row

    def _active(self, host_id: str) -> sqlite3.Row | None:
        return self._connection.execute(
            "SELECT * FROM attempts WHERE host_id = ? AND result_json IS NULL", (host_id,),
        ).fetchone()

    def register_host(self, host_id: str, owner: str, worker_id: str) -> dict:
        host_id = _identifier(host_id, "host_id")
        owner = _identifier(owner, "owner")
        worker_id = _identifier(worker_id, "worker_id")
        with self._transaction(write=True) as connection:
            host = connection.execute("SELECT * FROM hosts WHERE host_id = ?", (host_id,)).fetchone()
            if host is not None:
                if (host["owner"], host["worker_id"]) != (owner, worker_id):
                    raise ConflictError("a registered host's owner and worker identity are immutable")
            else:
                other = connection.execute("SELECT host_id FROM hosts WHERE worker_id = ?", (worker_id,)).fetchone()
                if other is not None:
                    raise ConflictError("worker identity is already registered to another host")
                connection.execute("INSERT INTO hosts (host_id, owner, worker_id) VALUES (?, ?, ?)",
                                   (host_id, owner, worker_id))
            return self._host_status(self._host(host_id), self._now())

    def report_host(self, host_id: str, worker_id: str, snapshot: Mapping) -> dict:
        host_id = _identifier(host_id, "host_id")
        worker_id = _identifier(worker_id, "worker_id")
        report = _snapshot(snapshot)
        encoded = _json(report)
        with self._transaction(write=True) as connection:
            host = self._worker(host_id, worker_id)
            now = self._now()
            if host["snapshot_sequence"] is not None and report["sequence"] <= host["snapshot_sequence"]:
                if report["sequence"] != host["snapshot_sequence"] or encoded != host["snapshot_json"]:
                    raise ConflictError("snapshot sequence is old or conflicts with its accepted payload")
                return self._host_status(host, now)
            connection.execute(
                "UPDATE hosts SET snapshot_json = ?, snapshot_sequence = ?, received_at = ? WHERE host_id = ?",
                (encoded, report["sequence"], now, host_id),
            )
            if report["pressure"]:
                self._request_guest_cancel(host, "host_pressure")
            return self._host_status(self._host(host_id), now)

    def submit(self, job_id: str, owner: str, profile: str, revision: str,
               requirements: Mapping, restartable: bool = False) -> dict:
        job_id = _identifier(job_id, "job_id")
        owner = _identifier(owner, "owner")
        if not isinstance(revision, str) or not re.fullmatch(r"[0-9a-fA-F]{40}|[0-9a-fA-F]{64}", revision):
            raise ValidationError("revision must be a full 40- or 64-character hexadecimal commit ID")
        payload = {
            "schema_version": 1,
            "job_id": job_id,
            "owner": owner,
            "profile": _identifier(profile, "profile"),
            "revision": revision.lower(),
            "requirements": _requirements(requirements),
            "restartable": _boolean(restartable, "restartable"),
        }
        encoded = _json(payload)
        digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        with self._transaction(write=True) as connection:
            existing = connection.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            if existing is not None:
                if existing["payload_hash"] != digest:
                    raise ConflictError("job_id was already submitted with a different immutable payload")
            else:
                connection.execute(
                    "INSERT INTO jobs (job_id, payload_json, payload_hash, owner, state, submitted_at) "
                    "VALUES (?, ?, ?, ?, 'queued', ?)", (job_id, encoded, digest, owner, self._now()),
                )
            return self._job_status(self._job(job_id), self._now())

    def _hosts(self) -> list[sqlite3.Row]:
        return self._connection.execute("SELECT * FROM hosts ORDER BY host_id").fetchall()

    def _queued(self) -> list[sqlite3.Row]:
        return self._connection.execute("SELECT * FROM jobs WHERE state = 'queued' ORDER BY rowid").fetchall()

    def _reasons(self, host: sqlite3.Row, payload: dict, now: float) -> list[str]:
        reasons = []
        if self._active(host["host_id"]) is not None:
            reasons.append("occupied")
        if host["snapshot_json"] is None:
            reasons.append("no_snapshot")
            return reasons
        age = now - host["received_at"]
        if age < 0 or age > self.freshness_seconds:
            reasons.append("stale")
        snapshot = json.loads(host["snapshot_json"])
        if not snapshot["ready"]:
            reasons.append("not_ready")
        if snapshot["pressure"]:
            reasons.append("pressure")
        if payload["owner"] != host["owner"]:
            if not snapshot["allow_guests"]:
                reasons.append("guests_disabled")
            if host["guests_paused"]:
                reasons.append("guests_paused")
        requirements = payload["requirements"]
        if snapshot["os"] != requirements["os"]:
            reasons.append("os_mismatch")
        if snapshot["arch"] != requirements["arch"]:
            reasons.append("arch_mismatch")
        if requirements["runtime"] not in snapshot["runtimes"]:
            reasons.append("runtime_missing")
        if payload["profile"] not in snapshot["profiles"]:
            reasons.append("profile_not_allowed")
        if requirements["memory_mb"] > snapshot["available_memory_mb"]:
            reasons.append("insufficient_memory")
        return reasons

    def _guest_reasons(self, host: sqlite3.Row, payload: dict, hosts: list, queued: list, now: float) -> list[str]:
        if host["owner"] == payload["owner"]:
            return []
        reasons = []
        for owner_job in queued:
            if owner_job["owner"] == host["owner"] and not self._reasons(host, json.loads(owner_job["payload_json"]), now):
                reasons.append("owner_priority")
                break
        for own_host in hosts:
            if own_host["owner"] == payload["owner"] and not self._reasons(own_host, payload, now):
                reasons.append("own_host_preferred")
                break
        return reasons

    def claim(self, host_id: str, worker_id: str) -> dict | None:
        host_id = _identifier(host_id, "host_id")
        worker_id = _identifier(worker_id, "worker_id")
        with self._transaction(write=True) as connection:
            host = self._worker(host_id, worker_id)
            active = self._active(host_id)
            if active is not None:
                return self._assignment(active)
            now = self._now()
            queued = self._queued()
            hosts = self._hosts()
            # Stable sort preserves FIFO within the owner's and guests' queues.
            candidates = sorted(queued, key=lambda job: job["owner"] != host["owner"])
            for job in candidates:
                payload = json.loads(job["payload_json"])
                if self._reasons(host, payload, now) or self._guest_reasons(host, payload, hosts, queued, now):
                    continue
                attempt_id = "attempt-" + uuid.uuid4().hex
                connection.execute(
                    "INSERT INTO attempts (attempt_id, job_id, host_id, worker_id, assigned_at) VALUES (?, ?, ?, ?, ?)",
                    (attempt_id, job["job_id"], host_id, worker_id, now),
                )
                connection.execute("UPDATE jobs SET state = 'assigned' WHERE job_id = ?", (job["job_id"],))
                return self._assignment(self._active(host_id))
            return None

    def _assignment(self, attempt: sqlite3.Row) -> dict:
        job = self._job(attempt["job_id"])
        return {
            "schema_version": 1,
            "attempt_id": attempt["attempt_id"],
            "host_id": attempt["host_id"],
            "worker_id": attempt["worker_id"],
            "job": json.loads(job["payload_json"]),
            "state": job["state"],
            "assigned_at": attempt["assigned_at"],
            "cancel_requested": attempt["cancel_reason"] is not None,
            "cancel_reason": attempt["cancel_reason"],
            "result": json.loads(attempt["result_json"]) if attempt["result_json"] is not None else None,
            "finished_at": attempt["finished_at"],
        }

    def _job_status(self, job: sqlite3.Row, now: float) -> dict:
        value = json.loads(job["payload_json"])
        value.update(state=job["state"], submitted_at=job["submitted_at"], payload_hash=job["payload_hash"])
        attempt = self._connection.execute("SELECT * FROM attempts WHERE job_id = ?", (job["job_id"],)).fetchone()
        value["attempt"] = self._assignment(attempt) if attempt is not None else None
        value["waiting"] = []
        if job["state"] == "queued":
            hosts, queued = self._hosts(), self._queued()
            for host in hosts:
                reasons = self._reasons(host, value, now) + self._guest_reasons(host, value, hosts, queued, now)
                value["waiting"].append({"host_id": host["host_id"], "reasons": reasons or ["awaiting_claim"]})
            if not hosts:
                value["waiting"].append({"host_id": None, "reasons": ["no_hosts"]})
        return value

    def status(self, job_id: str) -> dict:
        job_id = _identifier(job_id, "job_id")
        with self._transaction():
            return self._job_status(self._job(job_id), self._now())

    def jobs(self, owner: str | None = None) -> list[dict]:
        if owner is not None:
            owner = _identifier(owner, "owner")
        with self._transaction() as connection:
            rows = connection.execute("SELECT * FROM jobs ORDER BY rowid").fetchall()
            now = self._now()
            return [self._job_status(job, now) for job in rows if owner is None or job["owner"] == owner]

    def _host_status(self, host: sqlite3.Row, now: float) -> dict:
        active = self._active(host["host_id"])
        age = now - host["received_at"] if host["received_at"] is not None else None
        return {
            "schema_version": 1,
            "host_id": host["host_id"],
            "owner": host["owner"],
            "worker_id": host["worker_id"],
            "snapshot": json.loads(host["snapshot_json"]) if host["snapshot_json"] is not None else None,
            "received_at": host["received_at"],
            "fresh": age is not None and 0 <= age <= self.freshness_seconds,
            "guests_paused": bool(host["guests_paused"]),
            "active_attempt_id": active["attempt_id"] if active is not None else None,
        }

    def hosts(self) -> list[dict]:
        with self._transaction():
            now = self._now()
            return [self._host_status(host, now) for host in self._hosts()]

    def _request_cancel(self, attempt: sqlite3.Row, reason: str) -> None:
        self._connection.execute(
            "UPDATE attempts SET cancel_reason = COALESCE(cancel_reason, ?) WHERE attempt_id = ?", (reason, attempt["attempt_id"]),
        )
        self._connection.execute("UPDATE jobs SET state = 'cancel_requested' WHERE job_id = ?", (attempt["job_id"],))

    def cancel(self, job_id: str, owner: str) -> dict:
        job_id = _identifier(job_id, "job_id")
        owner = _identifier(owner, "owner")
        with self._transaction(write=True) as connection:
            job = self._job(job_id)
            if job["owner"] != owner:
                raise IdentityError("only the job owner may cancel it")
            if job["state"] == "queued":
                connection.execute("UPDATE jobs SET state = 'canceled' WHERE job_id = ?", (job_id,))
            elif job["state"] in {"assigned", "cancel_requested"}:
                attempt = connection.execute("SELECT * FROM attempts WHERE job_id = ?", (job_id,)).fetchone()
                self._request_cancel(attempt, "user_requested")
            return self._job_status(self._job(job_id), self._now())

    def _request_guest_cancel(self, host: sqlite3.Row, reason: str) -> None:
        attempt = self._active(host["host_id"])
        if attempt is None:
            return
        payload = json.loads(self._job(attempt["job_id"])["payload_json"])
        if payload["owner"] != host["owner"] and payload["restartable"]:
            self._request_cancel(attempt, reason)

    def pause_guests(self, host_id: str, owner: str, *, reclaim: bool = False) -> dict:
        return self._set_guests_paused(host_id, owner, True, _boolean(reclaim, "reclaim"))

    def resume_guests(self, host_id: str, owner: str) -> dict:
        return self._set_guests_paused(host_id, owner, False, False)

    def _set_guests_paused(self, host_id: str, owner: str, paused: bool, reclaim: bool) -> dict:
        host_id = _identifier(host_id, "host_id")
        owner = _identifier(owner, "owner")
        with self._transaction(write=True) as connection:
            host = self._host(host_id)
            if host["owner"] != owner:
                raise IdentityError("only the host owner may change guest intake")
            connection.execute("UPDATE hosts SET guests_paused = ? WHERE host_id = ?", (int(paused), host_id))
            if reclaim:
                self._request_guest_cancel(host, "owner_reclaim")
            return self._host_status(self._host(host_id), self._now())

    def finish(self, host_id: str, worker_id: str, attempt_id: str, result: Mapping) -> dict:
        host_id = _identifier(host_id, "host_id")
        worker_id = _identifier(worker_id, "worker_id")
        attempt_id = _identifier(attempt_id, "attempt_id")
        report = _result(result)
        encoded = _json(report)
        with self._transaction(write=True) as connection:
            self._worker(host_id, worker_id)
            attempt = connection.execute("SELECT * FROM attempts WHERE attempt_id = ?", (attempt_id,)).fetchone()
            if attempt is None:
                raise NotFoundError(f"unknown attempt {attempt_id}")
            if (attempt["host_id"], attempt["worker_id"]) != (host_id, worker_id):
                raise IdentityError("completion identity does not match the assigned attempt")
            if attempt["result_json"] is not None:
                if attempt["result_json"] != encoded:
                    raise ConflictError("attempt already has a different terminal result")
                return self._job_status(self._job(attempt["job_id"]), self._now())
            connection.execute("UPDATE attempts SET result_json = ?, finished_at = ? WHERE attempt_id = ?",
                               (encoded, self._now(), attempt_id))
            connection.execute("UPDATE jobs SET state = ? WHERE job_id = ?", (report["outcome"], attempt["job_id"]))
            return self._job_status(self._job(attempt["job_id"]), self._now())
