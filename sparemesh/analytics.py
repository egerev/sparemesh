"""Local, bounded host observations and durable attempt accounting.

Analytics use a dedicated SQLite file. Each thread must open its own store.
Host values are sampled observations. Attempt CPU is a sampled cumulative
estimate, and cross-day allocation assumes uniform use over known execution.
"""

from collections.abc import Callable, Mapping
from contextlib import contextmanager
from datetime import date as Date, datetime, time as Time, timedelta
from pathlib import Path
import sqlite3
import time
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .coordinator import ConflictError, ValidationError, _boolean, _identifier, _number, _version


_APPLICATION_ID = 0x5350414E
_TERMINAL = {"succeeded", "failed", "canceled"}
_STATES = ("assigned", "running", "uncertain", "cancel_requested", "succeeded", "failed", "canceled")
_STATE_RANK = {"assigned": 0, "running": 1, "uncertain": 1, "cancel_requested": 2,
               "succeeded": 3, "failed": 3, "canceled": 3}
_PRESSURES = ("normal", "warning", "critical", "unknown")
_SAMPLE_FIELDS = {
    "schema_version", "sample_id", "host_id", "sampled_at", "cpu_percent", "memory_total_mb",
    "memory_available_mb", "memory_pressure", "swap_used_mb", "guest_eligible", "active_attempt_id",
}
_ATTEMPT_FIELDS = {
    "schema_version", "attempt_id", "job_id", "host_id", "job_owner", "host_owner", "assigned_at",
    "started_at", "finished_at", "state", "cpu_seconds", "peak_rss_mb", "usage_sample_count",
}


def _fields(value: Mapping, required: set[str], allowed: set[str], name: str) -> dict:
    if not isinstance(value, Mapping) or not required <= value.keys() or value.keys() - allowed:
        raise ValidationError(f"{name} requires supported fields including {', '.join(sorted(required))}")
    _version(value["schema_version"])
    return dict(value)


def _optional_number(value, name: str):
    return None if value is None else _number(value, name)


def _optional_identifier(value, name: str):
    return None if value is None else _identifier(value, name)


def _sample(value: Mapping) -> dict:
    value = _fields(value, {"schema_version", "sample_id", "host_id", "sampled_at"}, _SAMPLE_FIELDS, "sample")
    result = {"sample_id": _identifier(value["sample_id"], "sample_id"),
              "host_id": _identifier(value["host_id"], "host_id"),
              "sampled_at": _number(value["sampled_at"], "sampled_at")}
    for key in ("cpu_percent", "memory_total_mb", "memory_available_mb", "swap_used_mb"):
        result[key] = _optional_number(value.get(key), key)
    if result["cpu_percent"] is not None and result["cpu_percent"] > 100:
        raise ValidationError("host cpu_percent must be at most 100")
    if (result["memory_total_mb"] is not None and result["memory_available_mb"] is not None
            and result["memory_available_mb"] > result["memory_total_mb"]):
        raise ValidationError("memory_available_mb must not exceed memory_total_mb")
    pressure = value.get("memory_pressure", "unknown")
    if not isinstance(pressure, str) or pressure not in _PRESSURES:
        raise ValidationError("memory_pressure must be normal, warning, critical, or unknown")
    result["memory_pressure"] = pressure
    eligible = value.get("guest_eligible")
    result["guest_eligible"] = None if eligible is None else _boolean(eligible, "guest_eligible")
    result["active_attempt_id"] = _optional_identifier(value.get("active_attempt_id"), "active_attempt_id")
    return result


def _attempt(value: Mapping) -> dict:
    required = {"schema_version", "attempt_id", "job_id", "host_id", "job_owner", "assigned_at", "state"}
    value = _fields(value, required, _ATTEMPT_FIELDS, "attempt")
    result = {key: _identifier(value[key], key) for key in ("attempt_id", "job_id", "host_id", "job_owner")}
    result["host_owner"] = _optional_identifier(value.get("host_owner"), "host_owner")
    result["assigned_at"] = _number(value["assigned_at"], "assigned_at")
    for key in ("started_at", "finished_at", "cpu_seconds", "peak_rss_mb"):
        result[key] = _optional_number(value.get(key), key)
    state = value["state"]
    if not isinstance(state, str) or state not in _STATES:
        raise ValidationError("attempt state is unsupported")
    result["state"] = state
    count = value.get("usage_sample_count", 0)
    if type(count) is not int or not 0 <= count <= 2**63 - 1:
        raise ValidationError("usage_sample_count must be a nonnegative signed 64-bit integer")
    result["usage_sample_count"] = count
    _check_times(result)
    return result


def _check_times(value: dict) -> None:
    started = value["started_at"]
    finished = value["finished_at"]
    if started is not None and started < value["assigned_at"]:
        raise ValidationError("started_at must not precede assigned_at")
    if finished is not None and finished < (started if started is not None else value["assigned_at"]):
        raise ValidationError("finished_at must not precede execution or assignment")
    if finished is not None and value["state"] not in _TERMINAL:
        raise ValidationError("finished_at requires a terminal attempt state")


def _maximum(values: list):
    return max(values) if values else None


def _workload() -> dict:
    return {"attempt_count": 0, "started_count": 0, "completed_count": 0,
            "state_counts": dict.fromkeys(_STATES, 0), "execution_seconds_estimate": None,
            "execution_measured_attempt_count": 0, "execution_unknown_attempt_count": 0,
            "unfinished_attempt_count": 0, "cpu_seconds_estimate": None, "cpu_measured_attempt_count": 0,
            "peak_rss_mb_max": None, "peak_rss_measured_attempt_count": 0}


class AnalyticsStore:
    """One connection per thread, with concurrent local readers/writers supported.

    Samples default to 30 days of retention plus one boundary sample per host.
    Attempts are retained. Repeated sample IDs are immutable, while attempt
    snapshots enrich existing records without regressing counters or state.
    """

    def __init__(self, database: str | Path, *, clock: Callable[[], float] = time.time,
                 max_sample_gap_seconds: float = 60, retention_days: int = 30,
                 sqlite_timeout: float = 5):
        self.clock = clock
        self.max_sample_gap_seconds = _number(max_sample_gap_seconds, "max_sample_gap_seconds", positive=True)
        if type(retention_days) is not int or retention_days < 1:
            raise ValidationError("retention_days must be a positive integer")
        self.retention_days = retention_days
        timeout = _number(sqlite_timeout, "sqlite_timeout")
        if str(database) != ":memory:":
            Path(database).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(database, timeout=timeout, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        try:
            self.connection.execute(f"PRAGMA busy_timeout = {int(timeout * 1000)}")
            with self._transaction():
                application_id = self.connection.execute("PRAGMA application_id").fetchone()[0]
                version = self.connection.execute("PRAGMA user_version").fetchone()[0]
                tables = self.connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
                if application_id != _APPLICATION_ID and (application_id or version or tables):
                    raise ValidationError("analytics requires a dedicated analytics database")
                if version not in (0, 1):
                    raise ValidationError("unsupported analytics database schema version")
                self.connection.execute("""CREATE TABLE IF NOT EXISTS host_samples (
                    sample_id TEXT PRIMARY KEY, host_id TEXT NOT NULL, sampled_at REAL NOT NULL,
                    cpu_percent REAL, memory_total_mb REAL, memory_available_mb REAL,
                    memory_pressure TEXT NOT NULL, swap_used_mb REAL, guest_eligible INTEGER,
                    active_attempt_id TEXT)""")
                self.connection.execute("""CREATE INDEX IF NOT EXISTS host_samples_time
                    ON host_samples(host_id, sampled_at)""")
                self.connection.execute("CREATE INDEX IF NOT EXISTS host_samples_retention ON host_samples(sampled_at)")
                self.connection.execute("""CREATE TABLE IF NOT EXISTS task_attempts (
                    attempt_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, host_id TEXT NOT NULL,
                    job_owner TEXT NOT NULL, host_owner TEXT, assigned_at REAL NOT NULL,
                    started_at REAL, finished_at REAL, state TEXT NOT NULL, cpu_seconds REAL,
                    peak_rss_mb REAL, usage_sample_count INTEGER NOT NULL, observed_at REAL NOT NULL)""")
                self.connection.execute("CREATE INDEX IF NOT EXISTS task_attempts_host ON task_attempts(host_id)")
                self.connection.execute("CREATE TABLE IF NOT EXISTS analytics_meta (key TEXT PRIMARY KEY, value REAL)")
                self.connection.execute(f"PRAGMA application_id = {_APPLICATION_ID}")
                self.connection.execute("PRAGMA user_version = 1")
            self.connection.execute("PRAGMA journal_mode = WAL")
        except Exception:
            self.connection.close()
            raise

    @contextmanager
    def _transaction(self, *, write: bool = True):
        self.connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
        try:
            yield
        except BaseException:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def record_sample(self, value: Mapping) -> None:
        sample = _sample(value)
        with self._transaction():
            previous = self.connection.execute("SELECT * FROM host_samples WHERE sample_id = ?",
                                               (sample["sample_id"],)).fetchone()
            if previous is not None:
                if dict(previous) != sample:
                    raise ConflictError("sample_id already identifies a different observation")
            else:
                columns = ", ".join(sample)
                placeholders = ", ".join("?" for _ in sample)
                self.connection.execute(f"INSERT INTO host_samples ({columns}) VALUES ({placeholders})", tuple(sample.values()))
            self._prune(self.retention_days, _number(self.clock(), "clock"))

    def record_attempt(self, value: Mapping) -> None:
        attempt = _attempt(value)
        now = _number(self.clock(), "clock")
        incoming_state = attempt["state"]
        with self._transaction():
            previous = self.connection.execute("SELECT * FROM task_attempts WHERE attempt_id = ?",
                                               (attempt["attempt_id"],)).fetchone()
            if previous is not None:
                old = dict(previous)
                for key in ("job_id", "host_id", "job_owner", "assigned_at"):
                    if old[key] != attempt[key]:
                        raise ConflictError(f"attempt {key} is immutable")
                for key in ("host_owner", "started_at", "finished_at"):
                    if old[key] is not None and attempt[key] is not None and old[key] != attempt[key]:
                        raise ConflictError(f"known attempt {key} is immutable")
                    if attempt[key] is None:
                        attempt[key] = old[key]
                if old["state"] in _TERMINAL and attempt["state"] in _TERMINAL and old["state"] != attempt["state"]:
                    raise ConflictError("terminal attempt state is immutable")
                if _STATE_RANK[attempt["state"]] < _STATE_RANK[old["state"]]:
                    attempt["state"] = old["state"]
                for key in ("cpu_seconds", "peak_rss_mb", "usage_sample_count"):
                    known = [item for item in (old[key], attempt[key]) if item is not None]
                    attempt[key] = _maximum(known)
                confirms_execution = incoming_state == "running" and old["state"] not in _TERMINAL
                confirms_finish = incoming_state in _TERMINAL and old["state"] not in _TERMINAL
                now = max(now, old["observed_at"]) if confirms_execution or confirms_finish else old["observed_at"]
            elif incoming_state not in {"running", *_TERMINAL} and attempt["started_at"] is not None:
                now = attempt["started_at"]
            _check_times(attempt)
            attempt["observed_at"] = now
            columns = ", ".join(attempt)
            placeholders = ", ".join("?" for _ in attempt)
            updates = ", ".join(f"{key} = excluded.{key}" for key in attempt if key != "attempt_id")
            self.connection.execute(f"INSERT INTO task_attempts ({columns}) VALUES ({placeholders}) "
                                    f"ON CONFLICT(attempt_id) DO UPDATE SET {updates}", tuple(attempt.values()))

    def _prune(self, days: int, now: float) -> int:
        cutoff = now - days * 86400
        cursor = self.connection.execute("""DELETE FROM host_samples WHERE sampled_at < ? AND sample_id NOT IN (
            SELECT sample_id FROM (
                SELECT sample_id, ROW_NUMBER() OVER (
                    PARTITION BY host_id ORDER BY sampled_at DESC, sample_id DESC) AS position
                FROM host_samples WHERE sampled_at < ?) WHERE position = 1)""", (cutoff, cutoff))
        self.connection.execute("""INSERT INTO analytics_meta (key, value) VALUES ('samples_pruned_before', ?)
            ON CONFLICT(key) DO UPDATE SET value = MAX(value, excluded.value)""", (cutoff,))
        return cursor.rowcount

    def prune(self, days: int | None = None, *, now: float | None = None) -> int:
        """Prune only host samples, preserving one boundary observation per host."""
        days = self.retention_days if days is None else days
        if type(days) is not int or days < 1:
            raise ValidationError("days must be a positive integer")
        instant = _number(self.clock() if now is None else now, "now")
        with self._transaction():
            return self._prune(days, instant)

    def report(self, date: str, timezone: str = "UTC", host_id: str | None = None) -> dict:
        """Return a half-open local calendar day, capped at the current instant."""
        try:
            day = Date.fromisoformat(date)
            if day.isoformat() != date:
                raise ValueError("date must use YYYY-MM-DD")
            zone = ZoneInfo(timezone)
            start = datetime.combine(day, Time.min, zone).timestamp()
            end = datetime.combine(day + timedelta(days=1), Time.min, zone).timestamp()
        except (TypeError, ValueError, OverflowError, ZoneInfoNotFoundError) as error:
            raise ValidationError("report requires a YYYY-MM-DD date and an IANA timezone") from error
        if host_id is not None:
            _identifier(host_id, "host_id")
        observed_until = min(end, max(start, _number(self.clock(), "clock")))
        with self._transaction(write=False):
            samples = [dict(row) for row in self.connection.execute(
                "SELECT * FROM host_samples WHERE sampled_at >= ? AND sampled_at < ? ORDER BY sampled_at, sample_id",
                (start - self.max_sample_gap_seconds, observed_until))]
            attempts = [dict(row) for row in self.connection.execute(
                "SELECT * FROM task_attempts WHERE assigned_at < ? ORDER BY assigned_at, attempt_id", (observed_until,))]
            retained = self.connection.execute(
                "SELECT value FROM analytics_meta WHERE key = 'samples_pruned_before'").fetchone()
        samples_by_host = {}
        for sample in samples:
            if host_id is None or sample["host_id"] == host_id:
                samples_by_host.setdefault(sample["host_id"], []).append(sample)
        attempts_by_host = {}
        for attempt in attempts:
            if host_id is not None and attempt["host_id"] != host_id:
                continue
            execution_end = attempt["finished_at"] if attempt["finished_at"] is not None else attempt["observed_at"]
            overlaps = attempt["started_at"] is not None and attempt["started_at"] < observed_until and execution_end > start
            assigned_today = start <= attempt["assigned_at"] < observed_until
            finished_today = attempt["finished_at"] is not None and start <= attempt["finished_at"] < observed_until
            if overlaps or assigned_today or finished_today:
                attempts_by_host.setdefault(attempt["host_id"], []).append(attempt)
        hosts = []
        for identifier in sorted(samples_by_host.keys() | attempts_by_host.keys()):
            host_samples = samples_by_host.get(identifier, [])
            host = {"host_id": identifier, "observation": self._observations(host_samples, start, observed_until),
                    "workload": {name: _workload() for name in ("own", "guest", "unclassified")}}
            for attempt in attempts_by_host.get(identifier, []):
                owner = attempt["host_owner"]
                category = "unclassified" if owner is None else "own" if owner == attempt["job_owner"] else "guest"
                self._add_attempt(host["workload"][category], attempt, start, observed_until)
            hosts.append(host)
        return {
            "schema_version": 1, "date": date, "timezone": timezone,
            "period": {"start": start, "end": end, "observed_until": observed_until},
            "retention": {"sample_days": self.retention_days,
                          "samples_pruned_before": None if retained is None else retained[0], "attempts": "retained"},
            "methodology": {
                "host_metrics": "Arithmetic summaries of samples timestamped within the day; missing values remain null.",
                "coverage": "Forward sample intervals capped at max_sample_gap_seconds; offline gaps are unobserved.",
                "max_sample_gap_seconds": self.max_sample_gap_seconds,
                "attempt_counts": "Attempts assigned, finished, or with known execution overlapping the day; starts and completions count timestamps within the day.",
                "execution": "Known start to finish, or last recorded observation, clipped to the day and current time; wall-time estimate.",
                "cpu": "Sampled cumulative CPU allocated proportionally across known execution interval, assuming uniform CPU use; per-day estimate.",
                "peak_rss": "Maximum sampled attempt RSS for attempts relevant to the day; peak may have occurred outside the day.",
            }, "hosts": hosts,
        }

    def _observations(self, samples: list[dict], start: float, end: float) -> dict:
        today = [sample for sample in samples if start <= sample["sampled_at"] < end]
        cpus = [sample["cpu_percent"] for sample in today if sample["cpu_percent"] is not None]
        memory = [sample["memory_available_mb"] for sample in today if sample["memory_available_mb"] is not None]
        totals = [sample["memory_total_mb"] for sample in today if sample["memory_total_mb"] is not None]
        swap = [sample["swap_used_mb"] for sample in today if sample["swap_used_mb"] is not None]
        result = {
            "sample_count": len(today), "first_sample_at": today[0]["sampled_at"] if today else None,
            "last_sample_at": today[-1]["sampled_at"] if today else None, "observed_seconds": 0.0,
            "cpu_sample_count": len(cpus), "cpu_percent_mean": sum(cpus) / len(cpus) if cpus else None,
            "cpu_percent_max": _maximum(cpus), "memory_sample_count": len(memory),
            "memory_total_mb_last": totals[-1] if totals else None,
            "memory_available_mb_min": min(memory) if memory else None,
            "memory_pressure_samples": {pressure: sum(sample["memory_pressure"] == pressure for sample in today)
                                        for pressure in _PRESSURES},
            "swap_used_mb_max": _maximum(swap), "guest_eligible_seconds": 0.0,
            "guest_ineligible_seconds": 0.0, "guest_unknown_seconds": 0.0,
        }
        for index, sample in enumerate(samples):
            following = samples[index + 1]["sampled_at"] if index + 1 < len(samples) else end
            interval_end = min(following, sample["sampled_at"] + self.max_sample_gap_seconds, end)
            duration = max(0.0, interval_end - max(start, sample["sampled_at"]))
            result["observed_seconds"] += duration
            eligible = sample["guest_eligible"]
            category = "guest_unknown_seconds" if eligible is None else "guest_eligible_seconds" if eligible else "guest_ineligible_seconds"
            result[category] += duration
        return result

    @staticmethod
    def _add_attempt(group: dict, attempt: dict, start: float, end: float) -> None:
        group["attempt_count"] += 1
        group["state_counts"][attempt["state"]] += 1
        group["unfinished_attempt_count"] += int(attempt["state"] not in _TERMINAL)
        execution_start = attempt["started_at"]
        execution_end = attempt["finished_at"] if attempt["finished_at"] is not None else attempt["observed_at"]
        if execution_start is not None:
            group["started_count"] += int(start <= execution_start < end)
            duration = max(0.0, min(end, execution_end) - max(start, execution_start))
            group["execution_seconds_estimate"] = (group["execution_seconds_estimate"] or 0.0) + duration
            group["execution_measured_attempt_count"] += 1
            total_duration = execution_end - execution_start
            instantaneous = total_duration == 0 and start <= execution_start < end
            if attempt["cpu_seconds"] is not None and (duration > 0 or instantaneous):
                cpu = attempt["cpu_seconds"] if instantaneous else attempt["cpu_seconds"] * duration / total_duration
                group["cpu_seconds_estimate"] = (group["cpu_seconds_estimate"] or 0.0) + cpu
                group["cpu_measured_attempt_count"] += 1
        else:
            group["execution_unknown_attempt_count"] += 1
        if attempt["finished_at"] is not None:
            group["completed_count"] += int(start <= attempt["finished_at"] < end)
        if attempt["peak_rss_mb"] is not None:
            group["peak_rss_mb_max"] = _maximum([value for value in (group["peak_rss_mb_max"], attempt["peak_rss_mb"]) if value is not None])
            group["peak_rss_measured_attempt_count"] += 1
