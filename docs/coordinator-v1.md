# Coordinator contract v1

This draft provides a Python library and local CLI backed by one SQLite database. Every JSON record includes `schema_version` equal to `1`. Database schema version `1` is recorded with SQLite `user_version`. Unknown input fields and unsupported versions are rejected.

The `demo` command uses simulated workers. The [remote pilot](remote-pilot.md) connects this contract to real local and SSH workers. Its operator-controlled configuration binds each transport to a host and worker identity.

## Responsibility boundaries

The coordinator owns durable jobs, logical owner queues, matching, one reserved slot per host, cancellation requests, and terminal reconciliation. SQLite transactions serialize assignments across independent coordinator connections. The active-slot uniqueness constraint also protects the reservation in the database. Keep the database on a local filesystem. Create one `Coordinator` instance per thread.

The worker owns its profile allowlist, repository acquisition, full commit verification, restricted execution account, isolated working directory, fixed command, environment, runtime verification, timeout, resource measurements, logs, artifacts, process-tree lifecycle, and cleanup. A profile name must resolve to an agreed immutable execution policy on each worker. Update that policy by introducing a new profile name. Job payloads carry a profile name and full commit ID. The coordinator executes scheduling transactions only.

An assignment reserves a slot before it is returned. The worker must persist the assignment identity before starting a process, reconcile it after restart, and avoid starting another process for the same attempt. It must report a terminal result only after the entire attempt's process tree has exited and its resources have been reclaimed. A successful result also requires that the worker verified and ran the assigned profile and revision.

### Identity and access

The local CLI assumes trusted operator access to the database. Its `--owner` and `--worker` flags are identity assertions by that operator. A remote adapter must authenticate every mutation and derive its principal from authenticated credentials. For example, it can bind a protected bearer token or SSH principal to a registered worker identity. The adapter must bind the transport identity to the registered application identity.

| Operation | Required authenticated identity in a remote adapter |
| --- | --- |
| Register host | Trusted operator authorizing the host owner and worker pair |
| Submit job, cancel job | Job owner |
| Pause or resume guests | Registered host owner |
| Report snapshot, claim, finish | Registered worker bound to that exact host |
| Read status | Authorized participant, with scope chosen by the deployment |

Host ownership and `worker_id` are immutable after registration. A worker identity belongs to one host. Completion checks both the registered host/worker binding and the durable attempt's binding. A changed worker identity or host owner requires a future explicit recovery operation.

## Job payload

```json
{
  "schema_version": 1,
  "job_id": "alice-smoke-001",
  "owner": "alice",
  "profile": "smoke-tests",
  "revision": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "requirements": {
    "os": "darwin",
    "arch": "arm64",
    "runtime": "python3.11",
    "memory_mb": 512.0
  },
  "restartable": false
}
```

Identifiers match `[A-Za-z0-9][A-Za-z0-9._:-]{0,127}`. OS, architecture, runtime, profile, and owner labels use exact case-sensitive matching. `revision` is a full 40- or 64-character hexadecimal commit ID, normalized to lowercase. Memory uses decimal megabytes, where one MB is 1,000,000 bytes. Job memory must be finite and positive. Booleans must be JSON booleans.

The client chooses a globally unique, durable `job_id`. Submitting it again returns the existing job only if its normalized immutable payload has the same SHA-256 hash. Normalization includes lowercase revisions and floating-point memory values. Changed owners, profiles, revisions, requirements, or restartability conflict even after completion. Canonical JSON uses sorted keys and compact separators.

Each job has at most one attempt in v1. A deliberate retry or another run uses a new job ID. `restartable` permits host-reclaim or pressure-driven cancellation. A canceled restartable job reaches a terminal state. The submitter decides whether to create another job.

## Host snapshots

Register `host_id`, owner, and `worker_id` before reporting this snapshot.

```json
{
  "schema_version": 1,
  "sequence": 1,
  "os": "darwin",
  "arch": "arm64",
  "runtimes": ["python3.11"],
  "profiles": ["smoke-tests"],
  "available_memory_mb": 2048.0,
  "ready": true,
  "allow_guests": true,
  "pressure": false
}
```

The sequence is a nonnegative signed 64-bit integer. The worker must persist it across worker restarts and strictly increase it for each new snapshot. Identical retries of the latest snapshot are accepted without refreshing its receipt timestamp. Conflicting equal sequences and lower sequences are rejected. Runtime and profile arrays contain unique identifiers and are normalized into sorted order.

`received_at` is the coordinator's Unix timestamp in seconds. A snapshot is eligible for 30 seconds by default, configurable through the Python constructor. A future receipt time after a coordinator clock rollback is treated as stale until wall time catches up. `ready` is a worker-owned admission signal. The worker should apply its own pressure thresholds, recovery hysteresis, and headroom margin before declaring readiness. Available memory is finite, nonnegative decimal MB after that margin.

Memory is an admission request. Actual per-process memory enforcement belongs to the worker policy. One slot is reserved per host regardless of its reported memory or readiness. Host snapshots do not overwrite slot reservations.

## Matching and queue order

1. A registered worker's claim first returns that host's existing active attempt, including any cancellation request. This works after reconnect, stale snapshots, or coordinator restart.
2. A fresh, ready, pressure-free host can accept a new job only when its slot is free and it matches the job's OS, architecture, runtime, profile allowlist, and requested memory.
3. Eligible jobs owned by that host's owner have priority. Within the owner's queue, submission order is preserved among eligible jobs. Incompatible jobs can be skipped.
4. New guest work additionally requires `allow_guests` and an open manual guest-intake gate. Guest candidates follow submission order. An eligible free host owned by the job submitter is preferred, so a peer claim waits while that own host can accept the job.

Owner priority applies to the next assignment. Submitting an owner job preserves a running guest's reservation. An owner may explicitly pause guest intake with reclaim enabled. Reclaim requests cancellation only when the active job is a guest and explicitly restartable. Worker-reported pressure applies the same restriction. Running owner jobs and nonrestartable guest jobs keep their reservation. A readiness or pressure change closes intake immediately through normal eligibility checks.

Claim selection and reservation occur in one `BEGIN IMMEDIATE` transaction. Simultaneous claims from two hosts cannot assign the same job. Repeated claims for one host return its single active attempt.

### Waiting explanations

Queued status contains a `waiting` array with each registered `host_id` and its reason codes. Multiple reasons can apply to one host.

| Code | Meaning |
| --- | --- |
| `no_hosts` | Register a worker host first |
| `no_snapshot` | The host has yet to report capabilities and readiness |
| `stale` | Snapshot receipt falls outside the freshness window |
| `occupied` | An unresolved active attempt holds the host slot |
| `not_ready` | Worker admission is paused |
| `pressure` | Worker reports resource pressure |
| `guests_disabled` | Worker currently allows owner work only |
| `guests_paused` | Owner manually paused guest intake |
| `os_mismatch` | Required OS differs |
| `arch_mismatch` | Required architecture differs |
| `runtime_missing` | Required runtime is unavailable |
| `profile_not_allowed` | Worker does not advertise the requested profile |
| `insufficient_memory` | Requested memory exceeds reported headroom |
| `owner_priority` | An eligible job from this host owner's queue has priority |
| `own_host_preferred` | The submitter has a free eligible own host |
| `awaiting_claim` | This host can accept the job on a worker poll |

## Attempts, cancellation, and reconciliation

An assignment returned by `claim` includes these fields. The `job` field contains the full immutable job payload above.

```json
{
  "schema_version": 1,
  "attempt_id": "attempt-06a0a9f0320f4aeab4c41f269df49bd0",
  "host_id": "alice-mac",
  "worker_id": "worker-alice",
  "job": {},
  "state": "assigned",
  "assigned_at": 1700000000.0,
  "cancel_requested": false,
  "cancel_reason": null,
  "result": null,
  "finished_at": null
}
```

The example's empty `job` is a placeholder for the full payload. Attempt IDs are generated once and stored durably. The worker retains `attempt_id`, `host_id`, and `worker_id` through execution and retries. After a disconnected claim, the next claim for the same host recovers the same assignment.

The job lifecycle is `queued` → `assigned` → `succeeded`, `failed`, or `canceled`. A cancellation request changes an assigned job to `cancel_requested` until the worker reconciles a terminal result. Canceling a queued job immediately makes it `canceled` without creating an attempt. Repeated cancellations are idempotent. `cancel_reason` records the first of `user_requested`, `owner_reclaim`, or `host_pressure`.

Only terminal reconciliation releases an active slot. Stale snapshots, missed heartbeats, reconnects, coordinator restarts, cancellation requests, and fresh readiness reports preserve that slot. Permanent worker-state loss can therefore keep a host reserved until an explicit recovery protocol is implemented and safe cleanup is established.

The worker passes its identity and this result to `finish` after confirmed cleanup.

```json
{
  "schema_version": 1,
  "outcome": "succeeded",
  "exit_code": 0,
  "summary": "All smoke tests passed"
}
```

`outcome` is `succeeded`, `failed`, or `canceled`. `exit_code` is null or a signed 32-bit integer. Success requires zero. Failure requires a nonzero or null exit code. A summary has at most 4096 characters. Detailed logs and artifact storage belong to the worker integration.

The exact normalized terminal result can be retried indefinitely, including after another job occupies the same host. A conflicting terminal result is rejected without overwriting the stored outcome or releasing the newer reservation. A completed task can still report success if it raced with a cancellation request. The worker must report the actual terminal outcome.

## Python API

```python
from sparemesh import Coordinator

with Coordinator("state.sqlite3") as coordinator:
    coordinator.register_host("alice-mac", "alice", "worker-alice")
    print(coordinator.hosts())
```

`Coordinator(database, clock=time.time, freshness_seconds=30)` accepts an injectable clock for deterministic tests. Every method returns JSON-compatible dictionaries, lists, or `None`.

| Method | Return |
| --- | --- |
| `register_host(host_id, owner, worker_id)` | Host status |
| `report_host(host_id, worker_id, snapshot)` | Host status |
| `submit(job_id, owner, profile, revision, requirements, restartable=False)` | Job status |
| `claim(host_id, worker_id)` | Active/new assignment or `None` |
| `status(job_id)` | Job status |
| `jobs(owner=None)` | Job statuses in submission order |
| `hosts()` | Host statuses in host-ID order |
| `cancel(job_id, owner)` | Job status |
| `pause_guests(host_id, owner, reclaim=False)` | Host status |
| `resume_guests(host_id, owner)` | Host status |
| `finish(host_id, worker_id, attempt_id, result)` | Job status |
| `close()` | Close connection, also provided by context manager |

Job status extends the immutable payload with `state`, `submitted_at`, `payload_hash`, `attempt`, and `waiting`. Host status includes the registered identity, latest `snapshot`, `received_at`, `fresh`, `guests_paused`, and `active_attempt_id`.

Domain failures derive from `CoordinatorError`. `ValidationError` covers malformed values, `NotFoundError` unknown records, `IdentityError` ownership mismatches, and `ConflictError` immutable or idempotency conflicts. SQLite I/O and locking failures propagate as `sqlite3.Error` so an adapter can distinguish infrastructure failures.

## Local CLI walkthrough

Run the isolated demonstration and test suite from the repository directory.

```sh
python3 -m sparemesh demo
python3 -m unittest discover -s tests -v
```

The demo creates a temporary database, simulates two worker identities, reopens the database to exercise restart, prints checked events and final job states, and removes its temporary files. The `demo` command uses its own temporary database even when `--db` is supplied.

For a persistent local manual walkthrough, register a host and report its snapshot. The following examples use the default database in the current directory.

```sh
python3 -m sparemesh register-host alice-mac --owner alice --worker worker-alice
python3 -m sparemesh report-host alice-mac --worker worker-alice --snapshot - <<'JSON'
{"schema_version":1,"sequence":1,"os":"darwin","arch":"arm64","runtimes":["python3.11"],"profiles":["smoke-tests"],"available_memory_mb":2048,"ready":true,"allow_guests":true,"pressure":false}
JSON
python3 -m sparemesh submit smoke-tests --job-id alice-smoke-001 --owner alice \
  --ref aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa --os darwin --arch arm64 \
  --runtime python3.11 --memory-mb 512
python3 -m sparemesh claim alice-mac --worker worker-alice
python3 -m sparemesh status alice-smoke-001
python3 -m sparemesh cancel alice-smoke-001 --owner alice
python3 -m sparemesh claim alice-mac --worker worker-alice
```

The final claim returns the original attempt with `cancel_requested` set. Copy its `attempt_id` into the completion command after the simulated worker has stopped.

```sh
python3 -m sparemesh finish <attempt-id> --host alice-mac --worker worker-alice --result - <<'JSON'
{"schema_version":1,"outcome":"canceled","exit_code":null,"summary":"Simulated worker stopped and cleaned up"}
JSON
python3 -m sparemesh hosts
python3 -m sparemesh jobs --owner alice
```

For a later new snapshot, increase `sequence`. Report within the freshness window before requesting new work. For another run, choose a new job ID. Successful commands emit JSON on stdout. Domain or database errors emit a JSON error object on stderr and exit with status `2`. Argument parsing errors use argparse's usage output and status `2`.
