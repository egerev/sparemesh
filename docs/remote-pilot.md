# Two-Mac remote pilot

The pilot keeps one SQLite coordinator on the controller Mac. A controller pass reads both workers' capacity, assigns queued jobs, transfers the selected source bundle, and reconciles execution results. Worker commands travel over ordinary SSH on your Tailscale network. The worker supervisor continues after the SSH request or controller exits.

Each worker admits one task at a time. The coordinator prefers the submitter's available host, then a compatible peer. A host owner's queued tasks take precedence over new guest work. Guest intake and cancellation use the existing coordinator commands.

## Prepare the machines

Install Python 3.11 or newer and Git. Use an existing SSH account authorized by the machine owner. Verify its host key through that owner before connecting. Check that a noninteractive connection succeeds with the same target you will put in the controller configuration.

```sh
ssh -o BatchMode=yes -o StrictHostKeyChecking=yes friend-mac 'hostname; /opt/homebrew/bin/python3 --version'
```

Clone the same Sparemesh revision onto both Macs. Until this change is merged, the pilot branch is `feat/remote-pilot`.

```sh
git clone --branch feat/remote-pilot https://github.com/egerev/sparemesh.git
cd sparemesh
/opt/homebrew/bin/python3 -m unittest discover -s tests -q
```

Choose private state directories with space for source archives and run logs. All configured paths must point to the correct machine. The examples below assume Python 3.15 on both Macs. Set the runtime label to the actual major/minor version reported by your profile's interpreter.

## Worker configuration

Create `/Users/alice/sparemesh-worker.json` on the first Mac, adjusting paths and identities.

```json
{
  "schema_version": 1,
  "host_id": "alice-mac",
  "worker_id": "worker-alice",
  "state_dir": "/Users/alice/sparemesh-state",
  "reserve_memory_mb": 4096,
  "allow_guests": true,
  "profiles": {
    "smoke": {
      "argv": ["/opt/homebrew/bin/python3", "-I", "smoke.py"],
      "runtime": "python3.15",
      "timeout_seconds": 60
    }
  }
}
```

On the peer, create the equivalent configuration with its own `host_id`, `worker_id`, paths, and reserve. Profile names and runtime labels must match the jobs it accepts. The command is chosen by the worker's owner and runs from the extracted `source/` directory. The worker verifies the Python interpreter's version before advertising that profile.

A local probe also checks configuration and capacity.

```sh
printf '%s\n' '{"op":"probe"}' | /opt/homebrew/bin/python3 -m sparemesh.worker \
  --config /Users/alice/sparemesh-worker.json rpc
```

Memory is an admission estimate after the configured reserve. macOS pressure information can close admission. These settings do not impose a hard RAM or CPU limit on a running process.

## Controller configuration

Create `/Users/alice/sparemesh-pilot.json` on the controller Mac.

```json
{
  "schema_version": 1,
  "database": "/Users/alice/sparemesh-controller.sqlite3",
  "artifacts_dir": "/Users/alice/sparemesh-bundles",
  "timeout_seconds": 30,
  "poll_seconds": 2,
  "freshness_seconds": 30,
  "hosts": [
    {
      "host_id": "alice-mac",
      "owner": "alice",
      "worker_id": "worker-alice",
      "transport": {
        "kind": "local",
        "python": "/opt/homebrew/bin/python3",
        "package_dir": "/Users/alice/sparemesh",
        "worker_config": "/Users/alice/sparemesh-worker.json"
      }
    },
    {
      "host_id": "bob-mac",
      "owner": "bob",
      "worker_id": "worker-bob",
      "transport": {
        "kind": "ssh",
        "target": "friend-mac",
        "python": "/opt/homebrew/bin/python3",
        "package_dir": "/Users/bob/sparemesh",
        "worker_config": "/Users/bob/sparemesh-worker.json"
      }
    }
  ]
}
```

The SSH target can be an existing alias or an authorized `user@tailscale-address`. Host-key verification is mandatory. The transport disables SSH agent forwarding and runs the configured module with a quoted command. The controller database stays on its local filesystem.

## Submit and observe a task

Commit a small trusted project containing `smoke.py`. This sample identifies the executing machine and checks a calculation.

```python
import socket

assert sum(range(10000)) == 49995000
print(socket.gethostname(), flush=True)
```

Submit the full commit ID after committing this file.

```sh
commit="$(git -C /path/to/project rev-parse HEAD)"
python3 -m sparemesh pilot-submit --config /Users/alice/sparemesh-pilot.json \
  --repo /path/to/project --ref "$commit" --profile smoke \
  --job-id alice-smoke-001 --owner alice \
  --os darwin --arch arm64 --runtime python3.15 --memory-mb 128
python3 -m sparemesh pilot-run --config /Users/alice/sparemesh-pilot.json
```

Keep the controller running while it should assign work and collect results. Ctrl-C stops polling. Restart it with the same configuration and state directories to reconcile existing attempts. For a bounded pass, use `pilot-tick --config ...` or `pilot-run --config ... --max-ticks 10`.

From another terminal, inspect the coordinator and retrieve worker logs.

```sh
python3 -m sparemesh --db /Users/alice/sparemesh-controller.sqlite3 status alice-smoke-001
python3 -m sparemesh pilot-result --config /Users/alice/sparemesh-pilot.json --job-id alice-smoke-001
```

To test peer routing, run a finite sleeping task on Alice's host and submit a second Alice task while its slot is occupied. Bob's host should run the second task if compatible and ready. Repeat with the owners exchanged. Submit a third task while both hosts are occupied and verify that it waits.

## Cancellation and reconnect

Request cancellation through the coordinator while the controller is polling.

```sh
python3 -m sparemesh --db /Users/alice/sparemesh-controller.sqlite3 cancel alice-smoke-001 --owner alice
```

The next pass delivers the request to the worker. A slot becomes free after confirmed process-group cleanup and terminal reconciliation. To reclaim a host from a restartable guest, use `pause-guests HOST --owner OWNER --reclaim`. Submit that guest with `--restartable`. Resume intake with `resume-guests HOST --owner OWNER`.

Test controller restart while a task runs, then verify the same attempt finishes once. An SSH timeout leaves the assignment reserved. A later pass retries status or the same idempotent start. If the supervisor dies and process cleanup cannot be confirmed, the worker reports uncertainty and the slot remains reserved for manual inspection.

## Execution boundary

This pilot runs agreed source under the configured OS account. Use a dedicated unprivileged account when sharing beyond your own trusted test. The account's accessible files and network remain available to its processes. The supervisor controls the task process group. Code that deliberately escapes that group requires stronger OS isolation. Test tasks should be finite and cooperative.

Source preparation excludes uncommitted files and Git history. Committed secrets remain part of the source, so inspect the selected commit before sharing it. No dependency installation is performed by the runner. Install the profile's dependencies on each eligible host beforehand.

Worker state and logs persist under `state_dir`, and source bundles persist under `artifacts_dir`. Remove completed pilot state only after checking that no task is still active. Keep the state of unresolved attempts for reconciliation.

## Validation performed

The integration was exercised on a MacBook Pro and a Mac mini on 2026-11-09, both Apple Silicon with Python 3.15.2. One worker ran locally and the other through SSH over Tailscale. Two logical owners represented the participants on these two test machines.

Eight acceptance checks covered automatic overflow in both directions, controller restart preserving the same attempt, local cancellation, failed SSH connection preserving the remote assignment, queuing with both slots occupied, remote cancellation making capacity available, and all six test jobs reaching terminal states. The final pass used the same worker source on both machines. These were small finite Python smoke tasks, with sleeping tasks used to hold slots.

The 89-test suite passed on Python 3.11.14 and 3.15.2. The Python 3.15 run also treated ResourceWarning as an error. Worker tests include repeated and conflicting starts, malformed source bundles, supervisor loss, timeout, child-process cleanup, cancellation, and a disconnected RPC client.

The two-Mac test used the owner's existing accounts and temporary state directories. Dependencies and permissions for a real project still need to be configured in its worker-owned profile. Test services were stopped and temporary execution state was removed afterward.
