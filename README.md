# Sparemesh

Sparemesh is an open-source project for sharing spare compute between people who trust each other. Submit a test run or an agent task from the CLI and get a job ID, status, logs, and a result. Sparemesh chooses where the task runs based on which machines are available and what their owners allow.

## How sharing works

Each participant has a personal job queue. One coordinator stores and schedules both queues in the first pilot. A job stays in its submitter's queue even when a peer's computer runs it.

1. **Use the submitter's machine first.** If it has capacity and supports the task, it takes the job.
2. **Use a peer's spare capacity.** If the submitter's machine is busy, paused, offline, or incompatible, an eligible peer can take the job when its owner has enabled guest workloads.
3. **Give the host owner priority.** A worker takes eligible jobs from its owner's queue before taking new guest jobs. The owner can pause guest intake and stop a running guest job. The pilot starts with one execution slot per machine and checks memory pressure before taking work.
4. **Wait when there is no match.** The job stays queued. Its status explains why it is waiting.

## First pilot

Run one agreed test profile on two Apple Silicon Macs. The controller polls a worker on each Mac over SSH and Tailscale, collects readiness, and delivers assigned jobs. Each worker profile specifies a fixed Python command, its runtime, and a timeout. Each job supplies a verified source bundle for a full commit. Each run uses a specific commit in a dedicated working directory under the configured account.

The proposed CLI looks like this:

```sh
sparemesh submit smoke-tests --ref <commit-sha>
sparemesh status <job-id>
sparemesh logs <job-id>
sparemesh cancel <job-id>
```

The first demo should show a local run, a run on each peer when the submitter's Mac is unavailable, and a job that waits while both Macs are busy. It should also show that a job is assigned once when both workers ask for work and that a worker can report the same attempt's result after reconnecting.

The pilot is for known participants and agreed repositories. Tailscale connects the participants. The coordinator handles assignment and the worker controls what runs on each Mac.

## Next steps

- Agree on the test profile and the job and result formats.
- Implement the coordinator, CLI, and macOS worker, then run the two-Mac demo.
- Add a Linux worker and evaluate Slurm for Linux execution. Expose the same job API to agents through MCP.

## Coordinator draft

The first prototype includes a durable SQLite coordinator, a local JSON CLI, and a simulated-worker demo. Python 3.11 or newer is required. Run these commands from this checkout.

```sh
python3 -m unittest discover -s tests -v
python3 -m sparemesh demo
python3 -m sparemesh --help
```

The demo exercises automatic local and peer routing, waiting while both hosts are busy, cancellation, coordinator restart, and repeated or conflicting worker reports. It uses a temporary database and simulated completions. The remote pilot adds worker execution and SSH transport to the same coordinator.

The CLI stores persistent state in `sparemesh.sqlite3` by default. Use `--db /path/to/state.sqlite3` before the subcommand to select another local database. It assumes a trusted local operator with access to that file. Remote authentication belongs in the transport adapter.

[Coordinator and worker contract](docs/coordinator-v1.md) describes the Python API, JSON formats, state transitions, ownership checks, and runnable CLI examples. Execution, process cleanup, SSH transport, logs, and fixed profile definitions belong to the worker integration.

## Remote pilot

The CLI can now prepare source, assign jobs to compatible Macs, transfer the source bundle, and run a worker-owned profile. Workers retain attempt identity and logs across controller reconnects. A repeated start returns the existing attempt. Confirmed terminal results release the coordinator slot.

[Two-Mac setup and commands](docs/remote-pilot.md) covers worker profiles, the controller configuration, submission, logs, peer routing, and cancellation. The pilot uses trusted source under the configured OS account. Process-group supervision and memory admission do not provide a sandbox or hard resource quotas.

## Fixed-commit source bundles

Package a local repository's committed source for the worker with the `bundle-source` command. It creates a deterministic `source.tar` and a SHA-256 manifest in a new output directory. Python 3.11 or newer and Git are required.

```sh
python3 -m sparemesh bundle-source --repo /path/to/project \
  --commit <full-commit-sha> --output /path/to/bundles/run-001
```

[Source bundle contract v1](docs/source-bundle-v1.md) defines the archive root, manifest, accepted paths, size limits, and worker verification requirements.

## License

[MIT](LICENSE).
