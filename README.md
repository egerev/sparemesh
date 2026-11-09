# Sparemesh

Sparemesh is an open-source project for sharing spare compute between people who trust each other. Submit a test run or an agent task from the CLI and get a job ID, status, logs, and a result. Sparemesh chooses where the task runs based on which machines are available and what their owners allow.

## How sharing works

Each participant has a personal job queue. One coordinator stores and schedules both queues in the first pilot. A job stays in its submitter's queue even when a peer's computer runs it.

1. **Use the submitter's machine first.** If it has capacity and supports the task, it takes the job.
2. **Use a peer's spare capacity.** If the submitter's machine is busy, paused, offline, or incompatible, an eligible peer can take the job when its owner has enabled guest workloads.
3. **Give the host owner priority.** A worker takes eligible jobs from its owner's queue before taking new guest jobs. The owner can pause guest intake and stop a running guest job. The pilot starts with one execution slot per machine and checks memory pressure before taking work.
4. **Wait when there is no match.** The job stays queued. Its status explains why it is waiting.

## First pilot

Run one agreed test profile on two Apple Silicon Macs. A worker on each Mac reports readiness and pulls jobs from the coordinator over Tailscale. Each profile specifies a repository, a fixed test command, and a timeout. Each run uses a specific commit in a dedicated working directory under a restricted account.

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

**Status:** design and pilot planning.

## License

[MIT](LICENSE).
