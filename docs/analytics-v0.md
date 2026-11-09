# Analytics v0

Analytics records host capacity and Sparemesh execution history. The first version reports observations for a selected calendar day. Report commands default to UTC; pass an IANA timezone such as `Asia/Makassar` for your local day. It can run locally on one machine or collect both workers' observations through the pilot controller.

## Local collection

Run the foreground collector from the Sparemesh checkout. It uses the same worker configuration and stores analytics in `analytics.sqlite3` under that worker's private `state_dir`.

```sh
python3 -m sparemesh.worker --config /absolute/path/worker.json collect --interval 15
```

Ctrl-C stops collection. For a bounded check, add `--max-samples 3`. Run a report against the recorded local history.

```sh
python3 -m sparemesh.worker --config /absolute/path/worker.json report \
  --date 2026-11-09 --timezone Asia/Makassar
```

An optional `owner` field in the worker configuration identifies the host owner for local reports. Existing worker configurations continue to work. Without an owner, local task classification is unknown. Controller reports use the registered owner in the pilot configuration.

## Controller collection and report

Normal `pilot-run` or `pilot-tick` requests collect worker observations when available. The controller stores them in a separate local analytics database. By default, `coordinator.sqlite3` uses `coordinator.analytics.sqlite3` in the same directory. Set `analytics_database` in the pilot configuration to choose another path.

```sh
python3 -m sparemesh pilot-run --config /absolute/path/pilot.json
python3 -m sparemesh pilot-report --config /absolute/path/pilot.json \
  --date 2026-11-09 --timezone Asia/Makassar
```

Add `--host HOST_ID` to select one host. Reports are JSON, matching the other CLI commands. Generating a report reads stored history and does not contact the workers.

## Measurements

Host observations include CPU utilization, physical memory, an estimate of available memory, swap usage, memory pressure, and whether the host currently admits guest work. macOS pressure comes from the native pressure level. On Linux, the pressure label is an estimate from available-memory fractions. Unsupported or unavailable measurements are represented as unknown. The first CPU observation needs a later counter sample to establish utilization.

For Sparemesh tasks, the worker records start and finish times and samples resource use for its task process group. CPU time and peak resident memory are observed estimates. A short-lived child can execute between samples. Summed RSS can count shared pages more than once. CPU is allocated across day boundaries in proportion to the known execution interval, assuming uniform use. These figures are utilization estimates. Unknown values remain distinct from zero.

An attempt belongs to the host owner's own workload when its job owner matches that host owner. Other known owners count as guests. Missing ownership is reported separately. The report clips execution time to the requested day and keeps repeated RPC responses from counting the same attempt twice.

Host idleness and guest availability are separate measures. The owner can disable guest work on an otherwise idle machine. Admission also depends on memory headroom, configured profiles, active tasks, and pressure. A low CPU percentage by itself does not grant capacity to another user.

## Observation coverage

A report uses the selected timezone's calendar-day boundaries, including daylight-saving changes. It identifies the observed portion of the day and leaves long gaps uncovered. The maximum gap used for availability estimates is 60 seconds. Current-day reports stop at the current time.

Host samples are retained for 30 days, plus one boundary sample per host. Task records are retained. The history begins when collection starts. A machine asleep or disconnected from the collector supplies no observations for that period. Prediction of future availability is a later feature.

## Privacy and operation

Stored host metrics are aggregates. Task accounting uses Sparemesh identifiers and ownership. Collection does not record unrelated application names, command lines, window titles, or file contents. The local collector keeps history on that host. The controller receives the observations and Sparemesh task usage exposed by its configured workers.

Analytics failures are reported separately from execution. A telemetry write failure must not convert a completed task to a failed task or release a reserved slot. Execution and cancellation remain governed by the worker and coordinator contracts.

## Validation

The 128-test suite passes on Python 3.11 and 3.15, including a Python 3.15 run with resource warnings treated as errors. A real local pilot completed one owner task and one guest task, and both the worker and controller reports recorded their CPU time and peak RSS. Native collection was also checked on a second Mac over SSH, including elevated memory pressure and disabled guest admission. These bounded checks were stopped and their temporary state removed.
