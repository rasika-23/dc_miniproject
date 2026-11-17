# Distributed Monitoring & Failure Detection Platform

Five identical Python node processes monitor each other with heartbeats, detect failures, and elect a
coordinator with the **Bully algorithm**. Any node can become coordinator; Node 5 only wins because it has
the highest priority and the election says so. A **Streamlit dashboard** (light theme) shows live cluster
state and lets you inject faults and run repeatable experiments.

```
Streamlit dashboard :8501  ──HTTP──►  Node 1..5 :8001-8005  (FastAPI, SQLite journal each)
 (a client: never required                │
  for monitoring/elections)               └── heartbeats, failure detection, Bully election, membership,
                                              fault injection (delay / loss), event + metrics logging
Experiment controller (fault_injection/controller.py) ── kills / restarts node processes, records ground truth
```

## 1. Quick start (VS Code, no Docker)

Requires Python 3.10+.

```bash
python -m venv .venv
# Windows:  .venv\Scripts\activate        macOS/Linux:  source .venv/bin/activate
pip install -r requirements.txt

python scripts/start_cluster.py          # starts 5 node processes, waits for the first election
streamlit run dashboard/app.py           # opens http://localhost:8501
```

Then in the dashboard open **Fault controls** and press **Crash the coordinator**. Within about 4-5 seconds
Node 4 is elected (epoch +1); restart Node 5 and it reclaims leadership through a new election.

You can also start everything from the dashboard: **Overview → Start all nodes** (shown when no node is reachable).

VS Code helpers are included: **Run and Debug** has "Dashboard (Streamlit)", "Start cluster", a single-node
debug launch and an experiment picker; **Terminal → Run Task** has start/stop/test tasks.

Other commands:

```bash
python scripts/start_cluster.py --fresh        # wipe node databases first
python scripts/stop_node.py 5                  # crash node 5 (kill -9 equivalent, ground truth is recorded)
python scripts/stop_node.py --all              # stop all nodes
python scripts/run_experiment.py --list        # list scenarios and their parameters
python scripts/run_experiment.py coordinator_crash --reps 5 --csv results/
python -m pytest                               # 35 tests (~40 s)
```

Node logs are in `runtime/logs/node<ID>.log`; node databases in `data/node_<ID>.db`.
Nodes run as detached processes, so **closing the dashboard (or VS Code) does not stop the cluster**. Stop it
with `python scripts/stop_node.py --all`.

## 2. Dashboard pages

| Page | What it shows |
| --- | --- |
| **Overview** | Cluster state banner (operational / election in progress / unavailable / conflicting coordinators), KPI cards (healthy, suspected, recovering, failed, coordinator + epoch, observed monitoring uptime), topology graph, live event feed, one card per node |
| **Node details** | Table of role, status, priority, port, last heartbeat, response time, uptime, failures; per-node response-time history and failure history |
| **Election history** | Previous coordinator, new coordinator, start/end, duration, epoch, retries and trigger for every election; coordinator-over-time chart |
| **Event timeline** | Merged, de-duplicated journals of all reachable nodes (+ controller ground truth), filterable by category / node / text |
| **Fault controls** | Crash, heartbeat delay, message loss, restart/recover, clear faults, quick actions, log of injected faults |
| **Metrics and reports** | Run any of the 8 scenarios (repeat N times), summary with average / min / max, charts, live message counters, CSV downloads |

The sidebar has an auto-refresh toggle and interval. The dashboard polls every configured node address,
finds the coordinator itself (`election/leader_discovery.py`) and never assumes Node 5 leads. If there is no
coordinator it shows "election in progress" or "cluster unavailable" instead of failing.

## 3. How it works

**Roles.** Every node runs the same service (`node/service.py`). Role (`worker` / `candidate` / `coordinator`)
is runtime state decided by the election.

**Messages** (all HTTP/JSON, each carries sender id, process incarnation and, where relevant, the coordinator epoch):

| Message | Endpoint | Direction |
| --- | --- | --- |
| `REGISTER` | `POST /register` | node → coordinator |
| `HEARTBEAT` | `POST /heartbeat` | worker → coordinator |
| `COORDINATOR_HEARTBEAT` | `POST /coordinator-heartbeat` | coordinator → workers (carries a lease expiry) |
| `ELECTION` / `OK` | `POST /election`, `POST /election/ok` | candidate → higher priority / reply |
| `COORDINATOR` | `POST /coordinator` | election winner → all |
| `STATUS_SYNC` | `POST /status-sync` | coordinator → workers (membership snapshot) |
| `FAULT_CONTROL` | `POST /faults` | controller → node (loopback only) |

Read-only: `GET /health`, `GET /cluster/status`, `GET /cluster/events`, `GET /cluster/metrics`.
Interactive API docs for any node: `http://127.0.0.1:8001/docs`.

**Startup.** A node waits `startup_grace`, asks its peers who leads, and follows an existing coordinator if it has
higher priority; otherwise it starts a Bully election. Nothing is hard-coded to Node 5.

**Worker failure detection** (coordinator side): no heartbeat for `suspicion_timeout` → `SUSPECTED`; still silent after
`failure_confirmation` more → `FAILED`. A heartbeat while suspected clears the suspicion (logged as a false
suspicion). A heartbeat from a `FAILED` node, or a re-registration with a new incarnation, moves it to `RECOVERING`;
after `recovery_validation_beats` valid heartbeats it is `HEALTHY` again.

**Coordinator failure and failover:** every node watches `COORDINATOR_HEARTBEAT`. After `coordinator_timeout` it
clears the coordinator and runs the Bully election (higher priority nodes go first, see *Design notes*). The
winner picks `highest epoch seen + 1`, announces itself, and workers register so it can rebuild the membership
view from REGISTER and HEARTBEAT messages. When the old coordinator returns it starts an election and reclaims
leadership if it is still the highest-priority node.

**Storage.** One SQLite file per node (`data/node_<id>.db`) with `node_events`, `election_events`,
`fault_experiments`, `metric_results`; every event has an event id, timestamp, node id, incarnation, type and
epoch. Databases are *not* replicated. The dashboard and the experiment runner collect journals from reachable
nodes and de-duplicate by event id. Membership lives only in coordinator memory. A restarting node reads the
highest epoch from its own journal. The experiment controller keeps its own file, `data/controller.db`.

**Fault injection.**
* *Crash / recovery*: `fault_injection/process_control.py` kills and restarts real OS processes (via `psutil`). It
  is independent of the node API, so the coordinator can be killed while being the process under test.
* *Heartbeat delay* and *message loss* are applied to a node's **outgoing** messages (`fault_injection/heartbeat_faults.py`)
  with scope `heartbeat`, `election` or `all`. `POST /faults` accepts loopback clients only.

## 4. Configuration (`config/nodes.yaml`)

Node ids, priorities, hosts and ports, plus every timing value. Defaults match the suggested starting values:

| Parameter | Default |
| --- | --- |
| heartbeat_interval | 1 s |
| suspicion_timeout | 3 s |
| failure_confirmation | 2 s (FAILED after 5 s of silence) |
| coordinator_heartbeat_interval | 1 s |
| coordinator_timeout | 4 s |

These are experimental settings, not guarantees. If a busy laptop produces false suspicions, increase them. To use a
different file: `set DMP_CONFIG=path\to\file.yaml` (Windows) / `export DMP_CONFIG=...`, or `--config` on node/scripts.
For quick demos, a copy of the file with `heartbeat_interval: 0.3`, `suspicion_timeout: 1`, `coordinator_timeout: 1.2`
makes failovers take about a second.

## 5. Metrics and experiments

| Metric | Calculation |
| --- | --- |
| Failure detection latency | first failure declaration about the target − injected fault time (`NODE_FAILED` or `COORDINATOR_TIMEOUT`) |
| Election duration | coordinator announcement − election start (first completed election after the fault) |
| Failover time | first new-coordinator announcement − fault time |
| Recovery time | node healthy (or announced as coordinator) − node restart time |
| False-positive rate | failure declarations about a node that was not actually down ÷ all declarations |
| Monitoring availability | share of the observation window with a reachable coordinator (sampled every 0.4 s) |
| Message overhead | protocol messages sent by all node incarnations during the run |

Fault times are ground truth recorded by the controller at the moment it kills a process or applies a fault.
Run each scenario several times (`--reps` / the Repetitions field) and the summary reports average, min and max.
Each repetition first restores a healthy cluster and restores it again afterwards.

| # | Scenario key | What it demonstrates |
| --- | --- | --- |
| 1 | `initial_election` | all nodes start; the highest priority wins |
| 2 | `worker_crash` | worker declared failed with **no** new election; then recovery |
| 3 | `coordinator_crash` | timeout → Bully election → next-highest node leads |
| 4 | `coordinator_recovery` | restarted Node 5 triggers an election and reclaims leadership |
| 5 | `delayed_heartbeat` | delay above the suspicion/failure timeouts → false suspicion / false failure that heals |
| 6 | `message_loss` | heartbeat loss (false suspicions) and election-message loss (retries, conflicting claims) |
| 7 | `repeated_failovers` | coordinator crashed 3 times in a row (5, 4, 3) |
| 8 | `dashboard_resilience` | no client polls the cluster; nodes still detect the crash and elect a replacement |

With the default timing a run takes roughly 40-120 s because it waits for real timeouts. Results are stored in
`data/controller.db`; export from the dashboard or with `--csv DIR` (`*_runs.csv` one row per run, `*_summary.csv`
average/min/max). For scenario 8 you can also close the dashboard completely and run
`python scripts/run_experiment.py dashboard_resilience` from a terminal.

## 6. Project layout

```
node/            main.py (one node process), service.py (wiring + FastAPI app), node_state.py, config.py
communication/   messages.py, http_client.py (counters + fault hooks), peer_registry.py
election/        bully.py (rules + one round), election_manager.py, coordinator_state.py, leader_discovery.py
monitoring/      heartbeat_sender/receiver.py, failure_detector.py, membership_registry.py, recovery_manager.py
fault_injection/ heartbeat_faults.py (delay/loss), process_control.py (kill/start), controller.py (ground truth)
storage/         schema.sql, database.py, event_repository.py
metrics/         calculator.py, experiment_runner.py, export_results.py
api/             routes.py, schemas.py, dependencies.py
dashboard/       app.py + discovery.py, cluster_view.py, election_view.py, fault_controls.py, metrics_view.py,
                 theme.py, components.py
scripts/         start_cluster.py, stop_node.py, run_experiment.py
tests/           bully, heartbeats, failure detection, failover, recovery, metrics (35 tests)
config/nodes.yaml, .streamlit/config.toml (light theme), .vscode/ (launch + tasks)
```

## 7. Design notes (choices beyond the spec)

* **Bully details.** `OK` is a separate `POST /election/ok`. A node that receives `ELECTION` from a lower-priority node
  replies `OK` and runs its own election, *except* that a sitting coordinator re-asserts itself at the same epoch
  instead of bumping the epoch for everyone. A node never follows a lower-priority claimant (it starts its own election).
* **Election stagger.** After a coordinator timeout, a node waits `election_stagger × (max_priority − priority)` before
  electing, so higher-priority nodes usually finish first and the rest find an announcement waiting. Set it to 0 for
  the textbook behaviour (more `ELECTION` messages).
* **Epochs** order leadership claims (higher epoch wins, then higher priority) and let nodes reject stale
  `COORDINATOR` / `COORDINATOR_HEARTBEAT` / heartbeat messages. Stale incarnations are rejected by the coordinator.
* **Heartbeat events** are sampled in the journal (`heartbeat_log_every`) to keep SQLite small; state changes are
  always logged. Message *counters* count every message.
* **Delay is per message** (each heartbeat is sent from its own task), so an injected delay behaves like network
  latency: a one-off gap at the start, then a steady, late stream.
* **"Response time"** on the dashboard is heartbeat transit time (receiver clock − sender timestamp). All nodes share one
  host clock here; across machines it would need clock sync.
* A dashboard-side "Not responding" state appears when probes fail but the coordinator has not yet declared the node
  failed; it is counted as *Suspected* in the KPI cards.

## 8. Known limitations (by design)

* **Timeouts cannot tell a crashed node from a slow or partitioned one.** Epochs reject *stale* announcements but do
  **not** guarantee split-brain freedom; the lease field in `COORDINATOR_HEARTBEAT` is informational and there is no
  quorum or fencing. This is a teaching project, not production consensus. The dashboard flags conflicting
  coordinators when it sees them, and priority + epoch ordering normally resolves them within a few heartbeats.
* Message loss and delay are injected at the sender only.
* SQLite journals are local and unreplicated; a crashed node's recent history is visible again after it restarts.
* Message-overhead counts for a killed process are sampled every 0.4 s, so the last fraction of a second is missed.
* Fault controls are meant for a local test environment (loopback-only endpoint, real process kills).

## 9. Troubleshooting

* **"Cluster unavailable" in the dashboard** - nothing listens on 8001-8005. Run `python scripts/start_cluster.py` and
  check `runtime/logs/node1.log`. Another program using a port shows up as a bind error there; change ports in `config/nodes.yaml`.
* **Windows firewall prompt** - allow Python on private networks (everything stays on 127.0.0.1).
* **False suspicions on a slow machine** - raise `suspicion_timeout` / `coordinator_timeout`.
* **Nodes still running after a crash of the terminal** - `python scripts/stop_node.py --all`.
* **Stale history** - `python scripts/start_cluster.py --fresh` wipes node databases and logs (the controller database with
  experiment results is kept; delete `data/controller.db` to reset it).
