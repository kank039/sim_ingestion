# DevOps Board Backlog: Data Ingestion & Enrichment Simulation

This document contains the robust breakdown of all implementation tasks mapped into epics, user stories, and acceptance criteria suitable for import into a DevOps board (Jira, Azure DevOps, etc.). Each task includes a probable human-effort hour estimate.

## Epic 1: Infrastructure & Data Persistence
**Description:** Set up the underlying foundational infrastructure, container orchestration, and database schemas required for the streaming architecture simulation.

* **Task 1.1: Container Orchestration (`docker-compose`)**
  * **Estimate:** ~6 Hours
  * **Description:** Spin up local instances of SQL Server, Redpanda (Kafka), Debezium, and Apache Flink (JobManager & TaskManager).
  * **Acceptance Criteria:**
    * All containers boot successfully on a shared bridge network (`sim_network`).
    * Explicit CPU (`cpus`) and memory (`memory`) limit constraints are applied to all services to allow accurate system bottleneck simulations.

* **Task 1.2: Database Initialization Automation**
  * **Estimate:** ~4 Hours
  * **Description:** Create a PowerShell initialization script (`run_init.ps1`) to automate database and schema creation.
  * **Acceptance Criteria:**
    * Script idempotently creates `sim_db`.
    * Script creates required tables (`billing_record`, `invoice_batch`).
    * Script automatically enables SQL Server Change Data Capture (CDC) at both the database and table levels.

## Epic 2: High-Throughput Load Simulator (Backend)
**Description:** Engineer a highly concurrent Node.js backend capable of pushing massive Requests Per Second (RPS) directly against the database to simulate peak traffic conditions.

* **Task 2.1: Multi-Threaded Load Generation Engine**
  * **Estimate:** ~8 Hours
  * **Description:** Break out of the single-threaded Node.js event loop limitation by implementing the `worker_threads` module.
  * **Acceptance Criteria:**
    * Orchestrator spawns 4 isolated worker threads to evenly distribute the RPS load.
    * Load generation is entirely decoupled from the main API thread.

* **Task 2.2: Extreme Connection Pooling**
  * **Estimate:** ~3 Hours
  * **Description:** Optimize the `mssql` connection pool settings inside `tarn.js`.
  * **Acceptance Criteria:**
    * Connection pool maximum is increased to 200 concurrent connections to prevent `TimeoutError` exceptions during high-RPS spikes.
    * Database handles simultaneous rapid-fire INSERT statements from all 4 workers safely.

* **Task 2.3: Real-Time Telemetry & Orchestration API**
  * **Estimate:** ~6 Hours
  * **Description:** Build an Express.js REST API to coordinate the simulation state and fetch live Docker container metrics.
  * **Acceptance Criteria:**
    * `/api/simulate/start` and `/api/simulate/stop` endpoints dictate worker execution states.
    * A background polling loop leverages Node's `child_process.exec` to natively execute `docker stats --no-stream` and parse the raw stdout buffer.
    * `/api/stats` endpoint yields aggregated telemetry (App Latency, RPS target, CPU%, MEM%) back to the frontend every 1000ms.

## Epic 3: Glassmorphism Control Dashboard (Frontend)
**Description:** Build a premium, dark-mode React application to visualize system limits and provide interactive control over the simulated architectures.

* **Task 3.1: Premium UI & Core Layout**
  * **Estimate:** ~10 Hours
  * **Description:** Implement a strict, single-page CSS Flexbox/Grid layout using a glassmorphism design language.
  * **Acceptance Criteria:**
    * 100% viewport bounded layout (`100vh`) with zero vertical scrolling (Scrollbars strictly prohibited).
    * High-end styling implementation including translucent gradient backgrounds, backdrop-filters (blur), and dynamic accent colors.

* **Task 3.2: Interactive Architectural Control Panel**
  * **Estimate:** ~4 Hours
  * **Description:** Build the control mechanisms to command the backend simulation state.
  * **Acceptance Criteria:**
    * RPS Slider strictly steps in exact increments (0 to 5000 RPS).
    * Dynamic state buttons push the selected architectural strategy to the Node orchestrator payload.

* **Task 3.3: Dynamic Performance Graphing (Recharts)**
  * **Estimate:** ~6 Hours
  * **Description:** Render a highly responsive Cartesian graph to visualize latency and CPU trends under load.
  * **Acceptance Criteria:**
    * Graph actively plots `App Latency (ms)`, `SQL Server CPU (%)`, and `SQL Server I/O`.
    * Implements Windows Task Manager-style timescale controls (Slow/Normal/Fast) which actively alter the historical array slice rendering window (30 to 150 points) without dropping data.

* **Task 3.4: Live System Health Grid**
  * **Estimate:** ~5 Hours
  * **Description:** Present a real-time tracking interface showing exact resource loads against individual Docker containers.
  * **Acceptance Criteria:**
    * 2-column symmetric grid layout matching exact system topology (Kafka, Flink, Debezium, SQL Server).
    * Visual progress bars for CPU and MEM percentage that re-calculate dynamically per second.
    * Extremely compact typography and padding configurations to ensure no overflow clipping occurs on small monitors.
