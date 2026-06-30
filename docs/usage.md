# Usage Guide

Follow these steps to spin up the Data Ingestion & Enrichment Simulation platform on your local machine.

## Prerequisites
- **Docker & Docker Compose** installed and running.
- **Node.js** (v18+ recommended) installed.

---

## 1. Start the Infrastructure
Navigate to the root directory of the project and start all backend services (SQL Server, Kafka, Debezium, Flink):

```powershell
docker-compose up -d
```
Wait a few seconds for the containers to fully initialize, particularly SQL Server.

---

## 2. Initialize the Database
Once the infrastructure is up, you must create the database, tables, and enable Change Data Capture (CDC). Run the provided initialization script:

```powershell
.\run_init.ps1
```
You should see output confirming the database `sim_db` was created and CDC jobs were started successfully.

---

## 3. Start the Backend Simulator
The backend Node.js orchestrator handles load generation via worker threads and serves telemetry APIs. 

Navigate to the `simulator` directory, install dependencies, and run it:

```powershell
cd simulator
npm install
npx ts-node src/index.ts
```
The backend API will start on `http://localhost:3001`. Keep this terminal window open.

---

## 4. Start the Frontend Dashboard
The React dashboard visualizes the simulation and controls the load.

Open a **new** terminal window, navigate to the `frontend` directory, install dependencies, and run the Vite dev server:

```powershell
cd frontend
npm install
npm run dev
```

---

## 5. Run the Simulation
1. Open your web browser to **[http://localhost:5173](http://localhost:5173)**.
2. Under the **Control Panel**, use the slider to select your desired Load Generation rate (RPS).
3. Select the **Architectural Approach** you want to test (e.g., Transactional Outbox).
4. Click **START SIMULATION**.
5. Observe the **Performance Telemetry** graph and **System Health** bars dynamically react to the load!

---

## Troubleshooting

### "System Health grid disappeared / UI not updating"
If you restart the Docker containers (e.g., `docker-compose restart`), the SQL Server database will be temporarily offline. The Node.js simulator maintains an active TCP connection pool to the database, which will be severed. 

If this happens, the Node.js backend might get stuck timing out, which halts the `/api/stats` endpoint and causes the frontend to hide the telemetry grid.
**Fix:** 
1. Re-run `.\run_init.ps1` to ensure the database schema wasn't lost.
2. Kill the running simulator backend terminal (`Ctrl+C`).
3. Restart the simulator (`npx ts-node src/index.ts`). The dashboard will instantly recover.
