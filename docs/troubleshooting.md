# Troubleshooting

This document outlines common issues encountered when running the Data Ingestion & Enrichment Simulation and provides clear steps to resolve them.

## Issue: "Backend Connection Lost" (Stuck State)

**Symptoms:**
- The frontend displays a continuous "Backend Connection Lost" alert.
- The system logs show repeated failed attempts to reconnect or fetch data.
- Pressing "Reconnect" doesn't fix the issue.

**Cause:**
This typically happens when you make changes to the backend (`simulator/src/*` code) but do not rebuild the Docker container. By default, the `docker-compose.yml` uses the pre-built image of the simulator that was created when you first ran `docker-compose up`. The frontend expects new endpoints or logic (like Server-Sent Events `/api/stats/stream`) that do not exist in the stale running container.

**Resolution:**
Whenever you modify backend code, you must rebuild the simulator container and restart it. Run the following command from the root of the project:

```powershell
docker-compose up -d --build simulator
```

This will:
1. Recompile the TypeScript backend code.
2. Build a new Docker image for the `simulator` service.
3. Restart the `simulator` container without touching Kafka, Flink, or SQL Server.

Once the container restarts, refresh your frontend dashboard. The connection should be restored.
