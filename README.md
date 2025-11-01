# RPyC MapReduce in Docker

A minimal MapReduce system with one **Coordinator** and N **Workers**, communicating over **RPyC** inside Docker containers. The system downloads a UTF-8 dataset (ZIP or plain text), **chunks it on disk**, runs **map** and **reduce** tasks across workers with **timeout + reassignment**, aggregates results, prints a **Top-20** word frequency table, and exits. Worker reduce outputs are also persisted as JSON files on the host.

- Runs **1 Coordinator** container and **3+ Worker** containers.
- Coordinator downloads a dataset (UTF-8 txt or zip with txt files), splits into chunks,
  sends **map** tasks to workers via **RPyC**, shuffles, then sends **reduce** tasks.
- Detects **worker failure / timeout (20s)** and **reassigns** tasks.
- **Configurable** reducers and workers.

---

## Requirements

1) **Coordinator & Worker in separate containers; multiple workers (3+); Docker DNS hostnames; RPyC**  
   - `docker-compose.yml` defines **coordinator** and **worker** services.  
   - Scale workers via `--scale worker=N`.  
   - Coordinator connects to hostname **`worker`** (round-robin across replicas) via **RPyC** on port `18861`.

2) **Coordinator should allow the user to specify a URL to download a dataset. Assume it can only download UTF-8 files.**  

3) **Workers will periodically check or receive Tasks with/from the Coordinator via RPyC. A Task can either be a Map or Reduce task.**  
   - Worker exposes `exposed_map()` and `exposed_reduce()`.

4) **Map tasks produce results that may be either returned to the Coordinator, or buffered in memory in the Worker machine. The Map Worker is also responsible for partitioning the intermediate pairs into its respective Regions. This is done using a partitioning function. When a Worker completes a Map Task, it communicates with the Coordinator.**  
   - Worker map partitions counts to reducers via MD5 hash → reducer index, then returns dict-of-dicts to coordinator.

5) **Reduce tasks are responsible for reducing the intermediate value keys found in all the Worker containers. These are coordinated with Workers by the Coordinator.**  
   - Coordinator shuffles/group-bys by partition; dispatches reduce payloads to workers; workers sum and return results.

6) **The Coordinator aggregates the final outputs after all Reduce Workers return their results. If an implementation allows concurrent updates to a shared state (e.g., a global counter or store), those updates must be mutually exclusive (e.g., using locks, semaphores, or transactional primitives) to ensure correctness under concurrent access.**  
   - Coordinator merges per-partition reduced results **after** join; no shared mutable state across threads without control.  
   - Per-partition outputs also written by workers to `./data/out/reduce-<part>.json`.

7) **Map Tasks performed by a failed container need to be reassigned. If a Worker fails or becomes unresponsive, its assigned Map (or Reduce) Task must be reassigned to another available Worker. Failure can be assumed if a Worker exceeds a predefined timeout period for completing its assigned Task. The Coordinator is responsible for detecting such failures and rescheduling tasks.**  
   - Coordinator wraps every remote call in a `TaskFuture` with **per-task timeout and retry**; re-queues task on timeout/failure.

8) **The Coordinator can exit when all Map and Reduce Tasks have finished.**  
   - After successful reduce aggregation + printout, coordinator exits. Compose flag `--abort-on-container-exit` stops workers.

9) **Number of Map and Reduce Workers should be configurable via command-line arguments or environment variables.**  
   - Set `USE_WORKERS` and scale with `--scale worker=N`.  
   - `NUM_REDUCERS` defaults to `2 * MAP_PARALLELISM` (which defaults to `USE_WORKERS`).

---

## Repository Layout
.
├── README.md
├── docker-compose.yml
├── coordinator/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── coordinator.py
├── worker/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── worker.py
└── data/
    ├── txt/        # downloaded & transcoded UTF-8 dataset
    ├── chunks/     # chunk-*.txt created by coordinator
    └── out/        # reduce-<part>.json written by workers

## Prerequisites

- Docker Desktop (or Docker Engine)  
- Docker Compose v2+

## Configuration
| Env Variable        | Description                                                                                  |
|---------------------|----------------------------------------------------------------------------------------------|
| `USE_WORKERS`       | Number of worker replicas to run and the scheduling pool size used by the coordinator.       |
| `MAP_PARALLELISM`   | Map-side parallelism hint (defaults to `USE_WORKERS`) used to infer reducer count.           |
| `NUM_REDUCERS`      | Number of reduce partitions; controls shuffle fan-out and reduce concurrency.                |
| `WORKER_HOSTS`      | Hostname used by the coordinator to reach workers (e.g., `worker` for Docker DNS round-robin). |
| `RPYC_PORT`         | TCP port workers listen on for RPyC requests from the coordinator.                           |
| `TASK_TIMEOUT_SECS` | Per-task timeout. Timed-out map/reduce tasks are reassigned to another worker.               |
| `DATA_DIR`          | Directory for downloaded/transcoded UTF-8 dataset files inside the container (`./data/txt` on host). |
| `CHUNK_DIR`         | Directory where on-disk chunk files are written/read (`./data/chunks` on host).              |


## BUILD & RUN
### 1 worker
- USE_WORKERS=1 docker compose up --build --scale worker=1 --abort-on-container-exit

### 5 workers
- USE_WORKERS=5 docker compose up --build --scale worker=5 --abort-on-container-exit

### 10 workers
- USE_WORKERS=10 docker compose up --build --scale worker=10 --abort-on-container-exit

## QUICK RESET

### Stop containers and remove Compose network:
- docker compose down
### Remove Data
- rm -rf ./data/chunks ./data/out ./data/txt