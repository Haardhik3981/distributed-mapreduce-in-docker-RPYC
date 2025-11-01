import os, sys, time, zipfile, io, socket, glob, collections, itertools, threading
import requests
import rpyc
from rpyc.utils.classic import obtain


# ------------ Config ------------
RPYC_PORT = int(os.getenv("RPYC_PORT", "18861"))
TASK_TIMEOUT_SECS = int(os.getenv("TASK_TIMEOUT_SECS", "20"))
DATA_DIR = os.getenv("DATA_DIR", "/data/txt")
POOL_NAME = os.getenv("WORKER_HOSTS", "worker").strip()
MAP_PARALLELISM = int(os.getenv("MAP_PARALLELISM", "3"))
NUM_REDUCERS = int(os.getenv("NUM_REDUCERS", str(2 * MAP_PARALLELISM)))
os.makedirs(DATA_DIR, exist_ok=True)

# ------------ Utilities ------------
def download(url: str) -> list[str]:
    os.makedirs(DATA_DIR, exist_ok=True)

    # Skip if a .txt already exists (re-runs are fast)
    txts = [os.path.join(DATA_DIR, f)
            for f in os.listdir(DATA_DIR)
            if f.lower().endswith(".txt")]
    if txts:
        print(f"[coordinator] found {len(txts)} .txt file(s) in {DATA_DIR}; skipping download.")
        return txts

    zip_path = os.path.join(DATA_DIR, "dataset.zip")
    if not os.path.exists(zip_path):
        print(f"[coordinator] downloading: {url}")
        import requests
        with requests.get(url, stream=True, timeout=600) as r:
            r.raise_for_status()
            with open(zip_path, "wb") as out:
                for chunk in r.iter_content(1024 * 1024):
                    if chunk:
                        out.write(chunk)
    else:
        print(f"[coordinator] reusing existing zip: {zip_path}")

    # Extract to UTF-8 .txt using streaming read
    import zipfile
    txt_paths = []
    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.namelist():
            if member.endswith("/"):
                continue
            # Stream unzip to a temp file (binary), then re-open and transcode to utf-8
            raw_tmp = os.path.join(DATA_DIR, "tmp_extracted.bin")
            with zf.open(member) as src, open(raw_tmp, "wb") as out:
                for b in iter(lambda: src.read(1024 * 1024), b""):
                    out.write(b)
            base = os.path.splitext(os.path.basename(member))[0] or "dataset"
            out_txt = os.path.join(DATA_DIR, f"{base}.txt")
            with open(raw_tmp, "rb") as inp, open(out_txt, "w", encoding="utf-8", errors="ignore") as out:
                out.write(inp.read().decode("utf-8", errors="ignore"))
            os.remove(raw_tmp)
            txt_paths.append(out_txt)

    if not txt_paths:
        raise RuntimeError("No .txt files found after download/unzip.")
    print(f"[coordinator] prepared {len(txt_paths)} .txt file(s).")
    return txt_paths

def chunk_file_by_size(path: str, n_chunks: int, target_bytes: int = 32 * 1024 * 1024):
    """
    Make ~n_chunks chunks from a large text file, ~target_bytes each, line-aligned.
    Avoids loading the whole file (enwik9) into RAM.
    """
    chunks, cur, cur_bytes = [], [], 0
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            cur.append(line)
            cur_bytes += len(line.encode("utf-8", "ignore"))
            if cur_bytes >= target_bytes:
                chunks.append("".join(cur))
                cur, cur_bytes = [], 0
        if cur:
            chunks.append("".join(cur))
    if len(chunks) < n_chunks:
        chunks += [""] * (n_chunks - len(chunks))
    return chunks

def split_text(files: list[str], n_chunks: int) -> list[str]:
    """
    Simple chunker: interleave lines across chunks to keep sizes balanced.
    Returns list of string chunks.
    """
    buckets = [io.StringIO() for _ in range(n_chunks)]
    idx = 0
    for path in files:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                buckets[idx % n_chunks].write(line)
                idx += 1
    return [b.getvalue() for b in buckets]

def partition(key: str, num_reducers: int) -> int:
    return (hash(key) & 0x7fffffff) % num_reducers

# ------------ RPC helpers ------------
def connect(host: str):
    # allow per-call timeout for sync requests
    return rpyc.connect(host, RPYC_PORT, config={
        "sync_request_timeout": TASK_TIMEOUT_SECS + 1,
        "allow_pickle": True,
        "allow_getattr": True,
        "allow_all_attrs": True,
        })

# A small wrapper to run an RPC call with timeout and detect failures.
class TaskFuture:
    def __init__(self, host: str, method: str, *args, **kwargs):
        self.host = host
        self.method = method
        self.args = args
        self.kwargs = kwargs
        self.start = time.time()
        self.done = threading.Event()
        self.result = None
        self.exc = None
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        try:
            with connect(self.host) as c:
                fn = getattr(c.root, self.method)
                remote = fn(*self.args, **self.kwargs)
                self.result = obtain(remote)
        except Exception as e:
            self.exc = e
        finally:
            self.done.set()

    def ready(self):
        return self.done.is_set()

    def timed_out(self, timeout=TASK_TIMEOUT_SECS):
        return (time.time() - self.start) > timeout and not self.ready()

def worker_hosts() -> list[str]:
    """
    Return a list of worker hostnames to target.
    With docker compose scaling, the DNS name 'worker' round-robins across replicas.
    Repeating 'worker' N times lets us do a simple round-robin scheduler in code.
    """
    n = int(os.getenv("WORKERS", os.getenv("USE_WORKERS", "3")))
    return ["worker"] * max(1, n)

# ------------ Coordinator ------------
def mapreduce_wordcount(dataset_url: str):
    # 1) Download/prepare data
    files = download(dataset_url)
    path = files[0]
    hosts = worker_hosts()
    n_workers = len(hosts)

    # 2) Create chunks = number of workers
    """n_workers = MAP_PARALLELISM
    if n_workers < 1:
        raise RuntimeError("Need at least one worker.")
    chunks = split_text(files, n_workers)"""

    chunks = chunk_file_by_size(
        path,
        n_chunks=max(1, len(hosts) * 3),
        target_bytes=32 * 1024 * 1024
    )

    # 3) MAP PHASE: dispatch chunks to workers (with timeout & reassignment)
    print(f"[coordinator] starting MAP on {n_workers} workers ...")
    task_queue = collections.deque([(i, chunks[i]) for i in range(len(chunks))])
    inflight: dict[int, TaskFuture] = {}
    assigned_to: dict[int, str] = {}  # task_id -> host
    map_results = []  # list of dict(partition -> {word:count})

    free_hosts = collections.deque(hosts)

    def assign_next():
        if not task_queue or not free_hosts:
            return
        task_id, text = task_queue.popleft()
        host = free_hosts.popleft()
        fut = TaskFuture(host, "map", text, NUM_REDUCERS)
        inflight[task_id] = fut
        assigned_to[task_id] = host
        print(f"[coordinator] dispatched MAP task {task_id} -> {host}")

    # initial dispatch
    for _ in range(min(len(task_queue), len(free_hosts))):
        assign_next()

    while inflight or task_queue:
        # check completions and timeouts
        finished_ids = []
        for tid, fut in list(inflight.items()):
            if fut.ready():
                if fut.exc:
                    print(f"[coordinator] MAP task {tid} on {fut.host} failed: {fut.exc}")
                    # requeue
                    task_queue.appendleft((tid, chunks[tid]))
                else:
                    map_results.append(fut.result)
                    print(f"[coordinator] MAP task {tid} done on {fut.host}")
                finished_ids.append(tid)
                free_hosts.append(assigned_to[tid])
            elif fut.timed_out():
                print(f"[coordinator] MAP task {tid} timed out on {fut.host} — reassigning")
                finished_ids.append(tid)
                # put back to queue
                task_queue.appendleft((tid, chunks[tid]))
                free_hosts.append(assigned_to[tid])
        for tid in finished_ids:
            inflight.pop(tid, None)
            assigned_to.pop(tid, None)

        # keep assigning while we can
        while task_queue and free_hosts:
            assign_next()

        time.sleep(0.1)

    # 4) SHUFFLE: group all intermediate counts by reducer partition
    print(f"[coordinator] shuffle phase ...")
    grouped_by_part = [collections.defaultdict(int) for _ in range(NUM_REDUCERS)]
    for part_dict in map_results:
        for p, wc in part_dict.items():
            for w, c in wc.items():
                grouped_by_part[p][w] += c
    
    reducer_payloads = []
    for d in grouped_by_part:
        reducer_payloads.append({str(k): int(v) for k, v in d.items()})

    # 5) REDUCE PHASE: send each partition to a worker to sum (parallel + timeout)
    print(f"[coordinator] starting REDUCE on {n_workers} workers ...")
    reduce_tasks = list(enumerate(reducer_payloads))  # (partition_id, {word:count})
    task_queue = collections.deque(reduce_tasks)
    inflight.clear()
    assigned_to.clear()
    free_hosts = collections.deque(hosts)
    reduced_parts: dict[int, dict[str, int]] = {}

    def assign_reduce():
        if not task_queue or not free_hosts:
            return
        pid, payload = task_queue.popleft()
        host = free_hosts.popleft()
        fut = TaskFuture(host, "reduce", payload)  # send plain dict
        inflight[pid] = fut
        assigned_to[pid] = host
        print(f"[coordinator] dispatched REDUCE part {pid} -> {host}")

    for _ in range(min(len(task_queue), len(free_hosts))):
        assign_reduce()

    while inflight or task_queue:
        finished = []
        for pid, fut in list(inflight.items()):
            if fut.ready():
                if fut.exc:
                    print(f"[coordinator] REDUCE part {pid} failed on {fut.host}: {fut.exc}")
                    task_queue.appendleft((pid, grouped_by_part[pid]))
                else:
                    reduced_parts[pid] = fut.result
                    print(f"[coordinator] REDUCE part {pid} done on {fut.host}")
                finished.append(pid)
                free_hosts.append(assigned_to[pid])
            elif fut.timed_out():
                print(f"[coordinator] REDUCE part {pid} timed out on {fut.host} — reassigning")
                finished.append(pid)
                task_queue.appendleft((pid, grouped_by_part[pid]))
                free_hosts.append(assigned_to[pid])
        for pid in finished:
            inflight.pop(pid, None)
            assigned_to.pop(pid, None)
        while task_queue and free_hosts:
            assign_reduce()
        time.sleep(0.1)

    # 6) FINAL AGGREGATION: merge reduced parts
    final_counts = collections.Counter()
    for pid in range(NUM_REDUCERS):
        final_counts.update(reduced_parts[pid])

    return final_counts

# ------------ CLI entry ------------
if __name__ == "__main__":
    dataset_url = sys.argv[1] if len(sys.argv) > 1 else "https://mattmahoney.net/dc/enwik9.zip"
    start = time.time()
    counts = mapreduce_wordcount(dataset_url)
    # Top 20 by frequency (word length desc as tie-break)
    top20 = sorted(counts.items(), key=lambda kv: (-kv[1], -len(kv[0])))[:20]
    print("\nTOP 20 WORDS BY FREQUENCY")
    for i, (w, c) in enumerate(top20, 1):
        print(f"{i:>2}. {w} -> {c}")
    elapsed = time.time() - start
    print(f"\nElapsed Time: {elapsed:.2f} seconds")