import os, sys, time, glob, collections, threading, zipfile
import requests
import rpyc
from rpyc.utils.classic import obtain

# ------------ Config ------------
RPYC_PORT = int(os.getenv("RPYC_PORT", "18861"))
TASK_TIMEOUT_SECS = int(os.getenv("TASK_TIMEOUT_SECS", "20"))
DATA_DIR = os.getenv("DATA_DIR", "/data/txt")
MAP_PARALLELISM = int(os.getenv("MAP_PARALLELISM", "3"))
NUM_REDUCERS = int(os.getenv("NUM_REDUCERS", str(2 * MAP_PARALLELISM)))
CHUNK_DIR = os.getenv("CHUNK_DIR", "/data/chunks")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(CHUNK_DIR, exist_ok=True)

# ------------ Utilities ------------
def download(url: str) -> list[str]:
    """Download a UTF-8 dataset .zip (once), extract to .txt in DATA_DIR, skip if a .txt already exists."""
    txts = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.lower().endswith(".txt")]
    if txts:
        print(f"[coordinator] found {len(txts)} .txt file(s) in {DATA_DIR}; skipping download.")
        return txts

    zip_path = os.path.join(DATA_DIR, "dataset.zip")
    if not os.path.exists(zip_path):
        print(f"[coordinator] downloading: {url}")
        with requests.get(url, stream=True, timeout=600) as r:
            r.raise_for_status()
            with open(zip_path, "wb") as out:
                for chunk in r.iter_content(1024 * 1024):
                    if chunk:
                        out.write(chunk)
    else:
        print(f"[coordinator] reusing existing zip: {zip_path}")

    txt_paths = []
    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.namelist():
            if member.endswith("/"):
                continue
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

def chunk_file_to_paths(src_path: str, target_bytes: int = 32 * 1024 * 1024) -> list[str]:
    """Split a large UTF-8 text into ~target_bytes chunks (line-aligned) under CHUNK_DIR. Return chunk paths."""
    for old in glob.glob(os.path.join(CHUNK_DIR, "chunk-*.txt")):
        try:
            os.remove(old)
        except FileNotFoundError:
            pass

    paths, buf, buf_bytes, idx = [], [], 0, 0
    with open(src_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            buf.append(line)
            buf_bytes += len(line.encode("utf-8", "ignore"))
            if buf_bytes >= target_bytes:
                outp = os.path.join(CHUNK_DIR, f"chunk-{idx:05d}.txt")
                with open(outp, "w", encoding="utf-8") as out:
                    out.write("".join(buf))
                paths.append(outp)
                idx += 1
                buf, buf_bytes = [], 0

    if buf:
        outp = os.path.join(CHUNK_DIR, f"chunk-{idx:05d}.txt")
        with open(outp, "w", encoding="utf-8") as out:
            out.write("".join(buf))
        paths.append(outp)

    if not paths:
        outp = os.path.join(CHUNK_DIR, "chunk-00000.txt")
        open(outp, "w", encoding="utf-8").close()
        paths.append(outp)

    return paths

# ------------ RPC helpers ------------
def connect(host: str):
    return rpyc.connect(
        host, RPYC_PORT,
        config={
            "sync_request_timeout": TASK_TIMEOUT_SECS + 1,
            "allow_pickle": True,
            "allow_getattr": True,
            "allow_all_attrs": True,
        },
    )

class TaskFuture:
    """Run one RPC and capture result/exception with timeout awareness."""
    def __init__(self, host: str, method: str, *args, **kwargs):
        self.host = host
        self.method = method
        self.args = args
        self.kwargs = kwargs
        self.start = time.time()
        self.done = threading.Event()
        self.result = None
        self.exc = None
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        try:
            with connect(self.host) as c:
                remote = getattr(c.root, self.method)(*self.args, **self.kwargs)
                self.result = obtain(remote)
        except Exception as e:
            self.exc = e
        finally:
            self.done.set()

    def ready(self): return self.done.is_set()
    def timed_out(self, timeout=TASK_TIMEOUT_SECS): return (time.time() - self.start) > timeout and not self.ready()

def worker_hosts() -> list[str]:
    """Repeat 'worker' N times; docker DNS round-robins across replicas."""
    n = int(os.getenv("WORKERS", os.getenv("USE_WORKERS", "3")))
    return ["worker"] * max(1, n)

# ------------ Coordinator ------------
def mapreduce_wordcount(dataset_url: str):
    # 1) Download and select the single source file (first .txt)
    files = download(dataset_url)
    path = files[0]
    hosts = worker_hosts()
    n_workers = len(hosts)

    # 2) Chunk the file on disk (workers read by path)
    chunk_paths = chunk_file_to_paths(path, target_bytes=32 * 1024 * 1024)

    # 3) MAP PHASE with timeout & reassignment
    print(f"[coordinator] starting MAP on {n_workers} workers ...")
    task_queue = collections.deque([(i, chunk_paths[i]) for i in range(len(chunk_paths))])
    inflight, assigned_to = {}, {}
    map_results = []
    free_hosts = collections.deque(hosts)

    def assign_next():
        if task_queue and free_hosts:
            tid, file_path = task_queue.popleft()
            host = free_hosts.popleft()
            inflight[tid] = TaskFuture(host, "map", file_path, NUM_REDUCERS)
            assigned_to[tid] = host
            print(f"[coordinator] dispatched MAP task {tid} -> {host}")

    for _ in range(min(len(task_queue), len(free_hosts))): assign_next()

    while inflight or task_queue:
        finished = []
        for tid, fut in list(inflight.items()):
            if fut.ready():
                if fut.exc:
                    print(f"[coordinator] MAP task {tid} on {fut.host} failed: {fut.exc}")
                    task_queue.appendleft((tid, chunk_paths[tid]))
                else:
                    map_results.append(fut.result)
                    print(f"[coordinator] MAP task {tid} done on {fut.host}")
                finished.append(tid)
                free_hosts.append(assigned_to[tid])
            elif fut.timed_out():
                print(f"[coordinator] MAP task {tid} timed out on {fut.host} — reassigning")
                finished.append(tid)
                task_queue.appendleft((tid, chunk_paths[tid]))
                free_hosts.append(assigned_to[tid])

        for tid in finished:
            inflight.pop(tid, None)
            assigned_to.pop(tid, None)

        while task_queue and free_hosts: assign_next()
        time.sleep(0.1)

    # 4) SHUFFLE
    print(f"[coordinator] shuffle phase ...")
    grouped_by_part = [collections.defaultdict(int) for _ in range(NUM_REDUCERS)]
    for part_dict in map_results:
        for p, wc in part_dict.items():
            for w, c in wc.items():
                grouped_by_part[p][w] += c

    reducer_payloads = [{str(k): int(v) for k, v in d.items()} for d in grouped_by_part]

    # 5) REDUCE PHASE with timeout & reassignment
    print(f"[coordinator] starting REDUCE on {n_workers} workers ...")
    task_queue = collections.deque(list(enumerate(reducer_payloads)))
    inflight.clear()
    assigned_to.clear()
    free_hosts = collections.deque(hosts)
    reduced_parts = {}

    def assign_reduce():
        if task_queue and free_hosts:
            pid, payload = task_queue.popleft()
            host = free_hosts.popleft()
            inflight[pid] = TaskFuture(host, "reduce", payload, int(pid))
            assigned_to[pid] = host
            print(f"[coordinator] dispatched REDUCE part {pid} -> {host}")

    for _ in range(min(len(task_queue), len(free_hosts))): assign_reduce()

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

        while task_queue and free_hosts: assign_reduce()
        time.sleep(0.1)

    # 6) FINAL AGGREGATION
    final = collections.Counter()
    for pid in range(NUM_REDUCERS):
        final.update(reduced_parts[pid])
    return final

# ------------ CLI entry ------------
if __name__ == "__main__":
    dataset_url = sys.argv[1] if len(sys.argv) > 1 else "https://mattmahoney.net/dc/enwik9.zip"
    start = time.time()
    counts = mapreduce_wordcount(dataset_url)
    top20 = sorted(counts.items(), key=lambda kv: (-kv[1], -len(kv[0])))[:20]
    print("\nTOP 20 WORDS BY FREQUENCY")
    for i, (w, c) in enumerate(top20, 1):
        print(f"{i:>2}. {w} -> {c}")
    
    elapsed = time.time() - start
    print(f"\nElapsed Time: {elapsed:.2f} seconds")