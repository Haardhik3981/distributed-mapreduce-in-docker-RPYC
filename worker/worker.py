import os, re, hashlib, collections, rpyc, json
from rpyc.utils.server import ThreadedServer
from rpyc.utils.classic import obtain
from rpyc.core.netref import BaseNetref

RPYC_PORT = int(os.getenv("RPYC_PORT", "18861"))
WORD = re.compile(r"[A-Za-z']+")
DATA_DIR = os.getenv("DATA_DIR", "/data")  # shared volume mount

def partition(word: str, num_reducers: int) -> int:
    return int(hashlib.md5(word.encode()).hexdigest(), 16) % num_reducers

class MapReduceService(rpyc.Service):
    def exposed_map(self, file_path: str, num_reducers: int) -> dict:
        counts = collections.Counter()
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                counts.update(w.lower() for w in WORD.findall(line))
        parts = {i: {} for i in range(num_reducers)}
        for w, c in counts.items():
            parts[partition(w, num_reducers)][w] = int(c)
        return parts

    def exposed_reduce(self, grouped, part_id: int):
        if isinstance(grouped, BaseNetref):
            grouped = obtain(grouped)
        os.makedirs(os.path.join(DATA_DIR, "out"), exist_ok=True)

        out = collections.Counter()
        for w, c in grouped.items():
            out[w] += int(c)
        result = dict(out)

        out_path = os.path.join(DATA_DIR, "out", f"reduce-{part_id}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False)
        return result

if __name__ == "__main__":
    print("[worker] RPyC listening...")
    ThreadedServer(
        MapReduceService,
        port=RPYC_PORT,
        protocol_config={"allow_pickle": True}
    ).start()