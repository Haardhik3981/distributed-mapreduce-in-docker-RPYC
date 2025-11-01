# worker.py (lean version)
import os, re, hashlib, collections, rpyc
from rpyc.utils.server import ThreadedServer
from rpyc.utils.classic import obtain
from rpyc.core.netref import BaseNetref

RPYC_PORT = int(os.getenv("RPYC_PORT", "18861"))
WORD = re.compile(r"[A-Za-z']+")

def partition(word: str, num_reducers: int) -> int:
    return int(hashlib.md5(word.encode()).hexdigest(), 16) % num_reducers

class MapReduceService(rpyc.Service):
    def exposed_map(self, text: str, num_reducers: int) -> dict:
        counts = collections.Counter(w.lower() for w in WORD.findall(text))
        parts = {i: {} for i in range(num_reducers)}
        for w, c in counts.items():
            parts[partition(w, num_reducers)][w] = int(c)
        return parts

    def exposed_reduce(self, grouped):  # expects {word:int}
        if isinstance(grouped, BaseNetref):
            grouped = obtain(grouped)
        # grouped is a plain dict now
        out = collections.Counter()
        for w, c in grouped.items():
            out[w] += int(c)
        return dict(out)

if __name__ == "__main__":
    print("[worker] RPyC listening...")
    ThreadedServer(
        MapReduceService, port=RPYC_PORT,
        protocol_config={"allow_pickle": True}
    ).start()