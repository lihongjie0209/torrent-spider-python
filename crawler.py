import asyncio
import json
import os
import random
import time
import signal
from dataclasses import dataclass, field
from typing import Optional, Set, Dict, Any, List
from types import SimpleNamespace
import click
try:
    from pybloom_live import ScalableBloomFilter
except ImportError:
    ScalableBloomFilter = None

try:
    import libtorrent as lt
except ImportError as e:  # Fallback stub so the file imports for documentation even if libtorrent missing
    print(f"libtorrent import failed: {e}. Please install libtorrent to run the crawler.")
    lt = None

# -------- Configuration Dataclasses --------
@dataclass
class DHTNode:
    index: int
    port: int
    session: Any = field(init=False, default=None)
    started: bool = field(init=False, default=False)

    def start(self):
        if lt is None:
            raise RuntimeError("libtorrent not installed. Please pip install libtorrent.")
        # libtorrent 2.x uses dict-based settings
        settings = {
            'listen_interfaces': f'0.0.0.0:{self.port}',
            'enable_dht': True,
            'enable_lsd': False,
            'enable_upnp': False,
            'enable_natpmp': False,
            'alert_mask': lt.alert.category_t.dht_notification |
                         lt.alert.category_t.status_notification |
                         lt.alert.category_t.error_notification,
            'active_downloads': 3,
            'active_seeds': 1,
            'active_limit': 10,
        }
        self.session = lt.session(settings)
        # Add DHT bootstrap nodes (host and port as separate arguments)
        self.session.add_dht_router("router.bittorrent.com", 6881)
        self.session.add_dht_router("router.utorrent.com", 6881)
        self.session.add_dht_router("dht.libtorrent.org", 25401)
        self.started = True

    def pop_alerts(self):
        if not self.started:
            return []
        return self.session.pop_alerts()


class StatsCollector:
    def __init__(self, node_count: int):
        self.node_infohash_counts: List[int] = [0] * node_count
        self.global_infohash_count: int = 0
        self.download_success: int = 0
        self.download_failure: int = 0
        self.download_started: int = 0

    def record_announce(self, node_index: int):
        self.global_infohash_count += 1
        self.node_infohash_counts[node_index] += 1

    def record_download_start(self):
        self.download_started += 1

    def record_download_result(self, success: bool):
        if success:
            self.download_success += 1
        else:
            self.download_failure += 1

    def snapshot(self):
        return {
            'global_infohash_count': self.global_infohash_count,
            'node_infohash_counts': self.node_infohash_counts[:],
            'download_success': self.download_success,
            'download_failure': self.download_failure,
            'download_started': self.download_started,
        }


class InfoHashQueue:
    def __init__(self, bloom: Optional[ScalableBloomFilter] = None):
        self._seen: Set[str] = set()  # in-memory quick set for recent items
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._bloom = bloom

    async def add(self, ih: str):
        if ih in self._seen:
            return
        if self._bloom and ih in self._bloom:
            return
        self._seen.add(ih)
        if self._bloom:
            self._bloom.add(ih)
        await self._queue.put(ih)

    async def get(self) -> str:
        return await self._queue.get()

    def task_done(self):
        self._queue.task_done()

    def size(self):
        return self._queue.qsize()

    def total_seen(self):
        return len(self._seen) if not self._bloom else len(self._bloom)


class JSONLWriter:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
        self._lock = asyncio.Lock()

    async def write(self, obj: Dict[str, Any]):
        line = json.dumps(obj, ensure_ascii=False)
        async with self._lock:
            with open(self.path, 'a', encoding='utf-8') as f:
                f.write(line + '\n')


class Downloader:
    def __init__(self, concurrency: int, results: JSONLWriter, session: Any, stats: StatsCollector, max_torrents: Optional[int] = None):
        if lt is None:
            raise RuntimeError("libtorrent not installed. Please pip install libtorrent.")
        self.concurrency = concurrency
        self.results = results
        self.max_torrents = max_torrents
        self.session = session  # reuse existing DHT-warmed session
        self._active: Dict[str, Any] = {}
        self._semaphore = asyncio.Semaphore(concurrency)
        self._stats = stats

    async def download(self, infohash: str, timeout: int = 300):
        async with self._semaphore:
            if self.max_torrents and len(self._active) >= self.max_torrents:
                await self.results.write({
                    "infohash": infohash,
                    "timestamp": int(time.time()),
                    "status": "failed",
                    "failure_reason": "max_torrents_limit"
                })
                return
            try:
                self._stats.record_download_start()
                ih = lt.sha1_hash(bytes.fromhex(infohash))
                params = lt.add_torrent_params()
                params.info_hash = ih
                params.flags |= lt.add_torrent_params_flags.flag_seed_mode  # start minimal
                handle = self.session.add_torrent(params)
                self._active[infohash] = handle
                start_time = time.time()
                while True:
                    await asyncio.sleep(1)
                    alerts = self.session.pop_alerts()
                    for a in alerts:
                        if isinstance(a, lt.torrent_error_alert):
                            if a.handle == handle:
                                raise RuntimeError(str(a.message()))
                    s = handle.status()
                    if s.has_metadata:
                        ti = handle.get_torrent_info()
                        files: List[Dict[str, Any]] = []
                        for f in ti.files():
                            files.append({"path": f.path, "size": f.size})
                        total_size = ti.total_size()
                        await self.results.write({
                            "infohash": infohash,
                            "timestamp": int(time.time()),
                            "status": "success",
                            "name": ti.name(),
                            "files": files,
                            "total_size": total_size,
                            "failure_reason": None
                        })
                        self._stats.record_download_result(True)
                        break
                    if time.time() - start_time > timeout:
                        raise TimeoutError("metadata_timeout")
            except Exception as e:
                self._stats.record_download_result(False)
                await self.results.write({
                    "infohash": infohash,
                    "timestamp": int(time.time()),
                    "status": "failed",
                    "failure_reason": str(e)
                })
            finally:
                if infohash in self._active:
                    try:
                        self.session.remove_torrent(self._active[infohash])
                    except Exception:
                        pass
                    del self._active[infohash]


async def node_alert_collector(nodes: List[DHTNode], info_queue: InfoHashQueue, stats: StatsCollector, batch_interval: float = 0.2):
    """Poll alerts from all nodes and push discovered infohashes."""
    while True:
        for n in nodes:
            for alert in n.pop_alerts():
                # dht_announce_alert has info_hash attribute
                if lt and isinstance(alert, lt.dht_announce_alert):
                    ih_bytes = alert.info_hash.to_bytes()
                    ih_hex = ih_bytes.hex()
                    stats.record_announce(n.index)
                    await info_queue.add(ih_hex)
        await asyncio.sleep(batch_interval)


async def download_worker(name: str, queue: InfoHashQueue, downloader: Downloader):
    while True:
        ih = await queue.get()
        await downloader.download(ih)
        queue.task_done()


async def periodic_stats(args, info_queue: InfoHashQueue, downloader: Downloader, stats: StatsCollector, interval: int, top_n: int, stop_event: asyncio.Event):
    prev = stats.snapshot()
    start_time = time.time()
    prev_unique = info_queue.total_seen()
    while not stop_event.is_set():
        await asyncio.sleep(interval)
        snap = stats.snapshot()
        dt = time.time() - start_time
        global_rate = (snap['global_infohash_count'] - prev['global_infohash_count']) / interval
        unique_total = info_queue.total_seen()
        unique_delta = unique_total - prev_unique
        global_delta = snap['global_infohash_count'] - prev['global_infohash_count']
        duplicate_total = snap['global_infohash_count'] - unique_total
        duplicate_delta = global_delta - unique_delta
        duplicate_rate = duplicate_delta / interval if interval > 0 else 0.0
        duplicate_pct_interval = (duplicate_delta / global_delta * 100.0) if global_delta > 0 else 0.0
        duplicate_pct_total = (duplicate_total / snap['global_infohash_count'] * 100.0) if snap['global_infohash_count'] > 0 else 0.0
        download_count_delta = (snap['download_started'] - prev['download_started'])
        download_rate = download_count_delta / interval
        success_delta = snap['download_success'] - prev['download_success']
        failure_delta = snap['download_failure'] - prev['download_failure']
        # top sessions by cumulative count
        indexed_counts = list(enumerate(snap['node_infohash_counts']))
        indexed_counts.sort(key=lambda x: x[1], reverse=True)
        top = indexed_counts[:top_n]
        # compute per-session rate based on delta from prev snapshot
        prev_counts = prev['node_infohash_counts']
        top_display = []
        for idx, total in top:
            delta = total - prev_counts[idx]
            rate = delta / interval
            top_display.append(f"{idx}:{total}:{rate:.1f}")
        print(
            f"[STAT] queue={info_queue.size()} active={len(downloader._active)} ann_total={snap['global_infohash_count']} ann_rate={global_rate:.1f}/s "
            f"unique_total={unique_total} dup_total={duplicate_total} dup_rate={duplicate_rate:.2f}/s dup_pct_int={duplicate_pct_interval:.1f}% dup_pct_all={duplicate_pct_total:.1f}% "
            f"downloads(total={snap['download_started']} rate={download_rate:.2f}/s success={snap['download_success']}+{success_delta} failure={snap['download_failure']}+{failure_delta}) "
            f"top_sessions[{','.join(top_display)}]"
        )
        prev = snap
        start_time = time.time()
        prev_unique = unique_total


async def periodic_bloom_flush(bloom: Optional[ScalableBloomFilter], path: Optional[str], interval: int, stop_event: asyncio.Event):
    if not bloom or not path:
        return
    while not stop_event.is_set():
        await asyncio.sleep(interval)
        try:
            with open(path, 'wb') as f:
                bloom.tofile(f)
            print(f"[FLUSH] bloom saved -> {path}")
        except Exception as e:
            print(f"[FLUSH][ERROR] {e}")


async def main(args):
    if lt is None:
        raise RuntimeError("libtorrent not installed. Install before running crawler.")
    random.seed(time.time())

    # Create DHT nodes gradually to reduce resource spike
    nodes: List[DHTNode] = []
    for i in range(args.nodes):
        port = args.listen_start + i
        node = DHTNode(index=i, port=port)
        node.start()
        nodes.append(node)
        # Stagger startup
        if (i + 1) % 50 == 0:
            await asyncio.sleep(1.0)

    # Bloom filter persistence load/create
    bloom = None
    if args.bloom_file:
        if ScalableBloomFilter is None:
            print("pybloom_live not installed; bloom disabled")
        else:
            if os.path.exists(args.bloom_file):
                try:
                    with open(args.bloom_file, 'rb') as f:
                        bloom = ScalableBloomFilter.fromfile(f)
                    print(f"[BLOOM] Loaded existing bloom from {args.bloom_file}")
                except Exception as e:
                    print(f"[BLOOM] Failed load existing: {e}; creating new")
                    bloom = ScalableBloomFilter(initial_capacity=args.bloom_capacity, error_rate=args.bloom_error_rate)
            else:
                bloom = ScalableBloomFilter(initial_capacity=args.bloom_capacity, error_rate=args.bloom_error_rate)
                print(f"[BLOOM] New bloom created capacity={args.bloom_capacity} error_rate={args.bloom_error_rate}")

    info_queue = InfoHashQueue(bloom=bloom)
    results_writer = JSONLWriter(args.results)
    # Reuse first node session for downloads (already warmed with DHT routers)
    stats = StatsCollector(len(nodes))
    downloader = Downloader(args.download_concurrency, results_writer, session=nodes[0].session, stats=stats, max_torrents=args.max_torrents)

    # Workers
    workers = [asyncio.create_task(download_worker(f"worker-{i}", info_queue, downloader)) for i in range(args.download_concurrency)]
    collector_task = asyncio.create_task(node_alert_collector(nodes, info_queue, stats))

    stop_event = asyncio.Event()

    stats_task = asyncio.create_task(periodic_stats(args, info_queue, downloader, stats, args.stats_interval, args.top_sessions, stop_event))
    flush_task = asyncio.create_task(periodic_bloom_flush(bloom, args.bloom_file, args.flush_interval, stop_event))

    loop = asyncio.get_running_loop()
    def request_shutdown():
        print("[SIGNAL] shutdown requested")
        stop_event.set()
    for sig in (getattr(signal, 'SIGINT', None), getattr(signal, 'SIGTERM', None)):
        if sig is not None:
            try:
                loop.add_signal_handler(sig, request_shutdown)
            except NotImplementedError:
                pass  # Windows may not support all

    try:
        await stop_event.wait()
    finally:
        collector_task.cancel()
        stats_task.cancel()
        flush_task.cancel()
        for w in workers:
            w.cancel()
        # Final bloom flush
        if bloom and args.bloom_file:
            try:
                with open(args.bloom_file, 'wb') as f:
                    bloom.tofile(f)
                print(f"[FLUSH] final bloom saved -> {args.bloom_file}")
            except Exception as e:
                print(f"[FLUSH][ERROR] final save failed: {e}")


@click.command(help="Passive DHT crawler using libtorrent (multiple lightweight DHT sessions).")
@click.option('--nodes', type=int, default=1000, show_default=True, help='Number of DHT sessions to create')
@click.option('--listen-start', type=int, default=6880, show_default=True, help='Starting port for DHT nodes (incremented per node)')
@click.option('--download-concurrency', type=int, default=10, show_default=True, help='Concurrent download workers')
@click.option('--results', type=click.Path(dir_okay=False, writable=True), default='results.jsonl', show_default=True, help='JSON Lines output path')
@click.option('--max-torrents', type=int, default=None, help='Limit active torrents in downloader session')
@click.option('--stats-interval', type=int, default=10, show_default=True, help='Seconds between stats output')
@click.option('--bloom-file', type=click.Path(dir_okay=False, writable=True), default='bloom.bin', show_default=True, help='Bloom filter persistence file')
@click.option('--bloom-capacity', type=int, default=500000, show_default=True, help='Initial bloom capacity (approx elements)')
@click.option('--bloom-error-rate', type=float, default=0.001, show_default=True, help='Bloom filter error rate')
@click.option('--flush-interval', type=int, default=120, show_default=True, help='Seconds between bloom periodic flush')
@click.option('--top-sessions', type=int, default=10, show_default=True, help='Top N sessions to display in stats')
def cli(nodes, listen_start, download_concurrency, results, max_torrents, stats_interval, bloom_file, bloom_capacity, bloom_error_rate, flush_interval, top_sessions):
    args = SimpleNamespace(nodes=nodes, listen_start=listen_start,
                           download_concurrency=download_concurrency,
                           results=results, max_torrents=max_torrents,
                           stats_interval=stats_interval, bloom_file=bloom_file,
                           bloom_capacity=bloom_capacity, bloom_error_rate=bloom_error_rate,
                           flush_interval=flush_interval, top_sessions=top_sessions)
    try:
        asyncio.run(main(args))
    except RuntimeError as e:
        print(f"Error: {e}")


if __name__ == '__main__':
    cli()
