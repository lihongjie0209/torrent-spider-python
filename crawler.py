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
        # Generate random DHT node_id for better network distribution
        # Spread nodes across DHT keyspace for maximum coverage
        node_id = os.urandom(20)  # 160-bit random ID
        
        # libtorrent 2.x uses dict-based settings
        settings = {
            'listen_interfaces': f'0.0.0.0:{self.port}',
            'enable_dht': True,
            'enable_lsd': False,
            'enable_upnp': False,
            'enable_natpmp': False,
            'dht_bootstrap_nodes': '',  # We'll add routers manually
            'peer_fingerprint': node_id[:20],  # Use as peer_id prefix for uniqueness
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
    
    def request_dht_sample(self, endpoint):
        """Send a sample_infohashes request to a remote DHT endpoint if supported.

        endpoint: tuple[str, int] (ip, port)
        """
        if not self.started or lt is None:
            return
        if not hasattr(self.session, 'dht_sample_infohashes'):
            return
        try:
            # random 20-byte target for key-space traversal hint (value itself not used by sampler response content)
            target = lt.sha1_hash(os.urandom(20))
            self.session.dht_sample_infohashes(endpoint, target)
        except Exception:
            pass  # Ignore failures (unsupported endpoint/IP version etc.)


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
    def __init__(self, bloom: Optional[Any] = None):
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
                # Do not persist failures; just log to console per new requirement
                print(f"[DL-FAIL] {infohash} skipped: max_torrents_limit")
                self._stats.record_download_result(False)
                return
            try:
                self._stats.record_download_start()
                # In libtorrent 2.x, use dict-based add_torrent_params
                # info_hashes expects sha1_hash object, not string
                params = {
                    'info_hashes': lt.info_hash_t(lt.sha1_hash(bytes.fromhex(infohash))),
                    'save_path': '/tmp/torrents',  # temp path, we only want metadata
                }
                handle = self.session.add_torrent(params)
                self._active[infohash] = handle
                start_time = time.time()
                while True:
                    await asyncio.sleep(1)
                    alerts = self.session.pop_alerts()
                    for a in alerts:
                        # Check for error alerts by category instead of type
                        if a.category() & lt.alert.category_t.error_notification:
                            if hasattr(a, 'handle') and a.handle == handle:
                                raise RuntimeError(a.message())
                    s = handle.status()
                    if s.has_metadata:
                        ti = handle.torrent_file()
                        files: List[Dict[str, Any]] = []
                        # In libtorrent 2.x, iterate files by index
                        fs = ti.files()
                        for i in range(fs.num_files()):
                            files.append({"path": fs.file_path(i), "size": fs.file_size(i)})
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
                print(f"[DL-FAIL] {infohash} error: {e}")
            finally:
                if infohash in self._active:
                    try:
                        self.session.remove_torrent(self._active[infohash])
                    except Exception:
                        pass
                    del self._active[infohash]


@dataclass
class RemoteNodeEntry:
    endpoint: tuple
    last_seen: float = field(default_factory=lambda: time.time())
    next_request: float = field(default_factory=lambda: time.time())


class RemoteNodeRegistry:
    """Maintains a registry of remote DHT nodes discovered via alerts for BEP 51 sampling."""
    def __init__(self, prune_interval: int = 1800, min_request_interval: int = 10):
        self._nodes: Dict[tuple, RemoteNodeEntry] = {}
        self._lock = asyncio.Lock()
        self._prune_interval = prune_interval
        self._min_request_interval = min_request_interval
        self._last_prune = time.time()

    async def add_or_update(self, endpoint: tuple):
        async with self._lock:
            entry = self._nodes.get(endpoint)
            now = time.time()
            if entry is None:
                self._nodes[endpoint] = RemoteNodeEntry(endpoint=endpoint, last_seen=now, next_request=now)
            else:
                entry.last_seen = now

    async def schedule_from_sample_alert(self, endpoint: tuple, interval_seconds: int):
        async with self._lock:
            entry = self._nodes.get(endpoint)
            if entry:
                # Respect node-provided interval but enforce a minimum to avoid spamming
                wait = max(interval_seconds, self._min_request_interval)
                entry.next_request = time.time() + wait

    async def get_due_endpoints(self, limit: int = 50) -> List[tuple]:
        async with self._lock:
            now = time.time()
            due = [e for e, v in self._nodes.items() if v.next_request <= now]
            # simple fairness: earliest next_request first
            due.sort(key=lambda ep: self._nodes[ep].next_request)
            return due[:limit]

    async def mark_requested(self, endpoint: tuple):
        async with self._lock:
            entry = self._nodes.get(endpoint)
            if entry:
                # provisional push forward – will be refined by sample response alert
                entry.next_request = time.time() + self._min_request_interval

    async def prune(self):
        async with self._lock:
            now = time.time()
            if now - self._last_prune < 300:
                return
            self._last_prune = now
            to_delete = [ep for ep, v in self._nodes.items() if now - v.last_seen > self._prune_interval]
            for ep in to_delete:
                del self._nodes[ep]


async def node_alert_collector(nodes: List[DHTNode], info_queue: InfoHashQueue, stats: StatsCollector, registry: RemoteNodeRegistry, batch_interval: float = 0.2, expand_random_per: int = 0):
    """Poll alerts from all nodes, push discovered infohashes, collect remote endpoints."""
    while True:
        for n in nodes:
            for alert in n.pop_alerts():
                if not (lt and hasattr(alert, 'category')):
                    continue
                alert_type = alert.what().lower()

                # Collect remote endpoints from packet-level alerts (dht_pkt_alert) if available
                if 'dht_pkt' in alert_type:
                    # Some builds expose .node (udp endpoint) or .endpoint
                    ep = None
                    if hasattr(alert, 'node'):
                        ep_obj = getattr(alert, 'node')
                        try:
                            ep = (ep_obj.address, ep_obj.port)
                        except Exception:
                            pass
                    elif hasattr(alert, 'endpoint'):
                        ep_obj = getattr(alert, 'endpoint')
                        try:
                            ep = (ep_obj.address, ep_obj.port)
                        except Exception:
                            pass
                    if ep:
                        await registry.add_or_update(ep)
                    continue  # pkt alerts generally not carrying infohash

                if alert.category() & lt.alert.category_t.dht_notification:
                    info_hash = None

                    # Announce & get_peers discovery
                    if hasattr(alert, 'info_hash') and ('announce' in alert_type or 'get_peers' in alert_type):
                        info_hash = alert.info_hash
                        # Seed registry with announcer's endpoint to bootstrap BEP51
                        if 'announce' in alert_type:
                            try:
                                if hasattr(alert, 'ip') and hasattr(alert, 'port'):
                                    ip = str(getattr(alert, 'ip'))
                                    port = int(getattr(alert, 'port'))
                                    if ip and port > 0:
                                        await registry.add_or_update((ip, port))
                            except Exception:
                                pass

                    # Sample infohashes response
                    elif alert_type == 'dht_sample_infohashes_alert' and hasattr(alert, 'samples'):
                        # Update scheduling for this remote node
                        if hasattr(alert, 'endpoint') and hasattr(alert, 'interval'):
                            try:
                                ep_obj = alert.endpoint
                                ep = (ep_obj.address, ep_obj.port)
                                interval_secs = int(getattr(alert, 'interval').total_seconds()) if hasattr(alert.interval, 'total_seconds') else 60
                                await registry.add_or_update(ep)
                                await registry.schedule_from_sample_alert(ep, interval_secs)
                            except Exception:
                                pass
                        try:
                            for sample in alert.samples():
                                sample_hex = bytes(sample).hex()
                                stats.record_announce(n.index)
                                await info_queue.add(sample_hex)
                        except Exception:
                            pass
                        continue

                    elif hasattr(alert, 'info_hash'):
                        info_hash = alert.info_hash

                        if info_hash:
                            ih_hex = bytes(info_hash).hex()
                            stats.record_announce(n.index)
                            await info_queue.add(ih_hex)
                            # Random expansion: generate synthetic infohashes to probe network / fill queue
                            if expand_random_per > 0:
                                for _ in range(expand_random_per):
                                    synthetic = os.urandom(20).hex()
                                    await info_queue.add(synthetic)
        await registry.prune()
        await asyncio.sleep(batch_interval)


async def dht_sample_requester(nodes: List[DHTNode], registry: RemoteNodeRegistry, interval: int, stop_event: asyncio.Event):
    """Periodically send sample_infohashes requests to discovered remote nodes.

    interval: fallback interval if registry empty
    """
    await asyncio.sleep(30)  # bootstrap grace period
    target_node = nodes[0]  # use first local session for outbound sample queries
    while not stop_event.is_set():
        due_eps = await registry.get_due_endpoints()
        if not due_eps:
            # No endpoints yet – retry after fallback interval
            await asyncio.sleep(interval)
            continue
        for ep in due_eps:
            target_node.request_dht_sample(ep)
            await registry.mark_requested(ep)
        # Short sleep to avoid tight loop; actual pacing controlled by per-node next_request
        await asyncio.sleep(1.0)


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


async def periodic_bloom_flush(bloom: Optional[Any], path: Optional[str], interval: int, stop_event: asyncio.Event):
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


async def periodic_queue_refill(queue: InfoHashQueue, interval: int, batch_size: int, stop_event: asyncio.Event):
    """Refill queue with random synthetic infohashes if it becomes empty.

    This helps keep download workers active during early cold start or sparse network periods.
    """
    while not stop_event.is_set():
        await asyncio.sleep(interval)
        if queue.size() == 0:
            for _ in range(batch_size):
                synthetic = os.urandom(20).hex()
                await queue.add(synthetic)
            print(f"[REFILL] Added {batch_size} synthetic infohashes (queue was empty)")


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
    registry = RemoteNodeRegistry()
    collector_task = asyncio.create_task(node_alert_collector(nodes, info_queue, stats, registry, expand_random_per=args.expand_random_per))

    stop_event = asyncio.Event()

    stats_task = asyncio.create_task(periodic_stats(args, info_queue, downloader, stats, args.stats_interval, args.top_sessions, stop_event))
    flush_task = asyncio.create_task(periodic_bloom_flush(bloom, args.bloom_file, args.flush_interval, stop_event))
    sampler_task = asyncio.create_task(dht_sample_requester(nodes, registry, args.sample_interval, stop_event))

    # Initial random seeding
    for _ in range(args.initial_random):
        seed_ih = os.urandom(20).hex()
        await info_queue.add(seed_ih)

    # Periodic queue refill task
    refill_task = asyncio.create_task(periodic_queue_refill(info_queue, args.refill_interval, args.refill_batch, stop_event))

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
        sampler_task.cancel()
        refill_task.cancel()
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
@click.option('--sample-interval', type=int, default=60, show_default=True, help='Seconds between DHT sample requests (BEP 51)')
@click.option('--initial-random', type=int, default=10, show_default=True, help='Initial random synthetic infohash seeds to enqueue at startup')
@click.option('--expand-random-per', type=int, default=2, show_default=True, help='For each discovered infohash, generate this many random synthetic ones')
@click.option('--refill-interval', type=int, default=300, show_default=True, help='Seconds between checking for empty queue to refill')
@click.option('--refill-batch', type=int, default=50, show_default=True, help='Synthetic batch size when refilling empty queue')
def cli(nodes, listen_start, download_concurrency, results, max_torrents, stats_interval, bloom_file, bloom_capacity, bloom_error_rate, flush_interval, top_sessions, sample_interval, initial_random, expand_random_per, refill_interval, refill_batch):
    args = SimpleNamespace(nodes=nodes, listen_start=listen_start,
                           download_concurrency=download_concurrency,
                           results=results, max_torrents=max_torrents,
                           stats_interval=stats_interval, bloom_file=bloom_file,
                           bloom_capacity=bloom_capacity, bloom_error_rate=bloom_error_rate,
                           flush_interval=flush_interval, top_sessions=top_sessions,
                           sample_interval=sample_interval, initial_random=initial_random,
                           expand_random_per=expand_random_per, refill_interval=refill_interval,
                           refill_batch=refill_batch)
    try:
        asyncio.run(main(args))
    except RuntimeError as e:
        print(f"Error: {e}")


if __name__ == '__main__':
    cli()
