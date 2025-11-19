# DHT Torrent Crawler (Passive + Active)

使用 libtorrent Python 绑定实现 DHT 爬虫，支持多种 infohash 发现方式：
- 创建 1000 个 DHT 节点（session），均匀加入 DHT 网络（分批渐进启动避免端口风暴）
- **被动监听 DHT announce**：其他客户端主动通告的 infohash
- **监听 get_peers 查询**：捕获别人正在搜索的 infohash
- **主动采样（BEP 51）**：周期性向 DHT 节点请求随机 infohash 样本
- 将 infohash 加入异步下载队列执行下载
- 下载成功与失败结果统一写入 JSON Lines 文件（`results.jsonl`）
- Bloom Filter 去重，避免重复处理相同 infohash

## 运行环境准备
1. Python 3.10+ (已在本项目使用 3.12)
2. 安装依赖：

```pwsh
pip install -r requirements.txt
```

### Windows 重要说明 - libtorrent DLL 问题

Windows 下 `libtorrent` 可能遇到 DLL 加载失败 (`ImportError: DLL load failed`)，原因：
1. **缺少 Visual C++ 运行库**：下载安装 [Microsoft Visual C++ Redistributable](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist)（推荐安装 x64 版本的最新版）
2. **libtorrent 依赖其他系统库**：如 Boost、OpenSSL 等

**解决方案**：
- **方案1（推荐）**：在 Linux/WSL2 环境运行本项目，`libtorrent` 在 Linux 下支持更好
- **方案2**：使用 conda 环境安装 libtorrent（conda 会自动处理依赖）:
  ```pwsh
  conda create -n torrent python=3.12
  conda activate torrent
  conda install -c conda-forge libtorrent
  pip install click pybloom_live==4.0.0
  ```
- **方案3**：手动编译或寻找第三方预编译 wheel（不推荐，复杂度高）

**验证安装**：
```pwsh
python -c "import libtorrent as lt; print(lt.__version__)"
```
如能正常输出版本号（如 `2.0.11`），说明安装成功。

## 使用方法
```pwsh
python crawler.py --nodes 1000 --listen-start 6880 --download-concurrency 10 --results results.jsonl
```

主要参数说明：
- `--nodes`: 创建的 DHT session 数量，默认 1000
- `--listen-start`: 监听端口起始值，默认 6880（每个节点递增）
- `--download-concurrency`: 同时下载的最大种子数，默认 10
- `--results`: 输出 JSONL 文件路径，默认 `results.jsonl`
- `--max-torrents`: （可选）限制同时活跃的 torrent 数量
- `--sample-interval`: DHT 采样间隔（秒），默认 60（BEP 51 主动采样）
- `--stats-interval`: 统计信息输出间隔（秒），默认 10
- `--bloom-file`: Bloom Filter 持久化文件路径，默认 `bloom.bin`
- `--bloom-capacity`: Bloom Filter 初始容量，默认 500000
- `--flush-interval`: Bloom Filter 刷盘间隔（秒），默认 120
- `--top-sessions`: 统计中显示的 Top N 节点数，默认 10

查看所有参数：
```pwsh
python crawler.py --help
```

## 输出 JSONL 格式
每行一个 JSON 对象：
```json
{
  "infohash": "abcdef0123456789abcdef0123456789abcdef01",
  "timestamp": 1731993600,
  "status": "success",  // 或 failed
  "name": "Example Torrent",
  "files": [
    {"path": "folder/file.txt", "size": 12345}
  ],
  "total_size": 12345,
  "failure_reason": null
}
```
失败时：
```json
{
  "infohash": "...",
  "timestamp": 1731993600,
  "status": "failed",
  "failure_reason": "timeout"
}
```

## 功能特性
### 多源 infohash 发现
1. **被动 announce 监听**：捕获其他客户端发布到 DHT 的 announce 消息
2. **get_peers 查询监听**：监控网络中正在被搜索的 infohash
3. **BEP 51 主动采样**：周期性向 DHT 节点请求随机 infohash 样本，显著提升发现速率
   - 使用 `session.dht_sample_infohashes(endpoint, target)` API
   - 接收 `dht_sample_infohashes_alert` 响应，提取多个样本 infohash
   - 需要 libtorrent 2.0.9+ 支持（如版本不支持会自动跳过）

### 去重与统计
- Bloom Filter 高效去重（支持持久化与自动扩容）
- 实时统计：全局/节点级别 infohash 速率、下载成功/失败率、重复率
- Top N 节点排名展示

### 性能优化
- 异步下载队列，避免阻塞 DHT 监听
- 复用第一个 DHT 节点进行元数据下载，减少资源开销
- 随机 node_id 分布，提高 DHT 覆盖率

## 注意事项
- 启动 1000 个 libtorrent session 资源占用较大，确保机器有足够端口与内存。
- 可以通过减少 `--nodes` 或增大启动间隔（源码中可调整）来降低压力。
- BEP 51 采样功能需要 libtorrent 2.0.9+ 支持（如不支持会自动跳过）。
- Bloom Filter 持久化文件随时间增长，建议定期清理或重建。

## 后续改进建议
- 使用单 session 多 socket 的方式（自行打补丁）以减少资源占用。
- 增加持久化数据库（如 SQLite）代替 JSONL。
- 增加 Redis 分布式去重支持。
- 增加动态自适应下载并发控制策略。
- 支持增量恢复（从 Bloom Filter 和 JSONL 恢复状态）。
