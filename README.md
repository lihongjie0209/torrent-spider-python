# Passive DHT Torrent Crawler

使用 libtorrent Python 绑定实现被动 DHT 爬虫：
- 创建 1000 个 DHT 节点（session），均匀加入 DHT 网络（分批渐进启动避免端口风暴）
- 被动监听 DHT announce，提取发现的 infohash
- 将 infohash 加入异步下载队列执行下载
- 下载成功与失败结果统一写入 JSON Lines 文件（`results.jsonl`）

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

参数说明：
- `--nodes`: 创建的 DHT session 数量，默认 1000
- `--listen-start`: 监听端口起始值，默认 6880（每个节点递增）
- `--download-concurrency`: 同时下载的最大种子数，默认 10
- `--results`: 输出 JSONL 文件路径
- `--max-torrents`: （可选）限制同时活跃的 torrent 数量

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

## 注意事项
- 启动 1000 个 libtorrent session 资源占用较大，确保机器有足够端口与内存。
- 可以通过减少 `--nodes` 或增大启动间隔（源码中可调整）来降低压力。
- 纯被动监听依赖其他客户端的 announce，数据速率不可控。

## 后续改进建议
- 使用单 session 多 socket 的方式（自行打补丁）以减少资源占用。
- 增加持久化数据库（如 SQLite）代替 JSONL。
- 增加 Bloom Filter / Redis 去重。
- 增加动态自适应下载并发控制策略。
