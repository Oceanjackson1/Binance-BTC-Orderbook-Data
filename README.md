# Binance BTC 订单簿数据采集系统

生产级 Python 系统，用于采集 Binance 现货（Spot）和 USD-M 永续合约（Futures）上 BTC/USDT 的**完整 L2 订单簿数据**，专为量化回溯研究设计。

同时提供**历史数据批量下载工具**，支持从 Binance 公开数据门户和 CoinGlass API 获取历史成交、深度及市场指标数据。

在实时采集之外，当前仓库还包含一套**可部署的数据发布栈**：可选写入 TimescaleDB、周期性物化 `publish/` 目录、通过 FastAPI 对外提供只读查询 API，并支持 Docker Compose、Nginx 反向代理、腾讯云 COS 备份。

---

## 目录

1. [数据类型与范畴说明](#1-数据类型与范畴说明)
2. [系统架构](#2-系统架构)
3. [快速开始](#3-快速开始)
4. [实时采集——Full L2 Order Book](#4-实时采集full-l2-order-book)
5. [历史数据下载——Binance 公开数据](#5-历史数据下载binance-公开数据)
6. [历史数据下载——CoinGlass Order Book 深度](#6-历史数据下载coinglass-order-book-深度)
7. [实时采集——现货 bookDepth](#7-实时采集现货-bookdepth)
8. [全部数据字段参考](#8-全部数据字段参考)
9. [回溯研究使用指南](#9-回溯研究使用指南)
10. [数据发布与只读 API（可选）](#10-数据发布与只读-api可选)
11. [Docker Compose / 腾讯云部署（可选）](#11-docker-compose--腾讯云部署可选)
12. [配置说明](#12-配置说明)
13. [TimescaleDB 与数据库表（可选）](#13-timescaledb-与数据库表可选)
14. [系统要求与存储估算](#14-系统要求与存储估算)
15. [已知限制](#15-已知限制)
16. [常见问题排查](#16-常见问题排查)

---

## 1. 数据类型与范畴说明

### 1.1 完整订单簿（Fully Order Book）

订单簿是交易所在某一时刻**所有未成交挂单**的集合，按价格档位聚合展示。

| 术语 | 含义 |
|------|------|
| **L1 数据** | 仅买一价/卖一价（Best Bid & Ask），深度最浅 |
| **L2 数据** | 按价格档位聚合的完整挂单深度（Price → Total Qty），不含单笔订单 ID |
| **L3 数据** | 每笔订单的完整信息（Order ID、队列位置、单笔数量），Binance 不提供 |

**本系统采集 L2 订单簿数据。**

#### "Fully（完整）"的具体含义

| 层面 | 现货 Spot | USD-M 合约 | 说明 |
|------|-----------|-----------|------|
| 初始快照（REST API） | 每侧最多 **5,000 档** | 每侧最多 **1,000 档** | 采集启动时一次性拉取 |
| 增量 Diff 流（WebSocket） | **无上限** | **无上限** | 任何档位变化均推送 |

初始快照 + 持续 Diff 事件流的组合，是 Binance 公开 API 所能提供的最完整 L2 订单簿采集方案。

### 1.2 本系统完整数据范畴

| 数据类型 | 来源 | 模式 | 说明 |
|----------|------|------|------|
| **L2 完整订单簿快照** | Binance WebSocket + REST | 实时采集 | 初始快照 + 每小时检查点，`snapshots_*.parquet` |
| **L2 订单簿增量 Diff** | Binance WebSocket | 实时采集 | 100ms 推送，无档位上限，`events_*.parquet` |
| **逐笔成交 trades** | Binance 公开数据 | 历史下载 | 每笔撮合记录，含价格/数量/方向 |
| **聚合成交 aggTrades** | Binance 公开数据 | 历史下载 | 合并同条件 trade，减少数据量 |
| **合约深度摘要 bookDepth** | Binance 公开数据 | 历史下载 | ±1-5% 累计深度，~30 秒一条 |
| **现货深度摘要 bookDepth** | Binance WebSocket + REST | 实时采集 | ±1-5% 累计深度，~30 秒一条（自行计算） |
| **现货 K 线 klines** | Binance 公开数据 | 历史下载 | OHLCV + Taker 买入量，1m/5m/30m/1h |
| **合约市场指标 metrics** | Binance 公开数据 | 历史下载 | 未平仓量、多空比等，5 分钟一条 |
| **Order Book 深度历史** | CoinGlass API | 历史下载 | ±1/2/3/5/10% 聚合深度，30 分钟一条，支持跨交易所 |

---

## 2. 系统架构

```
┌──────────────────────────────────────────────────────────────┐
│                       Binance 交易所                          │
│  现货 REST (api.binance.com)    现货 WS (stream.binance.com) │
│  合约 REST (fapi.binance.com)   合约 WS (fstream.b...com)    │
└────────────────────┬────────────────────────┬────────────────┘
                     │                        │
           ┌─────────▼──────────┐  ┌──────────▼──────────┐
           │   SpotCollector    │  │  FuturesCollector    │
           │  Binance 7步同步   │  │  Binance 7步同步     │
           │  间隙检测+指数退避  │  │  pu字段连续性校验    │
           └─────────┬──────────┘  └──────────┬──────────┘
                     │  asyncio.Queue          │
                     └──────────┬──────────────┘
                                │  maxsize=50,000
               ┌────────────────▼─────────────────┐
               │           EventFanout             │
               │ collector 事件复制到多写入通道      │
               └───────┬────────────────────┬──────┘
                       │                    │
          ┌────────────▼────────┐  ┌────────▼──────────────┐
          │    ParquetWriter    │  │   TimescaleWriter      │
          │ snapshots/events    │  │ raw + summary + health │
          └────────────┬────────┘  └────────┬──────────────┘
                       │                    │
          ┌────────────▼────────┐  ┌────────▼──────────────┐
          │ snapshots_*.parquet │  │     TimescaleDB        │
          │ events_*.parquet    │  │  events/snapshots/...  │
          └─────────────────────┘  └────────┬──────────────┘
                                            │
                                 ┌──────────▼───────────┐
                                 │ PublishMaterializer  │
                                 │ raw / curated / meta │
                                 └──────────┬─────┬─────┘
                                            │     │
                           ┌────────────────▼┐ ┌──▼──────────────┐
                           │   FastAPI API   │ │  BackupManager   │
                           │ Bearer 只读接口   │ │ COS + DB dump    │
                           └─────────────────┘ └──────────────────┘

  ┌────────────────────────────────────────────────────────────┐
  │              历史数据下载工具（独立运行）                     │
  │                                                            │
  │  download_historical.py     Binance公开数据门户              │
  │    ├── futures: aggTrades, trades, bookDepth, metrics       │
  │    └── spot:    aggTrades, trades, klines (1m/5m/30m/1h)   │
  │                                                            │
  │  collect_spot_bookdepth.py    Binance WebSocket + REST     │
  │    └── 现货 bookDepth 等价数据 (±1~5%, ~30s)                │
  │                                                            │
  │  download_coinglass_orderbook.py    CoinGlass API          │
  │    ├── Binance 合约/现货 Order Book 深度                    │
  │    └── 跨交易所聚合 Order Book 深度                          │
  └────────────────────────────────────────────────────────────┘
```

### 各组件职责

| 文件 | 职责 |
|------|------|
| `src/main.py` | 入口，组装采集器和写入器，处理 SIGINT/SIGTERM 优雅关闭 |
| `src/collector/base_collector.py` | Binance 7 步同步算法、间隙检测、指数退避重连、24h 主动重连 |
| `src/collector/spot_collector.py` | 现货专属端点及同步规则（`U == prev_u+1` 连续性校验） |
| `src/collector/futures_collector.py` | 合约专属端点及同步规则（`pu == prev_u` 连续性校验） |
| `src/orderbook/local_book.py` | 基于 `SortedDict` + `Decimal` 的内存 L2 订单簿 |
| `src/event_fanout.py` | 将采集事件复制到 Parquet / TimescaleDB 等多个下游写入队列 |
| `src/writer/parquet_writer.py` | Parquet 文件写入，快照立即写、Diff 批量写、原子 rename |
| `src/writer/timescale_writer.py` | 写入 TimescaleDB：原始 Diff、完整快照、1 秒摘要、采集器健康状态 |
| `src/publish/materializer.py` | 周期性生成 `publish/raw`、`publish/curated`、`publish/meta` 发布目录 |
| `src/api/main.py` | FastAPI 只读 API，提供元数据、聚合数据、原始快照 / 事件查询 |
| `src/api/security.py` | API 鉴权、中间件限流、内网免鉴权白名单控制 |
| `scripts/backup_manager.py` | 本地 PostgreSQL dump 轮转，并同步 `publish/` 和备份文件到 COS |
| `scripts/download_historical.py` | 从 Binance data.binance.vision 批量下载历史数据 (含 klines) |
| `scripts/download_coinglass_orderbook.py` | 从 CoinGlass API 下载 Order Book 深度历史 |
| `scripts/collect_spot_bookdepth.py` | 实时采集现货 bookDepth 等价数据（WebSocket + REST） |
| `scripts/generate_sample.py` | 采集样本数据并自动验证完整性 |
| `scripts/parquet_to_csv.py` | 将 Parquet 转为 CSV（展开版） |
| `scripts/init_db.sql` | TimescaleDB 表结构 |
| `config/symbols.yaml` | 品种配置——添加新交易对只需修改此文件 |

---

## 3. 快速开始

### 前提条件

- Python 3.9+
- Docker（可选，用于 TimescaleDB）

### 安装

```bash
cd Binance-BTC-Orderbook-Data

# 创建虚拟环境（推荐）
python3 -m venv .venv
source .venv/bin/activate

# 安装依赖
pip install -r requirements.txt

# 复制环境配置
cp .env.example .env
```

### 四种使用模式

```bash
# 模式 1：实时采集 Full L2 Order Book（从现在开始持续运行）
python -m src.main

# 模式 2：下载 Binance 历史数据（trades/aggTrades/bookDepth/metrics）
python scripts/download_historical.py --start 2026-01-01 --end 2026-03-01

# 模式 3：下载 CoinGlass Order Book 深度历史
python scripts/download_coinglass_orderbook.py --api-key YOUR_KEY

# 模式 4：启动完整发布栈（TimescaleDB + API + 备份）
docker compose up -d --build
```

---

## 4. 实时采集——Full L2 Order Book

### 采集内容

| 市场 | 品种 | WebSocket 流 | REST 快照深度 |
|------|------|-------------|--------------|
| 现货 Spot | BTCUSDT | `btcusdt@depth@100ms` | 每侧最多 5,000 档 |
| USD-M 合约 | BTCUSDT | `btcusdt@depth@100ms` | 每侧最多 1,000 档 |

### 启动

```bash
# 编辑 .env 设置数据输出目录
# DATA_DIR=./data

python -m src.main
```

启动后应看到：

```
spot.BTCUSDT — LIVE  lastUpdateId=89049289512  restarts=0
spot.BTCUSDT — Emitting initial snapshot  bids=5020  asks=4999
futures.BTCUSDT — LIVE  lastUpdateId=10014300212554  restarts=0
futures.BTCUSDT — Emitting initial snapshot  bids=1150  asks=1052
```

按 `Ctrl-C` 停止。所有缓冲数据在退出前自动刷盘。

### 输出文件结构

```
<DATA_DIR>/
├── spot/
│   └── BTCUSDT/
│       ├── snapshots_20260301T154502Z.parquet   ← 启动时初始快照
│       ├── events_20260301T160142Z.parquet      ← 5 分钟批量 Diff 事件
│       ├── snapshots_20260301T164502Z.parquet   ← 整点检查点快照
│       └── ...
└── futures/
    └── BTCUSDT/
        ├── snapshots_20260301T154514Z.parquet
        ├── events_20260301T160204Z.parquet
        └── ...
```

### 核心设计决策

| 决策 | 方案 | 原因 |
|------|------|------|
| 数值精度 | `Decimal` / 字符串存储 | 避免浮点精度损失 |
| 存储格式 | Parquet + Snappy 压缩 | 列式高效，兼容 pandas/DuckDB/Spark |
| 同步算法 | Binance 官方 7 步流程 | 正确的间隙检测与快照桥接 |
| 合约连续性 | `pu` 字段校验 | 合约使用不同的连续性协议 |
| 断线重连 | 指数退避（1s → 60s） | 对抗网络抖动 |
| 24h 限制 | 23h50m 时主动重连 | 服务器强制断开前零数据缺口 |

---

## 5. 历史数据下载——Binance 公开数据

从 [Binance 公开数据门户](https://data.binance.vision/) 批量下载历史交易和深度数据。**无需 API Key，完全免费。**

### 可下载数据类型

| 数据类型 | 市场 | 频率 | 内容说明 |
|----------|------|------|---------|
| `aggTrades` | 现货 + 合约 | 逐笔 | 聚合成交记录：价格、数量、方向 |
| `trades` | 现货 + 合约 | 逐笔 | 原始成交明细：每笔撮合的完整记录 |
| `bookDepth` | 合约 | ~30 秒 | 订单簿深度摘要：±1-5% 累计深度和名义价值 |
| `klines` | 现货 | 1m/5m/30m/1h | K 线数据：OHLCV + 成交笔数 + Taker 买入量 |
| `metrics` | 合约 | 5 分钟 | 未平仓量、多空比、Taker 买卖量比 |

### 使用方法

```bash
# 下载 2026.01.01 - 2026.03.01 全部可用数据
python scripts/download_historical.py

# 自定义时间范围和输出目录
python scripts/download_historical.py \
  --start 2026-01-01 \
  --end 2026-03-01 \
  --output ~/Desktop/BTC-Historical-Data
```

### 输出目录结构

```
BTC-Historical-Data/
├── futures/
│   └── BTCUSDT/
│       ├── aggTrades/          ← 59 个 zip 文件，约 1.4 GB
│       │   ├── BTCUSDT-aggTrades-2026-01-01.zip
│       │   └── ...
│       ├── trades/             ← 59 个 zip 文件，约 2.1 GB
│       ├── bookDepth/          ← 59 个 zip 文件，约 29 MB
│       └── metrics/            ← 59 个 zip 文件，约 1 MB
└── spot/
    └── BTCUSDT/
        ├── aggTrades/          ← 59 个 zip 文件，约 1.1 GB
        ├── trades/             ← 59 个 zip 文件，约 2.1 GB
        ├── klines_1m/          ← 59 个 zip 文件，约 4.3 MB
        ├── klines_5m/          ← 59 个 zip 文件，约 1.2 MB
        ├── klines_30m/         ← 59 个 zip 文件，约 472 KB
        ├── klines_1h/          ← 59 个 zip 文件，约 472 KB
        └── bookDepth/          ← 实时采集 CSV（非历史下载）
```

### 下载性能

- 并发下载：8 路
- 每日文件大小：aggTrades 约 5-50 MB/天（波动与市场活跃度相关），trades 约 10-60 MB/天
- 已存在的文件自动跳过，支持断点续传
- 附带 CHECKSUM 文件用于数据校验

---

## 6. 历史数据下载——CoinGlass Order Book 深度

从 [CoinGlass API](https://docs.coinglass.com/) 下载 BTC 订单簿聚合深度历史数据。**需要 API Key**（免费套餐可用）。

### 数据特征

CoinGlass 提供的不是逐价格档位的完整订单簿，而是**在中间价 ±X% 范围内的聚合 bid/ask 总量**，适合分析流动性分布和买卖力量对比。

| 特性 | 说明 |
|------|------|
| 时间间隔 | 30 分钟（付费套餐），1 小时（免费套餐），5m/1m 需更高套餐 |
| 范围档位 | ±1%, ±2%, ±3%, ±5%, ±10% |
| 历史深度 | 30m 约 90 天，1h 约 180 天 |
| 交易所 | Binance 单独 + 全交易所聚合 |
| API 速率 | 80 次/分钟 |

### 使用方法

```bash
python scripts/download_coinglass_orderbook.py \
  --api-key YOUR_COINGLASS_API_KEY \
  --interval 30m \
  --start 2026-01-01 \
  --end 2026-03-01
```

### 下载的 4 类数据

| 数据类 | 说明 | 覆盖交易所 |
|--------|------|-----------|
| `futures_binance` | Binance 合约 Order Book 深度 | Binance |
| `spot_binance` | Binance 现货 Order Book 深度 | Binance |
| `futures_aggregated` | 全交易所聚合合约深度 | Binance, OKX, Bybit, Bitget, Gate, dYdX, Hyperliquid |
| `spot_aggregated` | 全交易所聚合现货深度 | Binance, OKX, Bybit, Coinbase, Bitfinex, Gate |

### 输出目录结构

```
BTC-Historical-Data/coinglass/
├── futures_binance/
│   ├── futures_binance_range1pct_30m.csv    ← ±1% 范围
│   ├── futures_binance_range2pct_30m.csv    ← ±2% 范围
│   ├── futures_binance_range3pct_30m.csv
│   ├── futures_binance_range5pct_30m.csv
│   └── futures_binance_range10pct_30m.csv
├── spot_binance/                            ← 同上结构
├── futures_aggregated/                      ← 跨交易所聚合
├── spot_aggregated/                         ← 跨交易所聚合
├── futures_binance_all_ranges_30m.csv       ← 合并文件（14,160 行）
├── spot_binance_all_ranges_30m.csv
├── futures_aggregated_all_ranges_30m.csv
└── spot_aggregated_all_ranges_30m.csv
```

---

## 7. 实时采集——现货 bookDepth

币安 data.binance.vision **不提供**历史现货 bookDepth 数据（仅合约有）。本脚本通过 WebSocket diff depth 流维护完整的现货本地订单簿，每 30 秒自动计算 ±1~5% 范围内的聚合深度，输出格式与合约 bookDepth 完全一致。

### 工作原理

1. 连接 `wss://stream.binance.com:9443/ws/btcusdt@depth@100ms`（100ms 推送频率）
2. 从 REST API `/api/v3/depth?limit=5000` 获取初始订单簿快照
3. 按 Binance 7 步同步算法将 WebSocket 增量事件应用到本地订单簿
4. 每 30 秒计算中间价，将所有挂单按 ±1/2/3/4/5% 百分比范围聚合
5. 输出与合约 bookDepth 相同的 CSV 格式

### 使用方法

```bash
# 持续采集（Ctrl+C 停止）
python3 scripts/collect_spot_bookdepth.py

# 指定采集时长（秒）
python3 scripts/collect_spot_bookdepth.py --duration 86400   # 运行 24 小时

# 自定义参数
python3 scripts/collect_spot_bookdepth.py \
  --symbol BTCUSDT \
  --interval 30 \
  --output ~/Desktop/BTC-Historical-Data/spot/BTCUSDT/bookDepth
```

### 输出格式

```csv
timestamp,percentage,depth,notional
2026-03-02 07:27:35,-5,591.52743000,38296603.93248190
2026-03-02 07:27:35,-4,453.98524000,29630035.40548190
2026-03-02 07:27:35,-3,453.18459000,29578993.96798190
2026-03-02 07:27:35,-2,453.18459000,29578993.96798190
2026-03-02 07:27:35,-1,196.10205000,12866369.08222965
2026-03-02 07:27:35,1,246.70092000,16259912.67587841
2026-03-02 07:27:35,2,264.94824000,17475580.98836712
2026-03-02 07:27:35,3,264.94824000,17475580.98836712
2026-03-02 07:27:35,4,264.94850000,17475598.62472672
2026-03-02 07:27:35,5,264.94850000,17475598.62472672
```

> **注意**: 采集器启动后需要约 1-2 分钟让 WebSocket 增量流填充足够的价格档位，之后 ±5% 范围的数据会逐步完善。文件按日期自动轮转。

---

## 8. 全部数据字段参考

### 8.1 实时采集——快照文件（`snapshots_*.parquet`）

| 字段 | 类型 | 可空 | 说明 |
|------|------|------|------|
| `snapshot_time_ns` | int64 | 否 | 快照时间（Unix 纳秒） |
| `market` | string | 否 | `"spot"` 或 `"futures"` |
| `symbol` | string | 否 | `"BTCUSDT"` |
| `snapshot_type` | string | 否 | `"initial"`（启动时）或 `"checkpoint"`（每小时） |
| `last_update_id` | int64 | 否 | 最近事件序列号，用于与 events 对齐 |
| `bid_count` | int32 | 否 | 买方档位数量 |
| `ask_count` | int32 | 否 | 卖方档位数量 |
| `bids` | list | 否 | 完整买方订单簿 `[["价格", "数量"], ...]`，高价到低价 |
| `asks` | list | 否 | 完整卖方订单簿 `[["价格", "数量"], ...]`，低价到高价 |

### 8.2 实时采集——增量事件文件（`events_*.parquet`）

| 字段 | 类型 | 可空 | 说明 |
|------|------|------|------|
| `exchange_time_ms` | int64 | 否 | Binance 事件时间（Unix 毫秒），**时间序列分析以此为准** |
| `transaction_time_ms` | int64 | 是 | 撮合引擎时间（仅合约，现货为 null） |
| `receive_time_ns` | int64 | 否 | 本地接收时间（Unix 纳秒），与 exchange_time_ms 之差 = 网络延迟 |
| `market` | string | 否 | `"spot"` 或 `"futures"` |
| `symbol` | string | 否 | `"BTCUSDT"` |
| `first_update_id` | int64 | 否 | 事件起始序列号 |
| `last_update_id` | int64 | 否 | 事件终止序列号 |
| `prev_final_update_id` | int64 | 是 | 上一事件终止序列号（仅合约 `pu` 字段） |
| `bids` | list | 否 | 变化的买档。数量 `"0"` = 删除，`> 0` = 设置（绝对量） |
| `asks` | list | 否 | 变化的卖档，同 bids 规则 |

> **关键**: events 只包含**变化的档位**，不是完整订单簿。要重建任意时刻的完整订单簿，需先加载 snapshot，再逐条应用 events。

### 8.3 Binance 历史——合约 aggTrades

| 字段 | 类型 | 说明 |
|------|------|------|
| `agg_trade_id` | int | 聚合成交唯一 ID，全局递增 |
| `price` | decimal | 成交价格（USDT） |
| `quantity` | decimal | 成交数量（BTC） |
| `first_trade_id` | int | 该聚合中第一笔 trade 的 ID |
| `last_trade_id` | int | 该聚合中最后一笔 trade 的 ID |
| `transact_time` | int | 成交时间（Unix 毫秒） |
| `is_buyer_maker` | bool | `true` = 卖方主动成交（下行压力）；`false` = 买方主动成交（上行压力） |

### 8.4 Binance 历史——合约 trades

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | int | 单笔成交唯一 ID |
| `price` | decimal | 成交价格（USDT） |
| `qty` | decimal | 成交数量（BTC） |
| `quote_qty` | decimal | 成交额（USDT），= price × qty |
| `time` | int | 成交时间（Unix 毫秒） |
| `is_buyer_maker` | bool | 同 aggTrades |

### 8.5 Binance 历史——现货 aggTrades

| 字段 | 类型 | 说明 |
|------|------|------|
| `agg_trade_id` | int | 聚合成交唯一 ID |
| `price` | decimal | 成交价格（USDT） |
| `quantity` | decimal | 成交数量（BTC） |
| `first_trade_id` | int | 第一笔 trade ID |
| `last_trade_id` | int | 最后一笔 trade ID |
| `transact_time` | int | 成交时间（Unix **微秒**，注意精度与合约不同） |
| `is_buyer_maker` | bool | 买方是否为挂单方 |
| `is_best_match` | bool | 是否为最优价格撮合 |

### 8.6 Binance 历史——现货 trades

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | int | 单笔成交唯一 ID |
| `price` | decimal | 成交价格（USDT） |
| `qty` | decimal | 成交数量（BTC） |
| `quote_qty` | decimal | 成交额（USDT） |
| `time` | int | 成交时间（Unix **微秒**） |
| `is_buyer_maker` | bool | 买方是否为挂单方 |
| `is_best_match` | bool | 是否为最优价格撮合 |

### 8.7 Binance 历史——合约 bookDepth（深度摘要）

| 字段 | 类型 | 说明 |
|------|------|------|
| `timestamp` | datetime | 快照时间（UTC），约每 30-60 秒一条 |
| `percentage` | int | 距中间价的百分比。负数 = bid 侧，正数 = ask 侧 |
| `depth` | decimal | 该范围内的**累计挂单量**（BTC） |
| `notional` | decimal | 该范围内的**累计挂单金额**（USDT） |

> 每条 timestamp 对应 10 行：-5, -4, -3, -2, -1, 1, 2, 3, 4, 5。例：`percentage=-5, depth=6419.548` 表示中间价以下 0-5% 内买方共挂 6419.548 BTC。

### 8.8 Binance 历史——合约 metrics（市场指标）

| 字段 | 类型 | 说明 |
|------|------|------|
| `create_time` | datetime | 记录时间（UTC），每 5 分钟一条 |
| `symbol` | string | 交易对 |
| `sum_open_interest` | decimal | 全市场未平仓合约总量（BTC） |
| `sum_open_interest_value` | decimal | 未平仓合约总价值（USDT） |
| `count_toptrader_long_short_ratio` | decimal | 头部交易者多空**人数**比 |
| `sum_toptrader_long_short_ratio` | decimal | 头部交易者多空**持仓量**比 |
| `count_long_short_ratio` | decimal | 全市场多空人数比 |
| `sum_taker_long_short_vol_ratio` | decimal | 主动买/卖成交量比。>1 多方强，<1 空方强 |

### 8.9 CoinGlass——单交易所 Order Book 深度

| 字段 | 类型 | 说明 |
|------|------|------|
| `timestamp_utc` | datetime | 数据时间（UTC），每 30 分钟一条 |
| `timestamp_ms` | int | Unix 毫秒时间戳 |
| `range_pct` | int | 距中间价的百分比范围（1/2/3/5/10） |
| `bids_usd` | decimal | 该范围内**买方挂单总金额**（USD） |
| `bids_quantity` | decimal | 该范围内**买方挂单总量**（BTC） |
| `asks_usd` | decimal | 该范围内**卖方挂单总金额**（USD） |
| `asks_quantity` | decimal | 该范围内**卖方挂单总量**（BTC） |

### 8.10 CoinGlass——跨交易所聚合 Order Book 深度

| 字段 | 类型 | 说明 |
|------|------|------|
| `timestamp_utc` | datetime | 数据时间（UTC） |
| `timestamp_ms` | int | Unix 毫秒时间戳 |
| `range_pct` | int | 距中间价的百分比范围 |
| `aggregated_bids_usd` | decimal | **所有交易所合计**买方挂单总金额 |
| `aggregated_bids_quantity` | decimal | **所有交易所合计**买方挂单总量 |
| `aggregated_asks_usd` | decimal | **所有交易所合计**卖方挂单总金额 |
| `aggregated_asks_quantity` | decimal | **所有交易所合计**卖方挂单总量 |

### 数据层次对比

| 数据 | 粒度 | 内容 | 适用场景 |
|------|------|------|---------|
| 实时 snapshots + events | 100ms | 逐价格档位 Full L2 | 精确重建订单簿、Spread 分析、流动性微观结构 |
| CoinGlass 深度 | 30 分钟 | ±X% 聚合 bid/ask | 流动性趋势、买卖力量对比、跨交易所深度 |
| Binance bookDepth | ~30 秒 | ±1-5% 累计深度 | 合约深度变化、流动性分布 |
| trades / aggTrades | 逐笔 | 实际成交 | VWAP、大单追踪、Taker 分析、Tick 回测 |
| metrics | 5 分钟 | 未平仓量/多空比 | 市场情绪、持仓结构 |

---

## 9. 回溯研究使用指南

### 9.1 订单簿重建（快照 + 事件回放）

```python
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from src.orderbook.local_book import LocalOrderBook

MARKET = "spot"
SYMBOL = "BTCUSDT"
DATA_DIR = Path("data")

# 第 1 步：加载最新快照作为种子
snap_dir = DATA_DIR / MARKET / SYMBOL
snap_files = sorted(snap_dir.glob("snapshots_*.parquet"))
snaps = pd.concat([pq.read_table(f).to_pandas() for f in snap_files])
seed = snaps.sort_values("snapshot_time_ns").iloc[-1]

# 第 2 步：初始化本地订单簿
book = LocalOrderBook(SYMBOL, MARKET)
book.init_from_snapshot({
    "lastUpdateId": seed["last_update_id"],
    "bids": seed["bids"],
    "asks": seed["asks"],
})

# 第 3 步：加载并回放 Diff 事件
event_files = sorted(snap_dir.glob("events_*.parquet"))
events = pd.concat([pq.read_table(f).to_pandas() for f in event_files])
events = events[events["last_update_id"] > seed["last_update_id"]]
events = events.sort_values("exchange_time_ms")

for _, row in events.iterrows():
    book.apply_bids_asks(row["bids"], row["asks"])
    book.last_update_id = row["last_update_id"]
    # 此处可记录中间价、价差等指标
```

### 9.2 DuckDB 快速分析

```sql
-- 1 秒粒度中间价时间序列
SELECT
    time_bucket(INTERVAL '1 second',
        to_timestamp(exchange_time_ms / 1000.0)) AS ts,
    AVG(CAST(bids[1][1] AS DOUBLE)) AS approx_best_bid,
    AVG(CAST(asks[1][1] AS DOUBLE)) AS approx_best_ask
FROM read_parquet('data/spot/BTCUSDT/events_*.parquet')
GROUP BY 1 ORDER BY 1;

-- 查询历史成交
SELECT * FROM read_csv_auto('BTC-Historical-Data/futures/BTCUSDT/aggTrades/*.csv')
WHERE is_buyer_maker = false  -- 买方主动成交
ORDER BY transact_time LIMIT 100;
```

### 9.3 利用检查点跨越长历史

```python
# 找到目标时刻之前最近的检查点
target_ns = int(pd.Timestamp("2026-03-01 09:00:00", tz="UTC").value)
seed = snaps[snaps["snapshot_time_ns"] <= target_ns].iloc[-1]
# 只回放该检查点之后的事件
```

---

## 10. 数据发布与只读 API（可选）

当设置 `TIMESCALE_DSN` 并运行 `docker compose up -d --build` 后，仓库会形成一条完整发布链路：

1. `collector` 采集原始订单簿并写入 Parquet 与 TimescaleDB。
2. `materialize` 周期性读取数据库和已稳定的 Parquet 文件，生成 `publish/raw`、`publish/curated`、`publish/meta`。
3. `api` 从 `publish/` 和 TimescaleDB 提供只读查询接口。
4. `backup` 定期生成 PostgreSQL dump，并把 `publish/` 与数据库备份同步到腾讯云 COS。

### 发布目录结构

```
publish/
├── raw/           ← 稳定后的原始 parquet 镜像
├── curated/       ← 聚合后的 JSON 数据集
└── meta/          ← markets/files/keysets/status 等元信息
```

### 当前已发布的数据集

| 数据集 | 来源 | 说明 |
|--------|------|------|
| `book_snapshots` | `orderbook_snapshots` | 各 timeframe 的买一卖一、中间价、深度摘要 |
| `price_changes` | `orderbook_snapshots` | timeframe 级别价格变化聚合 |
| `recovery_events` | `collector_health` | 重连/恢复情况聚合 |
| `trades` | 预留 | 当前 agent 未发布成交数据，接口会返回空结果 |

### 只读 API 端点

| 路径 | 说明 |
|------|------|
| `/health` | 健康检查，不需要鉴权 |
| `/v1/keysets/index` | 可用分区索引 |
| `/v1/keysets/{dt}/{timeframe}` | 指定日期 + timeframe 的 keyset manifest |
| `/v1/meta/markets` | 可用市场 / 品种 |
| `/v1/meta/files` | 发布文件索引，可按 dataset / dt / timeframe / market 过滤 |
| `/v1/curated/{dataset}` | 读取聚合后的发布数据 |
| `/v1/collector-health` | 数据库中的最新采集器状态 |
| `/v1/orderbook/latest-summary` | 最新 1 条 top-of-book 摘要 |
| `/v1/orderbook/latest-full-snapshot` | 最新完整订单簿快照 |
| `/v1/orderbook/snapshots` | 按时间范围查询完整快照 |
| `/v1/orderbook/events` | 按时间范围查询原始 Diff 事件 |

### 鉴权与访问控制

- `/health` 始终匿名可访问。
- 其他 `/v1/*` 接口默认要求 `Authorization: Bearer <ORDERBOOK_API_TOKEN>`。
- 可通过 `ALLOW_PRIVATE_NETWORK_WITHOUT_AUTH=true` + `PRIVATE_ACCESS_CIDRS=...` 允许内网来源免 token。
- API 文档页（Swagger / OpenAPI）默认关闭，面向生产只读访问。

### 调用示例

```bash
# 健康检查
curl http://127.0.0.1:18080/health

# 读取可用市场
curl -H "Authorization: Bearer $ORDERBOOK_API_TOKEN" \
  http://127.0.0.1:18080/v1/meta/markets

# 读取 5m 订单簿摘要
curl -H "Authorization: Bearer $ORDERBOOK_API_TOKEN" \
  "http://127.0.0.1:18080/v1/curated/book_snapshots?market_slug=spot-btcusdt&timeframe=5m&limit=5"

# 查询最新完整快照
curl -H "Authorization: Bearer $ORDERBOOK_API_TOKEN" \
  "http://127.0.0.1:18080/v1/orderbook/latest-full-snapshot?market=spot&symbol=BTCUSDT"
```

---

## 11. Docker Compose / 腾讯云部署（可选）

仓库当前自带完整容器编排，`docker-compose.yml` 默认会启动以下服务：

| 服务 | 作用 |
|------|------|
| `timescaledb` | 保存原始 Diff、完整快照、1 秒摘要和采集器健康状态 |
| `collector` | 持续采集 Binance Spot / Futures L2 数据 |
| `materialize` | 生成 `publish/` 发布目录 |
| `api` | 提供只读查询 API |
| `backup` | 轮转本地 DB dump，并同步到 COS |

### 本地启动

```bash
cp .env.example .env
# 编辑 .env，至少设置 POSTGRES_PASSWORD / ORDERBOOK_API_TOKEN

docker compose up -d --build
docker compose ps
docker compose logs -f collector
```

默认情况下：

- API 暴露在 `127.0.0.1:${API_PORT}`，默认端口 `18080`
- TimescaleDB 暴露在 `127.0.0.1:5432`
- 数据卷包括 `timescale_data`、`orderbook_data`、`orderbook_publish`、`orderbook_backups`

### 腾讯云 / Nginx / 飞连相关文档

- [DEPLOY_TENCENT_CLOUD.md](./DEPLOY_TENCENT_CLOUD.md)：腾讯云整体部署流程
- [TENCENT_CLOUD_SECURITY_GROUP.md](./TENCENT_CLOUD_SECURITY_GROUP.md)：安全组放行规则
- [FEILIAN_PRIVATE_ACCESS.md](./FEILIAN_PRIVATE_ACCESS.md)：飞连内网免鉴权访问
- [deploy/install_nginx_proxy.sh](./deploy/install_nginx_proxy.sh)：安装 Nginx 反向代理与自签名证书

如果需要把 API 暴露为 `https://<host>/<agent_id>/v1/...`，建议先跑通本地 `docker compose`，再按上面的文档启用 Nginx 反向代理。

---

## 12. 配置说明

### `config/symbols.yaml`

```yaml
spot:
  - BTCUSDT
  # - ETHUSDT   ← 添加更多品种

futures:
  - BTCUSDT
  # - ETHUSDT
```

### `.env` 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `DATA_DIR` | `./data` | Parquet 文件输出根目录 |
| `PUBLISH_DIR` | `./publish` | 物化后的发布目录根路径 |
| `TIMESCALE_DSN` | （未设置） | TimescaleDB 连接串；未设置则跳过数据库写入 |
| `AGENT_ID` | `binance-orderbook` | 发布目录与反向代理路径中的 agent 标识 |
| `PUBLISH_TIMEFRAMES` | `5m,15m,1h,4h` | 物化输出的聚合周期 |
| `PUBLISH_LOOKBACK_DAYS` | `2` | 每次物化回看的自然日数量 |
| `PUBLISH_STABLE_SECONDS` | `120` | Parquet 文件稳定多久后才镜像进 `publish/raw` |
| `MATERIALIZE_INTERVAL_SECONDS` | `60` | 物化任务轮询间隔 |
| `ORDERBOOK_API_DSN` | 与 `TIMESCALE_DSN` 相同 | API 查询数据库时使用的连接串 |
| `API_HOST` | `127.0.0.1` | API 监听地址（本地运行时） |
| `API_PORT` | `18080` | API 监听端口 |
| `ORDERBOOK_API_TOKEN` | （必填） | `/v1/*` 接口 Bearer Token |
| `API_RATE_LIMIT_PER_MINUTE` | `240` | 单 IP 每分钟限流 |
| `ALLOW_PRIVATE_NETWORK_WITHOUT_AUTH` | `false` | 是否允许内网来源免 token |
| `PRIVATE_ACCESS_CIDRS` | RFC1918 + loopback | 内网免鉴权的来源网段 |
| `POSTGRES_PASSWORD` | `password` | Docker Compose 初始化数据库密码 |
| `TENCENT_COS_BUCKET` | （未设置） | 远程备份桶名 |
| `TENCENT_COS_REGION` | （未设置） | COS 区域 |
| `TENCENT_COS_PREFIX` | `agents/binance-orderbook` | 远程对象前缀 |
| `TENCENTCLOUD_SECRET_ID` | （未设置） | 腾讯云 API 凭证 |
| `TENCENTCLOUD_SECRET_KEY` | （未设置） | 腾讯云 API 凭证 |
| `TENCENT_COS_SYNC_INTERVAL_SECONDS` | `30` | 远程同步扫描间隔 |
| `TENCENT_COS_STABLE_SECONDS` | `10` | 文件稳定多久后允许上传 |
| `DB_BACKUP_INTERVAL_SECONDS` | `3600` | PostgreSQL dump 周期 |
| `DB_BACKUP_RETENTION_COUNT` | `48` | 本地保留备份份数 |
| `LOG_LEVEL` | `INFO` | 日志级别：`DEBUG` / `INFO` / `WARNING` / `ERROR` |

---

## 13. TimescaleDB 与数据库表（可选）

```bash
docker compose up -d
echo "TIMESCALE_DSN=postgresql://postgres:password@localhost:5432/orderbook" >> .env
python -m src.main
```

当前实现会写入 4 张表：

| 表 | 频率 | 字段 |
|------|------|------|
| `orderbook_events_raw` | Diff 批量刷写 | exchange_time, first_update_id, last_update_id, bids, asks |
| `orderbook_full_snapshots_raw` | 初始快照 + 每小时检查点 | snapshot_time, snapshot_type, bid_count, ask_count, bids, asks |
| `orderbook_snapshots` | 每 1 秒 | ts, market, symbol, best_bid, best_ask, spread, mid_price, bid_depth_10, ask_depth_10 |
| `collector_health` | 每 10 秒 | ts, market, symbol, restarts, last_update_id, is_live |

---

## 14. 系统要求与存储估算

### 实时采集

| 资源 | 估算 |
|------|------|
| CPU | < 10%（I/O 密集型） |
| 内存 | 200–400 MB（BTC 现货 + 合约） |
| 磁盘 | 约 350–650 MB/天 |
| 网络 | ~500 KB–1 MB/s 入站 |

### Docker 一体化部署建议

| 资源 | 建议 |
|------|------|
| CPU | 2 vCPU+ |
| 内存 | 4 GB+（含 TimescaleDB / API / 备份） |
| 磁盘 | 100 GB SSD+，按保留天数线性增长 |
| 网络 | 需访问 Binance、CoinGlass（可选）、腾讯云 COS（可选） |

### 历史数据（BTC/USDT，59 天示例）

| 数据类型 | 大小 |
|----------|------|
| 合约 aggTrades | ~1.4 GB |
| 合约 trades | ~2.1 GB |
| 合约 bookDepth | ~29 MB |
| 合约 metrics | ~1 MB |
| 现货 aggTrades | ~1.1 GB |
| 现货 trades | ~2.1 GB |
| CoinGlass 深度 | ~2.4 MB |
| **合计** | **~6.8 GB** |

---

## 15. 已知限制

| 限制 | 说明 |
|------|------|
| **L2 数据，无 L3** | Binance 不提供单笔订单 ID 或队列位置 |
| **初始快照深度上限** | 现货 5,000 档 / 合约 1,000 档（REST API 限制） |
| **实时采集无回填** | 只能采集启动后的数据，历史数据需使用下载工具 |
| **24h 强制重连** | Binance 每 24h 断开 WS，系统在 23h50m 主动重连（~1-3 秒暂停） |
| **CoinGlass 间隔限制** | 付费套餐最小 30m（90 天），免费 1h（180 天），5m/1m 需更高套餐 |
| **Binance 合约 bookDepth** | data.binance.vision 仅合约可用，±1-5% 汇总，非逐档位 |
| **现货 bookDepth 无历史** | 币安不提供历史现货 bookDepth，需通过 `collect_spot_bookdepth.py` 实时采集 |

---

## 16. 常见问题排查

| 问题 | 原因 | 解决方案 |
|------|------|---------|
| `Gap detected in live stream` | 序列号不连续，事件丢失 | 检查网络；系统会自动重连恢复 |
| `Snapshot too old` | REST 快照过旧 | 自动重新拉取，无需干预 |
| Parquet 文件为空 | 在首次刷盘前停止（< 5 分钟） | 用 `Ctrl-C` 优雅关闭，不要用 `kill -9` |
| 价格看起来异常 | 字符串存储被当 float 解析 | 用 `Decimal(row["bids"][0][0])` 解析 |
| CoinGlass 返回 500 | symbol 格式错误 | 使用 `BTCUSDT`（非 `BTC`） |
| `Upgrade plan` | CoinGlass 当前套餐不支持该间隔 | 付费可用 30m，免费用 1h，5m/1m 需更高套餐 |

---

## 项目文件结构

```
Binance-BTC-Orderbook-Data/
├── src/
│   ├── main.py                          # 入口：python -m src.main
│   ├── event_fanout.py                  # 事件复制到多个 writer 队列
│   ├── collector/
│   │   ├── base_collector.py            # Binance 7 步同步算法核心
│   │   ├── spot_collector.py            # 现货：WS + REST + U==prev_u+1 校验
│   │   └── futures_collector.py         # 合约：WS + REST + pu==prev_u 校验
│   ├── orderbook/
│   │   └── local_book.py               # 内存 L2 订单簿（SortedDict + Decimal）
│   ├── api/
│   │   ├── main.py                      # FastAPI 只读 API
│   │   ├── publish_store.py             # 读取 publish/ 元数据与数据集
│   │   └── security.py                  # Bearer 鉴权 / 限流 / 内网白名单
│   ├── publish/
│   │   ├── common.py                    # 发布目录 / timeframe / keyset 公共逻辑
│   │   └── materializer.py              # 生成 raw / curated / meta 发布产物
│   └── writer/
│       ├── parquet_writer.py            # Parquet 输出（快照立即写/事件批量写）
│       └── timescale_writer.py          # TimescaleDB 输出（raw + summary + health）
├── scripts/
│   ├── download_historical.py           # Binance 历史数据批量下载
│   ├── download_coinglass_orderbook.py  # CoinGlass Order Book 深度下载
│   ├── collect_spot_bookdepth.py       # 现货 bookDepth 实时采集（WebSocket）
│   ├── backup_manager.py                # DB dump 轮转 + COS 同步
│   ├── generate_sample.py              # 样本采集 + 自动验证
│   ├── parquet_to_csv.py               # Parquet → CSV 转换
│   └── init_db.sql                     # TimescaleDB 表结构
├── config/
│   └── symbols.yaml                    # 品种配置（添加品种只改这里）
├── deploy/
│   ├── install_nginx_proxy.sh          # Nginx 反向代理安装脚本
│   └── nginx/binance-orderbook-api.conf.template
├── Dockerfile                          # 采集 / materialize / api / backup 共用镜像
├── docker-compose.yml                  # 五服务一体化部署
├── DEPLOY_TENCENT_CLOUD.md             # 腾讯云部署说明
├── TENCENT_CLOUD_SECURITY_GROUP.md     # 腾讯云安全组规则
├── FEILIAN_PRIVATE_ACCESS.md           # 飞连内网免鉴权说明
├── .env.example                        # 环境变量模板
└── requirements.txt                    # Python 依赖
```

---

## 依赖项

```
aiohttp>=3.9          # 异步 HTTP + WebSocket 客户端
pyarrow>=14.0         # Parquet 读写
sortedcontainers>=2.4 # SortedDict（高性能有序字典）
python-dotenv>=1.0    # .env 文件加载
pyyaml>=6.0           # YAML 配置解析
asyncpg>=0.29         # TimescaleDB 异步驱动（可选）
fastapi>=0.115        # 只读 API
uvicorn>=0.30         # API 服务启动
cos-python-sdk-v5     # 腾讯云 COS 备份
```

---

*数据为 Binance L2 聚合订单簿，不含 L3 单笔订单信息。*
*Binance API 使用请遵守 [Binance 使用条款](https://www.binance.com/en/terms)。*
*CoinGlass API 使用请遵守 [CoinGlass 使用条款](https://www.coinglass.com/terms)。*
