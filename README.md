# Binance BTC 订单簿数据采集系统

生产级 Python 系统，用于采集 Binance 现货（Spot）和 USD-M 永续合约（Futures）上 BTC/USDT 的**完整 L2 订单簿数据**，专为量化回溯研究设计。

同时提供**历史数据批量下载工具**，支持从 Binance 公开数据门户和 CoinGlass API 获取历史成交、深度及市场指标数据。

---

## 目录

1. [数据类型与范畴说明](#1-数据类型与范畴说明)
2. [系统架构](#2-系统架构)
3. [快速开始](#3-快速开始)
4. [实时采集——Full L2 Order Book](#4-实时采集full-l2-order-book)
5. [历史数据下载——Binance 公开数据](#5-历史数据下载binance-公开数据)
6. [历史数据下载——CoinGlass Order Book 深度](#6-历史数据下载coinglass-order-book-深度)
7. [全部数据字段参考](#7-全部数据字段参考)
8. [回溯研究使用指南](#8-回溯研究使用指南)
9. [配置说明](#9-配置说明)
10. [TimescaleDB 监控（可选）](#10-timescaledb-监控可选)
11. [系统要求与存储估算](#11-系统要求与存储估算)
12. [已知限制](#12-已知限制)
13. [常见问题排查](#13-常见问题排查)

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
               │          ParquetWriter            │
               │  快照 → 立即写入                   │
               │  Diff → 缓冲批量写入（10k条/5min） │
               └───────┬────────────────────┬──────┘
                       │                    │
          ┌────────────▼────────┐  ┌────────▼──────────────┐
          │ snapshots_*.parquet │  │  events_*.parquet      │
          └─────────────────────┘  └───────────────────────┘

  ┌────────────────────────────────────────────────────────────┐
  │              历史数据下载工具（独立运行）                     │
  │                                                            │
  │  download_historical.py     Binance公开数据门户              │
  │    ├── futures: aggTrades, trades, bookDepth, metrics       │
  │    └── spot:    aggTrades, trades                           │
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
| `src/writer/parquet_writer.py` | Parquet 文件写入，快照立即写、Diff 批量写、原子 rename |
| `src/writer/timescale_writer.py` | 定期将买一/卖一及健康指标写入 TimescaleDB（可选） |
| `scripts/download_historical.py` | 从 Binance data.binance.vision 批量下载历史数据 |
| `scripts/download_coinglass_orderbook.py` | 从 CoinGlass API 下载 Order Book 深度历史 |
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

### 三种使用模式

```bash
# 模式 1：实时采集 Full L2 Order Book（从现在开始持续运行）
python -m src.main

# 模式 2：下载 Binance 历史数据（trades/aggTrades/bookDepth/metrics）
python scripts/download_historical.py --start 2026-01-01 --end 2026-03-01

# 模式 3：下载 CoinGlass Order Book 深度历史
python scripts/download_coinglass_orderbook.py --api-key YOUR_KEY
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
        └── trades/             ← 59 个 zip 文件，约 2.1 GB
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

## 7. 全部数据字段参考

### 7.1 实时采集——快照文件（`snapshots_*.parquet`）

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

### 7.2 实时采集——增量事件文件（`events_*.parquet`）

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

### 7.3 Binance 历史——合约 aggTrades

| 字段 | 类型 | 说明 |
|------|------|------|
| `agg_trade_id` | int | 聚合成交唯一 ID，全局递增 |
| `price` | decimal | 成交价格（USDT） |
| `quantity` | decimal | 成交数量（BTC） |
| `first_trade_id` | int | 该聚合中第一笔 trade 的 ID |
| `last_trade_id` | int | 该聚合中最后一笔 trade 的 ID |
| `transact_time` | int | 成交时间（Unix 毫秒） |
| `is_buyer_maker` | bool | `true` = 卖方主动成交（下行压力）；`false` = 买方主动成交（上行压力） |

### 7.4 Binance 历史——合约 trades

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | int | 单笔成交唯一 ID |
| `price` | decimal | 成交价格（USDT） |
| `qty` | decimal | 成交数量（BTC） |
| `quote_qty` | decimal | 成交额（USDT），= price × qty |
| `time` | int | 成交时间（Unix 毫秒） |
| `is_buyer_maker` | bool | 同 aggTrades |

### 7.5 Binance 历史——现货 aggTrades

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

### 7.6 Binance 历史——现货 trades

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | int | 单笔成交唯一 ID |
| `price` | decimal | 成交价格（USDT） |
| `qty` | decimal | 成交数量（BTC） |
| `quote_qty` | decimal | 成交额（USDT） |
| `time` | int | 成交时间（Unix **微秒**） |
| `is_buyer_maker` | bool | 买方是否为挂单方 |
| `is_best_match` | bool | 是否为最优价格撮合 |

### 7.7 Binance 历史——合约 bookDepth（深度摘要）

| 字段 | 类型 | 说明 |
|------|------|------|
| `timestamp` | datetime | 快照时间（UTC），约每 30-60 秒一条 |
| `percentage` | int | 距中间价的百分比。负数 = bid 侧，正数 = ask 侧 |
| `depth` | decimal | 该范围内的**累计挂单量**（BTC） |
| `notional` | decimal | 该范围内的**累计挂单金额**（USDT） |

> 每条 timestamp 对应 10 行：-5, -4, -3, -2, -1, 1, 2, 3, 4, 5。例：`percentage=-5, depth=6419.548` 表示中间价以下 0-5% 内买方共挂 6419.548 BTC。

### 7.8 Binance 历史——合约 metrics（市场指标）

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

### 7.9 CoinGlass——单交易所 Order Book 深度

| 字段 | 类型 | 说明 |
|------|------|------|
| `timestamp_utc` | datetime | 数据时间（UTC），每 30 分钟一条 |
| `timestamp_ms` | int | Unix 毫秒时间戳 |
| `range_pct` | int | 距中间价的百分比范围（1/2/3/5/10） |
| `bids_usd` | decimal | 该范围内**买方挂单总金额**（USD） |
| `bids_quantity` | decimal | 该范围内**买方挂单总量**（BTC） |
| `asks_usd` | decimal | 该范围内**卖方挂单总金额**（USD） |
| `asks_quantity` | decimal | 该范围内**卖方挂单总量**（BTC） |

### 7.10 CoinGlass——跨交易所聚合 Order Book 深度

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

## 8. 回溯研究使用指南

### 8.1 订单簿重建（快照 + 事件回放）

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

### 8.2 DuckDB 快速分析

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

### 8.3 利用检查点跨越长历史

```python
# 找到目标时刻之前最近的检查点
target_ns = int(pd.Timestamp("2026-03-01 09:00:00", tz="UTC").value)
seed = snaps[snaps["snapshot_time_ns"] <= target_ns].iloc[-1]
# 只回放该检查点之后的事件
```

---

## 9. 配置说明

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
| `TIMESCALE_DSN` | （未设置）| TimescaleDB 连接串；未设置则跳过数据库写入 |
| `LOG_LEVEL` | `INFO` | 日志级别：`DEBUG` / `INFO` / `WARNING` / `ERROR` |

---

## 10. TimescaleDB 监控（可选）

```bash
docker compose up -d
echo "TIMESCALE_DSN=postgresql://postgres:password@localhost:5432/orderbook" >> .env
python -m src.main
```

写入两张表：

| 表 | 频率 | 字段 |
|------|------|------|
| `orderbook_snapshots` | 每 1 秒 | ts, market, symbol, best_bid, best_ask, spread, mid_price, bid_depth_10, ask_depth_10 |
| `collector_health` | 每 10 秒 | ts, market, symbol, restarts, last_update_id, is_live |

---

## 11. 系统要求与存储估算

### 实时采集

| 资源 | 估算 |
|------|------|
| CPU | < 10%（I/O 密集型） |
| 内存 | 200–400 MB（BTC 现货 + 合约） |
| 磁盘 | 约 350–650 MB/天 |
| 网络 | ~500 KB–1 MB/s 入站 |

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

## 12. 已知限制

| 限制 | 说明 |
|------|------|
| **L2 数据，无 L3** | Binance 不提供单笔订单 ID 或队列位置 |
| **初始快照深度上限** | 现货 5,000 档 / 合约 1,000 档（REST API 限制） |
| **实时采集无回填** | 只能采集启动后的数据，历史数据需使用下载工具 |
| **24h 强制重连** | Binance 每 24h 断开 WS，系统在 23h50m 主动重连（~1-3 秒暂停） |
| **CoinGlass 间隔限制** | 付费套餐最小 30m（90 天），免费 1h（180 天），5m/1m 需更高套餐 |
| **Binance bookDepth** | 仅合约可用，仅 ±1-5% 汇总，非逐档位 |

---

## 13. 常见问题排查

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
│   ├── collector/
│   │   ├── base_collector.py            # Binance 7 步同步算法核心
│   │   ├── spot_collector.py            # 现货：WS + REST + U==prev_u+1 校验
│   │   └── futures_collector.py         # 合约：WS + REST + pu==prev_u 校验
│   ├── orderbook/
│   │   └── local_book.py               # 内存 L2 订单簿（SortedDict + Decimal）
│   └── writer/
│       ├── parquet_writer.py            # Parquet 输出（快照立即写/事件批量写）
│       └── timescale_writer.py          # TimescaleDB 输出（可选）
├── scripts/
│   ├── download_historical.py           # Binance 历史数据批量下载
│   ├── download_coinglass_orderbook.py  # CoinGlass Order Book 深度下载
│   ├── generate_sample.py              # 样本采集 + 自动验证
│   ├── parquet_to_csv.py               # Parquet → CSV 转换
│   └── init_db.sql                     # TimescaleDB 表结构
├── config/
│   └── symbols.yaml                    # 品种配置（添加品种只改这里）
├── .env.example                        # 环境变量模板
├── docker-compose.yml                  # TimescaleDB 一键启动
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
```

---

*数据为 Binance L2 聚合订单簿，不含 L3 单笔订单信息。*
*Binance API 使用请遵守 [Binance 使用条款](https://www.binance.com/en/terms)。*
*CoinGlass API 使用请遵守 [CoinGlass 使用条款](https://www.coinglass.com/terms)。*
