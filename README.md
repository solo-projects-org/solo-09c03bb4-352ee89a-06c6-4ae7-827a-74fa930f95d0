# 码头过磅明细按日数据集服务

把挂载目录里一组**同结构** Parquet 过磅明细，按**过磅时间的 UTC 日期**发布成可按日取文件/取数的数据集。
FastAPI + PyArrow 实现，Docker Compose 启动；原始文件只读挂载、绝不改写。

## 设计要点

### 1. 三级处理路径（尽量不读数据行）

对每个输入文件：

1. **页脚快路径 `reuse_footer`**：只读取 Parquet footer 的列统计（min/max/null_count，不读任何数据行）。
   若能证明时间列**非空且全部落在同一个 UTC 日**，直接在清单里登记原文件路径，零数据读取。
2. **读明细确认 `reuse_scan`**：页脚证明不了（如 `write_statistics=False`、字符串统计无法判定）时，
   只读时间列：确认非空且同日 → **仍复用原档**，不重写。
3. **跨日拆写 `split`**：确认一份文件混了多个 UTC 日时，才读全表并按日过滤，
   写到 `staged/v<版本号>/` 下的新 Parquet（临时文件 + `os.replace`，无半成品）。
   零行文件记为 `empty`，不产生任何按日条目。

### 2. 空时间与发布失败

- 时间列为 null、空白字符串或无法解析 → 立即中止发布（`PublishError`），API 返回 422。
- 写盘失败等任何异常同理：清理本次暂存目录，**`current.json` 不会被替换**。
- 按日取文件/取数的 API 永远只认 `current.json`，因此失败后读到的仍是上一版清单。

### 3. 原子换版与不重不漏

- 新版本的所有拆写文件全部写完、行数按来源文件核对无误后，才把新清单写入临时文件并 `os.replace`
  原子替换 `current.json`（归档到 `versions/manifest-vXXXXXX.json`）。
- 发布时按 `(date, source_file)` 复核行数，保证每条输入记录只在其实际过磅日出现一次，不重不漏。
- 发布进程间用 `flock` 互斥；输入路径限定在 `input_dir` 内，拒绝绝对路径与 `..`。

### 4. UTC 取日规则

- 带时区 timestamp：先转 UTC 再取日（例：北京 11-24 07:30 → UTC 11-23 23:30，归 11-23）。
- 无时区 timestamp/date：按存储字面值取日（落地统一 UTC 的约定）。
- 字符串：按 `WEIGH_TIME_STRING_FORMAT` 解析，未配置则尝试常见 ISO 格式；解析失败按空时间处理。

### 5. 批次回查与行位映射

稽核员按**原档行号**抽查过磅明细，接口 `GET /batches/lookup` 传当前清单版号、原档相对
路径、从 0 起算的起始行号与条数，按原档行序返回明细并在每行附 `source_row_index`：

- **复用原档（original）**：直接按行组剪枝读原文件，行号即文件内行号；
- **跨日拆片（split）**：每个拆片配套一份行位映射 Parquet（`.rowmap.parquet`，单列
  int64 `source_row_index`，与拆片 row group 逐组对齐、逐行对应原档 0 起行号）。
  回查时先用映射页脚统计剪掉无交集的行组，只读取命中行所在的映射组与拆片组；
  票号或整行内容相同的记录因原行号不同，会在各自原位置各返回一次，不去重。
- **版号快照**：请求开始先核对版号，整页只读这一版清单（一次 `current.json` 读取后
  全程用快照）。提交旧版号返回 409 `version_stale` 提示重取清单；版号无归档返回 404
  `version_missing`；当前版的旧拆片没有行位映射时返回 409 `row_map_missing`（无法回查，
  重新发布即补齐）。
- **换版闸门**：拆片与行位映射齐全、同一原档的映射行号合并后恰好连续覆盖
  `[0, 原档行数)`（无重复、无越界）且行组对齐，才允许原子换版；任一不满足即中止发布，
  `current.json` 不动，失败后上一版仍可回查。
- 发布时的页脚快路径只看 footer 统计，依旧不读任何数据行，也不产生映射。

## 目录结构

```
app/                  应用代码
  config.py           环境变量配置
  timeutil.py         时间列 → UTC 日期
  manifest.py         清单模型与原子换版存储（含 row_map 行位映射登记）
  publisher.py        发布核心（页脚判定/扫描/拆写+映射/校验闸门/锁/回滚）
  lookup.py           批次回查（版号快照、行组剪枝、原档/拆片两条路径）
  service.py          按日读取服务
  api.py              FastAPI 路由
  cli.py              命令行入口
docker/               Dockerfile 与 docker-compose.yml
scripts/make_sample_data.py  样例数据生成
tests/                pytest 测试（33 项）
data_in/              原始 Parquet（compose 只读挂载）
data_out/             current.json、versions/、staged/（拆片与 .rowmap.parquet 映射）
```

## Docker Compose 启动

```bash
# 1) 准备样例数据（可选，也可直接挂真实目录）
python scripts/make_sample_data.py ./data_in

# 2) 启动
docker compose -f docker/docker-compose.yml up --build -d
docker compose -f docker/docker-compose.yml logs -f
```

环境变量（compose 里已给默认值）：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `WEIGH_INPUT_DIR` | `/data/input` | 原始 Parquet 目录（容器内只读） |
| `WEIGH_OUTPUT_DIR` | `/data/output` | 清单与拆写产物目录 |
| `WEIGH_TIME_COLUMN` | `weigh_time` | 过磅时间列名 |
| `WEIGH_TIME_STRING_FORMAT` | 空（自动 ISO） | 字符串时间列的 strptime 格式 |
| `WEIGH_LOCK_TIMEOUT` | `10` | 发布锁等待秒数 |

## 可直接试的 API 请求

健康检查：

```bash
curl -s http://localhost:8000/health
```

发布（文件路径相对 `WEIGH_INPUT_DIR`，必须同结构）：

```bash
curl -s -X POST http://localhost:8000/publish \
  -H 'Content-Type: application/json' \
  -d '{"files":[
    "batch_a_sameday.parquet",
    "batch_b_mixed.parquet",
    "batch_d_nostats.parquet"
  ]}'
```

查看当前清单：

```bash
curl -s http://localhost:8000/manifest | python -m json.tool
```

按 UTC 日取文件清单（含下载地址、来源 original/split、行数）：

```bash
curl -s http://localhost:8000/days/2026-11-23/files | python -m json.tool
curl -s http://localhost:8000/days/2026-11-24/files | python -m json.tool
```

按 UTC 日直接读记录（多文件自动拼接，`limit` 控制返回行数）：

```bash
curl -s 'http://localhost:8000/days/2026-11-24/rows?limit=100' | python -m json.tool
```

批次回查（稽核抽查；`version` 取当前清单版号，`start` 为 0 起原档行号）：

```bash
# 跨日档 batch_b_mixed.parquet：从原档第 0 行起取 4 行（跨拆片按原行序拼回，
# 每行附 source_row_index；票号/内容相同的行也各返回一次）
curl -s 'http://localhost:8000/batches/lookup?version=1&source_file=batch_b_mixed.parquet&start=0&limit=4' \
  | python -m json.tool

# 翻页：从原档第 2 行起取 1 行
curl -s 'http://localhost:8000/batches/lookup?version=1&source_file=batch_b_mixed.parquet&start=2&limit=1' \
  | python -m json.tool

# 复用原档的同日文件直接回查
curl -s 'http://localhost:8000/batches/lookup?version=1&source_file=batch_a_sameday.parquet&start=1&limit=2' \
  | python -m json.tool

# 换版后再提交旧版号 → 409 version_stale，提示重新拉取清单
curl -s -w '\n[HTTP %{http_code}]\n' \
  'http://localhost:8000/batches/lookup?version=1&source_file=batch_b_mixed.parquet&start=0&limit=1'
```

下载当日某个 Parquet（URL 取自 files 接口的 `download_url`）：

```bash
curl -s -o day23-split.parquet \
  "http://localhost:8000/days/2026-11-23/files/split/<base64路径>"
```

失败场景（含空时间的文件，返回 422 且 `retained_version` 指向上一版）：

```bash
curl -s -X POST http://localhost:8000/publish \
  -H 'Content-Type: application/json' \
  -d '{"files":["batch_a_sameday.parquet","batch_c_null.parquet"]}'
# 再按日取，仍是旧版：
curl -s http://localhost:8000/days/2026-11-23/files
```

交互式文档：启动后访问 `http://localhost:8000/docs`。

## 命令行用法（容器外本地调试）

```bash
python -m app.cli publish batch_a_sameday.parquet batch_b_mixed.parquet
python -m app.cli manifest
python -m app.cli read 2026-11-23
# 批次回查：版号 原档相对路径 起始行号 条数
python -m app.cli lookup 1 batch_b_mixed.parquet 0 4
```

## 测试

```bash
python -m pytest tests/ -q
```

覆盖：页脚快路径零数据读取、无统计降级扫描复用、跨日拆写不重不漏、空时间中止、
模拟写盘失败保留旧版、成功换版、路径穿越/异结构拒绝、字符串与带时区时间、多 row group、
零行文件，以及 API 全流程与失败后回归旧版；批次回查另覆盖原档直查、拆片行位映射回查、
重复票号/同内容行按原位置各返回、只读命中行组的剪枝、版号快照（旧版号 409/无归档 404/
旧拆片无映射 409）、映射缺失阻断换版、失败后上一版仍可回查。
