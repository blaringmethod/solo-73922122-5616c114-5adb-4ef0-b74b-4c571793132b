# 收费公路通行明细分区重排服务

按日登记**源清单**（递增版本、各收费站估算字节数、现行 Parquet 分区范围），由维护员指定
新的分区数与单分区的字节上限，服务输出**重写字节数最少**的连续分区方案；方案确认后可发布
供重写作业读取的清单。

## 快速启动

```bash
docker compose up --build -d
open http://localhost:8000/docs     # Swagger UI，可直接调用
python3 scripts/demo.py             # 端到端演示（需要 httpx）
```

`docker compose down` 停止；数据保存在命名卷 `planner-data`（`/data/repartition.db`）。
Compose 里 `--workers 1` 启动，SQLite 与 WAL 由单进程串行写事务保护。源清单登记把耗时的
范围核验放在写事务之外，写锁只在版本分配与整批插入期间短暂持有，因此大清单登记不会阻塞
其他日期的发布（见 `tests/test_concurrency.py` 的交错测试）。

## 概念与规则

* **源清单**（source manifest）：某一日全部收费站的快照。按 `day` 独立编号，版本号必须
  严格递增（不传 `version` 时服务自动分配 `max+1`）。新版本一登记，旧版本即被取代。
  登记分两阶段：现行范围核验等纯 CPU 工作在**任何数据库事务之外**完成，只有版本分配与
  整批写入放在一个很短的 `BEGIN IMMEDIATE` 事务里。因此即使清单接近 20,000 站、核验耗时
  较长，也不会长期占用 SQLite 写锁——其他日期的方案确认/发布可照常提交；落库失败时整笔
  回滚，不会推进源版本、不会留下部分站点、也不会废弃已发布清单。
* **现行分区范围**（current range）：每个站可声明它当前所在 Parquet 文件的站号区间。
  站号允许有空档；同一区间的所有站必须在站号排序后连续，且该区间必须以已登记站号为两端、
  恰好覆盖区间内出现的全部站号，否则登记失败（`422 invalid_current_ranges`）。核验为
  线性 O(n)：一次遍历建立站号→位置索引并分组，每个区间只做 O(1) 检查，不会为每个区间
  重扫整张站表。
* **并发版本**：同一天并发登记时，自动版本取**落库时刻**的下一版（由写事务串行化，
  绝不会撞号）；显式版本在落库时仍须高于当前版本，否则返回 `409 version_not_increasing`，
  同号并发由 `UNIQUE(day, version)` 兜底为 `409 version_exists`。
* **分区规划**：按站号升序把全部站点切成 `partition_count` 个**连续且非空**的分区，
  同一个站不会被拆到两个分区。只有当某个分区的站号范围与源清单中的某个现行范围
  **完全相同**时，才可以复用该 Parquet 文件；其余必须重写。
* 服务在所有可行方案中选出**重写字节数最少**者（等价于复用字节数最多）；重写字节数相同
  时取分区边界站号字典序最小的方案，保证结果确定可复现。规划使用滑窗前缀 DP，
  复杂度 O(k·n)。无法满足分区数 / 单区上限 / 总容量时返回 `422`，并给出 `reason`
  （`station_exceeds_cap`、`too_many_partitions`、`capacity_exceeded`、
  `no_partitioning`、`plan_too_large`）。
* **确认 → 发布**：方案必须先 `confirm` 才能 `publish`。每个源版本**最多一份**发布
  （数据库唯一约束保证，并发下也成立）。同一方案重复发布返回原回执（HTTP 200,
  `created=false`）；换用另一方案发布到同一版本返回 `409 publication_exists`。
* **换版失效**：登记新版本后，旧版本的发布清单不再供应（`GET /publication` 返回
  `410 publication_superseded`），旧版本未确认或已确认的方案都不能再确认/发布
  （`409 stale_plan`）。
* **幂等**：相同的 `(源版本, partition_count, max_partition_bytes)` 重复规划返回同一
  `plan_id`。

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/days/{day}/source-manifests` | 登记下一版源清单（201） |
| GET | `/days/{day}/source-manifests` | 列出该日全部版本及当前版本 |
| GET | `/days/{day}/source-manifest` | 取当前版本 |
| POST | `/days/{day}/plans` | 规划最少重写方案（200，幂等） |
| GET | `/days/{day}/plans` | 列出该日方案及其确认状态 |
| GET | `/days/{day}/plans/{plan_id}` | 取单个方案 |
| POST | `/days/{day}/plans/{plan_id}/confirm` | 确认方案 |
| POST | `/days/{day}/plans/{plan_id}/publish` | 发布（首次 201；重试 200） |
| GET | `/days/{day}/publication` | 重写作业读取的已发布清单 |
| GET | `/health` | 健康检查 |

`/docs` 中的请求体都附带可直接发送的示例。

## 配置

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATABASE_PATH` | `/data/repartition.db` | SQLite 文件路径 |
| `MAX_STATIONS_PER_MANIFEST` | `20000` | 单次登记站数上限（超出 `413`） |
| `MAX_DP_CELLS` | `4000000` | 规划 DP 的 `k·n` 单元上限（超出 `422 plan_too_large`） |

## 测试

```bash
pip install -r requirements-dev.txt
python -m pytest tests -q
```

`tests/test_planner.py` 用穷举校验 DP 最优性与结构约束；`tests/test_api.py` 覆盖接口
语义；`tests/test_concurrency.py` 启动真实 uvicorn + SQLite 文件，验证并发登记版本、
并发规划与并发发布各自的结果。`scripts/demo.py` 是可对着运行中服务执行的端到端示例。

## 已知限制

* 只支持单实例（`--workers 1`）；SQLite 不适合多机横向扩展。
* 发布清单只记录分区边界与复用/重写动作，不生成实际 Parquet 文件——重写作业需按清单执行。
* 未实现鉴权、按日清理与保留策略；`operator` 目前仅作审计记录。
