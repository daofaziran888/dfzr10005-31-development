# 数仓治理与运维平台

面向数据仓库团队的治理与运维一体化平台。覆盖表生命周期管理、存储成本优化、数据热度分析、任务依赖编排、SLA告警、资源弹性调度与数仓健康度评估的完整治理链路。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 基线时间推演

基线配置里的承诺时间、预警时间不再手工填写，由 DAG 的关键路径推演得到：

- 输入：依赖范围、关键路径终点、节点超时、基线最近产出与 SLA 产出时间；
- 输出：最早可承诺时间、预警起点、零松弛节点、风险摘要和不可推演原因；
- 口径：自然日批次 06:00 起算，松弛以理论完成时间逆推，0 松弛即关键路径节点；
- 留痕：每次推演生成可追溯快照（DAG 版本 + 配置哈希 + 时间口径），同版本只保留一份；
- 联动：推演结果回写基线行、DAG 节点回执与 SLA 规则风险，动作回执同步到 DAG 节点清单。

完整调用链、流程图与判据见 [`docs/baseline-derivation.md`](docs/baseline-derivation.md)。

### 相关接口

```bash
# 推演全部启用基线
curl -X POST http://127.0.0.1:8000/api/baseline/derive -H 'Content-Type: application/json' -d '{}'
# 单条基线 / 按节点推演 / 只预演
curl -X POST .../api/baseline/derive -d '{"baseline_id": 1}'
curl -X POST .../api/baseline/derive -d '{"scope_ref": "DAG-0007", "dry_run": true}'
# 调用链、快照列表与单份快照
curl http://127.0.0.1:8000/api/baseline/derivation/chain
curl http://127.0.0.1:8000/api/baseline/snapshots
curl http://127.0.0.1:8000/api/baseline/snapshots/<snapshot_id>
# DAG 节点回执
curl http://127.0.0.1:8000/api/dag/receipts?baseline=BASE-0001
```

### 测试

```bash
cd backend
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest tests -q
```

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 表生命周期管理 | `lifecycle` | 生命周期策略 | 策略编号、表名称、创建日期 |
| 存储成本优化 | `cost` | 成本分析报告 | 报告编号、分析范围、存储总量 |
| 数据热度分析 | `heatmap` | 热度记录 | 记录编号、表名称、查询次数 |
| 任务依赖编排 | `dag` | 依赖图节点 | 节点编号、节点名称、上游节点 |
| SLA告警管理 | `sla` | SLA规则 + 基线风险 | 规则编号、监控对象、产出时间、基线风险 |
| 资源弹性调度 | `resource` | 资源池配置 | 配置编号、资源类型、最小配额 |
| 数仓健康度评估 | `score` | 健康度报告 | 报告编号、评估范围、存储健康分 |
| 数据漂移检测 | `drift` | 漂移记录 | 记录编号、表名称、漂移字段 |
| 数据资产盘点 | `catalog` | 资产记录 | 资产编号、表名称、资产分类 |
| 数据保留策略 | `retention` | 保留规则 | 规则编号、数据集范围、保留期限 |
| 数据去重管理 | `dedup` | 去重任务 | 任务编号、目标表名、去重字段 |
| 数据分层规划 | `tier` | 分层定义 | 分层编号、分层名称、层级别名 |
| 基线监控管理 | `baseline` | 基线配置 + 时间推演快照 | 基线编号、承诺时间、预警起点、零松弛节点、风险摘要 |
| 数据回收站 | `recycle` | 回收记录 | 记录编号、表名称、删除时间 |
| 变更审计追踪 | `audit` | 变更记录 | 记录编号、变更对象、变更类型 |
| 查询性能分析 | `benchmark` | 性能报告 | 报告编号、查询语句、执行耗时 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖
