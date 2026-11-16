# 数据服务编排发布平台

面向数据分析结果的API服务化、查询编排、接口限流、服务监控、版本管理与消费方登记的数据服务编排发布平台。

这是一个前后端分离的管理平台：前端 React + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 React + Vite + TypeScript 前端
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

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 服务注册 | `service` | 数据服务 | 服务编号、服务名称、数据来源 |
| 查询编排 | `flow` | 编排流程 | 流程编号、流程名称、步骤序列 |
| 接口限流 | `ratelimit` | 限流规则 | 规则编号、关联服务、每秒上限 |
| 鉴权管理 | `auth` | 鉴权配置 | 配置编号、关联服务、鉴权方式 |
| 服务监控 | `monitor` | 监控指标 | 指标编号、关联服务、QPS阈值 |
| 消费方登记 | `consumer` | 消费应用 | 应用编号、应用名称、订阅服务 |
| 版本管理 | `version` | 版本记录 | 版本号、关联服务、变更描述 |
| 结果缓存 | `cache` | 缓存策略 | 策略编号、关联服务、缓存键模板 |
| 响应转换 | `transform` | 转换模板 | 模板编号、关联服务、输入字段 |
| 网关路由 | `gateway` | 路由规则 | 规则编号、路径前缀、目标服务 |
| 接口文档 | `schema` | 接口定义 | 定义编号、关联服务、请求方法 |
| 数据源配置 | `datasource` | 数据源连接 | 连接编号、连接类型、接入地址 |
| 数据查询 | `query` | 查询定义 | 定义编号、关联数据源、查询模板 |
| 数据清洗 | `cleanse` | 清洗规则 | 规则编号、关联数据集、空值策略 |
| 调用日志 | `log` | 日志记录 | 记录编号、调用服务、请求时间 |
| 调用日志归档 | `log_archive` | 归档窗口/归档数据集 | 归档窗口、记录数量、来源编号、清单哈希 |
| 模拟数据 | `mock` | 模拟规则 | 规则编号、关联服务、触发条件 |
| 数据备份 | `backup` | 备份策略 | 策略编号、目标服务、备份周期 |
| 事件通知 | `notify` | 通知规则 | 规则编号、触发事件、通知渠道 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数据分析与数据应用

## 调用日志归档链路

标准化日志记录 → 按归档窗口写入调用日志归档数据集 → 检查点校验（记录数量、来源编号、
审计记录可复核）→ 审计记录与日志归档契约在同一事务里提交，任一环节失败整体回滚，
可用同一窗口编号从断点重做。

- 后端入口：`app/services/log_archive.py`、`app/routers/log_archive.py`
- 前端页面：`frontend/src/views/log-archive/`
- 备份模块经 `GET /api/backup/archive-contracts` 读取与归档数据集同一份快照
- 冲突优先级：来源版本高者胜 → 同版本状态优先级（已归档 > 记录中 > 草稿 > 已清理）→ 后到达者胜；
  相同调用日志（来源编号 + 请求时间 + 调用服务）在同一窗口只保留一份记录与来源版本
- 导出与打包：`/windows/{window_id}/export/{json|jsonl}` 下载清单，
  `/windows/{window_id}/package` 下载含清单、审计、契约与校验说明的 zip；清单缺行拒绝导出
- 回滚演练：创建窗口时传 `simulate_failure=stage|commit|verify` 制造事务中断
- 测试：`cd backend && .venv/bin/python -m unittest discover -s tests -v`
- 复测口径：以 LOG-0003 的 3 条日志归档，清单记录数量、大小、来源编号一致，重复归档不新增记录
