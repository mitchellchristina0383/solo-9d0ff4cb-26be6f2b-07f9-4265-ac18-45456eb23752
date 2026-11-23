# Kubernetes 本地卷调度演练（Flask + SQLite）

一个单页 Web 演练台：录入**带节点的本地 PV**（含容量）和**待用 PVC**（含申请容量），
为 Pod 选择候选节点与所需 PVC 后提交调度，页面展示选定节点与 PV-PVC 绑定关系。

## 一、调度规则（与 Kubernetes 本地卷语义对齐）

1. **PV 带节点亲和**：每个本地 PV 固定属于一个节点，只能被该节点上的 Pod 使用。
2. **容量匹配**：PV 容量 ≥ PVC 申请量才可绑定（单位 GiB，正整数）。
3. **候选节点按序尝试**：对每个候选节点判断能否满足本次**全部** PVC，
   第一个可行的节点即被选定，并在同一事务内落库本次新绑定。
4. **已绑定 PVC 沿用原卷（节点粘性）**：已绑定的 PVC 不再参与选卷，使用它的 Pod
   只能被调度到该卷所在节点；若候选节点与之无交集，直接判定失败并说明原因。
5. **选卷策略**：未绑定 PVC 按申请量降序处理，每个 PVC 在目标节点的空闲卷中选
   **容量最小且够用**的一卷（最小适配，给大 PVC 留大卷）。
6. **凑不齐即失败**：所有候选节点都无法满足时，给出每个节点的具体卡点（无空闲卷 /
   容量不足 / 节点粘性冲突），**保留原有绑定，本次不新增任何绑定**，仅写一条失败审计记录。

## 二、并发安全（同一 PVC 只绑一次、同一 PV 只分给一个 PVC）

两层保障：

| 层级 | 手段 | 作用 |
| --- | --- | --- |
| 事务 | 每次调度以 `BEGIN IMMEDIATE` 开启 | 立即获取写锁，并发提交在数据库层串行化，避免死锁与读后写竞争 |
| 约束 | `binding.pv_id UNIQUE`、`binding.pvc_id UNIQUE` | 数据库硬兜底：并发撞车时 `IntegrityError` → 整笔回滚，原有绑定不受影响 |

- 多 PVC Pod 的多笔新绑定在**同一事务**内提交，要么全部成功，要么全部回滚，不会出现半套绑定。
- SQLite 开启 WAL 模式与 `busy_timeout=30s`，锁等待期间请求排队而不是立即报错。
- Flask 以 `threaded=True` 启动，可真实并发处理 HTTP 请求。

## 三、目录结构

```
.
├── Dockerfile              # python:3.11-slim，启动时幂等建表再起服务
├── docker-compose.yml      # 一条命令启动；SQLite 数据持久化到命名卷 sched_data
├── requirements.txt        # Flask==3.1.3
├── app/
│   ├── __init__.py
│   ├── app.py              # 路由、调度算法、事务与并发控制
│   ├── schema.sql          # 4 张表 + UNIQUE/CHECK 约束
│   └── templates/index.html# 单页界面（录入 / 调度 / 清单 / 绑定 / 审计 / 重置）
└── tests/
    └── e2e_test.py         # 纯标准库端到端脚本（HTTP 并发 + 状态断言）
```

数据表：`pv`（本地卷）、`pvc`（声明）、`binding`（绑定，双向 UNIQUE）、
`pod_run`（每次调度的审计记录，含成功/失败与卡点说明）。

## 四、用 Docker Compose 启动

```bash
docker compose up --build
# 打开 http://localhost:5000
```

- 数据存于命名卷 `sched_data`（容器内 `/app/data/sched.db`），容器重建数据不丢。
- 页面上的「清空重置」可一键删空全部演练数据。

不用 Docker 时的本地启动方式（需要 Python 3.11 与 Flask）：

```bash
pip install -r requirements.txt
python -c "from app.app import app, init_db; init_db(); app.run(host='0.0.0.0', port=5000, threaded=True)"
```

## 五、页面操作

1. 「录入本地 PV」：名称、所在节点、容量（GiB），重名会被拒绝。
2. 「录入待用 PVC」：名称、申请容量（GiB），重名会被拒绝。
3. 「为 Pod 提交调度」：填 Pod 名称、逗号分隔的候选节点，勾选一个或多个 PVC 后提交。
4. 页面下方展示 PV/PVC 清单（空闲/已占用）、当前绑定关系、最近 20 条调度记录。
5. 顶部 flash 条展示本次结论：成功时列出节点与每条 PVC→PV 绑定；失败时说明卡点并确认零新增。

## 六、自动化试跑

`tests/e2e_test.py` 只依赖 Python 标准库，对运行中的服务发真实 HTTP 请求：

```bash
python tests/e2e_test.py            # 默认 http://127.0.0.1:5000
python tests/e2e_test.py http://host:port
```

覆盖：卷跨节点分散调度、最小适配、容量不足/无空闲卷的失败与零新增、已绑定 PVC
节点粘性、已占用 PV 不重复分配、同 PVC 并发只绑一次、不同 PVC 并发抢卷、
多 PVC Pod 并发无半套绑定、200 请求级压力。

## 七、实测结果摘要（交付前在本环境实测）

- 端到端脚本 **46 条断言，连续 5 轮全部通过**。
- 200 并发请求 × 3 轮压力：每轮恰好 5 条绑定、PV/PVC 均无重复、200 条审计完整、HTTP 无 500。
- 直接对 SQLite 验证：同 PV 重复分配、同 PVC 重复绑定、容量 ≤ 0、重名录入均被约束拒绝。

> 说明：交付环境未安装 Docker，`docker compose` 未在本机实际执行；已用与
> Dockerfile CMD 完全一致的启动命令在本机实测通过，compose 文件通过 YAML 合法性校验。
