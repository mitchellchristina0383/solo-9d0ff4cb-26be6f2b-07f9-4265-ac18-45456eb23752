-- Kubernetes 本地卷调度演练：SQLite 表结构
-- 并发安全依赖两层保障：
--   1) binding.pv_id / binding.pvc_id 上的 UNIQUE 约束（硬约束，数据库兜底）
--   2) 调度事务使用 BEGIN IMMEDIATE 提前拿写锁，让并发提交串行化

PRAGMA journal_mode=WAL;

-- 调度提交记录（先建，binding.run_id 外键引用它）
CREATE TABLE IF NOT EXISTS pod_run (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    pod_name        TEXT    NOT NULL,
    candidate_nodes TEXT    NOT NULL,          -- 候选节点，逗号分隔，保留提交顺序
    required_pvcs   TEXT    NOT NULL,          -- 所需 PVC 名称，逗号分隔
    status          TEXT    NOT NULL,          -- success / failed
    selected_node   TEXT,                      -- 成功时选定的节点
    message         TEXT    NOT NULL,          -- 给页面展示的结论/卡点说明
    created_at      TEXT    NOT NULL DEFAULT (datetime('now', 'localtime'))
);

-- 本地持久卷：带节点亲和（卷所在节点）与容量（整数 GiB）
CREATE TABLE IF NOT EXISTS pv (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT    NOT NULL UNIQUE,
    node         TEXT    NOT NULL,
    capacity_gib INTEGER NOT NULL CHECK (capacity_gib > 0),
    created_at   TEXT    NOT NULL DEFAULT (datetime('now', 'localtime'))
);

-- 持久卷声明：带申请容量（整数 GiB）
CREATE TABLE IF NOT EXISTS pvc (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT    NOT NULL UNIQUE,
    request_gib  INTEGER NOT NULL CHECK (request_gib > 0),
    created_at   TEXT    NOT NULL DEFAULT (datetime('now', 'localtime'))
);

-- PV-PVC 绑定：一个 PV 最多分给一个 PVC，一个 PVC 最多绑定一次
CREATE TABLE IF NOT EXISTS binding (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    pv_id      INTEGER NOT NULL UNIQUE REFERENCES pv(id),
    pvc_id     INTEGER NOT NULL UNIQUE REFERENCES pvc(id),
    run_id     INTEGER REFERENCES pod_run(id),
    created_at TEXT    NOT NULL DEFAULT (datetime('now', 'localtime'))
);
