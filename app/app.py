"""Kubernetes 本地卷调度演练页（Flask + SQLite）。

业务规则
--------
1. 页面录入带节点的本地 PV（含容量）和待用 PVC（含容量）。
2. 为 Pod 选择候选节点和所需 PVC 后提交一次调度：
   - 已绑定的 PVC 沿用原卷，使用它的 Pod 只能调度到该卷所在节点。
   - 对每个候选节点，尝试用该节点上仍空闲、且容量足够的 PV 满足全部未绑定 PVC
     （降序贪心 + 最小适配），只要有一个候选节点可行即选定该节点并落库新绑定。
   - 所有候选节点都凑不齐 → 说明卡点，保留原有绑定，本次不新增任何绑定。
3. 节点空闲卷盘点（/inventory，检修节点前的逐卷核对）：
   - 值班员选定节点后按卷名翻页，每页最多 50 条；
   - 盘点范围以打开第一页时该节点已登记且空闲的卷为准，用 pv / binding
     两个高水位 id 圈定（一条 SELECT 读取，不复制整份清单）；
   - 中途新录入的卷（id 大于水位）留到下一轮；原先空闲、中途被占用的卷
     仍在清单原位置列出，并显示占用它的 PVC；
   - 每页明细与占用状态来自同一条 SELECT 的已提交快照，
     不跨请求保留读事务，翻页期间照常录卷和调度。
4. 并发安全：
   - SQLite 调度事务以 BEGIN IMMEDIATE 开启，提交串行化；
   - binding 表 pv_id / pvc_id 均为 UNIQUE，数据库层面兜底，
     同一 PVC 只绑定一次、同一 PV 只分给一个 PVC。
"""

import os
import sqlite3

from flask import (
    Flask,
    flash,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("SCHED_DB_PATH", os.path.join(BASE_DIR, "data", "sched.db"))
SCHEMA_PATH = os.path.join(BASE_DIR, "schema.sql")

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "local-drill-secret-key")

# 盘点每页最多展示的卷明细条数
INVENTORY_PAGE_SIZE = 50


# --------------------------------------------------------------------------- #
# 数据库连接
# --------------------------------------------------------------------------- #
def get_db():
    if "db" not in g:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA busy_timeout = 30000")
        g.db = conn
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    """建表（幂等，容器启动时执行）。"""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
            conn.executescript(f.read())
        conn.commit()
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# 辅助
# --------------------------------------------------------------------------- #
def _parse_csv(raw):
    """把表单里的逗号/空白分隔文本解析成去重、去空、保序的列表。"""
    seen = set()
    items = []
    for part in raw.replace("，", ",").split(","):
        name = part.strip()
        if name and name not in seen:
            seen.add(name)
            items.append(name)
    return items


def _parse_int(raw, field):
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        raise ValueError(f"{field}必须是正整数（GiB）")
    if value <= 0:
        raise ValueError(f"{field}必须大于 0")
    return value


def _rollback_quietly(db):
    """回滚事务；连接已不在事务态时忽略错误，保证异常路径不二次抛错。"""
    try:
        db.execute("ROLLBACK")
    except sqlite3.Error:
        pass


def _load_lists(db):
    """读取 PVC / 绑定 / 最近调度记录（首页与 /state 共用）。"""
    pvcs = db.execute(
        "SELECT * FROM pvc ORDER BY name"
    ).fetchall()
    bindings = db.execute(
        """
        SELECT b.id, b.pv_id, b.pvc_id, b.run_id, b.created_at,
               p.name AS pv_name, p.node AS pv_node, p.capacity_gib,
               c.name AS pvc_name, c.request_gib
          FROM binding b
          JOIN pv p  ON p.id = b.pv_id
          JOIN pvc c ON c.id = b.pvc_id
         ORDER BY b.id
        """
    ).fetchall()
    runs = db.execute(
        "SELECT * FROM pod_run ORDER BY id DESC LIMIT 20"
    ).fetchall()
    return pvcs, bindings, runs


def _load_snapshot(db):
    """读取当前 PV / PVC / 绑定 / 最近调度记录，供 /state 全量快照。"""
    pvs = db.execute(
        "SELECT * FROM pv ORDER BY node, name"
    ).fetchall()
    pvcs, bindings, runs = _load_lists(db)
    return pvs, pvcs, bindings, runs


# --------------------------------------------------------------------------- #
# 节点空闲卷盘点：高水位圈定范围 + 卷名 keyset 翻页（服务端无状态）
# --------------------------------------------------------------------------- #
def _inventory_scope(db, node):
    """读取本轮盘点范围的高水位：单条 SELECT，两个水位来自同一已提交快照。

    pv 高水位     —— 该节点此刻已登记卷的最大 id（id 自增单调，之后新录入的
                     卷 id 更大，被范围条件自然排除、留到下一轮）；
    binding 高水位 —— 此刻已提交绑定的最大 id（调度以 BEGIN IMMEDIATE 串行
                     提交，id 顺序即提交顺序），用于判定"第一页时已占用"。
    """
    row = db.execute(
        """
        SELECT (SELECT COALESCE(MAX(id), 0) FROM pv WHERE node = ?) AS pv_hw,
               (SELECT COALESCE(MAX(id), 0) FROM binding)           AS binding_hw,
               datetime('now', 'localtime')                         AS since
        """,
        (node,),
    ).fetchone()
    return row["pv_hw"], row["binding_hw"], row["since"]


def _inventory_page(db, node, pv_hw, binding_hw, after):
    """按卷名游标取一页盘点明细（多取 1 条，用于判断是否还有下一页）。

    范围过滤（整个翻页周期固定不变，保证一轮不重不漏）：
      p.id <= pv_hw                       —— 只含打开第一页时已登记的卷；
      NOT EXISTS binding.id <= binding_hw —— 排除第一页时已占用的卷；
      p.name > after                      —— keyset 游标，卷名 UNIQUE 故全序稳定。
    当前占用状态由同一条 SELECT 的 LEFT JOIN 读出（一次读取的已提交数据）；
    中途新产生的绑定（id > binding_hw）不缩小范围，只把该卷标为已占用。
    """
    return db.execute(
        """
        SELECT p.name, p.capacity_gib, p.created_at,
               c.name AS pvc_name, b.created_at AS bound_at
          FROM pv p
          LEFT JOIN binding b ON b.pv_id = p.id
          LEFT JOIN pvc c ON c.id = b.pvc_id
         WHERE p.node = ?
           AND p.id <= ?
           AND p.name > ?
           AND NOT EXISTS (SELECT 1 FROM binding b0
                            WHERE b0.pv_id = p.id AND b0.id <= ?)
         ORDER BY p.name
         LIMIT ?
        """,
        (node, pv_hw, after, binding_hw, INVENTORY_PAGE_SIZE + 1),
    ).fetchall()


def _try_node(node, pvc_rows, pv_by_id, bound_by_pvc, bound_pv_ids):
    """判断某节点能否满足本次全部 PVC。

    返回 (plan, reason)：
      plan   —— 可行时为 {pvc_id: pv_id} 的新绑定方案（只含未绑定 PVC）
      reason —— 不可行时的人类可读卡点说明
    纯读操作，不写库；调用方在 IMMEDIATE 事务里串行调用。
    """
    plan = {}

    # 1) 已绑定 PVC：沿用原卷，原卷不在本节点即否决
    for pvc in pvc_rows:
        existing = bound_by_pvc.get(pvc["id"])
        if existing is None:
            continue
        pv = pv_by_id[existing["pv_id"]]
        if pv["node"] != node:
            return None, (
                f"PVC {pvc['name']} 已绑定卷 {pv['name']}（节点 {pv['node']}），"
                f"Pod 只能去 {pv['node']}，候选节点 {node} 不满足"
            )

    # 2) 未绑定 PVC：申请量大的先挑，同容量按名字稳定排序
    unbound = [p for p in pvc_rows if p["id"] not in bound_by_pvc]
    unbound.sort(key=lambda p: (-p["request_gib"], p["name"]))

    # 3) 该节点上仍空闲的本地卷：排除全局已绑定的卷；
    #    plan.values() 再排除本次方案内已占用的卷
    free_pvs = [
        pv for pv in pv_by_id.values()
        if pv["node"] == node and pv["id"] not in bound_pv_ids
    ]

    for pvc in unbound:
        candidates = [
            pv for pv in free_pvs
            if pv["id"] not in plan.values()
            and pv["capacity_gib"] >= pvc["request_gib"]
        ]
        if not candidates:
            free_on_node = [
                pv for pv in free_pvs if pv["id"] not in plan.values()
            ]
            if not free_on_node:
                reason = (
                    f"节点 {node} 上已无空闲本地卷可分配给 "
                    f"PVC {pvc['name']}（申请 {pvc['request_gib']} GiB）"
                )
            else:
                detail = "、".join(
                    f"{pv['name']} {pv['capacity_gib']}GiB" for pv in free_on_node
                )
                reason = (
                    f"节点 {node} 上没有容量 ≥ {pvc['request_gib']} GiB 的空闲卷"
                    f"可分配给 PVC {pvc['name']}（剩余空闲卷：{detail}）"
                )
            return None, reason
        # 最小适配：能装下的卷里选容量最小的，给大 PVC 留大卷
        chosen = min(candidates, key=lambda pv: (pv["capacity_gib"], pv["name"]))
        plan[pvc["id"]] = chosen["id"]

    return plan, None


def _do_schedule(db, pod_name, candidate_nodes, pvc_rows, pv_by_id,
                 bound_by_pvc, bound_pv_ids):
    """在已开启的 IMMEDIATE 事务内执行调度，返回 (status, node, message, run_id)。"""
    # 已绑定 PVC 会把候选节点收敛到原卷所在节点
    forced_nodes = {
        pv_by_id[bound_by_pvc[pvc["id"]]["pv_id"]]["node"]
        for pvc in pvc_rows
        if pvc["id"] in bound_by_pvc
    }
    effective_nodes = [
        n for n in candidate_nodes
        if not forced_nodes or n in forced_nodes
    ]

    failure_reasons = []
    for node in effective_nodes:
        plan, reason = _try_node(
            node, pvc_rows, pv_by_id, bound_by_pvc, bound_pv_ids
        )
        if plan is not None:
            cur = db.execute(
                "INSERT INTO pod_run (pod_name, candidate_nodes, required_pvcs,"
                " status, selected_node, message) VALUES (?, ?, ?, 'success', ?, ?)",
                (
                    pod_name,
                    ",".join(candidate_nodes),
                    ",".join(p["name"] for p in pvc_rows),
                    node,
                    f"已选定节点 {node}，新增 {len(plan)} 条绑定"
                    + ("，其余 PVC 沿用原卷" if len(plan) < len(pvc_rows) else ""),
                ),
            )
            run_id = cur.lastrowid
            for pvc_id, pv_id in plan.items():
                # UNIQUE(pv_id)/UNIQUE(pvc_id) 兜底：并发下撞车即 IntegrityError，
                # 由上层回滚整笔事务，原有绑定不受影响
                db.execute(
                    "INSERT INTO binding (pv_id, pvc_id, run_id) VALUES (?, ?, ?)",
                    (pv_id, pvc_id, run_id),
                )
            return "success", node, "调度成功", run_id
        failure_reasons.append(f"[{node}] {reason}")

    reason_text = "；".join(failure_reasons)
    if forced_nodes and not effective_nodes:
        reason_text = (
            f"候选节点 {candidate_nodes} 与已绑定 PVC 强制要求的节点 "
            f"{sorted(forced_nodes)} 无交集"
        )
    db.execute(
        "INSERT INTO pod_run (pod_name, candidate_nodes, required_pvcs,"
        " status, selected_node, message) VALUES (?, ?, ?, 'failed', NULL, ?)",
        (
            pod_name,
            ",".join(candidate_nodes),
            ",".join(p["name"] for p in pvc_rows),
            f"所有候选节点都凑不齐，未新增任何绑定。{reason_text}",
        ),
    )
    return "failed", None, reason_text, None


# --------------------------------------------------------------------------- #
# 路由
# --------------------------------------------------------------------------- #
@app.route("/")
def index():
    db = get_db()
    pvcs, bindings, runs = _load_lists(db)
    # 盘点入口的节点汇总：只按节点聚合计数，不全量加载 PV 清单
    # （卷明细走 /inventory 分页，每页最多 50 条）
    node_stats = db.execute(
        """
        SELECT p.node AS node,
               COUNT(*) AS total,
               SUM(CASE WHEN b.pv_id IS NULL THEN 1 ELSE 0 END) AS free
          FROM pv p
          LEFT JOIN binding b ON b.pv_id = p.id
         GROUP BY p.node
         ORDER BY p.node
        """
    ).fetchall()
    pv_total = sum(row["total"] for row in node_stats)
    return render_template(
        "index.html",
        pvcs=pvcs,
        bindings=bindings,
        runs=runs,
        node_stats=node_stats,
        pv_total=pv_total,
    )


@app.route("/inventory")
def inventory():
    """节点空闲卷盘点：选定节点后按卷名翻页，每页最多 50 条。

    无状态实现：范围高水位与卷名游标都放在翻页链接参数里，服务端不为每轮
    盘点复制整份清单，也不跨请求保留读事务——每个请求各自执行自己的
    SELECT，读完即释放。
    """
    node = request.args.get("node", "").strip()
    if not node:
        flash("请先选择要盘点的节点", "error")
        return redirect(url_for("index"))

    db = get_db()
    pvw_raw = request.args.get("pvw")
    bdw_raw = request.args.get("bdw")
    if pvw_raw is None or bdw_raw is None:
        # 第一页：圈定本轮盘点范围（此后各页沿用同一组水位，一轮不重不漏）
        pv_hw, binding_hw, since = _inventory_scope(db, node)
        after, page = "", 1
    else:
        try:
            pv_hw = int(pvw_raw)
            binding_hw = int(bdw_raw)
            page = max(1, int(request.args.get("page", "1")))
        except ValueError:
            flash("盘点链接参数无效，已重新开始本轮盘点", "error")
            return redirect(url_for("inventory", node=node))
        after = request.args.get("after", "")
        since = request.args.get("since", "")

    rows = _inventory_page(db, node, pv_hw, binding_hw, after)
    has_next = len(rows) > INVENTORY_PAGE_SIZE
    rows = rows[:INVENTORY_PAGE_SIZE]

    next_params = None
    if has_next:
        next_params = {
            "node": node,
            "pvw": pv_hw,
            "bdw": binding_hw,
            "after": rows[-1]["name"],
            "page": page + 1,
            "since": since,
        }

    items = [
        {
            "name": r["name"],
            "capacity_gib": r["capacity_gib"],
            "created_at": r["created_at"],
            "status": "occupied" if r["pvc_name"] is not None else "free",
            "pvc_name": r["pvc_name"],
            "bound_at": r["bound_at"],
        }
        for r in rows
    ]

    if request.args.get("format") == "json":
        # 与网页同一查询逻辑的 JSON 视图，供自动化试跑脚本逐页核对
        return jsonify(
            {
                "node": node,
                "page": page,
                "page_size": INVENTORY_PAGE_SIZE,
                "count": len(items),
                "has_next": has_next,
                "scope": {
                    "pv_highwater": pv_hw,
                    "binding_highwater": binding_hw,
                    "since": since,
                },
                "next_params": next_params,
                "items": items,
            }
        )

    return render_template(
        "inventory.html",
        node=node,
        items=items,
        page=page,
        page_size=INVENTORY_PAGE_SIZE,
        shown_total=(page - 1) * INVENTORY_PAGE_SIZE + len(items),
        has_next=has_next,
        next_url=url_for("inventory", **next_params) if next_params else None,
        restart_url=url_for("inventory", node=node),
        since=since,
    )


@app.route("/state")
def state():
    """JSON 快照，供自动化试跑脚本核对库内状态。"""
    db = get_db()
    pvs, pvcs, bindings, runs = _load_snapshot(db)
    return jsonify(
        {
            "pvs": [dict(row) for row in pvs],
            "pvcs": [dict(row) for row in pvcs],
            "bindings": [dict(row) for row in bindings],
            "runs": [dict(row) for row in runs],
            "run_count": db.execute(
                "SELECT COUNT(*) FROM pod_run"
            ).fetchone()[0],
            "run_success": db.execute(
                "SELECT COUNT(*) FROM pod_run WHERE status='success'"
            ).fetchone()[0],
            "run_failed": db.execute(
                "SELECT COUNT(*) FROM pod_run WHERE status='failed'"
            ).fetchone()[0],
        }
    )


@app.route("/pvs", methods=["POST"])
def create_pv():
    name = request.form.get("name", "").strip()
    node = request.form.get("node", "").strip()
    try:
        capacity = _parse_int(request.form.get("capacity_gib"), "容量")
        if not name:
            raise ValueError("PV 名称不能为空")
        if not node:
            raise ValueError("节点不能为空")
        db = get_db()
        db.execute(
            "INSERT INTO pv (name, node, capacity_gib) VALUES (?, ?, ?)",
            (name, node, capacity),
        )
        db.commit()
        flash(f"已录入本地 PV {name}（节点 {node}，{capacity} GiB）", "success")
    except sqlite3.IntegrityError:
        db.rollback()
        flash(f"PV 名称 {name} 已存在，未重复录入", "error")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("index"))


@app.route("/pvcs", methods=["POST"])
def create_pvc():
    name = request.form.get("name", "").strip()
    try:
        request_gib = _parse_int(request.form.get("request_gib"), "申请容量")
        if not name:
            raise ValueError("PVC 名称不能为空")
        db = get_db()
        db.execute(
            "INSERT INTO pvc (name, request_gib) VALUES (?, ?)",
            (name, request_gib),
        )
        db.commit()
        flash(f"已录入 PVC {name}（申请 {request_gib} GiB）", "success")
    except sqlite3.IntegrityError:
        db.rollback()
        flash(f"PVC 名称 {name} 已存在，未重复录入", "error")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("index"))


@app.route("/schedule", methods=["POST"])
def schedule():
    pod_name = request.form.get("pod_name", "").strip() or "未命名Pod"
    candidate_nodes = _parse_csv(request.form.get("candidate_nodes", ""))
    # 复选框同名多值，必须用 getlist
    pvc_names = _parse_csv(",".join(request.form.getlist("required_pvcs")))

    if not candidate_nodes:
        flash("至少填写一个候选节点", "error")
        return redirect(url_for("index"))
    if not pvc_names:
        flash("至少选择一个所需 PVC", "error")
        return redirect(url_for("index"))

    db = get_db()
    # BEGIN IMMEDIATE：立刻获取 RESERVED 写锁，并发调度在这里排队，避免死锁
    db.isolation_level = None
    db.execute("BEGIN IMMEDIATE")
    try:
        placeholders = ",".join("?" for _ in pvc_names)
        pvc_rows = db.execute(
            f"SELECT * FROM pvc WHERE name IN ({placeholders})", pvc_names
        ).fetchall()
        found_names = {row["name"] for row in pvc_rows}
        missing = [n for n in pvc_names if n not in found_names]
        if missing:
            _rollback_quietly(db)
            flash(f"PVC 不存在：{', '.join(missing)}，本次未调度", "error")
            return redirect(url_for("index"))

        # 按表单顺序还原 PVC 列表（IN 查询不保证顺序）
        pvc_by_name = {row["name"]: row for row in pvc_rows}
        pvc_rows = [pvc_by_name[n] for n in pvc_names]

        pv_rows = db.execute("SELECT * FROM pv").fetchall()
        pv_by_id = {row["id"]: row for row in pv_rows}
        bound_rows = db.execute("SELECT * FROM binding").fetchall()
        bound_by_pvc = {row["pvc_id"]: row for row in bound_rows}
        bound_pv_ids = {row["pv_id"] for row in bound_rows}

        # 兜底一致性检查：已绑定卷必须还在
        for row in bound_rows:
            if row["pv_id"] not in pv_by_id:
                _rollback_quietly(db)
                flash("数据异常：绑定引用的 PV 不存在，已中止", "error")
                return redirect(url_for("index"))

        status, node, reason, _ = _do_schedule(
            db, pod_name, candidate_nodes, pvc_rows, pv_by_id,
            bound_by_pvc, bound_pv_ids,
        )

        if status == "success":
            db.execute("COMMIT")
            # 提交后再查一次，展示本次实际落库的绑定
            fresh = db.execute(
                """
                SELECT p.name AS pv_name, p.node, c.name AS pvc_name
                  FROM binding b
                  JOIN pv p ON p.id = b.pv_id
                  JOIN pvc c ON c.id = b.pvc_id
                 WHERE c.name IN ({})
                 ORDER BY c.name
                """.format(",".join("?" for _ in pvc_names)),
                pvc_names,
            ).fetchall()
            detail = "；".join(
                f"PVC {r['pvc_name']} → 卷 {r['pv_name']}（节点 {r['node']}）"
                for r in fresh
            )
            flash(
                f"Pod {pod_name} 调度成功：选定节点 {node}。绑定：{detail}",
                "success",
            )
        else:
            # 失败也提交 pod_run 审计记录；INSERT binding 一条都没执行，
            # 原有绑定原样保留
            db.execute("COMMIT")
            flash(
                f"Pod {pod_name} 调度失败，保留原有绑定、本次不新增。卡点：{reason}",
                "error",
            )
    except sqlite3.IntegrityError as exc:
        # 并发下 UNIQUE(pv_id)/UNIQUE(pvc_id) 撞车：整笔回滚
        _rollback_quietly(db)
        flash(
            f"并发冲突：所需卷或 PVC 刚被其他请求占用（{exc}），本次提交已回滚，"
            "原有绑定不受影响，请重试",
            "error",
        )
    except Exception:
        _rollback_quietly(db)
        raise
    finally:
        db.isolation_level = ""
    return redirect(url_for("index"))


@app.route("/reset", methods=["POST"])
def reset():
    """清空演练数据（删表重建），方便反复试跑。"""
    db = get_db()
    db.execute("PRAGMA foreign_keys = OFF")
    db.execute("DELETE FROM binding")
    db.execute("DELETE FROM pod_run")
    db.execute("DELETE FROM pv")
    db.execute("DELETE FROM pvc")
    db.execute("PRAGMA foreign_keys = ON")
    db.commit()
    flash("已清空全部 PV、PVC、绑定与调度记录", "success")
    return redirect(url_for("index"))


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5000)
