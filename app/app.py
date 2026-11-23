"""Kubernetes 本地卷调度演练页（Flask + SQLite）。

业务规则
--------
1. 页面录入带节点的本地 PV（含容量）和待用 PVC（含容量）。
2. 为 Pod 选择候选节点和所需 PVC 后提交一次调度：
   - 已绑定的 PVC 沿用原卷，使用它的 Pod 只能调度到该卷所在节点。
   - 对每个候选节点，尝试用该节点上仍空闲、且容量足够的 PV 满足全部未绑定 PVC
     （降序贪心 + 最小适配），只要有一个候选节点可行即选定该节点并落库新绑定。
   - 所有候选节点都凑不齐 → 说明卡点，保留原有绑定，本次不新增任何绑定。
3. 并发安全：
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


def _load_snapshot(db):
    """读取当前 PV / PVC / 绑定 / 最近调度记录，供页面渲染。"""
    pvs = db.execute(
        "SELECT * FROM pv ORDER BY node, name"
    ).fetchall()
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
    return pvs, pvcs, bindings, runs


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
    pvs, pvcs, bindings, runs = _load_snapshot(db)
    return render_template(
        "index.html",
        pvs=pvs,
        pvcs=pvcs,
        bindings=bindings,
        runs=runs,
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
