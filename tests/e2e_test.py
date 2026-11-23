"""端到端演练脚本：通过真实 HTTP 请求验证调度规则与并发安全。

覆盖场景：
  A. 卷分散在不同节点的多节点调度
     - 候选节点按序尝试、最小适配选卷
     - 凑不齐时给出卡点且保留原有绑定、不新增绑定
     - 已绑定 PVC 沿用原卷、强制 Pod 去往卷所在节点（节点粘性）
     - 已占用 PV 不会再被分给其他 PVC
  B. 并发抢卷
     - 同一 PVC 并发提交只绑定一次
     - 不同 PVC 并发抢同一批 PV：每个 PV 只分给一个 PVC
     - 多 PVC Pod 并发：整笔事务原子，不出现半套绑定
用法：python tests/e2e_test.py [base_url]
"""
import json
import sys
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlencode

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:5000"
PASS, FAIL = 0, 0


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None  # 302 由 HTTPError 捕获


_OPENER = urllib.request.build_opener(_NoRedirect())


def post(path, data):
    body = urlencode(data, doseq=True).encode()
    req = urllib.request.Request(BASE + path, data=body, method="POST")
    try:
        with _OPENER.open(req, timeout=30) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        # 302 对本应用是正常响应（PRG 模式）
        return e.code, e.read().decode()


def get_state():
    with urllib.request.urlopen(BASE + "/state", timeout=10) as resp:
        return json.loads(resp.read().decode())


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name}  {detail}")


def add_pv(name, node, cap):
    post("/pvs", {"name": name, "node": node, "capacity_gib": cap})


def add_pvc(name, req):
    post("/pvcs", {"name": name, "request_gib": req})


def schedule(pod, nodes, pvcs):
    return post("/schedule", {
        "pod_name": pod,
        "candidate_nodes": nodes,
        "required_pvcs": pvcs,
    })


def wait_server():
    for _ in range(50):
        try:
            urllib.request.urlopen(BASE + "/", timeout=2)
            return
        except Exception:
            time.sleep(0.3)
    raise RuntimeError("server not reachable: " + BASE)


def section(title):
    print("\n=== " + title + " ===")


def binding_map(st):
    return {x["pvc_name"]: x for x in st["bindings"]}


# --------------------------------------------------------------------------- #
wait_server()
print("目标服务：", BASE)
post("/reset", {})

# --------------------------------------------------------------------------- #
section("A1. 卷分散在不同节点：数据准备")
# node-a: 10Gi, 5Gi ; node-b: 8Gi, 4Gi ; node-c: 20Gi
for args in [("pv-a1", "node-a", 10), ("pv-a2", "node-a", 5),
             ("pv-b1", "node-b", 8), ("pv-b2", "node-b", 4),
             ("pv-c1", "node-c", 20)]:
    add_pv(*args)
for args in [("pvc-x", 6), ("pvc-y", 4), ("pvc-z", 3), ("pvc-big", 18)]:
    add_pvc(*args)
st = get_state()
check("录入 5 个 PV、4 个 PVC", len(st["pvs"]) == 5 and len(st["pvcs"]) == 4,
      f"pv={len(st['pvs'])} pvc={len(st['pvcs'])}")

section("A2. 多 PVC Pod 首调：node-a 能凑齐，最小适配选卷")
code, _ = schedule("pod-1", "node-a,node-b", ["pvc-x", "pvc-y"])
check("HTTP 302", code == 302, f"code={code}")
st = get_state()
b = binding_map(st)
check("pvc-x 绑定 node-a 的卷", b["pvc-x"]["pv_node"] == "node-a")
check("pvc-y 绑定 node-a 的卷", b["pvc-y"]["pv_node"] == "node-a")
check("最小适配：pvc-x(6)→pv-a1(10)，pvc-y(4)→pv-a2(5)",
      b["pvc-x"]["pv_name"] == "pv-a1" and b["pvc-y"]["pv_name"] == "pv-a2")
check("node-b 的卷保持空闲",
      all(x["pv_node"] != "node-b" for x in st["bindings"]))

section("A3. 凑不齐：pvc-x 已绑 node-a，但 node-a 无空闲卷给 pvc-z → 失败且零新增")
code, _ = schedule("pod-2", "node-b,node-a", ["pvc-x", "pvc-z"])
st = get_state()
check("HTTP 302（失败也走重定向）", code == 302)
check("失败后绑定数不变（仍是 2 条）", len(st["bindings"]) == 2)
check("pvc-z 仍未绑定", "pvc-z" not in binding_map(st))
last = st["runs"][0]
check("审计记录标记 failed", last["status"] == "failed", last["status"])
check("卡点说明指出 node-a 无空闲卷且保留原绑定",
      "无空闲本地卷" in last["message"], last["message"])

section("A4. 节点粘性：只给 node-c/node-b，不含卷所在 node-a → 无交集失败")
code, _ = schedule("pod-2b", "node-c,node-b", ["pvc-x", "pvc-z"])
st = get_state()
check("绑定依旧 2 条、零新增", len(st["bindings"]) == 2)
check("失败信息说明候选节点与强制节点无交集",
      "无交集" in st["runs"][0]["message"], st["runs"][0]["message"])

section("A5. 未绑定 PVC 单独调度 node-b：pvc-z(3)→最小够装 pv-b2(4)")
schedule("pod-3", "node-b", ["pvc-z"])
st = get_state()
check("pvc-z 绑定 pv-b2（node-b，最小适配）",
      binding_map(st)["pvc-z"]["pv_name"] == "pv-b2")

section("A6. 已占用 PV 不会再分：node-b 剩 pv-b1(8)，pvc-big(18) 装不下")
schedule("pod-4", "node-b", ["pvc-big"])
st = get_state()
check("pvc-big 未绑定", "pvc-big" not in binding_map(st))
check("卡点列出 node-b 剩余空闲卷 pv-b1 的容量",
      "pv-b1" in st["runs"][0]["message"] and "没有容量" in st["runs"][0]["message"],
      st["runs"][0]["message"])
check("原 3 条绑定原样保留", len(st["bindings"]) == 3)

section("A7. 换 node-c：pv-c1(20) 满足 pvc-big(18)")
schedule("pod-4b", "node-c", ["pvc-big"])
st = get_state()
check("pvc-big 绑定 pv-c1@node-c",
      binding_map(st)["pvc-big"]["pv_name"] == "pv-c1")
check("累计 4 条绑定，每个 PV/PVC 各出现一次（无重复分配）",
      len(st["bindings"]) == 4
      and len({x["pv_id"] for x in st["bindings"]}) == 4
      and len({x["pvc_id"] for x in st["bindings"]}) == 4)

section("A8. 全部卷占满或容量不足：新 PVC 无卷可分，零新增")
# 此时唯一空闲卷是 pv-b1(8Gi)，申请 9Gi 则任何节点都装不下
add_pvc("pvc-extra", 9)
schedule("pod-5", "node-a,node-b,node-c", ["pvc-extra"])
st = get_state()
check("pvc-extra 未绑定、绑定仍为 4 条",
      "pvc-extra" not in binding_map(st) and len(st["bindings"]) == 4,
      f"bindings={len(st['bindings'])}")
check("失败记录逐节点说明卡点（node-b 明确容量不足，其余节点无空闲卷）",
      st["runs"][0]["status"] == "failed"
      and "node-b" in st["runs"][0]["message"]
      and "没有容量" in st["runs"][0]["message"],
      st["runs"][0]["message"])

section("A9. 已绑定 PVC 沿用原卷：新 Pod 复用 pvc-x/pvc-y，零新增且只能去 node-a")
schedule("pod-6", "node-a", ["pvc-x", "pvc-y"])
st = get_state()
check("绑定数仍为 4（沿用原卷，不新增）", len(st["bindings"]) == 4)
check("成功记录标注 0 条新绑定、沿用原卷",
      "新增 0 条绑定" in st["runs"][0]["message"], st["runs"][0]["message"])
# 换错节点必须失败
schedule("pod-7", "node-b", ["pvc-x"])
st = get_state()
check("复用已绑定 PVC 但节点不对 → 失败",
      st["runs"][0]["status"] == "failed" and len(st["bindings"]) == 4)

# --------------------------------------------------------------------------- #
section("B. 并发抢卷（重置到可控初始状态）")
post("/reset", {})
for args in [("pv1", "n1", 10), ("pv2", "n1", 10), ("pv3", "n2", 10)]:
    add_pv(*args)
for args in [("c1", 5), ("c2", 5), ("c3", 5), ("c4", 5)]:
    add_pvc(*args)

section("B1. 同一 PVC（c1）8 个并发请求：只允许绑定一次")
with ThreadPoolExecutor(max_workers=8) as pool:
    outs = list(pool.map(
        lambda i: schedule(f"race-pvc-{i}", "n1,n2", ["c1"]), range(8)))
st = get_state()
c1_binds = [x for x in st["bindings"] if x["pvc_name"] == "c1"]
check("c1 恰好 1 条绑定", len(c1_binds) == 1, f"got {len(c1_binds)}")
check("8 个请求均正常返回 302（无 500）", all(c == 302 for c, _ in outs))
# 后续 7 个请求复用已绑定的 c1（沿用原卷），同样成功但零新增
check("8 条调度均成功，但只有首条新增绑定，其余均为 0 新增",
      len([r for r in st["runs"] if r["status"] == "success"]) == 8
      and len(st["bindings"]) == 1)
check("复用记录明确标注沿用原卷",
      all("沿用原卷" in r["message"] for r in st["runs"][:7]))

section("B2. 不同 PVC 并发抢同一批卷：3 个卷 vs 4 种 PVC 各 4 个请求（共 16 个）")
before = get_state()
with ThreadPoolExecutor(max_workers=16) as pool:
    # c1 已在 B1 占用 1 个卷；c2/c3/c4 抢剩余 2 个卷
    outs = list(pool.map(
        lambda i: schedule(f"race-mix-c{(i % 4) + 1}-{i}", "n1,n2",
                           [f"c{(i % 4) + 1}"]),
        range(16),
    ))
st = get_state()
bound_pvcs = {x["pvc_name"] for x in st["bindings"]}
bound_pvs = {x["pv_name"] for x in st["bindings"]}
check("总绑定数 = 3（卷数上限），c1 占 1、c2~c4 中再占 2",
      len(st["bindings"]) == 3, f"got {len(st['bindings'])}: {sorted(bound_pvcs)}")
check("同一 PV 只分给一个 PVC（PV 无重复）",
      len(bound_pvs) == len(st["bindings"]))
check("同一 PVC 只绑定一次（PVC 无重复）",
      len(bound_pvcs) == len(st["bindings"]))
check("16 个请求无 500", all(c == 302 for c, _ in outs))
# 用全量计数核对：本批 16 条全部落审计，成功/失败之和为 16
delta_ok = st["run_success"] - before["run_success"]
delta_fail = st["run_failed"] - before["run_failed"]
check("本批 16 条全部落审计（成功为新绑或复用、失败零新增）",
      delta_ok + delta_fail == 16 and delta_fail >= 0,
      f"ok={delta_ok} fail={delta_fail}")

section("B3. 多 PVC Pod 并发：两个不同的双 PVC Pod 抢 3 个卷，必有一个整套失败、无半套绑定")
post("/reset", {})
for args in [("d1", "n1", 5), ("d2", "n1", 5), ("d3", "n1", 5)]:
    add_pv(*args)
for args in [("q1", 1), ("q2", 1), ("q3", 1), ("q4", 1)]:
    add_pvc(*args)
with ThreadPoolExecutor(max_workers=2) as pool:
    futures = [
        pool.submit(schedule, "pod-twin-A", "n1", ["q1", "q2"]),
        pool.submit(schedule, "pod-twin-B", "n1", ["q3", "q4"]),
    ]
    outs = [f.result() for f in futures]
st = get_state()
check("两个请求均 302", all(c == 302 for c, _ in outs))
bound_set = {x["pvc_name"] for x in st["bindings"]}
check("只有一个 Pod 的整套 PVC 被绑定，另一个整套不绑，绝无半套",
      bound_set in ({"q1", "q2"}, {"q3", "q4"}),
      f"bound={sorted(bound_set)}")
check("恰 1 成功 1 失败",
      len([r for r in st["runs"] if r["status"] == "success"]) == 1
      and len([r for r in st["runs"] if r["status"] == "failed"]) == 1,
      str([(r["pod_name"], r["status"]) for r in st["runs"]]))
check("成功 Pod 拿到两个不同的卷",
      len({x["pv_name"] for x in st["bindings"]}) == 2)
check("失败 Pod 的两个 PVC 均无绑定",
      not (bound_set & ({"q3", "q4"} if bound_set == {"q1", "q2"} else {"q1", "q2"})))

section("B4. 压力：20 个双 PVC Pod 并发抢 3 个卷，同一对 PVC 全局只绑一次")
post("/reset", {})
for i in range(3):
    add_pv(f"e{i}", "n1", 5)
for i in range(4):
    add_pvc(f"r{i}", 1)
with ThreadPoolExecutor(max_workers=20) as pool:
    # 所有 Pod 都要 r1+r2；只有第一个能新建绑定，其余沿用或失败
    outs = list(pool.map(
        lambda i: schedule(f"heavy-{i}", "n1", ["r1", "r2"]), range(20)))
st = get_state()
check("无 500", all(c == 302 for c, _ in outs))
check("r1、r2 各只绑定一次，共 2 条绑定、2 个不同 PV",
      len(st["bindings"]) == 2
      and {x["pvc_name"] for x in st["bindings"]} == {"r1", "r2"}
      and len({x["pv_name"] for x in st["bindings"]}) == 2,
      str([(x["pvc_name"], x["pv_name"]) for x in st["bindings"]]))

section("B5. 压力：30 个 Pod 各抢一个独立 PVC，3 个卷 → 恰好 3 个 PVC 各占一卷")
post("/reset", {})
for i in range(3):
    add_pv(f"g{i}", "n1", 5)
for i in range(10):
    add_pvc(f"s{i}", 1)
before = get_state()
with ThreadPoolExecutor(max_workers=30) as pool:
    outs = list(pool.map(
        lambda i: schedule(f"solo-{i}", "n1", [f"s{i}"]), list(range(10)) * 3))
st = get_state()
check("30 个请求无 500", all(c == 302 for c, _ in outs))
check("恰好 3 条绑定，PV/PVC 均不重复",
      len(st["bindings"]) == 3
      and len({x["pv_id"] for x in st["bindings"]}) == 3
      and len({x["pvc_id"] for x in st["bindings"]}) == 3,
      str(sorted(x["pvc_name"] for x in st["bindings"])))
# 用全量计数：30 条请求全部落审计，成功 ≥ 新绑数（含复用），其余失败且零新增
ok_n = st["run_success"] - before["run_success"]
fail_n = st["run_failed"] - before["run_failed"]
won = len(st["bindings"])
check(f"30 条请求全部落审计：{ok_n} 成功（含 {won} 次新绑与复用）、{fail_n} 失败且零新增",
      ok_n + fail_n == 30 and ok_n >= won,
      f"won={won} ok={ok_n} fail={fail_n}")

section("B6. 单卷死斗：1 个 PV、12 个不同 PVC 并发，只有 1 个 PVC 赢")
post("/reset", {})
add_pv("only", "n1", 9)
for i in range(12):
    add_pvc(f"u{i}", 1)
with ThreadPoolExecutor(max_workers=12) as pool:
    outs = list(pool.map(
        lambda i: schedule(f"duel-{i}", "n1", [f"u{i}"]), range(12)))
st = get_state()
check("12 个请求无 500", all(c == 302 for c, _ in outs))
check("全库恰好 1 条绑定，唯一 PV 只分给一个 PVC",
      len(st["bindings"]) == 1
      and st["bindings"][0]["pv_name"] == "only",
      str([(x["pvc_name"], x["pv_name"]) for x in st["bindings"]]))
check("1 成功 11 失败",
      len([r for r in st["runs"] if r["status"] == "success"]) == 1
      and len([r for r in st["runs"] if r["status"] == "failed"]) == 11)

print("\n========================================")
print(f"断言通过 {PASS} / 失败 {FAIL}")
print("========================================")
sys.exit(1 if FAIL else 0)
