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
  C. 节点空闲卷盘点（按卷名分页，每页最多 50 条）
     - 范围以打开第一页时已登记且空闲的卷为准，一轮翻完不重不漏
     - 中途新录入的卷留到下一轮；中途被占用的卷仍在原位置并显示占用它的 PVC
     - 翻页期间照常录卷和调度；网页与 JSON 视图一致
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


def get_raw(path):
    """GET 请求，不跟随 302，返回 (status, body)。"""
    req = urllib.request.Request(BASE + path, method="GET")
    try:
        with _OPENER.open(req, timeout=30) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def get_inventory(params):
    """盘点 JSON 视图：params 为首轮 {"node": ...} 或上一页返回的 next_params。"""
    qs = dict(params)
    qs["format"] = "json"
    with urllib.request.urlopen(
        BASE + "/inventory?" + urlencode(qs), timeout=30
    ) as resp:
        return json.loads(resp.read().decode())


def fetch_round(first_params):
    """跟随 next_params 翻完整轮盘点，返回页列表。"""
    pages = []
    params = first_params
    while True:
        page = get_inventory(params)
        pages.append(page)
        if not page["has_next"]:
            return pages
        params = page["next_params"]


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

# --------------------------------------------------------------------------- #
section("C1. 盘点数据准备：inv-a 120 卷（3 卷容量特殊）、inv-b 3 卷（含中文名）")
post("/reset", {})
special = {5: 9, 50: 10, 70: 7}          # 用于定点绑定的特殊容量
for i in range(1, 121):
    add_pv(f"inv-a-{i:04d}", "inv-a", special.get(i, 5))
for name in ["inv-b-1", "inv-b-2", "卷-甲"]:
    add_pv(name, "inv-b", 5)
# 第一页打开前就被占用的两卷：pvc-pre-9→inv-a-0005(9Gi)、pvc-pre-10→inv-a-0050(10Gi)
add_pvc("pvc-pre-9", 9)
add_pvc("pvc-pre-10", 10)
schedule("pod-pre-9", "inv-a", ["pvc-pre-9"])
schedule("pod-pre-10", "inv-a", ["pvc-pre-10"])
st = get_state()
b = binding_map(st)
check("定点绑定成功：pvc-pre-9→inv-a-0005、pvc-pre-10→inv-a-0050",
      b.get("pvc-pre-9", {}).get("pv_name") == "inv-a-0005"
      and b.get("pvc-pre-10", {}).get("pv_name") == "inv-a-0050",
      str([(x["pvc_name"], x["pv_name"]) for x in st["bindings"]]))

section("C2. 打开第一页：50 条、范围水位确定；随后翻页期间照常录卷和调度")
r1_pages = [get_inventory({"node": "inv-a"})]
r1_scope = r1_pages[0]["scope"]
check("第一页 50 条、还有下一页",
      r1_pages[0]["count"] == 50 and r1_pages[0]["has_next"] is True
      and r1_pages[0]["page"] == 1)
check("范围水位已圈定（pv/binding 高水位均 > 0）",
      r1_scope["pv_highwater"] > 0 and r1_scope["binding_highwater"] > 0,
      str(r1_scope))
# 翻页期间中途变化：新录 3 卷（分别落在已翻过/中间/未到的位置）+ 绑定 1 卷
add_pv("inv-a-0000", "inv-a", 5)         # 名字排在最前（已翻过的区域）
add_pv("inv-a-00605", "inv-a", 5)        # 名字插在未翻到的页中间
add_pv("inv-a-0130", "inv-a", 5)         # 名字排在最后
add_pvc("pvc-mid-7", 7)
code, _ = schedule("pod-mid", "inv-a", ["pvc-mid-7"])
st = get_state()
check("翻页期间照常调度：pvc-mid-7 绑到 inv-a-0070（7Gi 最小适配）",
      code == 302
      and binding_map(st).get("pvc-mid-7", {}).get("pv_name") == "inv-a-0070",
      str(binding_map(st).get("pvc-mid-7")))

section("C3. 继续翻完本轮：中途新录入的卷不出现，被占用的卷仍在原位置")
params = r1_pages[0]["next_params"]
while params is not None:
    r1_pages.append(get_inventory(params))
    params = r1_pages[-1]["next_params"]
counts = [p["count"] for p in r1_pages]
check("共 3 页，每页 ≤ 50 条（50/50/18）",
      counts == [50, 50, 18] and all(c <= 50 for c in counts), str(counts))
check("各页范围水位一致（同一轮）",
      all(p["scope"] == r1_scope for p in r1_pages))
names1 = [it["name"] for p in r1_pages for it in p["items"]]
expected1 = {f"inv-a-{i:04d}" for i in range(1, 121)} - {"inv-a-0005", "inv-a-0050"}
check("一轮 118 条不重不漏（全集 = 120 - 2 个事先占用）",
      len(names1) == len(set(names1)) == 118 and set(names1) == expected1,
      f"got {len(names1)} unique {len(set(names1))}")
check("整轮按卷名严格升序", names1 == sorted(names1))
check("中途新录入的 3 卷留到下轮（本轮任何页都不出现）",
      not ({"inv-a-0000", "inv-a-00605", "inv-a-0130"} & set(names1)))
mid = next((it for p in r1_pages for it in p["items"]
            if it["name"] == "inv-a-0070"), None)
check("中途被占用的 inv-a-0070 仍在原位置（第 2 页按名序）并显示占用 PVC",
      mid is not None and mid["status"] == "occupied"
      and mid["pvc_name"] == "pvc-mid-7"
      and r1_pages[1]["items"] and
      any(it["name"] == "inv-a-0070" for it in r1_pages[1]["items"]),
      str(mid))
check("其余各卷状态均为空闲",
      all(it["status"] == "free" for p in r1_pages for it in p["items"]
          if it["name"] != "inv-a-0070"))

section("C4. 重新开始一轮：上轮的占用卷退出范围，中途新录入的卷进入范围")
r2_pages = fetch_round({"node": "inv-a"})
names2 = [it["name"] for p in r2_pages for it in p["items"]]
expected2 = (expected1 | {"inv-a-0000", "inv-a-00605", "inv-a-0130"}) - {"inv-a-0070"}
check("第二轮 120 条不重不漏（+3 新录入 -1 上轮中途被占用）",
      len(names2) == len(set(names2)) == 120 and set(names2) == expected2,
      f"got {len(names2)}")
check("第二轮每页 ≤ 50 条且全部空闲",
      all(p["count"] <= 50 for p in r2_pages)
      and all(it["status"] == "free" for p in r2_pages for it in p["items"]))
check("两轮水位不同（新一轮重新圈定）",
      r2_pages[0]["scope"]["pv_highwater"] > r1_scope["pv_highwater"])

section("C5. 其他节点与边界：inv-b 一轮 3 卷（含中文名）、空节点、非法参数")
rb = fetch_round({"node": "inv-b"})
names_b = [it["name"] for p in rb for it in p["items"]]
check("inv-b 单页 3 卷、无下一页、含中文卷名",
      len(rb) == 1 and rb[0]["count"] == 3 and rb[0]["has_next"] is False
      and set(names_b) == {"inv-b-1", "inv-b-2", "卷-甲"}, str(names_b))
re_ = get_inventory({"node": "inv-c"})
check("无卷节点：0 条、无下一页", re_["count"] == 0 and re_["has_next"] is False)
code, _ = get_raw("/inventory")
check("未选节点 → 302 回首页", code == 302, f"code={code}")
code, _ = get_raw("/inventory?node=inv-a&pvw=abc&bdw=1")
check("非法水位参数 → 302 重新开始本轮", code == 302, f"code={code}")

section("C6. 网页自测：首页盘点入口与盘点页渲染、翻页链接携带水位与游标")
code, body = get_raw("/")
check("首页 200，含盘点入口与节点下拉",
      code == 200 and "节点空闲卷盘点" in body and 'name="node"' in body)
code, body = get_raw("/inventory?node=inv-a")
check("盘点第一页 200，含卷明细行与「下一页」链接（带水位 pvw/bdw 与游标 after）",
      code == 200 and 'data-pv="inv-a-0000"' in body
      and "下一页" in body and "pvw=" in body and "bdw=" in body and "after=" in body)
check("第一页恰含 50 行卷明细", body.count("data-pv=") == 50,
      f"rows={body.count('data-pv=')}")

print("\n========================================")
print(f"断言通过 {PASS} / 失败 {FAIL}")
print("========================================")
sys.exit(1 if FAIL else 0)
