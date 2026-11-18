"""观测链路缺口诊断验收：用虚构时序样本跑真实诊断与补传。

每个场景独立重置服务，先导入计划/任务/清单与平台接入，再触发诊断。
覆盖：
  * 序号连续但平台延迟（迟到候选，不写设备离线）
  * 网关收件后平台丢批（网关至平台）
  * 设备确认缺报（设备未产生）
  * 来源未完整（原因待核）
  * 设备更换采样计划（版本化，旧版本保留）
  * 重启后序号复位（待核）
  * 重复与乱序投递（折叠统计，缺号不被重复补足）
  * 跨货箱共享网关补传（只重算受影响区间，剩余缺口保留）
  * 人工标注与冲突重开（历史判断可追溯）
  * 解析积压（不计链路丢失）
  * 发布覆盖快照（只更新设备状态不会让已发布报表自动转完整）
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "sender"))

import chain_scenarios as CS  # noqa: E402

pytestmark = pytest.mark.usefixtures("server")


def _bootstrap(c, name):
    """重置服务 → 导入关系/计划/任务/清单 → 平台接入 → 刷新诊断。"""
    c.reset()
    c.seed()
    data = CS.build(name)
    c.chain_import({"relations": data["relations"], "plans": data["plans"],
                    "tasks": data["tasks"], "manifests": data["manifests"]})
    for ev in data.get("ingest") or []:
        c.chain_ingest(ev["device_id"], ev["boot_batch"], ev["events"])
    c.chain_refresh()
    return data


def _gaps(c, device_id="PRB-2002", include_closed=False):
    q = f"?device_id={device_id}"
    if include_closed:
        q += "&include_closed=true"
    return c.chain_gaps(q)["gaps"]


def _only(gaps):
    assert len(gaps) == 1, gaps
    return gaps[0]


# ---------------------------------------------------------------- 场景

def test_baseline_has_no_gap(client):
    _bootstrap(client, "baseline")
    assert _gaps(client) == []
    segs = client.chain_segments("?cargo_batch_id=LOT-A")["segments"]
    assert segs == []


def test_sequence_continuous_platform_late(client):
    """序号连续、上游完整，平台入库集中变晚 → 迟到候选，不写设备离线。"""
    _bootstrap(client, "platform-late")
    g = _only(_gaps(client))
    assert g["seq_from"] == 1020 and g["seq_to"] == 1027
    assert g["cause"] == "platform_transport_late_candidate"
    assert g["segment"] == "transport_or_platform"
    assert g["status"] == "pending"
    assert g["device_seq_continuous"] is True
    assert g["segment"] != "device"  # 不能归为设备侧原因
    assert g["last_device_seq"] == 1019
    assert any("入库时间集中偏晚" in s["summary"] or
               "迟到" in s["summary"] for s in g["supporting"])


def test_gateway_received_platform_lost_batch(client):
    """设备与网关都记录成功，平台完整范围内缺件 → 确认网关至平台。"""
    _bootstrap(client, "gateway-platform-loss")
    g = _only(_gaps(client))
    assert g["cause"] == "gateway_to_platform_loss"
    assert g["segment"] == "gateway_to_platform"
    assert g["status"] == "confirmed"
    assert g["confidence"] == "high"
    assert any("平台完整接收范围内缺件" in s["summary"]
               for s in g["supporting"])
    # 受影响区段按安装关系连接，且未补造温度
    assert g["affected_segments"], g
    seg = g["affected_segments"][0]
    assert seg["cargo_batch_id"] == "LOT-A" and seg["box_id"] == "BX-01"
    assert seg["coverage"] == "uncovered"
    assert seg["observed_count"] == 0
    assert seg["readings_basis"] == []
    curve = client.get("/api/curves?cargo_batch_id=LOT-A")
    cover = curve["coverage"][0]
    assert cover["coverage"] == "uncovered"
    assert any(gp["gap_id"] == g["id"] for gp in cover["gaps"])


def test_device_confirmed_missing(client):
    """设备侧完整生成清单缺号 → 设备未产生。"""
    _bootstrap(client, "device-missing")
    g = _only(_gaps(client))
    assert g["cause"] == "device_not_generated"
    assert g["segment"] == "device"
    assert g["status"] == "confirmed"
    assert g["confidence"] == "high"


def test_platform_only_confirms_platform_missing(client):
    """只有平台清单、无上游清单：单有平台序号断档只能确认平台缺数据。"""
    _bootstrap(client, "platform-only")
    g = _only(_gaps(client))
    assert g["cause"] == "platform_missing_only"
    assert g["segment"] == "platform"
    assert g["status"] == "confirmed"
    assert g["confidence"] != "high"
    assert g["unknown_segments"]  # 仍未知丢失环节
    assert g["segment"] not in ("device_to_gateway", "gateway_to_platform")


def test_scope_incomplete_stays_pending(client):
    """上游清单只声明范围、未逐条列出 → 原因待核，不定位丢失段。"""
    _bootstrap(client, "scope-incomplete")
    g = _only(_gaps(client))
    assert g["cause"] == "pending_scope_incomplete"
    assert g["segment"] == "unknown"
    assert g["status"] == "pending"
    unknown = " ".join(g["unknown_segments"] or [])
    assert "没有可逐序号核对" in unknown or "未覆盖" in unknown


def test_plan_change_keeps_versions(client):
    """设备更换采样计划：新增计划版本，旧任务与旧版本可追溯，无缺口。"""
    _bootstrap(client, "plan-change")
    assert _gaps(client) == []
    plans = client.get("/api/chain/plans")["plans"]
    versions = [(p["plan_key"], p["version"], p["interval_s"]) for p in plans]
    assert ("plan-2002", 1, 30.0) in versions
    assert ("plan-2002", 2, 60.0) in versions
    tasks = client.get("/api/chain/tasks")["tasks"]
    assert len(tasks) == 2
    assert {t["plan_version"] for t in tasks} == {1, 2}


def test_reboot_sequence_reset_pending(client):
    """重启后序号复位：缺号保持原因待核，不判设备未产生。"""
    _bootstrap(client, "reboot-reset")
    g = _only(_gaps(client))
    assert g["cause"] == "pending_sequence_reset"
    assert g["status"] == "pending"
    assert g["segment"] == "unknown"


def test_duplicates_and_unordered(client):
    """重复投递折叠、合并/拆分包、缺号不被重复事件补足。"""
    _bootstrap(client, "duplicates-unordered")
    g = _only(_gaps(client))
    assert g["seq_from"] == 1040 and g["seq_to"] == 1042
    assert g["cause"] == "gateway_to_platform_loss"
    ev = client.chain_events("PRB-2002", "1")["events"]
    # 网关事件只有一次到达记录（重试包是同一组事件的到达记录，不新增采样）
    gw = [e for e in ev if e["tier"] == "gateway_received"]
    assert len(gw) == 60
    assert {e["sample_seq"] for e in gw} == set(range(1000, 1060))
    # 平台重复投递：到达次数 >1，但事件数仍按身份折叠
    plat = [e for e in ev if e["tier"] == "platform_received"]
    assert len(plat) == 57
    assert any(e["arrival_count"] >= 2 for e in plat)
    packets = client.chain_events("PRB-2002", "1")["packets"]
    refs = {p["packet_ref"] for p in packets}
    assert {"PKT-MERGED", "PKT-SPLIT-A", "PKT-SPLIT-B", "PKT-RETRY"} <= refs


def test_content_conflict_pending_and_visible(client):
    """同身份内容不一致：显示具体冲突，判断保持待核。"""
    _bootstrap(client, "conflict")
    g = _only(_gaps(client))
    assert g["cause"] == "pending_content_conflict"
    assert g["status"] == "pending"
    assert g["conflicting"]
    conflicts = client.get("/api/chain/conflicts")["conflicts"]
    assert any(c["sample_seq"] == 1024 and c["status"] == "open"
               for c in conflicts)


def test_parse_backlog_not_link_loss(client):
    """平台已保存原文未解析 → 解析积压，不计链路丢失。"""
    _bootstrap(client, "parse-backlog")
    g = _only(_gaps(client))
    assert g["cause"] == "platform_parse_backlog"
    assert g["status"] == "parse_backlog"
    assert g["segment"] == "platform_parse"
    assert g["seq_from"] == 1000 and g["seq_to"] == 1059


def test_shared_gateway_backfill_scope(client):
    """跨货箱共享网关补传：只重算受影响区间；部分补回保留剩余缺口。"""
    data = _bootstrap(client, "shared-gateway-backfill")
    g = _only(_gaps(client, "PRB-3005"))
    assert g["cause"] == "gateway_to_platform_loss"
    assert g["seq_from"] == 508 and g["seq_to"] == 517
    # 另一货箱不受影响
    assert _gaps(client, "PRB-2002") == []

    # 部分补传（前 5 条）：保留剩余缺口、覆盖从 0 → 部分
    part = [e for e in data["backfill"]["events"] if e["seq"] <= 512]
    c = client
    c.chain_backfill("PRB-3005", "3", part)
    g2 = _only(_gaps(c, "PRB-3005"))
    assert g2["status"] == "partial"
    seg = g2["affected_segments"][0]
    assert seg["coverage"] == "partial"
    assert seg["observed_count"] == 5 and seg["expected_count"] == 10
    assert seg["remaining_gap"]["seqs"] == [513, 514, 515, 516, 517]
    # 不受影响的货箱仍未重开
    assert _gaps(c, "PRB-2002") == []

    # 全部补传：缺口补齐（默认列表只显示当前缺口），历史版本仍可查
    c.chain_backfill("PRB-3005", "3", data["backfill"]["events"])
    assert _gaps(c, "PRB-3005") == []
    g3 = _only(_gaps(c, "PRB-3005", include_closed=True))
    assert g3["status"] == "filled"
    seg = g3["affected_segments"][0]
    assert seg["coverage"] == "covered"
    assert seg["remaining_gap"] is None
    assert len(seg["readings_basis"]) == 10


def test_backfill_history_and_annotation_reopen(client):
    """补传后只重算必要区段；历史判断可追溯；标注后新证据重开但保留旧版本。"""
    data = _bootstrap(client, "gateway-platform-loss")
    g = _only(_gaps(client))
    assert g["version"] == 1

    ann = client.chain_annotate(g["id"], "on-call-zhang",
                                "网关日志 09:12 转发失败",
                                "PRB-2002 seq 1020-1027",
                                "gateway_to_platform_loss")
    assert ann["status"] == "annotated"
    assert ann["annotation"]["operator"] == "on-call-zhang"
    assert ann["version"] == 2

    # 新证据（平台出现该区间一条内容不同的记录）→ 冲突 → 重开
    client.chain_manifest({
        "tier": "platform_received", "device_id": "PRB-2002",
        "boot_batch": "1", "seq_from": 1020, "seq_to": 1027, "complete": False,
        "source": "audit", "source_ref": "AUD-1",
        "events": [{"seq": 1024, "event_id": "AUD-1024",
                    "content": {"std_c": 9.9, "seq": 1024},
                    "event_time": "2026-11-16T09:12:00+00:00"}]})
    client.chain_refresh()
    g2 = client.chain_gap(g["id"])
    assert g2["reopened"] is True
    assert g2["version"] > 2
    reasons = [v["change_reason"] for v in g2["versions"]]
    assert any("人工标注" in r for r in reasons)
    assert any("重开" in r for r in reasons)
    # 冲突记录保留
    assert any(c["sample_seq"] == 1024 for c in
               client.get("/api/chain/conflicts")["conflicts"])


def test_publication_coverage_snapshot(client):
    """发布快照引用当时诊断覆盖；只更新设备状态不会自动改写已发布报表。"""
    _bootstrap(client, "gateway-platform-loss")
    pub = client.build_publication("batch_audit")
    cov = {r["cargo_batch_id"]: r for r in pub["coverage"]}
    assert cov["LOT-A"]["coverage"] == "uncovered"
    snap = client.get(f"/api/publications/{pub['id']}/coverage")["coverage"]
    assert snap[0]["coverage"] == "uncovered"
    # 只改设备状态（补传一条不影响其它区间）→ 旧发布版本快照不变
    c = client
    c.chain_backfill("PRB-2002", "1", [
        {"seq": 1020, "content": {"std_c": 4.7},
         "event_time": "2026-11-16T09:10:00+00:00"}])
    snap2 = c.get(f"/api/publications/{pub['id']}/coverage")["coverage"]
    assert snap2[0]["coverage"] == "uncovered"     # 历史报表不被静默改写


def test_relation_edit_marks_scope_stale(client):
    """安装关系修订使相关区间失效，缺口明确标记滞后。"""
    _bootstrap(client, "gateway-platform-loss")
    g = _only(_gaps(client))
    assert g["scope_state"]["stale"] is False
    client._req("POST", "/api/relations", {
        "probe_id": "PRB-2002", "vehicle_id": "VH-09", "box_id": "BX-09",
        "cargo_batch_id": "LOT-Z", "valid_from": "2026-11-16T12:00:00+00:00",
        "valid_to": None})
    scopes = {s["scope_key"]: s for s in client.get("/api/chain/scopes")["scopes"]}
    assert scopes["PRB-2002/boot-1"]["stale"] is True
    g2 = client.chain_gap(g["id"])
    assert g2["scope_state"]["stale"] is True
    # 重算后恢复最新，且区段按安装关系有效时段重新计算
    client.chain_refresh()
    g3 = client.chain_gap(g["id"])
    assert g3["scope_state"]["stale"] is False


def test_device_to_gateway_loss(client):
    """设备已生成、网关完整接收清单缺记录 → 确认设备至网关缺失。"""
    _bootstrap(client, "device-to-gateway-loss")
    g = _only(_gaps(client))
    assert g["cause"] == "device_to_gateway_loss"
    assert g["segment"] == "device_to_gateway"
    assert g["status"] == "confirmed"
    assert any("网关完整接收清单缺少" in s["summary"] for s in g["supporting"])


def test_device_signals_keep_sources(client):
    """计划停用/维护/断网/缺心跳分别保留来源，不能被一个“离线”吞并。"""
    _bootstrap(client, "signals")
    g = _only(_gaps(client))
    # 不同来源并立时保持待核，不给出单一离线结论
    assert g["status"] == "pending"
    assert g["segment"] == "unknown"
    unknown = " ".join(g["unknown_segments"] or [])
    assert "planned_stop" in unknown
    assert "maintenance" in unknown
    assert "network_outage" in unknown
    assert "心跳" in unknown
    signals = client.get("/api/chain/signals?device_id=PRB-2002")["signals"]
    kinds = {s["kind"] for s in signals}
    assert {"planned_stop", "maintenance", "network_outage",
            "heartbeat_missing"} <= kinds


def test_annotation_survives_refresh(client):
    """常规重算不应把人工标注静默改回自动结论。"""
    _bootstrap(client, "gateway-platform-loss")
    g = _only(_gaps(client))
    ann = client.chain_annotate(g["id"], "op-li", "网关日志确认丢批",
                                "PRB-2002 seq 1020-1027")
    assert ann["status"] == "annotated"
    client.chain_refresh()
    g2 = client.chain_gap(g["id"])
    assert g2["status"] == "annotated"
    assert g2["annotation"]["operator"] == "op-li"
    assert g2["version"] == ann["version"]     # 无证据变化不新增版本
