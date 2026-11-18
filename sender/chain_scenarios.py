"""观测链路虚构样本生成器（设备侧 / 网关 / 平台三段），不接入厂商后台。

每一场景生成可导入的 JSON：
  plans     采样计划版本（换计划即新增版本，旧版本保留）
  tasks     采集任务（设备 + 启动批次 + 预期序号范围 + 有效时段）
  manifests 设备侧生成 / 网关接收 / 平台入库清单（含包与事件对应、
            重试、合并、拆分、完整性范围、内容摘要）
  ingest    平台实际接收入库并解析的事件（用于与清单对照）
  backfill  可选：网关恢复后补回的迟到数据（用于演示受影响区段重算）

所有数据为虚构样本；时间线固定为 2026-11-16 UTC。
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta

ANCHOR = "2026-11-16T09:00:00+00:00"
INTERVAL = 30.0
LO, HI = 1000, 1059


def _ts(anchor: str, seconds: float) -> str:
    return (datetime.fromisoformat(anchor) + timedelta(seconds=seconds)).isoformat()


def _ev(seq: int, anchor: str, interval: float, temp: float, event_id: str,
        arrival_delay_s: float = 60.0, arrival_base: str | None = None) -> dict:
    event_time = _ts(anchor, (seq - LO) * interval if anchor == ANCHOR else 0)
    if anchor != ANCHOR:
        event_time = _ts(anchor, 0)
    return {"seq": seq, "event_id": event_id, "event_time": event_time,
            "content": {"std_c": temp, "seq": seq},
            "arrival_at": _ts(arrival_base or anchor,
                              (seq - LO) * interval + arrival_delay_s
                              if anchor == ANCHOR else arrival_delay_s)}


def _temps(lo: int, hi: int, base: float = 4.6, spread: float = 0.3) -> dict:
    return {s: round(base + spread * ((s - lo) % 7) / 6.0, 3)
            for s in range(lo, hi + 1)}


def _plan(plan_key: str = "plan-2002", version: int = 1, device: str = "PRB-2002",
          anchor: str = ANCHOR, anchor_seq: int = LO, interval: float = INTERVAL,
          note: str = "") -> dict:
    return {"plan_key": plan_key, "version": version, "device_id": device,
            "interval_s": interval, "anchor_time": anchor,
            "anchor_seq": anchor_seq, "drift_s_per_step": 0.0,
            "effective_from": anchor,
            "note": note or f"每 {interval:.0f}s 采样（计划 v{version}）"}


def _task(task_key: str = "task-2002-b1", device: str = "PRB-2002",
          boot: str = "1", plan_version: int = 1, lo: int = LO, hi: int = HI,
          cargo: str = "LOT-A", box: str = "BX-01", vehicle: str = "VH-01",
          note: str = "") -> dict:
    return {"task_key": task_key, "device_id": device, "boot_batch": boot,
            "plan_key": "plan-2002" if device == "PRB-2002" else "plan-3005",
            "plan_version": plan_version, "expected_seq_from": lo,
            "expected_seq_to": hi, "cargo_batch_id": cargo, "box_id": box,
            "vehicle_id": vehicle, "note": note or "采集任务"}


def _device_manifest(device: str = "PRB-2002", boot: str = "1",
                     lo: int = LO, hi: int = HI, events: list | None = None,
                     complete: bool = True, source_ref: str = "DEV-MF-100",
                     content_only: bool = False) -> dict:
    m = {"tier": "device_generated", "device_id": device, "boot_batch": boot,
         "seq_from": lo, "seq_to": hi, "complete": complete,
         "source": "device-flash", "source_ref": source_ref,
         "received_from": _ts(ANCHOR, 5), "received_to": _ts(ANCHOR, 60 * INTERVAL),
         "events": events if events is not None else []}
    if content_only:
        m["content_only"] = True
    return m


def _gateway_manifest(device: str = "PRB-2002", boot: str = "1",
                      lo: int = LO, hi: int = HI, events: list | None = None,
                      complete: bool = True, source_ref: str = "GW-MF-100",
                      packets: list | None = None,
                      content_only: bool = False) -> dict:
    m = {"tier": "gateway_received", "device_id": device, "boot_batch": boot,
         "seq_from": lo, "seq_to": hi, "complete": complete,
         "source": "gateway", "source_ref": source_ref,
         "received_from": _ts(ANCHOR, 30), "received_to": _ts(ANCHOR, 60 * INTERVAL),
         "events": events if events is not None else [],
         "packets": packets or [{"packet_ref": f"{source_ref}-p1",
                                 "device_ids": [device],
                                 "event_count": len(events or []),
                                 "received_at": _ts(ANCHOR, 60 * INTERVAL)}]}
    if content_only:
        m["content_only"] = True
    return m


def _platform_manifest(device: str = "PRB-2002", boot: str = "1",
                       lo: int = LO, hi: int = HI, events: list | None = None,
                       complete: bool = True, source_ref: str = "PLT-MF-100",
                       unparsed: bool = False) -> dict:
    evs = events if events is not None else []
    if unparsed:
        evs = [dict(e, parsed=False) for e in evs]
    arrivals = sorted(e["arrival_at"] for e in evs if e.get("arrival_at"))
    return {"tier": "platform_received", "device_id": device, "boot_batch": boot,
            "seq_from": lo, "seq_to": hi, "complete": complete,
            "source": "platform", "source_ref": source_ref,
            "received_from": arrivals[0] if arrivals else _ts(ANCHOR, 61),
            "received_to": arrivals[-1] if arrivals else _ts(ANCHOR, 61),
            "events": evs}


def _relation(device: str, cargo: str, box: str, vehicle: str,
             start: str = "2026-11-16T08:00:00+00:00",
             end: str = "2026-11-16T12:00:00+00:00") -> dict:
    return {"probe_id": device, "vehicle_id": vehicle, "box_id": box,
            "cargo_batch_id": cargo, "valid_from": start, "valid_to": end}


RELATIONS = [
    _relation("PRB-2002", "LOT-A", "BX-01", "VH-01"),
    _relation("PRB-3005", "LOT-C", "BX-03", "VH-03"),
]


def _ingest(device: str, boot: str, events: list) -> dict:
    return {"device_id": device, "boot_batch": boot, "events": events}


# ---------------------------------------------------------------- 场景

def scenario_baseline() -> dict:
    """三层清单完整、平台正常入库：没有缺口。"""
    temps = _temps(LO, HI)
    dev = [_ev(s, ANCHOR, INTERVAL, temps[s], f"DEV-{s}") for s in range(LO, HI + 1)]
    gw = [_ev(s, ANCHOR, INTERVAL, temps[s], f"GW-{s}") for s in range(LO, HI + 1)]
    plat = [_ev(s, ANCHOR, INTERVAL, temps[s], f"PLT-{s}") for s in range(LO, HI + 1)]
    return {"relations": RELATIONS, "plans": [_plan()], "tasks": [_task()],
            "manifests": [_device_manifest(events=dev),
                          _gateway_manifest(events=gw),
                          _platform_manifest(events=plat)],
            "ingest": [_ingest("PRB-2002", "1", plat)]}


def scenario_platform_late() -> dict:
    """序号连续、上游完整；平台缺 8 条且其余入库时间集中变晚。

    期望：平台或传输迟到候选（不能写成设备离线）。
    """
    temps = _temps(LO, HI)
    dev = [_ev(s, ANCHOR, INTERVAL, temps[s], f"DEV-{s}") for s in range(LO, HI + 1)]
    gw = [_ev(s, ANCHOR, INTERVAL, temps[s], f"GW-{s}") for s in range(LO, HI + 1)]
    miss = set(range(1020, 1028))
    late = 3000.0
    plat = [_ev(s, ANCHOR, INTERVAL, temps[s], f"PLT-{s}",
                arrival_delay_s=late) for s in range(LO, HI + 1) if s not in miss]
    return {"relations": RELATIONS, "plans": [_plan()], "tasks": [_task()],
            "manifests": [_device_manifest(events=dev),
                          _gateway_manifest(events=gw),
                          _platform_manifest(events=plat)],
            "ingest": [_ingest("PRB-2002", "1", plat)]}


def scenario_gateway_platform_loss() -> dict:
    """设备与网关都记录成功，平台完整接收范围内缺 8 条（1020..1027）。"""
    data = scenario_baseline()
    miss = set(range(1020, 1028))
    data["manifests"][2] = _platform_manifest(
        events=[e for e in data["manifests"][2]["events"] if e["seq"] not in miss])
    data["ingest"] = [_ingest("PRB-2002", "1", data["manifests"][2]["events"])]
    return data


def scenario_device_missing() -> dict:
    """设备侧完整生成清单明确没有 1010..1013 → 设备未产生。"""
    data = scenario_baseline()
    miss = set(range(1010, 1014))
    for m in data["manifests"]:
        m["events"] = [e for e in m["events"] if e["seq"] not in miss]
    data["manifests"][2] = _platform_manifest(
        events=[e for e in data["manifests"][2]["events"] if e["seq"] not in miss])
    data["ingest"] = [_ingest("PRB-2002", "1", data["manifests"][2]["events"])]
    return data


def scenario_device_to_gateway_loss() -> dict:
    """设备与设备侧清单完整，网关完整接收清单缺 6 条 → 确认设备至网关缺失。"""
    data = scenario_baseline()
    miss = set(range(1034, 1040))
    # 设备侧仍然有生成记录；网关没有这些记录
    data["manifests"][1] = _gateway_manifest(
        events=[e for e in data["manifests"][1]["events"] if e["seq"] not in miss])
    data["manifests"][2] = _platform_manifest(
        events=[e for e in data["manifests"][2]["events"] if e["seq"] not in miss])
    data["ingest"] = [_ingest("PRB-2002", "1", data["manifests"][2]["events"])]
    return data


def scenario_signals() -> dict:
    """设备侧无完整生成清单，只有设备信号来源。

    计划停用、维护、运输途中断网与缺失心跳分别保留来源；
    不允许用一个“离线”状态吞并，也不能证明温度采集停止。
    """
    data = scenario_baseline()
    # 只保留平台清单（有完整性），没有设备/网关清单
    data["manifests"] = [data["manifests"][2]]
    miss = set(range(1044, 1052))
    data["manifests"][0] = _platform_manifest(
        events=[e for e in data["manifests"][0]["events"] if e["seq"] not in miss])
    data["manifests"][0]["signals"] = [
        {"kind": "heartbeat_missing", "at": _ts(ANCHOR, 44 * INTERVAL),
         "source": "gateway", "note": "该时段无心跳上报"},
        {"kind": "planned_stop", "at": _ts(ANCHOR, 44 * INTERVAL),
         "source": "dispatch", "note": "计划停用区间开始"},
        {"kind": "network_outage", "at": _ts(ANCHOR, 45 * INTERVAL),
         "source": "carrier", "note": "运输途中断网"},
        {"kind": "maintenance", "at": _ts(ANCHOR, 46 * INTERVAL),
         "source": "ops", "note": "已确认设备维护"},
    ]
    data["ingest"] = [_ingest("PRB-2002", "1", data["manifests"][0]["events"])]
    return data


def scenario_platform_only() -> dict:
    """只有平台清单（无上游清单）：平台序号断档只能确认平台缺数据。"""
    data = scenario_baseline()
    miss = set(range(1026, 1032))
    data["manifests"] = [_platform_manifest(
        events=[e for e in data["manifests"][2]["events"] if e["seq"] not in miss])]
    data["ingest"] = [_ingest("PRB-2002", "1", data["manifests"][0]["events"])]
    return data


def scenario_scope_incomplete() -> dict:
    """平台缺 6 条，但设备/网关只声明范围、未逐条列出 → 原因待核。"""
    data = scenario_baseline()
    miss = set(range(1030, 1036))
    data["manifests"][0] = _device_manifest(complete=True, content_only=True)
    data["manifests"][1] = _gateway_manifest(complete=True, content_only=True)
    data["manifests"][2] = _platform_manifest(
        events=[e for e in data["manifests"][2]["events"] if e["seq"] not in miss])
    data["ingest"] = [_ingest("PRB-2002", "1", data["manifests"][2]["events"])]
    return data


def scenario_plan_change() -> dict:
    """设备更换采样计划 30s → 60s：新增计划 v2，旧任务/旧版本保留。"""
    data = scenario_baseline()
    a2 = _ts(ANCHOR, 60 * INTERVAL)
    t2 = _temps(1060, 1089, base=4.2, spread=0.1)
    ev2 = [{"seq": s, "event_id": f"DEV2-{s}",
            "event_time": _ts(a2, (s - 1060) * 60.0),
            "content": {"std_c": t2[s], "seq": s},
            "arrival_at": _ts(a2, (s - 1060) * 60.0 + 45)} for s in range(1060, 1090)]
    data["plans"].append(_plan(version=2, anchor=a2, anchor_seq=1060,
                               interval=60.0, note="变更后每 60s 采样"))
    data["tasks"].append(_task(task_key="task-2002-b1-v2", plan_version=2,
                               lo=1060, hi=1089, note="换计划后的采集任务"))
    data["manifests"] += [
        _device_manifest(lo=1060, hi=1089, events=ev2, source_ref="DEV-MF-101"),
        _gateway_manifest(lo=1060, hi=1089, events=ev2, source_ref="GW-MF-101"),
        {"tier": "platform_received", "device_id": "PRB-2002",
         "boot_batch": "1", "seq_from": 1060, "seq_to": 1089,
         "complete": True, "source": "platform", "source_ref": "PLT-MF-101",
         "received_from": ev2[0]["arrival_at"],
         "received_to": ev2[-1]["arrival_at"], "events": ev2},
    ]
    data["ingest"].append(_ingest("PRB-2002", "1", ev2))
    return data


def scenario_reboot_reset() -> dict:
    """设备重启：boot 1 的空档期间发生重启，序号复位 → 保持原因待核。"""
    data = scenario_baseline()
    miss = set(range(1020, 1028))
    for m in data["manifests"][:2]:
        m["events"] = [e for e in m["events"] if e["seq"] not in miss]
    data["manifests"][1]["signals"] = [
        {"kind": "reboot", "at": _ts(ANCHOR, 20 * INTERVAL),
         "source": "gateway", "note": "设备重启，本地计数复位"}]
    data["manifests"][2] = _platform_manifest(
        events=[e for e in data["manifests"][2]["events"] if e["seq"] not in miss])
    data["ingest"] = [_ingest("PRB-2002", "1", data["manifests"][2]["events"])]
    return data


def scenario_duplicates_unordered() -> dict:
    """重复投递、合并/拆分包与乱序到达；缺号不能被重复事件补足。"""
    data = scenario_baseline()
    temps = _temps(LO, HI)
    packets = [
        {"packet_ref": "PKT-MERGED", "device_ids": ["PRB-2002", "PRB-1001"],
         "event_count": 60, "received_at": _ts(ANCHOR, 60 * INTERVAL + 60),
         "note": "网关把两设备多次发送合成一包"},
        {"packet_ref": "PKT-SPLIT-A", "device_ids": ["PRB-2002"],
         "event_count": 30, "split_of": "PKT-MERGED",
         "received_at": _ts(ANCHOR, 60 * INTERVAL + 90)},
        {"packet_ref": "PKT-SPLIT-B", "device_ids": ["PRB-2002"],
         "event_count": 30, "split_of": "PKT-MERGED",
         "received_at": _ts(ANCHOR, 60 * INTERVAL + 100)},
        {"packet_ref": "PKT-RETRY", "device_ids": ["PRB-2002"],
         "event_count": 60, "retry": True,
         "received_at": _ts(ANCHOR, 60 * INTERVAL + 900),
         "note": "重试包只是到达记录"},
    ]
    gw_events = [dict(_ev(s, ANCHOR, INTERVAL, temps[s], f"GW-{s}"),
                      via=["PKT-MERGED", "PKT-RETRY"])
                 for s in range(LO, HI + 1)]
    data["manifests"][1] = _gateway_manifest(events=gw_events, packets=packets)
    miss = {1040, 1041, 1042}
    plat = [_ev(s, ANCHOR, INTERVAL, temps[s], f"PLT-{s}")
            for s in range(LO, HI + 1) if s not in miss]
    # 前 5 条再投递一次（乱序到达，事件时间不变）
    plat += [dict(e, arrival_at=_ts(ANCHOR, 60 * INTERVAL + 3600))
             for e in plat[:5]]
    data["manifests"][2] = _platform_manifest(events=plat)
    data["ingest"] = [_ingest("PRB-2002", "1", plat)]
    return data


def scenario_conflict() -> dict:
    """同一事件身份在网关与平台内容不一致：显示冲突、不静默采用任一值。"""
    data = scenario_baseline()
    plat = data["manifests"][2]["events"]
    for i, e in enumerate(plat):
        if e["seq"] == 1024:
            plat[i] = dict(e, content={"std_c": 9.9, "seq": 1024})
    data["ingest"] = [_ingest("PRB-2002", "1", plat)]
    return data


def scenario_parse_backlog() -> dict:
    """平台已持久保存原文但尚未解析：解析积压，不是链路丢失。"""
    data = scenario_baseline()
    data["manifests"][2] = _platform_manifest(
        events=data["manifests"][2]["events"], unparsed=True)
    data["ingest"] = []          # 原文在，解析结果还没出来
    return data


def scenario_shared_gateway_backfill() -> dict:
    """跨货箱共享网关：一次补传补回两设备；只重算受影响区间。

    PRB-2002（LOT-A/BX-01）缺 8 条；PRB-3005（LOT-C/BX-03）也经同一网关，
    平台缺 10 条。补传只针对 PRB-3005。
    """
    data = scenario_baseline()
    a2 = ANCHOR
    t2 = _temps(500, 519, base=3.0, spread=0.05)
    ev2 = [{"seq": s, "event_id": f"DEV3-{s}",
            "event_time": _ts(a2, (s - 500) * 30.0),
            "content": {"std_c": t2[s], "seq": s},
            "arrival_at": _ts(a2, (s - 500) * 30.0 + 40)} for s in range(500, 520)]
    miss2 = set(range(508, 518))
    data["plans"].append(_plan(plan_key="plan-3005", device="PRB-3005",
                               anchor_seq=500, note="共享网关另一设备"))
    data["tasks"].append(_task(task_key="task-3005-b3", device="PRB-3005",
                               boot="3", lo=500, hi=519, cargo="LOT-C",
                               box="BX-03", vehicle="VH-03",
                               note="共享网关：另一货箱"))
    data["manifests"] += [
        _device_manifest(device="PRB-3005", boot="3", lo=500, hi=519,
                         events=ev2, source_ref="DEV-MF-300"),
        _gateway_manifest(device="PRB-3005", boot="3", lo=500, hi=519,
                          events=ev2, source_ref="GW-EDGE-2-MF-1",
                          packets=[{"packet_ref": "PKT-GW2-ALL",
                                    "device_ids": ["PRB-2002", "PRB-3005"],
                                    "event_count": 80,
                                    "received_at": _ts(a2, 20 * 30 + 60)}]),
        _platform_manifest(device="PRB-3005", boot="3", lo=500, hi=519,
                           events=[e for e in ev2 if e["seq"] not in miss2],
                           source_ref="PLT-MF-300"),
    ]
    data["ingest"] = [
        _ingest("PRB-2002", "1", data["manifests"][2]["events"]),
        _ingest("PRB-3005", "3", [e for e in ev2 if e["seq"] not in miss2]),
    ]
    data["backfill"] = {"device_id": "PRB-3005", "boot_batch": "3",
                        "events": [e for e in ev2 if e["seq"] in miss2]}
    return data


def load(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


SCENARIOS = {
    "baseline": scenario_baseline,
    "platform-late": scenario_platform_late,
    "gateway-platform-loss": scenario_gateway_platform_loss,
    "device-missing": scenario_device_missing,
    "device-to-gateway-loss": scenario_device_to_gateway_loss,
    "signals": scenario_signals,
    "platform-only": scenario_platform_only,
    "scope-incomplete": scenario_scope_incomplete,
    "plan-change": scenario_plan_change,
    "reboot-reset": scenario_reboot_reset,
    "duplicates-unordered": scenario_duplicates_unordered,
    "conflict": scenario_conflict,
    "parse-backlog": scenario_parse_backlog,
    "shared-gateway-backfill": scenario_shared_gateway_backfill,
}


def build(name: str) -> dict:
    return SCENARIOS[name]()


def dump(name: str, path: str) -> str:
    data = build(name)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, separators=(",", ":"))
    return path
