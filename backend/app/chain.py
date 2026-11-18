"""观测链路缺口诊断与下游影响。

与需求对应的设计要点：
  * 清单完整性按 (环节, 设备, 启动批次, 明确序号范围) 维护；一条“同步成功”
    不能代替所有设备的完整性。
  * 先统一事件身份 (设备, 启动批次, 序号) 与内容摘要，再比较各环节已确认
    范围内的事件集合；不比较传输包数量。重复投递只增加到达次数，不算新采样。
  * 只有证据支持的传输段才确认；证据不足保持 pending，不把曲线空档
    随意归因于“设备离线”。心跳缺失只表示缺少心跳证据。
  * 平台已持久保存原文但尚未解析 → 解析积压，不计链路丢失。
  * 结果增量维护：清单/计划/关系变化使区间失效并重算相关区间；
    未完成的重算在接口中明确滞后。
  * 人工标注记录操作者/证据/适用范围；后续证据冲突重开并保留旧版本。
  * 缺口按设备、有效安装关系与有效时段连接温控区段；未解析期间不补造温度。
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import or_
from sqlalchemy.orm import Session

from .config import settings
from .models import (
    AffectedSegment, ChainArrival, ChainConflict, ChainEvent, ChainGap,
    ChainGapEvidence, ChainGapVersion, ChainManifest, ChainPacket, ChainRefresh,
    ChainScopeState, ChainSegmentRevision, CollectionTask, CurrentReading,
    DeviceRelation, DeviceSignal, ParsedMessage, Publication, PublicationCoverage,
    RawEnvelope, ReadingVersion, Sample, SamplingPlan,
)

TIER_DEVICE = "device_generated"
TIER_GATEWAY = "gateway_received"
TIER_PLATFORM = "platform_received"
TIER_PARSED = "platform_parsed"
TIERS = (TIER_DEVICE, TIER_GATEWAY, TIER_PLATFORM)

TIER_LABELS = {
    TIER_DEVICE: "设备侧生成清单",
    TIER_GATEWAY: "网关接收清单",
    TIER_PLATFORM: "平台入库清单",
    TIER_PARSED: "平台解析清单",
}

SEG_DEVICE = "device"
SEG_DEVICE_GATEWAY = "device_to_gateway"
SEG_GATEWAY_PLATFORM = "gateway_to_platform"
SEG_PLATFORM = "platform"
SEG_TRANSPORT_PLATFORM = "transport_or_platform"
SEG_PARSE = "platform_parse"
SEG_UNKNOWN = "unknown"

SEGMENT_LABELS = {
    SEG_DEVICE: "设备未生成",
    SEG_DEVICE_GATEWAY: "设备至网关",
    SEG_GATEWAY_PLATFORM: "网关至平台",
    SEG_PLATFORM: "平台缺数据",
    SEG_TRANSPORT_PLATFORM: "平台或传输迟到（候选）",
    SEG_PARSE: "平台消费/解析",
    SEG_UNKNOWN: "原因待核",
}

CAUSE_DEVICE_NOT_GENERATED = "device_not_generated"
CAUSE_DEVICE_TO_GATEWAY = "device_to_gateway_loss"
CAUSE_GATEWAY_TO_PLATFORM = "gateway_to_platform_loss"
CAUSE_PLATFORM_MISSING = "platform_missing_only"
CAUSE_PLATFORM_LATE = "platform_transport_late_candidate"
CAUSE_PARSE_BACKLOG = "platform_parse_backlog"
CAUSE_PENDING_SCOPE = "pending_scope_incomplete"
CAUSE_PENDING_CONFLICT = "pending_content_conflict"
CAUSE_PENDING_ORDER = "pending_sequence_reset"
CAUSE_PENDING_EVIDENCE = "pending_insufficient_evidence"
CAUSE_DEVICE_POWER_OFF = "device_power_off_candidate"
CAUSE_PLANNED_STOP = "device_planned_stop"
CAUSE_MAINTENANCE = "device_maintenance"
CAUSE_NETWORK_OUTAGE = "transport_network_outage"
CAUSE_HEARTBEAT_MISSING = "heartbeat_evidence_missing"
CAUSE_ANNOTATED = "manual_annotation"

STATUS_LABELS = {
    "confirmed": "已确认",
    "pending": "原因待核",
    "partial": "部分补齐（剩余缺口）",
    "parse_backlog": "解析积压",
    "annotated": "人工标注",
    "filled": "已补齐",
    "superseded": "已调整",
}

CAUSE_LABELS = {
    CAUSE_DEVICE_NOT_GENERATED: "设备未产生（设备侧生成清单缺号）",
    CAUSE_DEVICE_TO_GATEWAY: "设备至网关缺失（已确认）",
    CAUSE_GATEWAY_TO_PLATFORM: "网关至平台缺失（已确认）",
    CAUSE_PLATFORM_MISSING: "平台缺数据（仅能确认平台缺）",
    CAUSE_PLATFORM_LATE: "平台或传输迟到候选",
    CAUSE_PARSE_BACKLOG: "解析积压（原文已入库未解析）",
    CAUSE_PENDING_SCOPE: "范围未完整，原因待核",
    CAUSE_PENDING_CONFLICT: "同身份内容冲突，原因待核",
    CAUSE_PENDING_ORDER: "序号复位/乱序，原因待核",
    CAUSE_PENDING_EVIDENCE: "证据不足，原因待核",
    CAUSE_DEVICE_POWER_OFF: "设备断电候选（无生成清单，未确认）",
    CAUSE_PLANNED_STOP: "计划停用（来源保留）",
    CAUSE_MAINTENANCE: "已确认设备维护（来源保留）",
    CAUSE_NETWORK_OUTAGE: "运输途中断网（来源保留）",
    CAUSE_HEARTBEAT_MISSING: "缺少心跳证据（不等于采集中断）",
    CAUSE_ANNOTATED: "人工标注",
}

CONFIRMED_CAUSES = {
    CAUSE_DEVICE_NOT_GENERATED, CAUSE_DEVICE_TO_GATEWAY,
    CAUSE_GATEWAY_TO_PLATFORM, CAUSE_PLATFORM_MISSING,
    CAUSE_PARSE_BACKLOG, CAUSE_PLANNED_STOP, CAUSE_MAINTENANCE,
    CAUSE_NETWORK_OUTAGE,
}


# ---------------------------------------------------------------- 基础工具

def _utc(dt):
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _iso(dt):
    dt = _utc(dt)
    return dt.isoformat() if dt else None


def _canon(obj) -> str:
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _hash(obj) -> str:
    return hashlib.sha256(_canon(obj).encode("utf-8")).hexdigest()


def _scope_key(device_id: str, boot_batch: str) -> str:
    return f"{device_id}/boot-{boot_batch}"


def parse_time(value) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return _utc(value)
    return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))


def invalidate_scope(db: Session, device_id: str, boot_batch: str, reason: str,
                     touch_refresh: bool = True):
    key = _scope_key(device_id, boot_batch)
    st = db.query(ChainScopeState).filter_by(scope_key=key).first()
    if not st:
        st = ChainScopeState(scope_key=key, device_id=device_id,
                             boot_batch=str(boot_batch), stale=True)
        db.add(st)
    st.stale = True
    st.stale_reason = reason[:256]
    if touch_refresh:
        refresh = ChainRefresh(scope_key=key, triggered_by=reason[:120], status="running")
        db.add(refresh)
        db.flush()
        st.last_refresh_id = refresh.id
    db.flush()
    return st


# ---------------------------------------------------------------- 计划与任务

def _plan_time(plan: SamplingPlan, seq: int) -> datetime:
    return _utc(plan.anchor_time) + timedelta(
        seconds=(seq - plan.anchor_seq) * plan.interval_s)


def ensure_sampling_plan(db: Session, plan_key: str, version: int, device_id: str | None,
                         interval_s: float, anchor_time, anchor_seq: int = 0,
                         drift_s_per_step: float = 0.0,
                         effective_from=None, effective_to=None,
                         note: str = "") -> SamplingPlan:
    plan = (db.query(SamplingPlan)
            .filter_by(plan_key=plan_key, version=version).first())
    if plan:
        return plan
    plan = SamplingPlan(
        plan_key=plan_key, version=version, device_id=device_id,
        interval_s=float(interval_s), drift_s_per_step=float(drift_s_per_step),
        anchor_time=parse_time(anchor_time), anchor_seq=int(anchor_seq),
        status="active", effective_from=parse_time(effective_from),
        effective_to=parse_time(effective_to),
        definition={"interval_s": interval_s, "anchor_seq": anchor_seq,
                    "drift_s_per_step": drift_s_per_step},
        note=note,
    )
    db.add(plan)
    db.flush()
    return plan


def ensure_task(db: Session, task_key: str, device_id: str, boot_batch: str,
                plan: SamplingPlan, seq_from: int, seq_to: int,
                cargo_batch_id: str | None = None, box_id: str | None = None,
                vehicle_id: str | None = None, window_start=None, window_end=None,
                note: str = "") -> CollectionTask:
    task = db.query(CollectionTask).filter_by(task_key=task_key).first()
    if task:
        return task
    # 任务未显式给出归属时，按计划锚点时间与安装关系推导（有效时段内）
    if not cargo_batch_id:
        rel = relation_for(db, device_id, _plan_time(plan, seq_from))
        if rel:
            cargo_batch_id, box_id, vehicle_id = (rel.cargo_batch_id, rel.box_id,
                                                  rel.vehicle_id)
    task = CollectionTask(
        task_key=task_key, device_id=device_id, boot_batch=str(boot_batch),
        cargo_batch_id=cargo_batch_id, box_id=box_id, vehicle_id=vehicle_id,
        plan_id=plan.id, expected_seq_from=int(seq_from),
        expected_seq_to=int(seq_to),
        window_start=parse_time(window_start) or _plan_time(plan, seq_from),
        window_end=parse_time(window_end) or _plan_time(plan, seq_to),
        status="active", note=note,
    )
    db.add(task)
    db.flush()
    invalidate_scope(db, device_id, boot_batch, f"新增采集任务 {task_key}")
    return task


def relation_for(db: Session, device_id: str, t) -> DeviceRelation | None:
    t = _utc(t)
    if t is None:
        return None
    rels = (db.query(DeviceRelation)
            .filter(DeviceRelation.probe_id == device_id,
                    DeviceRelation.valid_from <= t,
                    or_(DeviceRelation.valid_to.is_(None),
                        DeviceRelation.valid_to > t))
            .all())
    return rels[0] if len(rels) == 1 else None


# ---------------------------------------------------------------- 清单导入

def import_manifest(db: Session, payload: dict, trig: str = "清单导入") -> ChainManifest:
    """导入一条清单（含事件、包、信号）。返回落库的 ChainManifest。

    payload:
      tier, device_id, boot_batch, seq_from, seq_to, complete, source, source_ref,
      events: [{seq, event_id, content_hash?, content?, event_time?, via: [packet_ref...]}]
      packets: [{packet_ref, device_ids?, event_count?, content_digest?, received_at?,
                 retry?, split_of?, note?}]
      signals: [{kind, at, source?, note?}]
    """
    tier = payload["tier"]
    if tier not in TIERS:
        raise ValueError(f"未知环节 {tier!r}，可选 {TIERS}")
    device_id = str(payload["device_id"])
    boot_batch = str(payload["boot_batch"])
    seq_from = int(payload["seq_from"])
    seq_to = int(payload["seq_to"])
    events = payload.get("events") or []
    raw = {k: v for k, v in payload.items() if k not in ("events",)}
    if payload.get("content_only") and events:
        raw["events_digest"] = _hash([
            {"seq": e.get("seq"), "event_id": e.get("event_id"),
             "hash": e.get("content_hash") or _hash(e.get("content") or {})}
            for e in events])
    manifest = ChainManifest(
        tier=tier, device_id=device_id, boot_batch=boot_batch,
        seq_from=seq_from, seq_to=seq_to, complete=bool(payload.get("complete")),
        source=payload.get("source", ""), source_ref=payload.get("source_ref", ""),
        declared_event_from=parse_time(payload.get("event_from")),
        declared_event_to=parse_time(payload.get("event_to")),
        received_from=parse_time(payload.get("received_from")),
        received_to=parse_time(payload.get("received_to")),
        event_count=len(events), note=payload.get("note", ""),
        raw=raw, manifest_hash=_hash(raw),
    )
    db.add(manifest)
    db.flush()

    packets: dict[str, ChainPacket] = {}
    for p in payload.get("packets") or []:
        pkt = ChainPacket(
            manifest_id=manifest.id, tier=tier,
            packet_ref=str(p["packet_ref"]), device_ids=p.get("device_ids") or [device_id],
            event_count=int(p.get("event_count") or 0),
            content_digest=p.get("content_digest", ""),
            received_at=parse_time(p.get("received_at")),
            retry=bool(p.get("retry")), split_of=p.get("split_of"),
            note=p.get("note", ""))
        db.add(pkt)
        db.flush()
        packets[pkt.packet_ref] = pkt

    for ev in events:
        _upsert_event(db, manifest, tier, device_id, boot_batch, ev, packets)

    for sig in payload.get("signals") or []:
        _add_signal(db, device_id, boot_batch, sig, manifest)

    _detect_conflicts(db, device_id, boot_batch, manifest)
    _reopen_for_conflicts(db, device_id, boot_batch, events, trig)
    invalidate_scope(db, device_id, boot_batch, trig)
    db.flush()
    return manifest


def _upsert_event(db: Session, manifest: ChainManifest, tier: str, device_id: str,
                  boot_batch: str, ev: dict, packets: dict[str, ChainPacket]):
    seq = int(ev["seq"])
    content = ev.get("content") or {}
    content_hash = ev.get("content_hash") or _hash(content)
    event_time = parse_time(ev.get("event_time"))
    event_id = ev.get("event_id")
    arrival_at = (parse_time(ev.get("arrival_at"))
                  or (manifest.received_to if manifest.tier == TIER_PLATFORM
                      else None)
                  or manifest.received_from)

    row = (db.query(ChainEvent)
           .filter_by(tier=tier, device_id=device_id, boot_batch=boot_batch,
                      sample_seq=seq, content_hash=content_hash)
           .first())
    if row is None:
        row = ChainEvent(
            tier=tier, device_id=device_id, boot_batch=boot_batch, sample_seq=seq,
            event_id=event_id, event_ids=[event_id] if event_id else [],
            content_hash=content_hash, content=content, event_time=event_time,
            first_manifest_id=manifest.id, arrival_count=0,
            first_seen_at=arrival_at, last_seen_at=arrival_at,
            last_arrival_at=arrival_at,
        )
        db.add(row)
        db.flush()
    else:
        # 同一身份同内容：只记录到达（重复投递），不新增采样
        if event_id and event_id not in (row.event_ids or []):
            row.event_ids = list(row.event_ids or []) + [event_id]
        row.last_seen_at = arrival_at
        if event_time:
            # 乱序到达：事件时间取较新者；旧到达不覆盖较新状态
            if row.event_time is None or event_time > _utc(row.event_time):
                row.event_time = event_time
                row.content = content
        if arrival_at and (row.last_arrival_at is None
                           or arrival_at > _utc(row.last_arrival_at)):
            row.last_arrival_at = arrival_at

    via = ev.get("via")
    if via is None:
        # 默认一次到达：用清单里第一个包，或生成一个占位包
        if not packets:
            pkt = ChainPacket(manifest_id=manifest.id, tier=tier,
                              packet_ref=f"{manifest.source_ref or 'M' + str(manifest.id)}-p1",
                              device_ids=[device_id], event_count=manifest.event_count,
                              received_at=manifest.received_to or manifest.received_from)
            db.add(pkt)
            db.flush()
            packets[pkt.packet_ref] = pkt
        via = [next(iter(packets))]
    for ref in via:
        pkt = packets.get(ref)
        if not pkt:
            continue
        exists = (db.query(ChainArrival)
                  .filter_by(event_id=row.id, packet_id=pkt.id).first())
        if exists:
            continue
        db.add(ChainArrival(event_id=row.id, packet_id=pkt.id,
                            received_at=pkt.received_at or arrival_at))
        row.arrival_count = (row.arrival_count or 0) + 1
    db.flush()


def _reopen_for_conflicts(db, device_id, boot_batch, events, trig):
    seqs = {int(e["seq"]) for e in events if e.get("seq") is not None}
    for seq in sorted(seqs):
        c = (db.query(ChainConflict)
             .filter_by(device_id=device_id, boot_batch=boot_batch,
                        sample_seq=seq, status="open").first())
        if c:
            reopen_on_conflict(db, device_id, boot_batch, seq, trig)


def _add_signal(db: Session, device_id: str, boot_batch: str, sig: dict,
                manifest: ChainManifest):
    kind = sig.get("kind")
    at = parse_time(sig.get("at")) or manifest.received_from
    exists = (db.query(DeviceSignal)
              .filter_by(device_id=device_id, boot_batch=boot_batch, kind=kind,
                         at=at, note=sig.get("note", "")).first())
    if exists:
        return
    db.add(DeviceSignal(device_id=device_id, boot_batch=boot_batch, kind=kind,
                        at=at, source=sig.get("source", manifest.source or manifest.tier),
                        note=sig.get("note", "")))


def _detect_conflicts(db: Session, device_id: str, boot_batch: str,
                      manifest: ChainManifest):
    """同一 (设备, 启动批次, 序号) 在相同或不同环节出现不同内容 → 冲突。"""
    rows = (db.query(ChainEvent)
            .filter_by(device_id=device_id, boot_batch=boot_batch)
            .order_by(ChainEvent.sample_seq).all())
    by_seq: dict[int, list[ChainEvent]] = {}
    for r in rows:
        by_seq.setdefault(r.sample_seq, []).append(r)
    for seq, evs in by_seq.items():
        hashes = {e.content_hash for e in evs}
        if len(hashes) < 2:
            continue
        for i in range(len(evs)):
            for j in range(i + 1, len(evs)):
                a, b = evs[i], evs[j]
                if a.content_hash == b.content_hash:
                    continue
                exists = (db.query(ChainConflict)
                          .filter_by(device_id=device_id, boot_batch=boot_batch,
                                     sample_seq=seq, hash_a=a.content_hash,
                                     hash_b=b.content_hash).first())
                if exists:
                    continue
                db.add(ChainConflict(
                    device_id=device_id, boot_batch=boot_batch, sample_seq=seq,
                    tier_a=a.tier, tier_b=b.tier, hash_a=a.content_hash,
                    hash_b=b.content_hash, content_a=a.content,
                    content_b=b.content, manifest_a_id=a.first_manifest_id,
                    manifest_b_id=b.first_manifest_id, status="open"))


# ---------------------------------------------------------------- 范围与集合

def _manifests(db: Session, tier: str, device_id: str, boot_batch: str):
    return (db.query(ChainManifest)
            .filter_by(tier=tier, device_id=device_id, boot_batch=boot_batch)
            .all())


def _complete_intervals(db: Session, tier: str, device_id: str, boot_batch: str,
                        enumerated: bool = False):
    """该环节已确认完整的区间列表。

    enumerated=True 时只承认“逐条列出事件”的清单——一条完整声明但未列出
    事件内容的清单只能证明范围，不能逐序号判定缺件。
    """
    out = []
    for m in _manifests(db, tier, device_id, boot_batch):
        if not m.complete:
            continue
        if enumerated and m.event_count <= 0:
            continue
        out.append((int(m.seq_from), int(m.seq_to)))
    return out


def _complete_range(db: Session, tier: str, device_id: str, boot_batch: str):
    """已确认完整区间的外包；None 表示没有完整性证据。"""
    iv = _complete_intervals(db, tier, device_id, boot_batch)
    if not iv:
        return None
    return (min(a for a, _ in iv), max(b for _, b in iv))


def _in_intervals(intervals, seq: int) -> bool:
    return any(a <= seq <= b for a, b in intervals)


def _known_range(db: Session, tier: str, device_id: str, boot_batch: str):
    ms = [m for m in _manifests(db, tier, device_id, boot_batch)]
    if not ms:
        return None
    return (min(m.seq_from for m in ms), max(m.seq_to for m in ms))


def _event_seqs(db: Session, tier: str, device_id: str, boot_batch: str,
                seq_from: int, seq_to: int) -> set[int]:
    rows = (db.query(ChainEvent.sample_seq)
            .filter(ChainEvent.tier == tier, ChainEvent.device_id == device_id,
                    ChainEvent.boot_batch == boot_batch,
                    ChainEvent.sample_seq >= seq_from,
                    ChainEvent.sample_seq <= seq_to)
            .distinct().all())
    return {r[0] for r in rows}


def _platform_evidence(db: Session, device_id: str, boot_batch: str):
    """平台已入库的解析记录：返回 (seq -> ParsedMessage) 与原始信封到达时间。"""
    pms = (db.query(ParsedMessage)
           .filter(ParsedMessage.device_id == device_id,
                   ParsedMessage.boot_batch == boot_batch)
           .order_by(ParsedMessage.id).all())
    by_seq: dict[int, ParsedMessage] = {}
    for pm in pms:
        if pm.sample_seq is None:
            continue
        prev = by_seq.get(pm.sample_seq)
        # 乱序到达不覆盖事件时间较新的状态：保留语义更“确定”的一条
        if prev is None or _pm_rank(pm) > _pm_rank(prev):
            by_seq[pm.sample_seq] = pm
    return by_seq


def _pm_rank(pm: ParsedMessage) -> tuple:
    order = {"resolved": 0, "parsed_new": 1, "parsed_dup": 2, "conflict": 3,
             "pending_contract": 4, "error": 5}
    return (order.get(pm.status, 9), -pm.id)


def _received_at(db: Session, pm: ParsedMessage) -> datetime | None:
    if pm.envelope_id:
        env = db.get(RawEnvelope, pm.envelope_id)
        if env and env.received_at:
            return _utc(env.received_at)
    return _utc(pm.created_at)


def _late_received(db: Session, pm: ParsedMessage) -> datetime | None:
    """平台接收时间中集中变晚的那一支：解析记录本身（列表）的 lenient 标记。"""
    return _received_at(db, pm)


def _tenant_late(seqs: list[int], plan: SamplingPlan,
                 recv: dict[int, datetime | None]) -> bool:
    """平台入库时间相对预期采样集中变晚 → 迟到候选。"""
    late = 0
    total = 0
    for seq in seqs:
        rt = recv.get(seq)
        if rt is None:
            continue
        total += 1
        expected = _plan_time(plan, seq)
        if (rt - expected).total_seconds() > settings.CHAIN_LATE_THRESHOLD_S:
            late += 1
    return total > 0 and late == total


# ---------------------------------------------------------------- 诊断核心

def evaluate_task(db: Session, task: CollectionTask, refresh: ChainRefresh,
                  trig: str) -> dict:
    plan = db.get(SamplingPlan, task.plan_id)
    lo, hi = int(task.expected_seq_from), int(task.expected_seq_to)
    device, boot = task.device_id, str(task.boot_batch)
    out = {"created": 0, "changed": 0, "filled": 0, "gaps": 0}

    comp = {t: _complete_range(db, t, device, boot) for t in TIERS}
    comp_dev = _complete_intervals(db, TIER_DEVICE, device, boot, enumerated=True)
    comp_gw = _complete_intervals(db, TIER_GATEWAY, device, boot, enumerated=True)
    comp_plat = _complete_intervals(db, TIER_PLATFORM, device, boot)

    plat = _platform_evidence(db, device, boot)
    platform_event_seqs = _event_seqs(db, TIER_PLATFORM, device, boot, lo, hi)
    received_seqs = platform_event_seqs | set(plat)

    # 已确认范围的交集：只有各环节都确认完整的范围才能判定某段缺失
    confirmed_lo = lo
    confirmed_hi = hi
    have_any_complete = False
    for intervals in (comp_dev, comp_gw, comp_plat):
        if intervals:
            have_any_complete = True
            confirmed_lo = max(confirmed_lo, min(a for a, _ in intervals))
            confirmed_hi = min(confirmed_hi, max(b for _, b in intervals))
    device_complete = bool(comp_dev)
    gateway_complete = bool(comp_gw)
    platform_complete = bool(comp_plat)
    has_upstream = bool(_manifests(db, TIER_DEVICE, device, boot)
                        or _manifests(db, TIER_GATEWAY, device, boot))

    device_seqs = _event_seqs(db, TIER_DEVICE, device, boot, lo, hi)
    gateway_seqs = _event_seqs(db, TIER_GATEWAY, device, boot, lo, hi)
    platform_seqs = received_seqs

    recv_times = {s: _received_at(db, pm) for s, pm in plat.items()}
    for seq, t in _platform_receipt_times(db, device, boot).items():
        if t and (seq not in recv_times or recv_times[seq] is None
                  or t < recv_times[seq]):
            recv_times[seq] = t
    gateway_times = _gateway_received_times(db, device, boot)

    expected = set(range(lo, hi + 1))

    # 平台缺失序号的连续段（用于“序号连续但入库集中变晚”的判断）
    present = set(plat) | platform_seqs
    runs: list[list[int]] = []
    for seq in sorted(expected):
        if seq in present:
            continue
        if runs and runs[-1][-1] == seq - 1:
            runs[-1].append(seq)
        else:
            runs.append([seq])
    run_cont = {}
    run_late = {}
    known_recv = sorted(k for k in recv_times if recv_times[k])
    for run in runs:
        before = run[0] - 1
        after = run[-1] + 1
        cont = ((run[0] == lo or before in present)
                and (run[-1] == hi or after in present))
        # 用缺口两侧最近的已知入库时间判断“集中变晚”，不要求缺口内部有记录
        neighbours = ([k for k in known_recv if k < run[0]][-4:]
                      + [k for k in known_recv if k > run[-1]][:4])
        late = bool(neighbours) and _tenant_late(neighbours, plan, recv_times)
        for seq in run:
            run_cont[seq] = cont
            run_late[seq] = late

    # 每个缺失序号归因
    reasons: dict[int, str] = {}
    for seq in sorted(expected):
        reasons[seq] = _classify_seq(
            db, task, plan, seq,
            in_confirmed=confirmed_lo <= seq <= confirmed_hi,
            comp_dev=comp_dev, comp_gw=comp_gw, comp_plat=comp_plat,
            device_seqs=device_seqs, gateway_seqs=gateway_seqs,
            platform_seqs=platform_seqs, recv_times=recv_times,
            gateway_times=gateway_times, plat=plat,
            seq_continuous=run_cont.get(seq, False),
            neighbours_late=run_late.get(seq, False),
            has_upstream_manifest=has_upstream)

    # 连续序号聚成缺口
    groups: list[list[int]] = []
    for seq in sorted(expected):
        if reasons[seq] == "present":
            continue
        if groups and groups[-1][-1] == seq - 1 and reasons[groups[-1][-1]] == reasons[seq]:
            groups[-1].append(seq)
        else:
            groups.append([seq])

    seen_keys = set()
    seen_ids = set()
    for g in groups:
        cause = reasons[g[0]]
        gap = _upsert_gap(db, task, plan, g[0], g[-1], cause, refresh, trig,
                          device_complete, gateway_complete, platform_complete,
                          conn_lo=None if not have_any_complete else confirmed_lo,
                          conn_hi=None if not have_any_complete else confirmed_hi,
                          device_seqs=device_seqs, gateway_seqs=gateway_seqs,
                          platform_seqs=platform_seqs,
                          gateway_times=gateway_times, recv_times=recv_times,
                          plat=plat, exclude_ids=seen_ids)
        seen_keys.add(gap.gap_key)
        seen_ids.add(gap.id)
        out["gaps"] += 1
        if gap._new:
            out["created"] += 1
        elif gap._changed:
            out["changed"] += 1
        if gap.status == "filled":
            out["filled"] += 1

    # 之前存在、现已补齐的缺口：关闭并记版本
    existing = (db.query(ChainGap)
                .filter_by(collection_task_id=task.id, active=True).all())
    for gap in existing:
        if gap.id in seen_ids:
            continue
        covered = _missing_seqs(gap, reasons) == []
        vstatus = "filled" if covered else "superseded"
        if covered:
            rel = relation_for(db, task.device_id, gap.time_from)
            _recompute_affected_segments(db, gap, plan, rel, trig,
                                         mark_filled=False)
        _close_gap(db, gap, vstatus,
                   f"按最新清单重算：序号 [{gap.seq_from}, {gap.seq_to}] "
                   f"{'已全部补齐' if covered else '已合并/调整'}",
                   trig)
        if covered:
            out["filled"] += 1
        else:
            out["changed"] += 1
    return out


def _platform_receipt_times(db: Session, device: str, boot: str):
    """平台入库/接收回执时间（按序号取最早到达）。"""
    rows = (db.query(ChainEvent)
            .filter(ChainEvent.tier == TIER_PLATFORM,
                    ChainEvent.device_id == device,
                    ChainEvent.boot_batch == boot).all())
    times = {}
    for ev in rows:
        t = _utc(ev.last_arrival_at) or _utc(ev.first_seen_at)
        if t and (ev.sample_seq not in times or t < times[ev.sample_seq]):
            times[ev.sample_seq] = t
    return times


def _gateway_received_times(db: Session, device: str, boot: str):
    """网关接收时间按序号索引（取该设备/启动批次的最新到达）。"""
    rows = (db.query(ChainEvent, ChainManifest)
            .join(ChainManifest, ChainEvent.first_manifest_id == ChainManifest.id)
            .filter(ChainEvent.tier == TIER_GATEWAY, ChainEvent.device_id == device,
                    ChainEvent.boot_batch == boot).all())
    times: dict[int, datetime] = {}
    for ev, m in rows:
        rt = _utc(ev.last_arrival_at) or _utc(m.received_to) or _utc(m.received_from)
        if rt and (ev.sample_seq not in times or rt > times[ev.sample_seq]):
            times[ev.sample_seq] = rt
    return times


def _classify_seq(db, task, plan, seq, *, in_confirmed, comp_dev, comp_gw,
                  comp_plat, device_seqs, gateway_seqs, platform_seqs,
                  recv_times, gateway_times, plat,
                  seq_continuous: bool = False,
                  neighbours_late: bool = False,
                  has_upstream_manifest: bool = False) -> str:
    """单个序号的归因。

    顺序即证据强度：先“有人看见”，再排除解释，再逐段定位；
    只有证据支持的段才确认，其余保持待核。
    """
    # 1) 同身份内容冲突 → 无法判定可信值 → 待核（并显示具体冲突）
    conflict = (db.query(ChainConflict)
                .filter(ChainConflict.device_id == task.device_id,
                        ChainConflict.boot_batch == str(task.boot_batch),
                        ChainConflict.sample_seq == seq,
                        ChainConflict.status == "open").first())
    if conflict:
        return CAUSE_PENDING_CONFLICT
    # 2) 平台已解析（含重放解决）→ 不算缺口
    pm = plat.get(seq)
    if pm is not None and pm.status in ("parsed_new", "parsed_dup", "conflict",
                                        "resolved"):
        return "present"
    # 3) 平台已持久保存原文但尚未解析 → 解析积压，不计链路丢失
    if seq in platform_seqs or pm is not None:
        return CAUSE_PARSE_BACKLOG

    # 4) 重启后序号复位：显式重启信号，或已知清单整体在期望范围之上
    expected_t = _plan_time(plan, seq)
    reboot = (db.query(DeviceSignal)
              .filter(DeviceSignal.device_id == task.device_id,
                      DeviceSignal.kind.in_(("reboot", "seq_reset")),
                      DeviceSignal.at >= expected_t - timedelta(hours=1),
                      DeviceSignal.at <= expected_t + timedelta(hours=1))
              .first())
    if reboot:
        return CAUSE_PENDING_ORDER
    dev_known = _known_range(db, TIER_DEVICE, task.device_id, str(task.boot_batch))
    gw_known = _known_range(db, TIER_GATEWAY, task.device_id, str(task.boot_batch))
    if dev_known and dev_known[0] > seq:
        return CAUSE_PENDING_ORDER
    if gw_known and gw_known[0] > seq:
        return CAUSE_PENDING_ORDER

    # 5) 设备侧信号：各来源分别保留，不吞并为“离线”。
    #    单一已确认来源可支撑判断；同类来源互相矛盾或并立时保持待核。
    sigs = _signals_overlapping(db, task.device_id, str(task.boot_batch), expected_t)
    asserted = [x for x in sigs if x.kind in ("planned_stop", "maintenance",
                                              "network_outage", "decommission")]
    kinds = {x.kind for x in asserted}
    if len(kinds) == 1:
        kind = next(iter(kinds))
        return {"planned_stop": CAUSE_PLANNED_STOP,
                "maintenance": CAUSE_MAINTENANCE,
                "network_outage": CAUSE_NETWORK_OUTAGE,
                "decommission": CAUSE_NETWORK_OUTAGE}[kind]
    if len(kinds) > 1:
        return CAUSE_PENDING_EVIDENCE

    # 6) 设备侧完整生成清单证明未生成 → 设备未产生
    if _in_intervals(comp_dev, seq) and seq not in device_seqs:
        return CAUSE_DEVICE_NOT_GENERATED
    # 7) 设备已生成、网关完整接收清单缺记录 → 设备至网关缺失
    if seq in device_seqs and _in_intervals(comp_gw, seq) and seq not in gateway_seqs:
        return CAUSE_DEVICE_TO_GATEWAY
    # 8) 设备序号连续，平台其余入库时间集中变晚 → 迟到候选（先于段定位）
    if (seq_continuous and neighbours_late
            and (seq in gateway_seqs or seq in device_seqs)):
        return CAUSE_PLATFORM_LATE
    # 9) 设备与网关都记录成功、平台完整接收范围内缺件 → 网关至平台缺失
    if seq in gateway_seqs and _in_intervals(comp_plat, seq) and seq not in platform_seqs:
        return CAUSE_GATEWAY_TO_PLATFORM
    # 10) 完全没有上游清单：单有平台序号断档只能确认平台缺数据
    if comp_plat and not has_upstream_manifest:
        return CAUSE_PLATFORM_MISSING
    # 10b) 有上游清单但未逐条列出（来源未完整）：不能定位丢失环节
    if comp_plat and has_upstream_manifest and not comp_dev and not comp_gw:
        return CAUSE_PENDING_SCOPE
    # 11) 缺少心跳只表示缺少心跳证据，不证明采集中断
    heart = [x for x in _signals_overlapping(db, task.device_id,
                                             str(task.boot_batch), expected_t)
             if x.kind == "heartbeat_missing"]
    if heart:
        return CAUSE_HEARTBEAT_MISSING
    # 12) 来源清单不完整 → 原因待核，不能随意归因
    if not comp_dev or not comp_gw or not comp_plat:
        return CAUSE_PENDING_SCOPE
    return CAUSE_PENDING_EVIDENCE


def _signals_overlapping(db, device_id, boot_batch, t) -> list:
    """返回覆盖该时刻的原始设备信号（各自保留来源，不合并为“离线”）。"""
    sigs = (db.query(DeviceSignal)
            .filter(DeviceSignal.device_id == device_id,
                    or_(DeviceSignal.boot_batch == boot_batch,
                        DeviceSignal.boot_batch.is_(None)))
            .all())
    out = []
    for s in sigs:
        at = _utc(s.at)
        if at is None or t is None:
            continue
        if abs((at - t).total_seconds()) <= 3600:
            out.append(s)
    return out


def _missing_seqs(gap: ChainGap, reasons: dict[int, str]) -> list[int]:
    return [s for s in range(gap.seq_from, gap.seq_to + 1)
            if reasons.get(s, "present") != "present"]


def _gap_evidence(db, task, plan, seq_from, seq_to, cause, device_seqs,
                  gateway_seqs, platform_seqs, gateway_times, recv_times, plat):
    support, conflict, unknown = [], [], []
    device = task.device_id
    boot = str(task.boot_batch)
    refs, dups, raws = [], {}, []
    for tier in (TIER_DEVICE, TIER_GATEWAY, TIER_PLATFORM):
        ms = _manifests(db, tier, device, boot)
        for m in ms:
            if m.seq_to < seq_from or m.seq_from > seq_to:
                continue
            refs.append({"manifest_id": m.id, "tier": m.tier,
                         "seq_range": [m.seq_from, m.seq_to],
                         "complete": m.complete, "source": m.source,
                         "source_ref": m.source_ref})
    conflict_rows = (db.query(ChainConflict)
                     .filter(ChainConflict.device_id == device,
                             ChainConflict.boot_batch == boot,
                             ChainConflict.sample_seq >= seq_from,
                             ChainConflict.sample_seq <= seq_to,
                             ChainConflict.status == "open").all())
    if conflict_rows:
        conflict.insert(0, {"kind": "conflict",
                            "summary": f"序号 [{seq_from}, {seq_to}] 存在 "
                                       f"{len(conflict_rows)} 条同身份内容冲突，"
                                       "无法判定可信值，原因待核"})
    for c in conflict_rows:
        conflict.append({"kind": "conflict", "ref_id": str(c.id),
                         "summary": f"序号 {c.sample_seq} 在 {c.tier_a}/{c.tier_b} "
                                    f"内容不一致：{c.content_a} vs {c.content_b}"})
    evs = (db.query(ChainEvent)
           .filter(ChainEvent.device_id == device, ChainEvent.boot_batch == boot,
                   ChainEvent.sample_seq >= seq_from,
                   ChainEvent.sample_seq <= seq_to).all())
    for ev in evs:
        if ev.arrival_count > 1:
            dups[f"{ev.tier}:{ev.sample_seq}"] = ev.arrival_count
        if ev.tier == TIER_DEVICE:
            support.append({"kind": "event", "ref_id": str(ev.id),
                            "summary": f"设备侧已生成序号 {ev.sample_seq} "
                                       f"（{ev.event_id or '无事件号'}）"})
        if ev.tier == TIER_PLATFORM:
            raws.append({"kind": "event", "ref_id": str(ev.id),
                         "summary": f"平台已保存原文 序号 {ev.sample_seq}"})

    if cause == CAUSE_DEVICE_NOT_GENERATED:
        support.insert(0, {"kind": "manifest",
                           "summary": "设备侧完整生成清单在 "
                                      f"序号 [{seq_from}, {seq_to}] 内无此记录"})
    elif cause == CAUSE_DEVICE_TO_GATEWAY:
        support.insert(0, {"kind": "manifest",
                           "summary": f"设备侧已生成序号 [{seq_from}, {seq_to}]，"
                                      "网关完整接收清单缺少对应记录"})
    elif cause == CAUSE_GATEWAY_TO_PLATFORM:
        support.insert(0, {"kind": "manifest",
                           "summary": f"设备与网关均已确认序号 [{seq_from}, {seq_to}]，"
                                      "平台完整接收范围内缺件"})
    elif cause == CAUSE_PLATFORM_LATE:
        late = sorted({s: t for s, t in recv_times.items()
                       if t and seq_from - 3 <= s <= seq_to + 3}.items(),
                      key=lambda kv: kv[1])
        if late:
            support.insert(0, {"kind": "event",
                               "summary": f"平台入库时间集中偏晚，例如序号 "
                                          f"{late[0][0]} 于 {_iso(late[0][1])} 入库"})
    elif cause == CAUSE_PARSE_BACKLOG:
        support.insert(0, {"kind": "manifest",
                           "summary": "平台已持久保存原文，但对应事件尚未解析"})
    elif cause == CAUSE_PLATFORM_MISSING:
        unknown.append("没有设备侧生成清单或网关接收清单，无法定位丢失环节；"
                       "只能确认平台缺数据")
    elif cause == CAUSE_PENDING_EVIDENCE:
        unknown.append("设备侧未提供完整生成清单，无法区分断电/网络/其它")
        unknown.append("缺少心跳只表示缺少心跳证据，不能证明温度采集停止")
    elif cause == CAUSE_PENDING_SCOPE:
        for tier, enumerated in ((TIER_DEVICE, True), (TIER_GATEWAY, True),
                                 (TIER_PLATFORM, False)):
            iv = _complete_intervals(db, tier, device, boot, enumerated=enumerated)
            if not iv:
                unknown.append(f"{TIER_LABELS[tier]} 没有可逐序号核对的完整性证据")
                continue
            if not all(_in_intervals(iv, s) for s in (seq_from, seq_to)):
                unknown.append(f"{TIER_LABELS[tier]} 完整范围 {iv} 未覆盖缺口")
    signal_rows = _signals_overlapping(db, device, boot, _plan_time(plan, seq_from))
    for sig in signal_rows:
        summary = (f"设备信号 {sig.kind} @ {_iso(sig.at)} 来源 {sig.source}"
                   + (f"：{sig.note}" if sig.note else ""))
        if sig.kind == "heartbeat_missing":
            unknown.append(summary + "（只表示缺少心跳证据，不证明温度采集停止）")
        elif cause in (CAUSE_PLANNED_STOP, CAUSE_MAINTENANCE, CAUSE_NETWORK_OUTAGE):
            support.append({"kind": "signal", "ref_id": str(sig.id),
                            "summary": summary + "；来源保留，不作为温控正常结论"})
        else:
            unknown.append(summary + "；与当前判断不闭合，保留待核")
    if signal_rows and cause in (CAUSE_PLANNED_STOP, CAUSE_MAINTENANCE,
                                 CAUSE_NETWORK_OUTAGE):
        support.insert(0, {"kind": "signal",
                           "summary": f"{CAUSE_LABELS[cause]}：仅记录来源，"
                                      "该时段温控仍按未覆盖处理"})
    if not device_seqs and not gateway_seqs and not platform_seqs:
        unknown.append("三个环节都未记录该区间的任何事件")
    return support, conflict, unknown, refs, dups, raws


def _upsert_gap(db, task, plan, seq_from, seq_to, cause, refresh, trig,
                device_complete, gateway_complete, platform_complete,
                conn_lo, conn_hi, device_seqs, gateway_seqs, platform_seqs,
                gateway_times, recv_times, plat, exclude_ids=None) -> ChainGap:
    # 与已有活动缺口重叠 → 沿用同一缺口身份（保留版本历史与标注）
    q = (db.query(ChainGap)
         .filter(ChainGap.collection_task_id == task.id,
                 ChainGap.active.is_(True),
                 ChainGap.seq_from <= seq_to,
                 ChainGap.seq_to >= seq_from))
    if exclude_ids:
        q = q.filter(~ChainGap.id.in_(exclude_ids))
    existing = q.order_by(ChainGap.id).first()
    key = existing.gap_key if existing else f"task-{task.id}:{seq_from}-{seq_to}"
    support, conflict, unknown, refs, dups, raws = _gap_evidence(
        db, task, plan, seq_from, seq_to, cause, device_seqs, gateway_seqs,
        platform_seqs, gateway_times, recv_times, plat)
    seg = _segment_for_cause(cause)
    status = _status_for_cause(cause, conflict)
    confidence = "high" if cause in (CAUSE_DEVICE_NOT_GENERATED,
                                     CAUSE_DEVICE_TO_GATEWAY,
                                     CAUSE_GATEWAY_TO_PLATFORM,
                                     CAUSE_PARSE_BACKLOG) else (
        "medium" if cause == CAUSE_PLATFORM_LATE else "low")
    completeness = {
        TIER_DEVICE: _range_dict(_complete_range(db, TIER_DEVICE, task.device_id,
                                                 str(task.boot_batch))),
        TIER_GATEWAY: _range_dict(_complete_range(db, TIER_GATEWAY, task.device_id,
                                                  str(task.boot_batch))),
        TIER_PLATFORM: _range_dict(_complete_range(db, TIER_PLATFORM, task.device_id,
                                                   str(task.boot_batch))),
        "confirmed_overlap": [conn_lo, conn_hi],
    }
    gap = db.query(ChainGap).filter_by(gap_key=key).first()
    is_new = gap is None
    partial = False
    if (gap is not None and gap.cause == cause and gap.active
            and gap.seq_from <= seq_from and gap.seq_to >= seq_to
            and (gap.seq_from < seq_from or gap.seq_to > seq_to)
            # 只在缺口从一端收缩（补传）时沿用原身份；
            # 中间被新证据切开的分段各自成为独立缺口，不重叠
            and (seq_from == gap.seq_from or seq_to == gap.seq_to)):
        partial = True
        if gap.origin_seq_from is None:
            gap.origin_seq_from, gap.origin_seq_to = gap.seq_from, gap.seq_to
    if partial and status != "filled":
        status = "partial"
    times = _gap_times(plan, seq_from, seq_to, gateway_times, recv_times)
    relation = relation_for(db, task.device_id, times["time_from"])
    cargo = task.cargo_batch_id or (relation.cargo_batch_id if relation else None)
    box = task.box_id or (relation.box_id if relation else None)
    vehicle = task.vehicle_id or (relation.vehicle_id if relation else None)
    rationale = _rationale(cause, seq_from, seq_to, times)

    if gap is None:
        gap = ChainGap(
            gap_key=key, device_id=task.device_id, boot_batch=str(task.boot_batch),
            collection_task_id=task.id, seq_from=seq_from, seq_to=seq_to,
            origin_seq_from=seq_from, origin_seq_to=seq_to)
        db.add(gap)

    # 底层证据是否变化：原因/环节/区间变化，或区间内出现未处理的内容冲突
    open_conflict = (db.query(ChainConflict)
                     .filter(ChainConflict.device_id == task.device_id,
                             ChainConflict.boot_batch == str(task.boot_batch),
                             ChainConflict.status == "open",
                             ChainConflict.sample_seq >= seq_from,
                             ChainConflict.sample_seq <= seq_to)
                     .first())
    evidence_changed = (not is_new) and (
        gap.cause != cause or gap.segment != seg
        or gap.seq_from != seq_from or gap.seq_to != seq_to
        or bool(open_conflict))
    # 已有标注且底层证据未变化：保留标注状态，不让常规重算静默改写已查看的告警
    if (not is_new and gap.annotation_at and not evidence_changed
            and status != "filled" and gap.status != "filled"):
        status = "annotated"
    changed_fields = []
    for field, cur, new in (("cause", gap.cause, cause),
                            ("segment", gap.segment, seg),
                            ("status", gap.status, status),
                            ("seq_from", gap.seq_from, seq_from),
                            ("seq_to", gap.seq_to, seq_to)):
        if cur != new:
            changed_fields.append(f"{field}: {cur} → {new}")
    if evidence_changed and gap.annotation_at:
        # 已有标注后证据变化：重开诊断，旧判断版本保留
        gap.reopened = True
        changed_fields.append("因后续证据变化重开诊断，旧判断版本保留")

    gap._new = is_new
    gap._changed = bool(changed_fields) and not is_new
    if is_new:
        gap.version = 1
    else:
        if changed_fields:
            gap.version = (gap.version or 1) + 1
        gap.active = status != "filled"
        if status == "filled":
            gap.closed_at = datetime.now(timezone.utc)
        else:
            gap.closed_at = None

    gap.cargo_batch_id, gap.box_id, gap.vehicle_id = cargo, box, vehicle
    gap.plan_id, gap.plan_version = plan.id, plan.version
    gap.cause, gap.segment, gap.status = cause, seg, status
    gap.seq_from, gap.seq_to = seq_from, seq_to
    if gap.origin_seq_from is None:
        gap.origin_seq_from, gap.origin_seq_to = seq_from, seq_to
    gap.time_from, gap.time_to = times["time_from"], times["time_to"]
    gap.device_event_time = times["device_event_time"]
    gap.gateway_received_time = times["gateway_received_time"]
    gap.platform_received_time = times["platform_received_time"]
    gap.last_device_seq = times["last_device_seq"]
    gap.device_seq_continuous = times["device_seq_continuous"]
    gap.rationale = rationale
    gap.supporting, gap.conflicting = support, conflict
    gap.unknown_segments = unknown
    gap.scope_completeness = completeness
    gap.source_manifest_refs = refs
    gap.duplicate_counts = dups
    gap.raw_refs = raws
    gap.confidence = confidence
    gap.stale = False
    gap.last_evaluated_at = datetime.now(timezone.utc)
    gap.active = status != "filled"
    db.flush()

    if is_new:
        _write_version(db, gap, "created", trig, support, conflict, unknown,
                       completeness, None)
    elif changed_fields:
        _write_version(db, gap, "re-evaluated", trig, support, conflict,
                       unknown, completeness, "; ".join(changed_fields))
    _recompute_affected_segments(db, gap, plan, relation, trig)
    if gap.status == "filled":
        gap.active = False
    db.flush()
    return gap


def _range_dict(rng):
    return None if rng is None else {"seq_from": rng[0], "seq_to": rng[1]}


def _segment_for_cause(cause: str) -> str:
    return {
        CAUSE_DEVICE_NOT_GENERATED: SEG_DEVICE,
        CAUSE_DEVICE_TO_GATEWAY: SEG_DEVICE_GATEWAY,
        CAUSE_GATEWAY_TO_PLATFORM: SEG_GATEWAY_PLATFORM,
        CAUSE_PLATFORM_MISSING: SEG_PLATFORM,
        CAUSE_PLATFORM_LATE: SEG_TRANSPORT_PLATFORM,
        CAUSE_PARSE_BACKLOG: SEG_PARSE,
        CAUSE_PENDING_SCOPE: SEG_UNKNOWN,
        CAUSE_PENDING_CONFLICT: SEG_UNKNOWN,
        CAUSE_PENDING_ORDER: SEG_UNKNOWN,
        CAUSE_PENDING_EVIDENCE: SEG_UNKNOWN,
        CAUSE_DEVICE_POWER_OFF: SEG_DEVICE,
        CAUSE_PLANNED_STOP: SEG_DEVICE,
        CAUSE_MAINTENANCE: SEG_DEVICE,
        CAUSE_NETWORK_OUTAGE: SEG_DEVICE_GATEWAY,
        CAUSE_HEARTBEAT_MISSING: SEG_UNKNOWN,
        CAUSE_ANNOTATED: SEG_UNKNOWN,
    }.get(cause, SEG_UNKNOWN)


def _status_for_cause(cause: str, conflict) -> str:
    if cause == CAUSE_PARSE_BACKLOG:
        return "parse_backlog"
    if cause in (CAUSE_DEVICE_NOT_GENERATED, CAUSE_DEVICE_TO_GATEWAY,
                 CAUSE_GATEWAY_TO_PLATFORM, CAUSE_PLATFORM_MISSING,
                 CAUSE_PLANNED_STOP, CAUSE_MAINTENANCE, CAUSE_NETWORK_OUTAGE):
        return "confirmed"
    if conflict:
        return "pending"
    return "pending"


def _gap_times(plan, seq_from, seq_to, gateway_times, recv_times):
    tf = _plan_time(plan, seq_from)
    tt = _plan_time(plan, seq_to)
    gw = [gateway_times[s] for s in gateway_times
          if seq_from <= s <= seq_to]
    pr = [recv_times[s] for s in recv_times
          if recv_times[s] and seq_from - 1 <= s <= seq_to + 1]
    prev_seq = seq_from - 1
    continuous = _seq_continuous(plan, seq_from, gateway_times, recv_times)
    return {
        "time_from": tf, "time_to": tt,
        "device_event_time": tf,
        "gateway_received_time": max(gw) if gw else None,
        "platform_received_time": max(pr) if pr else None,
        "last_device_seq": prev_seq,
        "device_seq_continuous": continuous,
    }


def _seq_continuous(plan, seq_from, gateway_times, recv_times) -> bool | None:
    """缺口前是否设备序号连续（平台观测）。缺前序证据时返回 None。"""
    keys = sorted(set(gateway_times) | set(recv_times))
    if not keys:
        return None
    before = [k for k in keys if k < seq_from]
    if not before:
        return None
    return before[-1] == seq_from - 1


def _rationale(cause, seq_from, seq_to, times) -> str:
    base = CAUSE_LABELS.get(cause, cause)
    t = f"，时间 {_iso(times['time_from'])} ～ {_iso(times['time_to'])}" \
        if times["time_from"] else ""
    if times["device_seq_continuous"]:
        return (f"{base}：序号 [{seq_from}, {seq_to}] 之前设备序号连续{t}；"
                "若平台入库时间集中变晚，归为迟到候选而非设备离线")
    return f"{base}：序号 [{seq_from}, {seq_to}]{t}"


def _write_version(db, gap, change, trig, support, conflict, unknown,
                   completeness, reason):
    ver = ChainGapVersion(
        gap_id=gap.id, version=gap.version, status=gap.status, cause=gap.cause,
        segment=gap.segment, seq_from=gap.seq_from, seq_to=gap.seq_to,
        time_from=gap.time_from, time_to=gap.time_to,
        confidence=gap.confidence, rationale=gap.rationale,
        unknowns=unknown, scope_completeness=completeness,
        annotation=({"operator": gap.annotation_operator,
                     "evidence": gap.annotation_evidence,
                     "scope": gap.annotation_scope,
                     "cause": gap.annotation_cause,
                     "at": _iso(gap.annotation_at)}
                    if gap.annotation_operator else None),
        change_reason=(reason or change)[:128], triggered_by=trig,
    )
    db.add(ver)
    db.flush()
    for role, items in (("support", support), ("conflict", conflict)):
        for it in items:
            db.add(ChainGapEvidence(
                gap_version_id=ver.id, role=role,
                kind=it.get("kind", "note"), ref_id=it.get("ref_id"),
                summary=it.get("summary", "")[:512],
                detail={k: v for k, v in it.items() if k not in ("kind", "ref_id", "summary")}))
    return ver


def _close_gap(db, gap, status, reason, trig):
    gap.version = (gap.version or 1) + 1
    gap.status = status
    gap.active = False
    gap.closed_at = datetime.now(timezone.utc)
    gap.stale = False
    db.add(ChainGapVersion(
        gap_id=gap.id, version=gap.version, status=status, cause=gap.cause,
        segment=gap.segment, seq_from=gap.seq_from, seq_to=gap.seq_to,
        time_from=gap.time_from, time_to=gap.time_to, confidence=gap.confidence,
        rationale=gap.rationale, unknowns=gap.unknown_segments,
        scope_completeness=gap.scope_completeness,
        annotation=({"operator": gap.annotation_operator,
                     "evidence": gap.annotation_evidence,
                     "scope": gap.annotation_scope} if gap.annotation_operator else None),
        change_reason=status, triggered_by=trig))
    db.flush()


# ---------------------------------------------------------------- 受影响区段

def _recompute_affected_segments(db, gap: ChainGap, plan, relation, trig,
                                 mark_filled: bool = True):
    """按设备 + 有效安装关系 + 缺口时段计算温控区段覆盖。"""
    if gap.time_from is None or gap.time_to is None:
        return
    rels = (db.query(DeviceRelation)
            .filter(DeviceRelation.probe_id == gap.device_id,
                    DeviceRelation.valid_from < gap.time_to,
                    or_(DeviceRelation.valid_to.is_(None),
                        DeviceRelation.valid_to > gap.time_from))
            .all())
    total_observed = 0
    total_expected = 0
    seg_ids = []
    for rel in rels:
        range_from = (_plan_time(plan, gap.origin_seq_from)
                      if gap.status in ("filled", "partial")
                      and gap.origin_seq_from is not None
                      else _utc(gap.time_from))
        range_to = (_plan_time(plan, gap.origin_seq_to)
                    if gap.status in ("filled", "partial")
                    and gap.origin_seq_to is not None
                    else _utc(gap.time_to))
        eff_from = max(_utc(rel.valid_from), range_from)
        eff_to = min(_utc(rel.valid_to) if rel.valid_to else range_to, range_to)
        if eff_to <= eff_from:
            continue
        if gap.status in ("filled", "partial") and gap.origin_seq_from is not None:
            g_from, g_to = gap.origin_seq_from, gap.origin_seq_to
        else:
            g_from, g_to = gap.seq_from, gap.seq_to
        expected_seqs = [s for s in range(g_from, g_to + 1)
                         if eff_from <= _plan_time(plan, s) <= eff_to]
        if not expected_seqs:
            continue
        samples = (db.query(Sample, ReadingVersion)
                   .join(CurrentReading, CurrentReading.sample_id == Sample.id)
                   .join(ReadingVersion, ReadingVersion.id ==
                         CurrentReading.reading_version_id)
                   .filter(Sample.device_id == gap.device_id,
                           Sample.boot_batch == gap.boot_batch,
                           Sample.sample_seq.in_(expected_seqs))
                   .order_by(ReadingVersion.sample_time).all())
        samples = [(s, rv) for s, rv in samples if rv.sample_time]
        covered_seqs = set()
        basis = []
        for s, rv in samples:
            covered_seqs.add(int(s.sample_seq))
            basis.append({"sample_id": s.id, "seq": int(s.sample_seq),
                          "std_c": rv.std_c, "sample_time": _iso(rv.sample_time),
                          "source": rv.source,
                          "reading_version_id": rv.id})
        observed = len(covered_seqs)
        expected = len(expected_seqs)
        coverage = ("covered" if observed >= expected else
                    "partial" if observed > 0 else "uncovered")
        missing = [s for s in expected_seqs if s not in covered_seqs]
        remaining = None
        if missing:
            remaining = {"seq_from": missing[0], "seq_to": missing[-1],
                         "seqs": missing,
                         "time_from": _iso(_plan_time(plan, missing[0])),
                         "time_to": _iso(_plan_time(plan, missing[-1]))}

        seg = (db.query(AffectedSegment)
               .filter_by(gap_id=gap.id, relation_id=rel.id).first())
        if seg is None:
            seg = AffectedSegment(gap_id=gap.id, relation_id=rel.id,
                                  device_id=gap.device_id, revision=1,
                                  coverage=coverage, expected_count=expected,
                                  observed_count=observed)
            db.add(seg)
            db.flush()
            db.add(ChainSegmentRevision(
                affected_segment_id=seg.id, revision=1, coverage=coverage,
                expected_count=expected, observed_count=observed,
                sample_ids=sorted(covered_seqs), readings_basis=basis,
                remaining_gap=remaining, change_reason="initial"))
        else:
            changed = (seg.coverage != coverage or seg.observed_count != observed
                       or seg.expected_count != expected)
            seg.coverage, seg.expected_count, seg.observed_count = (
                coverage, expected, observed)
            seg.sample_ids, seg.readings_basis = sorted(covered_seqs), basis
            seg.remaining_gap = remaining
            seg.stale = False
            if changed:
                seg.revision += 1
                db.add(ChainSegmentRevision(
                    affected_segment_id=seg.id, revision=seg.revision,
                    coverage=coverage, expected_count=expected,
                    observed_count=observed, sample_ids=sorted(covered_seqs),
                    readings_basis=basis, remaining_gap=remaining,
                    change_reason=trig))
        seg.vehicle_id, seg.box_id = rel.vehicle_id, rel.box_id
        seg.cargo_batch_id = rel.cargo_batch_id
        seg.effective_from, seg.effective_to = eff_from, eff_to
        seg.last_recomputed = datetime.now(timezone.utc)
        db.flush()
        seg_ids.append(seg.id)
        total_observed += observed
        total_expected += expected

    if gap.time_from and gap.time_to and total_expected:
        # 缺口一旦被迟到数据填满，则标记 filled（保持历史版本）
        if mark_filled and total_observed >= total_expected and gap.status != "filled":
            gap.status = "filled"
            gap.active = False
            gap.closed_at = datetime.now(timezone.utc)
            gap.version = (gap.version or 1) + 1
            _write_version(db, gap, "filled", trig, gap.supporting or [],
                           gap.conflicting or [], gap.unknown_segments or [],
                           gap.scope_completeness or {},
                           "迟到数据补齐全部覆盖，更新有效采样覆盖")
    db.flush()


# ---------------------------------------------------------------- 区间刷新

def _recompute_scope(db: Session, device: str, boot: str, trig: str):
    st = (db.query(ChainScopeState)
          .filter_by(scope_key=_scope_key(device, boot)).first())
    refresh = None
    if st and st.last_refresh_id:
        refresh = db.get(ChainRefresh, st.last_refresh_id)
    if refresh is None:
        refresh = ChainRefresh(scope_key=_scope_key(device, boot),
                               triggered_by=trig, status="running")
        db.add(refresh)
        db.flush()
        if st:
            st.last_refresh_id = refresh.id
    tasks = (db.query(CollectionTask)
             .filter_by(device_id=device, boot_batch=str(boot)).all())
    stats = {"created": 0, "changed": 0, "filled": 0, "segs": 0, "gaps": 0}
    for task in tasks:
        out = evaluate_task(db, task, refresh, trig)
        stats["created"] += out["created"]
        stats["changed"] += out["changed"]
        stats["filled"] += out["filled"]
        stats["gaps"] += out["gaps"]
    refresh.tasks_evaluated = len(tasks)
    refresh.gaps_evaluated = stats["gaps"]
    refresh.gaps_created = stats["created"]
    refresh.gaps_changed = stats["changed"]
    refresh.gaps_filled = stats["filled"]
    refresh.status = "completed"
    refresh.finished_at = datetime.now(timezone.utc)
    refresh.note = f"{len(tasks)} 个采集任务，{stats['gaps']} 个缺口"
    if st:
        st.stale = False
        st.stale_reason = ""
        st.last_evaluated_at = datetime.now(timezone.utc)
    db.flush()
    return refresh


def refresh_scopes(db: Session, scopes: list[tuple[str, str]] | None = None,
                   trig: str = "手动重算") -> list[ChainRefresh]:
    """重算指定的 (设备, 启动批次)；None 表示所有已知区间。"""
    if scopes is None:
        pairs = (db.query(CollectionTask.device_id, CollectionTask.boot_batch)
                 .distinct().all())
        known = (db.query(ChainManifest.device_id, ChainManifest.boot_batch)
                 .distinct().all())
        scopes = [(d, str(b)) for d, b in set(pairs) | set(known)]
    out = []
    for device, boot in scopes:
        out.append(_recompute_scope(db, device, str(boot), trig))
    # 影响区段：清理因重算而失效的旧区段（保留历史版本）
    db.commit()
    return out


def mark_relation_change(db: Session, device_id: str, trig: str = "安装关系修订"):
    """设备换绑 → 相关区间（含区段）失效并重算受影响的任务。"""
    boots = {b for (b,) in (db.query(CollectionTask.boot_batch)
                            .filter_by(device_id=device_id).distinct().all())}
    boots |= {b for (b,) in (db.query(ChainManifest.boot_batch)
                             .filter_by(device_id=device_id).distinct().all())}
    for boot in boots:
        invalidate_scope(db, device_id, str(boot), trig, touch_refresh=False)
        st = db.query(ChainScopeState).filter_by(
            scope_key=_scope_key(device_id, str(boot))).first()
        if st:
            r = ChainRefresh(scope_key=st.scope_key, triggered_by=trig,
                             status="running")
            db.add(r)
            db.flush()
            st.last_refresh_id = r.id
    db.flush()


# ---------------------------------------------------------------- 查询与标注

def gap_dict(db: Session, gap: ChainGap, include_segments: bool = True) -> dict:
    d = {
        "id": gap.id, "gap_key": gap.gap_key, "device_id": gap.device_id,
        "boot_batch": gap.boot_batch, "cargo_batch_id": gap.cargo_batch_id,
        "box_id": gap.box_id, "vehicle_id": gap.vehicle_id,
        "collection_task_id": gap.collection_task_id,
        "plan_id": gap.plan_id, "plan_version": gap.plan_version,
        "segment": gap.segment, "segment_label": SEGMENT_LABELS.get(gap.segment, gap.segment),
        "cause": gap.cause, "cause_label": CAUSE_LABELS.get(gap.cause, gap.cause),
        "status": gap.status, "confidence": gap.confidence,
        "seq_from": gap.seq_from, "seq_to": gap.seq_to,
        "origin_seq_from": gap.origin_seq_from,
        "origin_seq_to": gap.origin_seq_to,
        "status_label": STATUS_LABELS.get(gap.status, gap.status),
        "time_from": _iso(gap.time_from), "time_to": _iso(gap.time_to),
        "last_device_seq": gap.last_device_seq,
        "device_event_time": _iso(gap.device_event_time),
        "gateway_received_time": _iso(gap.gateway_received_time),
        "platform_received_time": _iso(gap.platform_received_time),
        "device_seq_continuous": gap.device_seq_continuous,
        "rationale": gap.rationale,
        "supporting": gap.supporting, "conflicting": gap.conflicting,
        "unknown_segments": gap.unknown_segments,
        "scope_completeness": gap.scope_completeness,
        "source_manifest_refs": gap.source_manifest_refs,
        "duplicate_counts": gap.duplicate_counts,
        "raw_refs": gap.raw_refs,
        "version": gap.version, "active": gap.active, "stale": gap.stale,
        "reopened": gap.reopened,
        "annotation": ({"operator": gap.annotation_operator,
                        "evidence": gap.annotation_evidence,
                        "scope": gap.annotation_scope,
                        "cause": gap.annotation_cause,
                        "at": _iso(gap.annotation_at)}
                       if gap.annotation_operator else None),
        "last_evaluated_at": _iso(gap.last_evaluated_at),
        "created_at": _iso(gap.created_at), "closed_at": _iso(gap.closed_at),
    }
    st = (db.query(ChainScopeState)
          .filter_by(scope_key=_scope_key(gap.device_id, gap.boot_batch)).first())
    d["scope_state"] = ({"stale": st.stale, "stale_reason": st.stale_reason,
                         "last_evaluated_at": _iso(st.last_evaluated_at)}
                        if st else {"stale": True, "stale_reason": "尚未评估"})
    if include_segments:
        segs = (db.query(AffectedSegment).filter_by(gap_id=gap.id)
                .order_by(AffectedSegment.id).all())
        d["affected_segments"] = [_segment_dict(s) for s in segs]
    return d


def _segment_dict(s: AffectedSegment) -> dict:
    return {"id": s.id, "gap_id": s.gap_id, "relation_id": s.relation_id,
            "device_id": s.device_id, "vehicle_id": s.vehicle_id,
            "box_id": s.box_id, "cargo_batch_id": s.cargo_batch_id,
            "effective_from": _iso(s.effective_from),
            "effective_to": _iso(s.effective_to), "coverage": s.coverage,
            "expected_count": s.expected_count, "observed_count": s.observed_count,
            "sample_ids": s.sample_ids, "readings_basis": s.readings_basis,
            "remaining_gap": s.remaining_gap, "revision": s.revision,
            "stale": s.stale, "last_recomputed": _iso(s.last_recomputed)}


def versions_dict(db: Session, gap_id: int) -> list[dict]:
    vs = (db.query(ChainGapVersion).filter_by(gap_id=gap_id)
          .order_by(ChainGapVersion.version).all())
    out = []
    for v in vs:
        evs = db.query(ChainGapEvidence).filter_by(gap_version_id=v.id).all()
        out.append({
            "version": v.version, "status": v.status, "cause": v.cause,
            "cause_label": CAUSE_LABELS.get(v.cause, v.cause),
            "segment": v.segment, "seq_from": v.seq_from, "seq_to": v.seq_to,
            "time_from": _iso(v.time_from), "time_to": _iso(v.time_to),
            "confidence": v.confidence, "rationale": v.rationale,
            "unknowns": v.unknowns, "scope_completeness": v.scope_completeness,
            "annotation": v.annotation, "change_reason": v.change_reason,
            "triggered_by": v.triggered_by, "created_at": _iso(v.created_at),
            "evidence": [{"role": e.role, "kind": e.kind, "ref_id": e.ref_id,
                          "summary": e.summary, "detail": e.detail} for e in evs],
        })
    return out


def annotate_gap(db: Session, gap: ChainGap, operator: str, evidence: str,
                 scope: str, cause: str | None) -> dict:
    if not operator or not evidence:
        raise ValueError("标注必须记录操作者与证据")
    if cause and cause not in CAUSE_LABELS:
        raise ValueError(f"未知原因 {cause!r}")
    gap.annotation_operator = operator
    gap.annotation_evidence = evidence
    gap.annotation_scope = scope or f"{gap.device_id} 序号 [{gap.seq_from}, {gap.seq_to}]"
    gap.annotation_cause = cause or gap.cause
    gap.annotation_at = datetime.now(timezone.utc)
    if gap.status != "filled":
        gap.status = "annotated"
    gap.version = (gap.version or 1) + 1
    _write_version(db, gap, "annotated", f"人工标注：{operator}",
                   gap.supporting or [], gap.conflicting or [],
                   gap.unknown_segments or [], gap.scope_completeness or {},
                   f"人工标注 by {operator}，范围：{gap.annotation_scope}")
    db.commit()
    return gap_dict(db, gap)


def reopen_on_conflict(db: Session, device_id: str, boot_batch: str, sample_seq: int,
                       trig: str = "新冲突记录"):
    """检测到冲突记录：重开相关缺口并保留此前判断版本。"""
    gaps = (db.query(ChainGap)
            .filter(ChainGap.device_id == device_id,
                    ChainGap.boot_batch == str(boot_batch),
                    ChainGap.seq_from <= sample_seq,
                    ChainGap.seq_to >= sample_seq,
                    ChainGap.active.is_(True)).all())
    for gap in gaps:
        conflict = [{"kind": "conflict",
                     "summary": f"序号 {sample_seq} 出现内容冲突，判断重开"}]
        old_status = gap.status
        gap.status = "pending"
        gap.reopened = True
        gap.version = (gap.version or 1) + 1
        _write_version(db, gap, "reopened", trig,
                       gap.supporting or [], (gap.conflicting or []) + conflict,
                       gap.unknown_segments or [], gap.scope_completeness or {},
                       f"新冲突记录：原状态 {old_status} → pending，旧版本保留")
    db.flush()


# ---------------------------------------------------------------- 发布覆盖

def compute_publication_coverage(db: Session, publication: Publication) -> list[dict]:
    """发布快照的覆盖结论：引用当时诊断的覆盖判断，不回写历史报表。"""
    db.query(PublicationCoverage).filter_by(publication_id=publication.id).delete()
    from .models import PublicationItem
    items = db.query(PublicationItem).filter_by(publication_id=publication.id).all()
    gaps = (db.query(ChainGap).filter_by(active=True).all())
    keys = {(i.cargo_batch_id, i.box_id) for i in items}
    keys |= {(g.cargo_batch_id, g.box_id) for g in gaps
             if g.cargo_batch_id and g.cargo_batch_id in {k[0] for k in keys}}
    out = []
    for cargo, box in sorted(keys, key=lambda k: (str(k[0]), str(k[1]))):
        related = [g for g in gaps
                   if (g.cargo_batch_id == cargo and g.box_id == box)]
        segs = []
        for g in related:
            for s in db.query(AffectedSegment).filter_by(gap_id=g.id).all():
                segs.append({"gap_id": g.id, "segment_id": s.id,
                             "coverage": s.coverage,
                             "effective_from": _iso(s.effective_from),
                             "effective_to": _iso(s.effective_to),
                             "remaining_gap": s.remaining_gap})
        if related:
            if any(s["coverage"] == "uncovered" for s in segs):
                cov = "uncovered"
            else:
                cov = "has_gaps"
        else:
            cov = "complete"
        snapshot = {"gap_count": len(related),
                    "causes": sorted({g.cause for g in related}),
                    "gap_versions": {g.id: g.version for g in related}}
        row = PublicationCoverage(
            publication_id=publication.id, cargo_batch_id=cargo, box_id=box,
            coverage=cov, open_gaps=[g.id for g in related],
            affected_segments=segs, diagnosis_snapshot=snapshot)
        db.add(row)
        out.append({"cargo_batch_id": cargo, "box_id": box, "coverage": cov,
                    "open_gaps": [g.id for g in related],
                    "affected_segments": segs})
    return out


# ---------------------------------------------------------------- 补传与入库

def _attr_with_relation(db, device_id, t, err):
    from .pipeline import attribute
    return attribute(db, device_id, t, err)


def ingest_backfill(db: Session, device_id: str, boot_batch: str, events: list[dict],
                    source: str = "gateway_retry", received_at=None,
                    import_receipt: bool = True, trig: str = "补传",
                    source_kind: str = "backfill") -> dict:
    """补传迟到数据并更新影响。

    - 迟到数据进入样本/读数（source=backfill），按采样时间与安装关系归属；
    - 未解析期间不补造温度：这里只入库实际收到的事件；
    - 若 import_receipt，同时记录平台入库清单与解析结果，供诊断引用。
    """
    from .models import TransportBatch
    from .pipeline import standardize_sample
    boot = str(boot_batch)
    recv = parse_time(received_at) or datetime.now(timezone.utc)
    batch = TransportBatch(source=f"chain-{source}", protocol="json",
                           note=f"{device_id}/boot-{boot} "
                                f"{'接入' if source_kind == 'ingest' else '补传'} "
                                f"{len(events)} 条")
    db.add(batch)
    db.flush()

    added, updated, existing = [], [], []
    for ev in events:
        seq = int(ev["seq"])
        content = ev.get("content") or {}
        std = ev.get("std_c", content.get("std_c", content.get("c")))
        t = parse_time(ev.get("event_time") or content.get("event_time"))
        if std is None or t is None:
            raise ValueError(f"补传事件 序号 {seq} 缺温度或事件时间，拒绝补造")
        sample = (db.query(Sample)
                  .filter_by(device_id=device_id, boot_batch=boot, sample_seq=seq)
                  .first())
        if sample is not None:
            cur = db.get(CurrentReading, sample.id)
            if cur:
                existing.append({"seq": seq, "sample_id": sample.id,
                                 "reading_version_id": cur.reading_version_id,
                                 "note": "已存在，不覆盖"})
                continue
        payload_hash = _hash({"device_id": device_id, "boot_batch": boot,
                              "seq": seq, "std_c": std, "event_time": _iso(t)})
        pm = ParsedMessage(batch_id=batch.id, status="parsed_new",
                           device_id=device_id, boot_batch=boot, sample_seq=seq,
                           payload_hash=payload_hash,
                           fields={"raw_value": std, "raw_unit": "C",
                                   "raw_fields": content})
        db.add(pm)
        db.flush()
        if sample is None:
            sample = Sample(device_id=device_id, boot_batch=boot, sample_seq=seq,
                            raw_value=float(std), raw_unit="C",
                            raw_fields={"source": source, "content": content},
                            payload_hash=payload_hash, first_batch_id=batch.id,
                            first_parsed_id=pm.id)
            db.add(sample)
            db.flush()
        rel, why = _attr_with_relation(db, device_id, t, 5.0)
        rv = ReadingVersion(
            sample_id=sample.id, std_c=float(std), quality_flag="ok",
            sample_time=t, time_error_s=5.0,
            vehicle_id=rel.vehicle_id if rel else None,
            box_id=rel.box_id if rel else None,
            cargo_batch_id=rel.cargo_batch_id if rel else None,
            status="ok" if rel else "pending_attribution",
            status_detail="" if rel else why,
            contract_key="backfill", contract_revision=1,
            relation_revision=0,
            conversion={"steps": [{"step": "raw", "value": std, "unit": "C",
                                   "note": f"补传来源 {source}"}],
                        "clock": {"mode": "explicit", "actual": True}},
            source=source_kind)
        db.add(rv)
        db.flush()
        from .pipeline import switch_current
        switch_current(db, sample.id, rv.id)
        added.append({"seq": seq, "sample_id": sample.id,
                      "reading_version_id": rv.id, "std_c": std,
                      "sample_time": _iso(t),
                      "cargo_batch_id": rv.cargo_batch_id})

    if import_receipt:
        seqs = sorted(int(e["seq"]) for e in events)
        if seqs:
            import_manifest(db, {
                "tier": TIER_PLATFORM, "device_id": device_id, "boot_batch": boot,
                "seq_from": seqs[0], "seq_to": seqs[-1], "complete": False,
                "source": source, "source_ref": f"backfill-{batch.id}",
                "received_from": _iso(recv), "received_to": _iso(recv),
                "note": "补传回执（平台已入库并解析）",
                "events": [{"seq": int(ev["seq"]),
                            "event_id": ev.get("event_id"),
                            "content": ev.get("content") or {
                                "std_c": ev.get("std_c")},
                            "event_time": _iso(parse_time(ev.get("event_time"))),
                            "arrival_at": _iso(parse_time(ev.get("arrival_at"))
                                              or recv)}
                           for ev in events],
            }, trig=trig)
    invalidate_scope(db, device_id, boot, trig)
    db.commit()
    return {"batch_id": batch.id, "added": added, "existing": existing,
            "existing_count": len(existing)}


def load_bundle(db: Session, payload: dict, trig: str = "批量导入") -> dict:
    """一次性导入安装关系/计划/任务/清单（虚构或真实回执），并失效受影响区间。"""
    out = {"relations": [], "plans": [], "tasks": [], "manifests": []}
    for r in payload.get("relations") or []:
        rel = (db.query(DeviceRelation)
               .filter_by(probe_id=r["probe_id"], valid_from=parse_time(r["valid_from"]))
               .first())
        if rel is None:
            rel = DeviceRelation(
                probe_id=r["probe_id"], vehicle_id=r["vehicle_id"],
                box_id=r["box_id"], cargo_batch_id=r["cargo_batch_id"],
                valid_from=parse_time(r["valid_from"]),
                valid_to=parse_time(r.get("valid_to")))
            db.add(rel)
            db.flush()
            mark_relation_change(db, r["probe_id"], "导入安装关系")
        out["relations"].append({"id": rel.id, "probe_id": rel.probe_id})
    for p in payload.get("plans") or []:
        plan = ensure_sampling_plan(
            db, p["plan_key"], int(p["version"]), p.get("device_id"),
            p["interval_s"], p["anchor_time"], int(p.get("anchor_seq", 0)),
            float(p.get("drift_s_per_step", 0.0)),
            p.get("effective_from"), p.get("effective_to"), p.get("note", ""))
        out["plans"].append({"id": plan.id, "plan_key": plan.plan_key,
                             "version": plan.version})
    for t in payload.get("tasks") or []:
        plan = (db.query(SamplingPlan)
                .filter_by(plan_key=t["plan_key"], version=int(t["plan_version"]))
                .one())
        task = ensure_task(
            db, t["task_key"], t["device_id"], str(t["boot_batch"]), plan,
            int(t["expected_seq_from"]), int(t["expected_seq_to"]),
            t.get("cargo_batch_id"), t.get("box_id"), t.get("vehicle_id"),
            t.get("window_start"), t.get("window_end"), t.get("note", ""))
        out["tasks"].append({"id": task.id, "task_key": task.task_key})
    for m in payload.get("manifests") or []:
        man = import_manifest(db, m, trig=trig)
        out["manifests"].append({"id": man.id, "tier": man.tier,
                                 "device_id": man.device_id,
                                 "boot_batch": man.boot_batch})
    db.commit()
    return out


def events_dict(db: Session, device_id: str, boot_batch: str, seq_from: int | None,
                seq_to: int | None, tier: str | None) -> dict:
    """各环节保留的包与事件对应、内容摘要、重复到达次数。"""
    q = db.query(ChainEvent).filter_by(device_id=device_id,
                                       boot_batch=str(boot_batch))
    if tier:
        q = q.filter(ChainEvent.tier == tier)
    if seq_from is not None:
        q = q.filter(ChainEvent.sample_seq >= seq_from)
    if seq_to is not None:
        q = q.filter(ChainEvent.sample_seq <= seq_to)
    evs = q.order_by(ChainEvent.tier, ChainEvent.sample_seq).all()
    packets = (db.query(ChainPacket)
               .join(ChainManifest, ChainPacket.manifest_id == ChainManifest.id)
               .filter(ChainManifest.device_id == device_id,
                       ChainManifest.boot_batch == str(boot_batch)).all())
    arrivals = {}
    if evs:
        for a in (db.query(ChainArrival)
                  .filter(ChainArrival.event_id.in_([e.id for e in evs])).all()):
            arrivals.setdefault(a.event_id, []).append(
                {"packet_id": a.packet_id, "received_at": _iso(a.received_at)})
    return {
        "events": [{"id": e.id, "tier": e.tier, "sample_seq": e.sample_seq,
                    "event_ids": e.event_ids, "content_hash": e.content_hash,
                    "content": e.content, "event_time": _iso(e.event_time),
                    "arrival_count": e.arrival_count,
                    "arrivals": arrivals.get(e.id, []),
                    "first_manifest_id": e.first_manifest_id} for e in evs],
        "packets": [{"id": p.id, "manifest_id": p.manifest_id, "tier": p.tier,
                     "packet_ref": p.packet_ref, "device_ids": p.device_ids,
                     "event_count": p.event_count,
                     "content_digest": p.content_digest,
                     "received_at": _iso(p.received_at), "retry": p.retry,
                     "split_of": p.split_of, "note": p.note} for p in packets],
        "conflicts": [{"id": c.id, "sample_seq": c.sample_seq,
                       "tier_a": c.tier_a, "tier_b": c.tier_b,
                       "hash_a": c.hash_a, "hash_b": c.hash_b,
                       "content_a": c.content_a, "content_b": c.content_b,
                       "status": c.status, "detected_at": _iso(c.detected_at)}
                      for c in db.query(ChainConflict)
                      .filter_by(device_id=device_id, boot_batch=str(boot_batch))
                      .order_by(ChainConflict.id).all()],
    }


def chain_coverage_map(db: Session, cargo_batch_id: str) -> dict:
    """按货箱聚合温控覆盖：未覆盖/部分覆盖区间，用于曲线空档带。"""
    out: dict[str, dict] = {}
    segs = (db.query(AffectedSegment)
            .filter(AffectedSegment.cargo_batch_id == cargo_batch_id).all())
    for s in segs:
        gap = db.get(ChainGap, s.gap_id)
        if not gap:
            continue
        box = s.box_id or gap.box_id or "—"
        entry = out.setdefault(box, {
            "box_id": box, "coverage": "covered", "gaps": [],
            "uncovered": 0, "partial": 0, "expected": 0, "observed": 0})
        entry["gaps"].append({
            "gap_id": gap.id, "status": gap.status, "active": gap.active,
            "cause": gap.cause, "cause_label": CAUSE_LABELS.get(gap.cause, gap.cause),
            "segment_label": SEGMENT_LABELS.get(gap.segment, gap.segment),
            "device_id": gap.device_id,
            "seq_from": gap.seq_from, "seq_to": gap.seq_to,
            "time_from": _iso(s.effective_from or gap.time_from),
            "time_to": _iso(s.effective_to or gap.time_to),
            "segment_coverage": s.coverage,
            "observed_count": s.observed_count, "expected_count": s.expected_count,
            "remaining_gap": s.remaining_gap,
            "stale": gap.stale or bool((gap.scope_completeness or {}).get("stale")),
        })
        entry["expected"] += s.expected_count
        entry["observed"] += s.observed_count
        if s.coverage == "uncovered" and gap.active:
            entry["uncovered"] += 1
        elif s.coverage == "partial" and gap.active:
            entry["partial"] += 1
    for entry in out.values():
        if entry["uncovered"]:
            entry["coverage"] = "uncovered"
        elif entry["partial"]:
            entry["coverage"] = "has_gaps"
        else:
            entry["coverage"] = "covered"
    return out
