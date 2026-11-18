"""FastAPI 入口：全部 HTTP API。

集中解析架构：发送器只无损转发字节/报文，本服务统一组帧、按版本化契约解析、
标准化、发布。界面数据全部来自这些接口。
"""
import json as jsonlib  # noqa: F401  (保留给调试日志)
from datetime import datetime

from fastapi import Body, Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from .config import PURPOSES
from .db import Base, engine, get_db
from .models import (
    ClockAnchor, ConnectionState, Consumer, ConsumptionRecord, CurrentReading,
    DeviceRelation, Frame, ParsedMessage, ProtocolContract, Publication,
    RawEnvelope, ReadingVersion, Sample, SampleConflict, StandardRun,
    TransportBatch,
)
from .pipeline import (
    bump_counter, close_connection, get_counter, ingest_binary_chunk, ingest_json,
)
from .publish import (
    PublishError, build_publication, consumer_ack, consumer_pull,
    get_or_create_consumer,
)
from .replay import ReplayAbort, run_replay
from .seed import seed as seed_data
from . import chain as CH
from .models import (
    AffectedSegment, ChainConflict, ChainEvent, ChainGap, ChainGapVersion,
    ChainManifest, ChainPacket, ChainScopeState, ChainRefresh,
    ChainSegmentRevision, CollectionTask, DeviceSignal, PublicationCoverage,
    SamplingPlan,
)

app = FastAPI(title="冷链批次温控信号数据服务", version="1.0.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)


@app.on_event("startup")
def startup():
    Base.metadata.create_all(engine)
    db = next(get_db())
    try:
        seed_data(db)  # 空库时放入演示契约/关系/锚点，幂等
    finally:
        db.close()


# ---------------------------------------------------------------- 管理

@app.post("/api/admin/reset")
def admin_reset(db: Session = Depends(get_db)):
    """清空全部业务数据并重置自增序列（验收与演示用）。"""
    names = ", ".join(t.name for t in reversed(Base.metadata.sorted_tables))
    db.execute(text(f"TRUNCATE TABLE {names} RESTART IDENTITY CASCADE"))
    db.commit()
    return {"reset": True}


@app.post("/api/admin/seed")
def admin_seed(db: Session = Depends(get_db)):
    return seed_data(db)


@app.get("/api/health")
def health():
    return {"ok": True}


# ---------------------------------------------------------------- 接入

class BatchIn(BaseModel):
    source: str
    protocol: str
    note: str = ""


@app.post("/api/transport-batches")
def create_batch(body: BatchIn, db: Session = Depends(get_db)):
    if body.protocol not in ("json", "binary", "mixed"):
        raise HTTPException(400, "protocol 必须是 json | binary | mixed")
    b = TransportBatch(source=body.source, protocol=body.protocol, note=body.note)
    db.add(b)
    db.commit()
    return {"id": b.id}


@app.post("/api/transport-batches/{batch_id}/close")
def close_batch(batch_id: int, db: Session = Depends(get_db)):
    b = db.get(TransportBatch, batch_id)
    if not b:
        raise HTTPException(404, "批次不存在")
    b.open = False
    b.closed_at = func.now()
    db.commit()
    return {"id": b.id, "open": False}


@app.post("/api/ingest/json")
async def ingest_json_api(batch_id: int, conn: str = "json-1",
                          msg: dict = Body(...), db: Session = Depends(get_db)):
    batch = db.get(TransportBatch, batch_id)
    if not batch:
        raise HTTPException(404, "传输批次不存在")
    pm = ingest_json(db, batch, conn, msg)
    return {"parsed_id": pm.id, "status": pm.status, "error": pm.error,
            "device_id": pm.device_id, "boot_batch": pm.boot_batch,
            "sample_seq": pm.sample_seq}


@app.post("/api/ingest/binary")
async def ingest_binary_api(request: Request, batch_id: int, conn: str = "bin-1",
                            db: Session = Depends(get_db)):
    batch = db.get(TransportBatch, batch_id)
    if not batch:
        raise HTTPException(404, "传输批次不存在")
    chunk = await request.body()
    if not chunk:
        raise HTTPException(400, "空块")
    pms = ingest_binary_chunk(db, batch, conn, chunk)
    return {"frames_parsed": [{"parsed_id": p.id, "status": p.status,
                               "sample_seq": p.sample_seq} for p in pms]}


class ConnCloseIn(BaseModel):
    batch_id: int
    conn: str


@app.post("/api/connections/close")
def close_conn_api(body: ConnCloseIn, db: Session = Depends(get_db)):
    batch = db.get(TransportBatch, body.batch_id)
    if not batch:
        raise HTTPException(404, "传输批次不存在")
    frames = close_connection(db, batch, body.conn)
    return {"finalized": [{"id": f.id, "status": f.status,
                           "start_off": f.start_off, "end_off": f.end_off}
                          for f in frames]}


# ---------------------------------------------------------------- 批次与下钻

@app.get("/api/batches")
def list_batches(db: Session = Depends(get_db)):
    batches = db.query(TransportBatch).order_by(TransportBatch.id).all()
    out = []
    for b in batches:
        frames = db.query(Frame.status, func.count()).filter(Frame.batch_id == b.id).group_by(Frame.status).all()
        pms = db.query(ParsedMessage.status, func.count()).filter(ParsedMessage.batch_id == b.id).group_by(ParsedMessage.status).all()
        fstat = {s: n for s, n in frames}
        pstat = {s: n for s, n in pms}
        samples = db.query(func.count(Sample.id)).filter(Sample.first_batch_id == b.id).scalar()
        pend_attr = (db.query(func.count(ReadingVersion.id))
                     .join(CurrentReading, CurrentReading.reading_version_id == ReadingVersion.id)
                     .join(Sample, Sample.id == ReadingVersion.sample_id)
                     .filter(Sample.first_batch_id == b.id,
                             ReadingVersion.status == "pending_attribution").scalar())
        pend_anchor = (db.query(func.count(ReadingVersion.id))
                       .join(CurrentReading, CurrentReading.reading_version_id == ReadingVersion.id)
                       .join(Sample, Sample.id == ReadingVersion.sample_id)
                       .filter(Sample.first_batch_id == b.id,
                               ReadingVersion.status == "pending_anchor").scalar())
        out.append({
            "id": b.id, "source": b.source, "protocol": b.protocol, "note": b.note,
            "open": b.open, "created_at": _iso(b.created_at),
            "envelopes": db.query(func.count(RawEnvelope.id)).filter(RawEnvelope.batch_id == b.id).scalar(),
            "frames_ok": fstat.get("ok", 0), "frames_bad_crc": fstat.get("bad_crc", 0),
            "frames_bad_length": fstat.get("bad_length", 0),
            "frames_garbage": fstat.get("garbage", 0),
            "frames_truncated": fstat.get("truncated", 0),
            "parsed_new": pstat.get("parsed_new", 0),
            "parsed_dup": pstat.get("parsed_dup", 0),
            "conflicts": pstat.get("conflict", 0),
            "pending_contract": pstat.get("pending_contract", 0),
            "parse_error": pstat.get("error", 0),
            "samples": samples,
            "pending_attribution": pend_attr, "pending_anchor": pend_anchor,
        })
    return {"batches": out}


@app.get("/api/batches/{batch_id}")
def batch_detail(batch_id: int, db: Session = Depends(get_db)):
    b = db.get(TransportBatch, batch_id)
    if not b:
        raise HTTPException(404, "批次不存在")
    conns = db.query(ConnectionState).filter_by(batch_id=batch_id).all()
    envs = (db.query(RawEnvelope).filter_by(batch_id=batch_id)
            .order_by(RawEnvelope.id).all())
    frames = (db.query(Frame).filter_by(batch_id=batch_id)
              .order_by(Frame.connection_id, Frame.start_off).all())
    pms = (db.query(ParsedMessage).filter_by(batch_id=batch_id)
           .order_by(ParsedMessage.id).all())
    conflicts = (db.query(SampleConflict).join(ParsedMessage, SampleConflict.incoming_parsed_id == ParsedMessage.id)
                 .filter(ParsedMessage.batch_id == batch_id).all())
    return {
        "batch": {"id": b.id, "source": b.source, "protocol": b.protocol,
                  "note": b.note, "open": b.open, "created_at": _iso(b.created_at)},
        "connections": [{"id": c.id, "conn_key": c.conn_key, "protocol": c.protocol,
                         "open": c.open, "received_bytes": c.received_bytes,
                         "pending_bytes": len(c.buffer or b"")} for c in conns],
        "envelopes": [{"id": e.id, "kind": e.kind, "conn": e.connection_id,
                       "seq_in_conn": e.seq_in_conn,
                       "off": [e.conn_off_start, e.conn_off_end],
                       "size": len(e.payload), "received_at": _iso(e.received_at),
                       "hex": bytes(e.payload).hex()} for e in envs],
        "frames": [{"id": f.id, "conn": f.connection_id, "start_off": f.start_off,
                    "end_off": f.end_off, "status": f.status, "detail": f.detail,
                    "hex": bytes(f.payload).hex()} for f in frames],
        "parsed": [{"id": p.id, "frame_id": p.frame_id, "envelope_id": p.envelope_id,
                    "status": p.status, "device_id": p.device_id,
                    "boot_batch": p.boot_batch, "sample_seq": p.sample_seq,
                    "error": p.error, "contract_id": p.contract_id,
                    "fields": p.fields} for p in pms],
        "conflicts": [{"id": c.id, "sample_id": c.sample_id,
                       "incoming_parsed_id": c.incoming_parsed_id,
                       "incoming_fields": c.incoming_fields} for c in conflicts],
    }


@app.get("/api/frames/{frame_id}")
def frame_detail(frame_id: int, db: Session = Depends(get_db)):
    f = db.get(Frame, frame_id)
    if not f:
        raise HTTPException(404, "帧不存在")
    pm = db.query(ParsedMessage).filter_by(frame_id=f.id).order_by(ParsedMessage.id).first()
    reading = None
    if pm and pm.device_id:
        sample = (db.query(Sample).filter_by(device_id=pm.device_id, boot_batch=pm.boot_batch,
                                             sample_seq=pm.sample_seq).first())
        if sample:
            cur = db.get(CurrentReading, sample.id)
            if cur:
                rv = db.get(ReadingVersion, cur.reading_version_id)
                reading = _reading_dict(rv)
    return {"frame": {"id": f.id, "start_off": f.start_off, "end_off": f.end_off,
                      "status": f.status, "detail": f.detail,
                      "hex": bytes(f.payload).hex()},
            "parsed": {"status": pm.status, "error": pm.error,
                       "fields": pm.fields} if pm else None,
            "reading": reading}


# ---------------------------------------------------------------- 样本与曲线

@app.get("/api/samples")
def list_samples(device_id: str | None = None, cargo_batch_id: str | None = None,
                 status: str | None = None, db: Session = Depends(get_db)):
    q = (db.query(Sample, ReadingVersion)
         .join(CurrentReading, CurrentReading.sample_id == Sample.id)
         .join(ReadingVersion, ReadingVersion.id == CurrentReading.reading_version_id))
    if device_id:
        q = q.filter(Sample.device_id == device_id)
    if cargo_batch_id:
        q = q.filter(ReadingVersion.cargo_batch_id == cargo_batch_id)
    if status:
        q = q.filter(ReadingVersion.status == status)
    rows = q.order_by(Sample.device_id, Sample.boot_batch, Sample.sample_seq).all()
    return {"samples": [_sample_row(s, rv) for s, rv in rows]}


@app.get("/api/samples/{sample_id}")
def sample_detail(sample_id: int, db: Session = Depends(get_db)):
    s = db.get(Sample, sample_id)
    if not s:
        raise HTTPException(404, "样本不存在")
    cur = db.get(CurrentReading, s.id)
    rv = db.get(ReadingVersion, cur.reading_version_id) if cur else None
    history = (db.query(ReadingVersion).filter_by(sample_id=s.id)
               .order_by(ReadingVersion.id).all())
    conflicts = db.query(SampleConflict).filter_by(sample_id=s.id).all()
    return {"sample": {"id": s.id, "device_id": s.device_id, "boot_batch": s.boot_batch,
                       "sample_seq": s.sample_seq, "raw_value": s.raw_value,
                       "raw_unit": s.raw_unit, "raw_fields": s.raw_fields,
                       "first_batch_id": s.first_batch_id},
            "current": _reading_dict(rv) if rv else None,
            "history": [_reading_dict(h) for h in history],
            "conflicts": [{"id": c.id, "incoming_fields": c.incoming_fields,
                           "created_at": _iso(c.created_at)} for c in conflicts]}


@app.get("/api/curves")
def curves(cargo_batch_id: str | None = None, device_id: str | None = None,
           db: Session = Depends(get_db)):
    """批次温度曲线：按货物批次或设备取当前读数，按采样时间排序。"""
    q = (db.query(ReadingVersion, Sample)
         .join(CurrentReading, CurrentReading.reading_version_id == ReadingVersion.id)
         .join(Sample, Sample.id == ReadingVersion.sample_id)
         .filter(ReadingVersion.sample_time.isnot(None)))
    if cargo_batch_id:
        q = q.filter(ReadingVersion.cargo_batch_id == cargo_batch_id)
    if device_id:
        q = q.filter(Sample.device_id == device_id)
    rows = q.order_by(ReadingVersion.sample_time).all()
    series = {}
    for rv, s in rows:
        key = f"{s.device_id}/boot-{s.boot_batch}"
        series.setdefault(key, []).append({
            "t": rv.sample_time.isoformat(), "std_c": rv.std_c,
            "err_s": rv.time_error_s, "status": rv.status,
            "vehicle_id": rv.vehicle_id, "cargo_batch_id": rv.cargo_batch_id,
            "quality_flag": rv.quality_flag,
        })
    gaps = []
    if cargo_batch_id:
        from .config import settings as _s
        cg = CH.chain_coverage_map(db, cargo_batch_id)
        gaps = list(cg.values())
    return {"series": series, "coverage": gaps}


# ---------------------------------------------------------------- 契约与重放

class ContractIn(BaseModel):
    contract_key: str
    kind: str
    match: dict
    definition: dict
    note: str = ""


@app.get("/api/contracts")
def list_contracts(db: Session = Depends(get_db)):
    cs = (db.query(ProtocolContract)
          .order_by(ProtocolContract.contract_key, ProtocolContract.revision).all())
    return {"contracts": [{"id": c.id, "contract_key": c.contract_key,
                           "revision": c.revision, "kind": c.kind, "match": c.match,
                           "definition": c.definition, "status": c.status,
                           "note": c.note, "created_at": _iso(c.created_at)} for c in cs]}


@app.post("/api/contracts")
def create_contract(body: ContractIn, db: Session = Depends(get_db)):
    last = (db.query(ProtocolContract).filter_by(contract_key=body.contract_key)
            .order_by(ProtocolContract.revision.desc()).first())
    rev = (last.revision + 1) if last else 1
    c = ProtocolContract(contract_key=body.contract_key, revision=rev, kind=body.kind,
                         match=body.match, definition=body.definition,
                         status="draft", note=body.note)
    db.add(c)
    db.commit()
    return {"id": c.id, "revision": rev}


@app.post("/api/contracts/{contract_id}/activate")
def activate_contract(contract_id: int, db: Session = Depends(get_db)):
    c = db.get(ProtocolContract, contract_id)
    if not c:
        raise HTTPException(404, "契约不存在")
    others = (db.query(ProtocolContract)
              .filter(ProtocolContract.contract_key == c.contract_key,
                      ProtocolContract.status == "active",
                      ProtocolContract.id != c.id).all())
    for o in others:
        o.status = "retired"
    c.status = "active"
    db.commit()
    return {"id": c.id, "status": "active"}


@app.post("/api/contracts/{contract_id}/replay")
def replay_contract(contract_id: int, db: Session = Depends(get_db)):
    c = db.get(ProtocolContract, contract_id)
    if not c:
        raise HTTPException(404, "契约不存在")
    try:
        run = run_replay(db, c)
    except ReplayAbort as e:
        raise HTTPException(400, str(e))
    return {"run_id": run.id, "status": run.status, "stats": run.stats,
            "abort_reason": run.abort_reason}


@app.get("/api/runs")
def list_runs(db: Session = Depends(get_db)):
    runs = db.query(StandardRun).order_by(StandardRun.id.desc()).limit(50).all()
    return {"runs": [{"id": r.id, "kind": r.kind, "contract_key": r.contract_key,
                      "from_revision": r.from_revision, "to_revision": r.to_revision,
                      "status": r.status, "stats": r.stats,
                      "abort_reason": r.abort_reason,
                      "created_at": _iso(r.created_at)} for r in runs]}


# ---------------------------------------------------------------- 关系与锚点

class RelationIn(BaseModel):
    probe_id: str
    vehicle_id: str
    box_id: str
    cargo_batch_id: str
    valid_from: datetime
    valid_to: datetime | None = None


@app.get("/api/relations")
def list_relations(db: Session = Depends(get_db)):
    rs = db.query(DeviceRelation).order_by(DeviceRelation.probe_id, DeviceRelation.valid_from).all()
    return {"relation_revision": get_counter(db, "relation_revision"),
            "relations": [{"id": r.id, "probe_id": r.probe_id, "vehicle_id": r.vehicle_id,
                           "box_id": r.box_id, "cargo_batch_id": r.cargo_batch_id,
                           "valid_from": r.valid_from.isoformat(),
                           "valid_to": r.valid_to.isoformat() if r.valid_to else None} for r in rs]}


@app.post("/api/relations")
def create_relation(body: RelationIn, db: Session = Depends(get_db)):
    overlap = (db.query(DeviceRelation)
               .filter(DeviceRelation.probe_id == body.probe_id,
                       DeviceRelation.valid_from < (body.valid_to or datetime.max.replace(tzinfo=body.valid_from.tzinfo)),
                       (DeviceRelation.valid_to.is_(None)) | (DeviceRelation.valid_to > body.valid_from))
               .all())
    if overlap:
        raise HTTPException(409, f"与既有安装区间重叠：relation {overlap[0].id}")
    r = DeviceRelation(**body.model_dump())
    db.add(r)
    bump_counter(db, "relation_revision")
    db.flush()
    CH.mark_relation_change(db, body.probe_id, "安装关系修订")
    db.commit()
    return {"id": r.id, "relation_revision": get_counter(db, "relation_revision"),
            "note": "相关观测区间已失效，需刷新诊断（结果在此之前标记滞后）"}


class AnchorIn(BaseModel):
    device_id: str
    boot_batch: str
    counter: int
    server_time: datetime
    error_s: float = 2.0
    source: str = "gateway_sync"


@app.get("/api/anchors")
def list_anchors(db: Session = Depends(get_db)):
    aa = db.query(ClockAnchor).order_by(ClockAnchor.device_id, ClockAnchor.counter).all()
    return {"anchors": [{"id": a.id, "device_id": a.device_id, "boot_batch": a.boot_batch,
                         "counter": a.counter, "server_time": a.server_time.isoformat(),
                         "error_s": a.error_s, "source": a.source} for a in aa]}


@app.post("/api/anchors")
def create_anchor(body: AnchorIn, db: Session = Depends(get_db)):
    a = ClockAnchor(**body.model_dump())
    db.add(a)
    db.commit()
    return {"id": a.id}


# ---------------------------------------------------------------- 发布与消费

@app.post("/api/publications/build")
def build_pub(purpose: str, db: Session = Depends(get_db)):
    try:
        pub = build_publication(db, purpose)
    except PublishError as e:
        raise HTTPException(400, str(e))
    coverage = CH.compute_publication_coverage(db, pub)
    db.commit()
    return {"id": pub.id, "purpose": pub.purpose, "version": pub.version,
            "item_count": pub.item_count, "coverage": coverage}


@app.get("/api/publications")
def list_pubs(purpose: str | None = None, db: Session = Depends(get_db)):
    q = db.query(Publication)
    if purpose:
        q = q.filter_by(purpose=purpose)
    pubs = q.order_by(Publication.purpose, Publication.version).all()
    return {"purposes": PURPOSES,
            "publications": [{"id": p.id, "purpose": p.purpose, "version": p.version,
                              "status": p.status, "item_count": p.item_count,
                              "relation_revision": p.relation_revision,
                              "created_at": _iso(p.created_at)} for p in pubs]}


@app.get("/api/publications/{pub_id}/coverage")
def pub_coverage(pub_id: int, db: Session = Depends(get_db)):
    pub = db.get(Publication, pub_id)
    if not pub:
        raise HTTPException(404, "发布版本不存在")
    rows = (db.query(PublicationCoverage).filter_by(publication_id=pub_id)
            .order_by(PublicationCoverage.cargo_batch_id, PublicationCoverage.box_id)
            .all())
    return {"publication": {"id": pub.id, "purpose": pub.purpose,
                            "version": pub.version, "status": pub.status,
                            "item_count": pub.item_count},
            "coverage": [{"cargo_batch_id": r.cargo_batch_id, "box_id": r.box_id,
                          "coverage": r.coverage, "open_gaps": r.open_gaps,
                          "affected_segments": r.affected_segments,
                          "diagnosis_snapshot": r.diagnosis_snapshot,
                          "computed_at": _iso(r.computed_at)} for r in rows]}


@app.get("/api/publications/{pub_id}/items")
def pub_items(pub_id: int, db: Session = Depends(get_db)):
    pub = db.get(Publication, pub_id)
    if not pub:
        raise HTTPException(404, "发布版本不存在")
    from .models import PublicationItem
    items = (db.query(PublicationItem).filter_by(publication_id=pub_id)
             .order_by(PublicationItem.seq).all())
    return {"publication": {"id": pub.id, "purpose": pub.purpose, "version": pub.version,
                            "status": pub.status, "item_count": pub.item_count},
            "items": [{"seq": i.seq, "sample_id": i.sample_id, "device_id": i.device_id,
                       "std_c": i.std_c,
                       "sample_time": i.sample_time.isoformat() if i.sample_time else None,
                       "time_error_s": i.time_error_s, "vehicle_id": i.vehicle_id,
                       "box_id": i.box_id, "cargo_batch_id": i.cargo_batch_id}
                      for i in items]}


class ConsumerIn(BaseModel):
    name: str
    purpose: str


@app.post("/api/consumers")
def create_consumer(body: ConsumerIn, db: Session = Depends(get_db)):
    try:
        c = get_or_create_consumer(db, body.name, body.purpose)
    except PublishError as e:
        raise HTTPException(400, str(e))
    return {"id": c.id, "name": c.name, "purpose": c.purpose}


@app.get("/api/consumers")
def list_consumers(db: Session = Depends(get_db)):
    cs = db.query(Consumer).order_by(Consumer.id).all()
    return {"consumers": [_consumer_dict(db, c) for c in cs]}


@app.get("/api/consumers/{consumer_id}")
def get_consumer(consumer_id: int, db: Session = Depends(get_db)):
    c = db.get(Consumer, consumer_id)
    if not c:
        raise HTTPException(404, "消费端不存在")
    return _consumer_dict(db, c)


@app.post("/api/consumers/{consumer_id}/pull")
def pull(consumer_id: int, size: int | None = None, db: Session = Depends(get_db)):
    c = db.get(Consumer, consumer_id)
    if not c:
        raise HTTPException(404, "消费端不存在")
    return consumer_pull(db, c, size)


class AckIn(BaseModel):
    version: int
    from_seq: int
    to_seq: int


@app.post("/api/consumers/{consumer_id}/ack")
def ack(consumer_id: int, body: AckIn, db: Session = Depends(get_db)):
    c = db.get(Consumer, consumer_id)
    if not c:
        raise HTTPException(404, "消费端不存在")
    try:
        return consumer_ack(db, c, body.version, body.from_seq, body.to_seq)
    except PublishError as e:
        raise HTTPException(409, str(e))


@app.get("/api/consumption-records")
def consumption_records(consumer_id: int | None = None, db: Session = Depends(get_db)):
    q = db.query(ConsumptionRecord)
    if consumer_id:
        q = q.filter_by(consumer_id=consumer_id)
    recs = q.order_by(ConsumptionRecord.id).all()
    return {"records": [{"id": r.id, "consumer_id": r.consumer_id,
                         "publication_id": r.publication_id,
                         "from_seq": r.from_seq, "to_seq": r.to_seq,
                         "item_count": r.item_count,
                         "acked_at": _iso(r.acked_at)} for r in recs]}




# ---------------------------------------------------------------- 观测链路诊断

@app.post("/api/chain/import")
def chain_import(payload: dict = Body(...), db: Session = Depends(get_db)):
    """一次导入采样计划/采集任务/各环节清单（虚构或真实回执）。"""
    try:
        out = CH.load_bundle(db, payload)
    except (KeyError, ValueError) as e:
        raise HTTPException(400, f"清单导入失败：{e}")
    return out


@app.post("/api/chain/manifests")
def chain_import_manifest(payload: dict = Body(...), db: Session = Depends(get_db)):
    try:
        m = CH.import_manifest(db, payload)
    except (KeyError, ValueError) as e:
        raise HTTPException(400, f"清单导入失败：{e}")
    db.commit()
    return {"manifest_id": m.id, "tier": m.tier, "device_id": m.device_id,
            "boot_batch": m.boot_batch, "event_count": m.event_count}


@app.get("/api/chain/manifests")
def chain_list_manifests(device_id: str | None = None, boot_batch: str | None = None,
                         tier: str | None = None, db: Session = Depends(get_db)):
    q = db.query(ChainManifest)
    if device_id:
        q = q.filter_by(device_id=device_id)
    if boot_batch:
        q = q.filter_by(boot_batch=boot_batch)
    if tier:
        q = q.filter_by(tier=tier)
    ms = q.order_by(ChainManifest.id).all()
    return {"manifests": [{"id": m.id, "tier": m.tier,
                           "tier_label": CH.TIER_LABELS.get(m.tier, m.tier),
                           "device_id": m.device_id, "boot_batch": m.boot_batch,
                           "seq_from": m.seq_from, "seq_to": m.seq_to,
                           "complete": m.complete, "source": m.source,
                           "source_ref": m.source_ref,
                           "received_from": _iso(m.received_from),
                           "received_to": _iso(m.received_to),
                           "event_count": m.event_count, "note": m.note,
                           "imported_at": _iso(m.imported_at)} for m in ms]}


class PlanIn(BaseModel):
    plan_key: str
    version: int = 1
    device_id: str | None = None
    interval_s: float
    anchor_time: datetime
    anchor_seq: int = 0
    drift_s_per_step: float = 0.0
    effective_from: datetime | None = None
    effective_to: datetime | None = None
    note: str = ""


@app.get("/api/chain/plans")
def chain_plans(db: Session = Depends(get_db)):
    ps = db.query(SamplingPlan).order_by(SamplingPlan.plan_key,
                                         SamplingPlan.version).all()
    return {"plans": [{"id": p.id, "plan_key": p.plan_key, "version": p.version,
                       "device_id": p.device_id, "interval_s": p.interval_s,
                       "anchor_time": _iso(p.anchor_time),
                       "anchor_seq": p.anchor_seq,
                       "drift_s_per_step": p.drift_s_per_step,
                       "status": p.status, "note": p.note,
                       "effective_from": _iso(p.effective_from),
                       "effective_to": _iso(p.effective_to)} for p in ps]}


@app.post("/api/chain/plans")
def chain_create_plan(body: PlanIn, db: Session = Depends(get_db)):
    p = CH.ensure_sampling_plan(
        db, body.plan_key, body.version, body.device_id, body.interval_s,
        body.anchor_time, body.anchor_seq, body.drift_s_per_step,
        body.effective_from, body.effective_to, body.note)
    if body.version > 1:
        old = (db.query(SamplingPlan)
               .filter(SamplingPlan.plan_key == body.plan_key,
                       SamplingPlan.version < body.version).all())
        for o in old:
            o.status = "retired"
    db.commit()
    return {"id": p.id, "plan_key": p.plan_key, "version": p.version}


class TaskIn(BaseModel):
    task_key: str
    device_id: str
    boot_batch: str
    plan_key: str
    plan_version: int
    expected_seq_from: int
    expected_seq_to: int
    cargo_batch_id: str | None = None
    box_id: str | None = None
    vehicle_id: str | None = None
    window_start: datetime | None = None
    window_end: datetime | None = None
    note: str = ""


@app.get("/api/chain/tasks")
def chain_tasks(db: Session = Depends(get_db)):
    ts = db.query(CollectionTask).order_by(CollectionTask.id).all()
    out = []
    for t in ts:
        plan = db.get(SamplingPlan, t.plan_id)
        out.append({"id": t.id, "task_key": t.task_key, "device_id": t.device_id,
                    "boot_batch": t.boot_batch,
                    "cargo_batch_id": t.cargo_batch_id, "box_id": t.box_id,
                    "vehicle_id": t.vehicle_id,
                    "plan_id": t.plan_id,
                    "plan_key": plan.plan_key if plan else None,
                    "plan_version": plan.version if plan else None,
                    "expected_seq_from": t.expected_seq_from,
                    "expected_seq_to": t.expected_seq_to,
                    "window_start": _iso(t.window_start),
                    "window_end": _iso(t.window_end), "status": t.status,
                    "note": t.note})
    return {"tasks": out}


@app.post("/api/chain/tasks")
def chain_create_task(body: TaskIn, db: Session = Depends(get_db)):
    plan = (db.query(SamplingPlan)
            .filter_by(plan_key=body.plan_key, version=body.plan_version).first())
    if not plan:
        raise HTTPException(404, "采样计划版本不存在")
    t = CH.ensure_task(db, body.task_key, body.device_id, body.boot_batch, plan,
                       body.expected_seq_from, body.expected_seq_to,
                       body.cargo_batch_id, body.box_id, body.vehicle_id,
                       body.window_start, body.window_end, body.note)
    db.commit()
    return {"id": t.id, "task_key": t.task_key}


class RefreshIn(BaseModel):
    scopes: list[dict] | None = None   # [{"device_id":..,"boot_batch":..}]
    note: str = "手动刷新"


@app.post("/api/chain/refresh")
def chain_refresh(body: RefreshIn | None = None, db: Session = Depends(get_db)):
    scopes = None
    trig = "手动刷新"
    if body:
        trig = body.note or trig
        if body.scopes is not None:
            scopes = [(s["device_id"], str(s["boot_batch"])) for s in body.scopes]
    refs = CH.refresh_scopes(db, scopes, trig)
    return {"refreshes": [{"id": r.id, "scope_key": r.scope_key,
                           "status": r.status, "tasks": r.tasks_evaluated,
                           "gaps": r.gaps_evaluated,
                           "created": r.gaps_created,
                           "changed": r.gaps_changed,
                           "filled": r.gaps_filled,
                           "note": r.note,
                           "finished_at": _iso(r.finished_at)} for r in refs]}


@app.get("/api/chain/scopes")
def chain_scopes(db: Session = Depends(get_db)):
    sts = db.query(ChainScopeState).order_by(ChainScopeState.scope_key).all()
    refs = {r.id: r for r in db.query(ChainRefresh)
            .filter(ChainRefresh.id.in_([s.last_refresh_id for s in sts
                                         if s.last_refresh_id])).all()} if sts else {}
    out = []
    for s in sts:
        r = refs.get(s.last_refresh_id)
        out.append({"scope_key": s.scope_key, "device_id": s.device_id,
                    "boot_batch": s.boot_batch, "stale": s.stale,
                    "stale_reason": s.stale_reason,
                    "last_evaluated_at": _iso(s.last_evaluated_at),
                    "last_refresh": ({"id": r.id, "status": r.status,
                                      "note": r.note,
                                      "finished_at": _iso(r.finished_at)}
                                     if r else None)})
    return {"scopes": out}


@app.get("/api/chain/gaps")
def chain_gaps(cargo_batch_id: str | None = None, device_id: str | None = None,
               box_id: str | None = None, status: str | None = None,
               cause: str | None = None, segment: str | None = None,
               active_only: bool = False, include_closed: bool = False,
               db: Session = Depends(get_db)):
    q = db.query(ChainGap)
    if cargo_batch_id:
        q = q.filter(ChainGap.cargo_batch_id == cargo_batch_id)
    if device_id:
        q = q.filter(ChainGap.device_id == device_id)
    if box_id:
        q = q.filter(ChainGap.box_id == box_id)
    if status:
        q = q.filter(ChainGap.status == status)
    if cause:
        q = q.filter(ChainGap.cause == cause)
    if segment:
        q = q.filter(ChainGap.segment == segment)
    # 默认只看当前（未关闭）缺口；历史判断在缺口详情里按版本展开
    if active_only or not include_closed:
        q = q.filter(ChainGap.active.is_(True))
    gaps = q.order_by(ChainGap.device_id, ChainGap.seq_from, ChainGap.id).all()
    return {"gaps": [CH.gap_dict(db, g) for g in gaps],
            "segments": [{"key": k, "label": v} for k, v in
                         CH.SEGMENT_LABELS.items()],
            "causes": [{"key": k, "label": v} for k, v in
                       CH.CAUSE_LABELS.items()]}


@app.get("/api/chain/gaps/{gap_id}")
def chain_gap_detail(gap_id: int, db: Session = Depends(get_db)):
    g = db.get(ChainGap, gap_id)
    if not g:
        raise HTTPException(404, "缺口不存在")
    d = CH.gap_dict(db, g)
    d["versions"] = CH.versions_dict(db, gap_id)
    return d


@app.get("/api/chain/gaps/{gap_id}/versions")
def chain_gap_versions(gap_id: int, db: Session = Depends(get_db)):
    if not db.get(ChainGap, gap_id):
        raise HTTPException(404, "缺口不存在")
    return {"gap_id": gap_id, "versions": CH.versions_dict(db, gap_id)}


class AnnotateIn(BaseModel):
    operator: str
    evidence: str
    scope: str = ""
    cause: str | None = None


@app.post("/api/chain/gaps/{gap_id}/annotate")
def chain_gap_annotate(gap_id: int, body: AnnotateIn, db: Session = Depends(get_db)):
    g = db.get(ChainGap, gap_id)
    if not g:
        raise HTTPException(404, "缺口不存在")
    try:
        return CH.annotate_gap(db, g, body.operator, body.evidence,
                               body.scope, body.cause)
    except ValueError as e:
        raise HTTPException(400, str(e))


@app.get("/api/chain/events")
def chain_events(device_id: str, boot_batch: str,
                 seq_from: int | None = None, seq_to: int | None = None,
                 tier: str | None = None, db: Session = Depends(get_db)):
    return CH.events_dict(db, device_id, boot_batch, seq_from, seq_to, tier)


@app.get("/api/chain/segments")
def chain_segments(cargo_batch_id: str | None = None, box_id: str | None = None,
                   device_id: str | None = None, active_only: bool = True,
                   include_closed: bool = False,
                   db: Session = Depends(get_db)):
    q = db.query(AffectedSegment)
    if cargo_batch_id:
        q = q.filter(AffectedSegment.cargo_batch_id == cargo_batch_id)
    if box_id:
        q = q.filter(AffectedSegment.box_id == box_id)
    if device_id:
        q = q.filter(AffectedSegment.device_id == device_id)
    segs = q.order_by(AffectedSegment.device_id, AffectedSegment.effective_from).all()
    out = []
    for s in segs:
        gap = db.get(ChainGap, s.gap_id)
        if active_only and not include_closed and (not gap or not gap.active):
            continue
        d = CH._segment_dict(s)
        if gap:
            d["gap_status"] = gap.status
            d["gap_cause"] = gap.cause
            d["gap_cause_label"] = CH.CAUSE_LABELS.get(gap.cause, gap.cause)
            d["gap_version"] = gap.version
        out.append(d)
    return {"segments": out}


@app.get("/api/chain/segments/{segment_id}/history")
def chain_segment_history(segment_id: int, db: Session = Depends(get_db)):
    s = db.get(AffectedSegment, segment_id)
    if not s:
        raise HTTPException(404, "区段不存在")
    revs = (db.query(ChainSegmentRevision)
            .filter_by(affected_segment_id=segment_id)
            .order_by(ChainSegmentRevision.revision).all())
    return {"segment": CH._segment_dict(s),
            "history": [{"revision": r.revision, "coverage": r.coverage,
                         "expected_count": r.expected_count,
                         "observed_count": r.observed_count,
                         "sample_ids": r.sample_ids,
                         "readings_basis": r.readings_basis,
                         "remaining_gap": r.remaining_gap,
                         "change_reason": r.change_reason,
                         "created_at": _iso(r.created_at)} for r in revs]}


@app.get("/api/chain/conflicts")
def chain_conflicts(status: str | None = None, db: Session = Depends(get_db)):
    q = db.query(ChainConflict)
    if status:
        q = q.filter_by(status=status)
    cs = q.order_by(ChainConflict.id).all()
    return {"conflicts": [{"id": c.id, "device_id": c.device_id,
                           "boot_batch": c.boot_batch, "sample_seq": c.sample_seq,
                           "tier_a": c.tier_a, "tier_b": c.tier_b,
                           "hash_a": c.hash_a, "hash_b": c.hash_b,
                           "content_a": c.content_a, "content_b": c.content_b,
                           "status": c.status,
                           "detected_at": _iso(c.detected_at)} for c in cs]}


@app.get("/api/chain/signals")
def chain_signals(device_id: str | None = None, db: Session = Depends(get_db)):
    q = db.query(DeviceSignal)
    if device_id:
        q = q.filter_by(device_id=device_id)
    ss = q.order_by(DeviceSignal.at).all()
    return {"signals": [{"id": s.id, "device_id": s.device_id,
                         "boot_batch": s.boot_batch, "kind": s.kind,
                         "at": _iso(s.at), "source": s.source,
                         "note": s.note} for s in ss]}


class BackfillIn(BaseModel):
    device_id: str
    boot_batch: str
    source: str = "gateway_retry"
    received_at: datetime | None = None
    events: list[dict]
    refresh: bool = True
    source_kind: str = "backfill"    # backfill=迟到补传；ingest=平台正常接入


@app.post("/api/chain/backfill")
def chain_backfill(body: BackfillIn, db: Session = Depends(get_db)):
    """补传迟到数据：入库、更新区段覆盖与批次曲线，仅重算受影响区间。"""
    try:
        out = CH.ingest_backfill(db, body.device_id, body.boot_batch,
                                 body.events, body.source, body.received_at,
                                 source_kind=body.source_kind)
    except (KeyError, ValueError) as e:
        raise HTTPException(400, f"补传失败：{e}")
    refreshes = []
    if body.refresh:
        refreshes = CH.refresh_scopes(
            db, [(body.device_id, str(body.boot_batch))], "补传后重算")
    return {"backfill": out,
            "refreshes": [{"id": r.id, "scope_key": r.scope_key,
                           "status": r.status, "gaps": r.gaps_evaluated,
                           "filled": r.gaps_filled} for r in refreshes]}


@app.get("/api/chain/coverage")
def chain_coverage(cargo_batch_id: str | None = None, db: Session = Depends(get_db)):
    """批次/货箱的温控有效覆盖与剩余缺口（含曲线空档带）。"""
    q = db.query(AffectedSegment)
    if cargo_batch_id:
        q = q.filter(AffectedSegment.cargo_batch_id == cargo_batch_id)
    segs = q.order_by(AffectedSegment.effective_from).all()
    out = []
    for s in segs:
        gap = db.get(ChainGap, s.gap_id)
        if not gap or not gap.active:
            continue
        d = CH._segment_dict(s)
        d["gap_status"], d["gap_cause"] = gap.status, gap.cause
        d["gap_cause_label"] = CH.CAUSE_LABELS.get(gap.cause, gap.cause)
        d["segment_label"] = CH.SEGMENT_LABELS.get(gap.segment, gap.segment)
        out.append(d)
    return {"coverage": out}


# ---------------------------------------------------------------- 总览

@app.get("/api/overview")
def overview(db: Session = Depends(get_db)):
    def n(model):
        return db.query(func.count(model.id)).scalar()
    return {
        "batches": n(TransportBatch), "envelopes": n(RawEnvelope),
        "frames": n(Frame), "samples": n(Sample),
        "conflicts": n(SampleConflict),
        "pending_contract": db.query(func.count(ParsedMessage.id)).filter_by(status="pending_contract").scalar(),
        "pending_anchor": db.query(func.count(ReadingVersion.id)).join(CurrentReading, CurrentReading.reading_version_id == ReadingVersion.id).filter(ReadingVersion.status == "pending_anchor").scalar(),
        "pending_attribution": db.query(func.count(ReadingVersion.id)).join(CurrentReading, CurrentReading.reading_version_id == ReadingVersion.id).filter(ReadingVersion.status == "pending_attribution").scalar(),
        "contracts": n(ProtocolContract), "publications": n(Publication),
        "consumers": n(Consumer),
    }


# ---------------------------------------------------------------- 序列化辅助

def _iso(dt):
    return dt.isoformat() if dt else None


def _reading_dict(rv: ReadingVersion) -> dict:
    return {"id": rv.id, "sample_id": rv.sample_id, "std_c": rv.std_c,
            "quality_flag": rv.quality_flag,
            "sample_time": _iso(rv.sample_time), "time_error_s": rv.time_error_s,
            "vehicle_id": rv.vehicle_id, "box_id": rv.box_id,
            "cargo_batch_id": rv.cargo_batch_id, "status": rv.status,
            "status_detail": rv.status_detail,
            "contract_key": rv.contract_key, "contract_revision": rv.contract_revision,
            "relation_revision": rv.relation_revision, "conversion": rv.conversion,
            "source": rv.source, "run_id": rv.run_id, "created_at": _iso(rv.created_at)}


def _sample_row(s: Sample, rv: ReadingVersion) -> dict:
    return {"id": s.id, "device_id": s.device_id, "boot_batch": s.boot_batch,
            "sample_seq": s.sample_seq, "raw_value": s.raw_value, "raw_unit": s.raw_unit,
            "std_c": rv.std_c, "quality_flag": rv.quality_flag,
            "sample_time": _iso(rv.sample_time), "time_error_s": rv.time_error_s,
            "vehicle_id": rv.vehicle_id, "box_id": rv.box_id,
            "cargo_batch_id": rv.cargo_batch_id, "status": rv.status,
            "first_batch_id": s.first_batch_id}


def _consumer_dict(db: Session, c: Consumer) -> dict:
    records = db.query(func.count(ConsumptionRecord.id)).filter_by(consumer_id=c.id).scalar()
    return {"id": c.id, "name": c.name, "purpose": c.purpose,
            "cursor_version": c.cursor_version, "cursor_seq": c.cursor_seq,
            "pending_page": c.pending_page, "acked_pages": records}
