"""数据模型。

分层：
  原始接入层  transport_batch / connection_state / raw_envelope / frame
  协议契约层  protocol_contract
  设备关系层  device_relation / clock_anchor / meta_counter
  标准化层    parsed_message / sample / sample_conflict / reading_version / current_reading
  质量发布层  publication / publication_item / consumer / consumption_record
  运行控制    standard_run（标准化/重放运行，提交前校验）
  观测链路层  sampling_plan / collection_task / chain_manifest / chain_event / chain_packet
              chain_arrival / device_signal / chain_conflict / chain_gap / chain_gap_version
              chain_gap_evidence / affected_segment / chain_segment_revision
              chain_refresh / chain_scope_state / publication_coverage
"""
from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, Float, ForeignKey, Integer,
    LargeBinary, String, Text, UniqueConstraint, func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from .db import Base


# ---------------------------------------------------------------- 原始接入层

class TransportBatch(Base):
    """传输批次：发送器一次有序发送会话。原始信封都挂在传输批次下。"""
    __tablename__ = "transport_batch"

    id = Column(Integer, primary_key=True)
    source = Column(String(128), nullable=False)          # 发送端标识，如 sender-local-01
    protocol = Column(String(16), nullable=False)         # json | binary | mixed
    note = Column(String(256), default="")
    open = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    closed_at = Column(DateTime(timezone=True))


class ConnectionState(Base):
    """一条逻辑连接的组帧状态。残片只在本连接内续接，不同连接绝不串接。

    buffer 持久化保存，服务重启不丢；offset_base 是 buffer[0] 在连接字节流中的绝对偏移。
    """
    __tablename__ = "connection_state"
    __table_args__ = (UniqueConstraint("batch_id", "conn_key", name="uq_conn_batch_key"),)

    id = Column(Integer, primary_key=True)
    batch_id = Column(Integer, ForeignKey("transport_batch.id"), nullable=False)
    conn_key = Column(String(64), nullable=False)         # 发送方给的连接标识
    protocol = Column(String(16), nullable=False, default="binary")
    buffer = Column(LargeBinary, nullable=False, default=b"")
    offset_base = Column(BigInteger, nullable=False, default=0)   # buffer[0] 的绝对流偏移
    received_bytes = Column(BigInteger, nullable=False, default=0)
    open = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    closed_at = Column(DateTime(timezone=True))


class RawEnvelope(Base):
    """原始接入库：无损保存字节、来源、接收时间、传输批次。"""
    __tablename__ = "raw_envelope"

    id = Column(BigInteger, primary_key=True)
    batch_id = Column(Integer, ForeignKey("transport_batch.id"), nullable=False)
    connection_id = Column(Integer, ForeignKey("connection_state.id"))
    kind = Column(String(16), nullable=False)             # json_msg | bin_chunk
    seq_in_conn = Column(Integer, nullable=False, default=0)
    conn_off_start = Column(BigInteger)                   # 本块在连接流中的起始绝对偏移
    conn_off_end = Column(BigInteger)                     # 结束偏移（开区间）
    payload = Column(LargeBinary, nullable=False)         # 原始字节（JSON 报文为其 UTF-8 字节）
    received_at = Column(DateTime(timezone=True), server_default=func.now())


class Frame(Base):
    """组帧结果。status: ok / bad_crc / bad_length / garbage / truncated。

    坏校验与截断分开记录；一条坏帧只跳过自己，不吞后续完整帧。
    """
    __tablename__ = "frame"

    id = Column(BigInteger, primary_key=True)
    batch_id = Column(Integer, ForeignKey("transport_batch.id"), nullable=False)
    connection_id = Column(Integer, ForeignKey("connection_state.id"), nullable=False)
    start_off = Column(BigInteger, nullable=False)        # 连接流内绝对起始偏移
    end_off = Column(BigInteger, nullable=False)          # 结束偏移（开区间）
    payload = Column(LargeBinary, nullable=False)
    status = Column(String(16), nullable=False)
    detail = Column(Text, default="")
    created_at = Column(DateTime(timezone=True), server_default=func.now())


# ---------------------------------------------------------------- 协议契约层

class ProtocolContract(Base):
    """协议契约：固件适用范围、字段含义与换算规则。按 (contract_key, revision) 版本化。

    新固件没有明确契约时，报文保持 pending_contract，绝不凭字段形状猜单位。
    """
    __tablename__ = "protocol_contract"
    __table_args__ = (UniqueConstraint("contract_key", "revision", name="uq_contract_key_rev"),)

    id = Column(Integer, primary_key=True)
    contract_key = Column(String(64), nullable=False)     # 如 json-temp / bin-temp
    revision = Column(Integer, nullable=False)
    kind = Column(String(16), nullable=False)             # json | binary
    match = Column(JSONB, nullable=False)                 # 适用固件：{"fw":[...]} 或 {"versions":[...]}
    definition = Column(JSONB, nullable=False)            # 字段含义、单位、换算、时钟规则
    status = Column(String(16), nullable=False, default="draft")  # draft | active | retired
    note = Column(String(256), default="")
    created_at = Column(DateTime(timezone=True), server_default=func.now())


# ---------------------------------------------------------------- 设备关系层

class DeviceRelation(Base):
    """设备关系：探头→车辆/周转箱/货物批次 的安装区间（左闭右开）。"""
    __tablename__ = "device_relation"

    id = Column(Integer, primary_key=True)
    probe_id = Column(String(64), nullable=False)
    vehicle_id = Column(String(64), nullable=False)
    box_id = Column(String(64), nullable=False)
    cargo_batch_id = Column(String(64), nullable=False)
    valid_from = Column(DateTime(timezone=True), nullable=False)
    valid_to = Column(DateTime(timezone=True))            # NULL = 至今有效
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class ClockAnchor(Base):
    """可信锚点：同一启动批次内，设备本地计数 ↔ 可信服务器时间 的映射点。"""
    __tablename__ = "clock_anchor"

    id = Column(Integer, primary_key=True)
    device_id = Column(String(64), nullable=False)
    boot_batch = Column(String(64), nullable=False)
    counter = Column(BigInteger, nullable=False)          # 设备本地计数（样本序号）
    server_time = Column(DateTime(timezone=True), nullable=False)
    error_s = Column(Float, nullable=False, default=2.0)  # 锚点自身误差
    source = Column(String(64), default="gateway_sync")
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class MetaCounter(Base):
    """全局版本计数：relation_revision 等，重放提交时做乐观并发检查。"""
    __tablename__ = "meta_counter"

    key = Column(String(64), primary_key=True)
    value = Column(BigInteger, nullable=False, default=0)


# ---------------------------------------------------------------- 标准化层

class ParsedMessage(Base):
    """一次解析结果。status: parsed_new / parsed_dup / conflict / pending_contract / error"""
    __tablename__ = "parsed_message"

    id = Column(BigInteger, primary_key=True)
    batch_id = Column(Integer, ForeignKey("transport_batch.id"), nullable=False)
    frame_id = Column(BigInteger, ForeignKey("frame.id"))
    envelope_id = Column(BigInteger, ForeignKey("raw_envelope.id"))
    contract_id = Column(Integer, ForeignKey("protocol_contract.id"))
    status = Column(String(24), nullable=False)
    device_id = Column(String(64))                        # 契约换算后的规范设备号
    boot_batch = Column(String(64))
    sample_seq = Column(BigInteger)
    fields = Column(JSONB)                                # 解析出的规范字段
    payload_hash = Column(String(64))
    error = Column(Text, default="")
    run_id = Column(Integer, ForeignKey("standard_run.id"))  # 重放产生的解析标记所属运行
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Sample(Base):
    """样本身份：幂等键 (device_id, boot_batch, sample_seq)。原值与换算依据分开保存。"""
    __tablename__ = "sample"
    __table_args__ = (UniqueConstraint("device_id", "boot_batch", "sample_seq", name="uq_sample_key"),)

    id = Column(BigInteger, primary_key=True)
    device_id = Column(String(64), nullable=False)
    boot_batch = Column(String(64), nullable=False)
    sample_seq = Column(BigInteger, nullable=False)
    raw_value = Column(Float, nullable=False)             # 原始数值（未换算）
    raw_unit = Column(String(16), nullable=False)         # 原始单位/编码，如 C / F / fixed16
    raw_fields = Column(JSONB, nullable=False)            # 全部原始字段（含固件号、事件号等）
    payload_hash = Column(String(64), nullable=False)
    first_batch_id = Column(Integer, ForeignKey("transport_batch.id"), nullable=False)
    first_parsed_id = Column(BigInteger, ForeignKey("parsed_message.id"), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class SampleConflict(Base):
    """相同幂等标识、不同载荷 → 冲突。保留原值，不允许最后到达者覆盖。"""
    __tablename__ = "sample_conflict"

    id = Column(BigInteger, primary_key=True)
    sample_id = Column(BigInteger, ForeignKey("sample.id"), nullable=False)
    incoming_parsed_id = Column(BigInteger, ForeignKey("parsed_message.id"), nullable=False)
    existing_hash = Column(String(64), nullable=False)
    incoming_hash = Column(String(64), nullable=False)
    incoming_fields = Column(JSONB, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class StandardRun(Base):
    """标准化/重放运行。重放先产出完整新结果，校验通过后一次性切换当前索引。"""
    __tablename__ = "standard_run"

    id = Column(Integer, primary_key=True)
    kind = Column(String(16), nullable=False)             # standardize | replay
    contract_key = Column(String(64))
    from_revision = Column(Integer)
    to_revision = Column(Integer)
    started_relation_revision = Column(BigInteger, nullable=False, default=0)
    started_envelope_id = Column(BigInteger, nullable=False, default=0)  # 启动时最大信封 id
    status = Column(String(16), nullable=False, default="running")  # running|committed|aborted
    stats = Column(JSONB, default=dict)
    abort_reason = Column(Text, default="")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    finished_at = Column(DateTime(timezone=True))


class ReadingVersion(Base):
    """一次标准化结果（读数 + 时间误差 + 归属 + 换算依据）。可多次生成，当前指针唯一。"""
    __tablename__ = "reading_version"

    id = Column(BigInteger, primary_key=True)
    sample_id = Column(BigInteger, ForeignKey("sample.id"), nullable=False)
    run_id = Column(Integer, ForeignKey("standard_run.id"))   # NULL = 接入时即时标准化
    std_c = Column(Float)                                 # 标准摄氏温度
    quality_flag = Column(String(16), nullable=False, default="ok")  # ok | out_of_range
    sample_time = Column(DateTime(timezone=True))
    time_error_s = Column(Float)
    vehicle_id = Column(String(64))
    box_id = Column(String(64))
    cargo_batch_id = Column(String(64))
    status = Column(String(24), nullable=False)           # ok | pending_anchor | pending_attribution
    status_detail = Column(Text, default="")
    contract_key = Column(String(64))
    contract_revision = Column(Integer)
    relation_revision = Column(BigInteger)
    conversion = Column(JSONB)                            # 换算过程（逐步），供界面下钻
    source = Column(String(16), nullable=False, default="ingest")   # ingest | replay
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class CurrentReading(Base):
    """当前索引：每个样本最多一条当前读数。重放校验通过后原子切换。"""
    __tablename__ = "current_reading"

    sample_id = Column(BigInteger, ForeignKey("sample.id"), primary_key=True)
    reading_version_id = Column(BigInteger, ForeignKey("reading_version.id"), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


# ---------------------------------------------------------------- 质量发布层

class Publication(Base):
    """发布版本：某用途一份完整、校验通过的快照。消费端按版本+游标读取。"""
    __tablename__ = "publication"
    __table_args__ = (UniqueConstraint("purpose", "version", name="uq_pub_purpose_version"),)

    id = Column(Integer, primary_key=True)
    purpose = Column(String(32), nullable=False)
    version = Column(Integer, nullable=False)
    status = Column(String(16), nullable=False, default="current")  # current | superseded
    item_count = Column(Integer, nullable=False, default=0)
    relation_revision = Column(BigInteger, nullable=False, default=0)
    stats = Column(JSONB, default=dict)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class PublicationItem(Base):
    __tablename__ = "publication_item"
    __table_args__ = (UniqueConstraint("publication_id", "seq", name="uq_pubitem_seq"),)

    id = Column(BigInteger, primary_key=True)
    publication_id = Column(Integer, ForeignKey("publication.id"), nullable=False)
    seq = Column(Integer, nullable=False)
    sample_id = Column(BigInteger, ForeignKey("sample.id"), nullable=False)
    device_id = Column(String(64), nullable=False)
    std_c = Column(Float, nullable=False)
    sample_time = Column(DateTime(timezone=True), nullable=False)
    time_error_s = Column(Float, nullable=False)
    vehicle_id = Column(String(64))
    box_id = Column(String(64))
    cargo_batch_id = Column(String(64))


class Consumer(Base):
    """模拟消费端：游标 = (cursor_version, cursor_seq)；未确认分页重发，不跳过。"""
    __tablename__ = "consumer"

    id = Column(Integer, primary_key=True)
    name = Column(String(64), unique=True, nullable=False)
    purpose = Column(String(32), nullable=False)
    cursor_version = Column(Integer)                      # 正在读的发布版本
    cursor_seq = Column(Integer, nullable=False, default=0)
    pending_page = Column(JSONB)                          # {"version","from","to"} 未确认分页
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class ConsumptionRecord(Base):
    """消费记录：每次确认的分页落一条，重复确认幂等。"""
    __tablename__ = "consumption_record"
    __table_args__ = (UniqueConstraint("consumer_id", "publication_id", "from_seq", name="uq_consumption_page"),)

    id = Column(BigInteger, primary_key=True)
    consumer_id = Column(Integer, ForeignKey("consumer.id"), nullable=False)
    publication_id = Column(Integer, ForeignKey("publication.id"), nullable=False)
    from_seq = Column(Integer, nullable=False)
    to_seq = Column(Integer, nullable=False)
    item_count = Column(Integer, nullable=False)
    acked_at = Column(DateTime(timezone=True), server_default=func.now())


# ---------------------------------------------------------------- 观测链路层
#
# 证据分层（tier）：
#   device_generated  设备侧生成清单（计划内确实生成的样本）
#   gateway_received  网关接收清单（收到的包与事件）
#   platform_received 平台入库清单（已持久保存原文的事件）
#   platform_parsed   平台解析清单（可被温控结果使用的事件）
# 清单完整性始终限定到 (设备, 启动批次, 明确序号范围)；
# 一条网关“本次同步成功”不能代替所有设备的完整性。


class SamplingPlan(Base):
    """预期采样规则版本。设备更换计划即新增版本，旧版本保持可追溯。"""
    __tablename__ = "sampling_plan"
    __table_args__ = (UniqueConstraint("plan_key", "version", name="uq_plan_key_version"),)

    id = Column(Integer, primary_key=True)
    plan_key = Column(String(64), nullable=False)
    version = Column(Integer, nullable=False)
    device_id = Column(String(64))
    interval_s = Column(Float, nullable=False)
    drift_s_per_step = Column(Float, nullable=False, default=0.0)
    anchor_time = Column(DateTime(timezone=True), nullable=False)
    anchor_seq = Column(BigInteger, nullable=False, default=0)
    status = Column(String(16), nullable=False, default="active")  # active | retired
    effective_from = Column(DateTime(timezone=True))
    effective_to = Column(DateTime(timezone=True))
    definition = Column(JSONB, default=dict)
    note = Column(String(256), default="")
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class CollectionTask(Base):
    """采集任务：设备某启动批次在一段有效期内按某一采样计划采集。"""
    __tablename__ = "collection_task"

    id = Column(Integer, primary_key=True)
    task_key = Column(String(64), unique=True, nullable=False)
    device_id = Column(String(64), nullable=False)
    boot_batch = Column(String(64), nullable=False)
    cargo_batch_id = Column(String(64))
    box_id = Column(String(64))
    vehicle_id = Column(String(64))
    plan_id = Column(Integer, ForeignKey("sampling_plan.id"), nullable=False)
    expected_seq_from = Column(BigInteger, nullable=False)
    expected_seq_to = Column(BigInteger, nullable=False)
    window_start = Column(DateTime(timezone=True))
    window_end = Column(DateTime(timezone=True))
    status = Column(String(16), nullable=False, default="active")  # active | closed
    note = Column(String(256), default="")
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class ChainManifest(Base):
    """某一环节的已确认清单（导入的虚构或真实回执）。

    完整性限定到单设备 + 启动批次 + 序号范围。complete=True 表示该范围内
    该环节“已确认完整”，范围外不推断。
    """
    __tablename__ = "chain_manifest"

    id = Column(Integer, primary_key=True)
    tier = Column(String(24), nullable=False)             # device_generated|gateway_received|platform_received|platform_parsed
    device_id = Column(String(64), nullable=False)
    boot_batch = Column(String(64), nullable=False)
    seq_from = Column(BigInteger, nullable=False)
    seq_to = Column(BigInteger, nullable=False)
    complete = Column(Boolean, nullable=False, default=False)
    source = Column(String(128), default="")
    source_ref = Column(String(128), default="")
    declared_event_from = Column(DateTime(timezone=True))
    declared_event_to = Column(DateTime(timezone=True))
    received_from = Column(DateTime(timezone=True))
    received_to = Column(DateTime(timezone=True))
    event_count = Column(Integer, nullable=False, default=0)
    manifest_hash = Column(String(64), default="")
    note = Column(String(256), default="")
    raw = Column(JSONB, default=dict)
    imported_at = Column(DateTime(timezone=True), server_default=func.now())


class ChainEvent(Base):
    """统一事件身份：(层, 设备, 启动批次, 序号) + 内容摘要。

    同身份不同内容各保留一行并记冲突；重复投递只增加到达次数与到达记录。
    乱序到达不覆盖事件时间较新的状态。
    """
    __tablename__ = "chain_event"
    __table_args__ = (UniqueConstraint("tier", "device_id", "boot_batch", "sample_seq",
                                       "content_hash", name="uq_chain_event_identity"),)

    id = Column(BigInteger, primary_key=True)
    tier = Column(String(24), nullable=False)
    device_id = Column(String(64), nullable=False)
    boot_batch = Column(String(64), nullable=False)
    sample_seq = Column(BigInteger, nullable=False)
    event_id = Column(String(128))                        # 首个来源事件标识（按来源保留）
    event_ids = Column(JSONB, default=list)               # 同一身份出现过的全部来源事件标识
    content_hash = Column(String(64), nullable=False)
    content = Column(JSONB, nullable=False)               # 该层保留的原始内容（含温度）
    event_time = Column(DateTime(timezone=True))          # 事件时间；乱序到达不覆盖较新值
    first_manifest_id = Column(Integer, ForeignKey("chain_manifest.id"), nullable=False)
    arrival_count = Column(Integer, nullable=False, default=1)
    first_seen_at = Column(DateTime(timezone=True), server_default=func.now())
    last_seen_at = Column(DateTime(timezone=True), server_default=func.now())
    last_arrival_at = Column(DateTime(timezone=True))


class ChainPacket(Base):
    """包与事件的对应：网关可合并多个设备的发送，也可把同一包拆成多次转发。

    重试包是同一组事件的到达记录，不新增采样。
    """
    __tablename__ = "chain_packet"

    id = Column(BigInteger, primary_key=True)
    manifest_id = Column(Integer, ForeignKey("chain_manifest.id"), nullable=False)
    tier = Column(String(24), nullable=False)
    packet_ref = Column(String(128), nullable=False)
    device_ids = Column(JSONB, default=list)
    event_count = Column(Integer, nullable=False, default=0)
    content_digest = Column(String(64), default="")
    received_at = Column(DateTime(timezone=True))
    retry = Column(Boolean, nullable=False, default=False)
    split_of = Column(String(128))
    note = Column(String(256), default="")
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class ChainArrival(Base):
    """事件的每一次到达（包 × 事件）。重复投递可展开到原始到达次数。"""
    __tablename__ = "chain_arrival"
    __table_args__ = (UniqueConstraint("event_id", "packet_id", name="uq_arrival_event_packet"),)

    id = Column(BigInteger, primary_key=True)
    event_id = Column(BigInteger, ForeignKey("chain_event.id"), nullable=False)
    packet_id = Column(BigInteger, ForeignKey("chain_packet.id"), nullable=False)
    received_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class DeviceSignal(Base):
    """设备侧信号：心跳、计划停用、运输途中断网、已确认维护、重启。

    每类来源单独保留；缺少心跳只表示缺少心跳证据，不能证明温度采集停止。
    """
    __tablename__ = "device_signal"

    id = Column(BigInteger, primary_key=True)
    device_id = Column(String(64), nullable=False)
    boot_batch = Column(String(64))
    kind = Column(String(32), nullable=False)  # heartbeat|planned_stop|maintenance|network_outage|reboot|decommission
    at = Column(DateTime(timezone=True), nullable=False)
    source = Column(String(128), default="")
    note = Column(String(256), default="")
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class ChainConflict(Base):
    """同身份内容不一致：显示具体冲突，不静默采用任一值。"""
    __tablename__ = "chain_conflict"

    id = Column(BigInteger, primary_key=True)
    device_id = Column(String(64), nullable=False)
    boot_batch = Column(String(64), nullable=False)
    sample_seq = Column(BigInteger, nullable=False)
    tier_a = Column(String(24), nullable=False)
    tier_b = Column(String(24), nullable=False)
    hash_a = Column(String(64), nullable=False)
    hash_b = Column(String(64), nullable=False)
    content_a = Column(JSONB)
    content_b = Column(JSONB)
    manifest_a_id = Column(Integer)
    manifest_b_id = Column(Integer)
    status = Column(String(16), nullable=False, default="open")  # open | resolved
    detected_at = Column(DateTime(timezone=True), server_default=func.now())


class ChainGap(Base):
    """缺口（当前状态）。每次判断的完整快照在 chain_gap_version。"""
    __tablename__ = "chain_gap"

    id = Column(Integer, primary_key=True)
    gap_key = Column(String(128), unique=True, nullable=False)
    device_id = Column(String(64), nullable=False)
    boot_batch = Column(String(64), nullable=False)
    cargo_batch_id = Column(String(64))
    box_id = Column(String(64))
    vehicle_id = Column(String(64))
    collection_task_id = Column(Integer, ForeignKey("collection_task.id"))
    plan_id = Column(Integer, ForeignKey("sampling_plan.id"))
    plan_version = Column(Integer)
    segment = Column(String(32), nullable=False)          # device|device_to_gateway|gateway_to_platform|platform|transport_or_platform|platform_parse|unknown
    cause = Column(String(48), nullable=False)            # 见 chain.CAUSE_LABELS
    status = Column(String(24), nullable=False)           # confirmed|pending|parse_backlog|annotated|filled|superseded
    confidence = Column(String(16), nullable=False, default="low")
    seq_from = Column(BigInteger, nullable=False)          # 当前剩余缺口边界
    seq_to = Column(BigInteger, nullable=False)
    origin_seq_from = Column(BigInteger)                   # 缺口首次识别的完整范围
    origin_seq_to = Column(BigInteger)
    time_from = Column(DateTime(timezone=True))
    time_to = Column(DateTime(timezone=True))
    last_device_seq = Column(BigInteger)
    device_event_time = Column(DateTime(timezone=True))
    gateway_received_time = Column(DateTime(timezone=True))
    platform_received_time = Column(DateTime(timezone=True))
    device_seq_continuous = Column(Boolean)
    rationale = Column(Text, default="")
    supporting = Column(JSONB, default=list)
    conflicting = Column(JSONB, default=list)
    unknown_segments = Column(JSONB, default=list)
    scope_completeness = Column(JSONB, default=dict)
    source_manifest_refs = Column(JSONB, default=list)
    duplicate_counts = Column(JSONB, default=dict)
    raw_refs = Column(JSONB, default=list)
    annotation_operator = Column(String(64))
    annotation_evidence = Column(Text)
    annotation_scope = Column(String(256))
    annotation_at = Column(DateTime(timezone=True))
    annotation_cause = Column(String(48))
    version = Column(Integer, nullable=False, default=1)
    active = Column(Boolean, nullable=False, default=True)
    stale = Column(Boolean, nullable=False, default=False)
    reopened = Column(Boolean, nullable=False, default=False)
    last_evaluated_at = Column(DateTime(timezone=True), server_default=func.now())
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    closed_at = Column(DateTime(timezone=True))


class ChainGapVersion(Base):
    """不可变的判断版本：补传、标注、冲突重开后都新增版本，保留历史。"""
    __tablename__ = "chain_gap_version"
    __table_args__ = (UniqueConstraint("gap_id", "version", name="uq_gap_version"),)

    id = Column(BigInteger, primary_key=True)
    gap_id = Column(Integer, ForeignKey("chain_gap.id"), nullable=False)
    version = Column(Integer, nullable=False)
    status = Column(String(24), nullable=False)
    cause = Column(String(48), nullable=False)
    segment = Column(String(32), nullable=False)
    seq_from = Column(BigInteger, nullable=False)
    seq_to = Column(BigInteger, nullable=False)
    time_from = Column(DateTime(timezone=True))
    time_to = Column(DateTime(timezone=True))
    confidence = Column(String(16), nullable=False)
    rationale = Column(Text, default="")
    unknowns = Column(JSONB, default=list)
    scope_completeness = Column(JSONB, default=dict)
    annotation = Column(JSONB)
    change_reason = Column(String(128), nullable=False)
    triggered_by = Column(String(128), default="")
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class ChainGapEvidence(Base):
    """判断版本的可展开支持/冲突记录。"""
    __tablename__ = "chain_gap_evidence"

    id = Column(BigInteger, primary_key=True)
    gap_version_id = Column(BigInteger, ForeignKey("chain_gap_version.id"), nullable=False)
    role = Column(String(16), nullable=False)             # support | conflict
    kind = Column(String(32), nullable=False)             # manifest|event|packet|signal|conflict|plan|relation
    ref_id = Column(String(64))
    summary = Column(String(512), nullable=False)
    detail = Column(JSONB, default=dict)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class AffectedSegment(Base):
    """缺口与温控区段的连接：按设备 + 有效安装关系 + 缺口有效时段计算。"""
    __tablename__ = "affected_segment"
    __table_args__ = (UniqueConstraint("gap_id", "relation_id", name="uq_seg_gap_relation"),)

    id = Column(Integer, primary_key=True)
    gap_id = Column(Integer, ForeignKey("chain_gap.id"), nullable=False)
    relation_id = Column(Integer, ForeignKey("device_relation.id"))
    device_id = Column(String(64), nullable=False)
    vehicle_id = Column(String(64))
    box_id = Column(String(64))
    cargo_batch_id = Column(String(64))
    effective_from = Column(DateTime(timezone=True))
    effective_to = Column(DateTime(timezone=True))
    coverage = Column(String(16), nullable=False, default="uncovered")  # uncovered|partial|covered
    expected_count = Column(Integer, nullable=False, default=0)
    observed_count = Column(Integer, nullable=False, default=0)
    sample_ids = Column(JSONB, default=list)
    readings_basis = Column(JSONB, default=list)
    remaining_gap = Column(JSONB)
    revision = Column(Integer, nullable=False, default=1)
    stale = Column(Boolean, nullable=False, default=False)
    last_recomputed = Column(DateTime(timezone=True), server_default=func.now())
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class ChainSegmentRevision(Base):
    """区段覆盖变化历史（迟到数据补上/仍缺一部分），含前后读数依据。"""
    __tablename__ = "chain_segment_revision"
    __table_args__ = (UniqueConstraint("affected_segment_id", "revision", name="uq_seg_revision"),)

    id = Column(BigInteger, primary_key=True)
    affected_segment_id = Column(Integer, ForeignKey("affected_segment.id"), nullable=False)
    revision = Column(Integer, nullable=False)
    coverage = Column(String(16), nullable=False)
    expected_count = Column(Integer, nullable=False)
    observed_count = Column(Integer, nullable=False)
    sample_ids = Column(JSONB, default=list)
    readings_basis = Column(JSONB, default=list)
    remaining_gap = Column(JSONB)
    change_reason = Column(String(128), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class ChainScopeState(Base):
    """增量维护的区间粒度：device|boot_batch。清单/计划/关系变化即失效。"""
    __tablename__ = "chain_scope_state"

    id = Column(Integer, primary_key=True)
    scope_key = Column(String(128), unique=True, nullable=False)
    device_id = Column(String(64), nullable=False)
    boot_batch = Column(String(64), nullable=False)
    stale = Column(Boolean, nullable=False, default=True)
    stale_reason = Column(String(256), default="")
    last_refresh_id = Column(Integer, ForeignKey("chain_refresh.id"))
    last_evaluated_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class ChainRefresh(Base):
    """一次区间重算。未完成或失效时接口明确标出结果滞后。"""
    __tablename__ = "chain_refresh"

    id = Column(Integer, primary_key=True)
    scope_key = Column(String(128), nullable=False)
    triggered_by = Column(String(128), default="")
    status = Column(String(16), nullable=False, default="running")  # running|completed|failed
    started_at = Column(DateTime(timezone=True), server_default=func.now())
    finished_at = Column(DateTime(timezone=True))
    tasks_evaluated = Column(Integer, nullable=False, default=0)
    gaps_evaluated = Column(Integer, nullable=False, default=0)
    gaps_created = Column(Integer, nullable=False, default=0)
    gaps_changed = Column(Integer, nullable=False, default=0)
    gaps_filled = Column(Integer, nullable=False, default=0)
    segments_recomputed = Column(Integer, nullable=False, default=0)
    note = Column(String(256), default="")


class PublicationCoverage(Base):
    """发布版本的覆盖结论快照。后续设备状态变化不会自动改写已发布报表。"""
    __tablename__ = "publication_coverage"
    __table_args__ = (UniqueConstraint("publication_id", "cargo_batch_id", "box_id",
                                       name="uq_pub_coverage_batch_box"),)

    id = Column(Integer, primary_key=True)
    publication_id = Column(Integer, ForeignKey("publication.id"), nullable=False)
    cargo_batch_id = Column(String(64))
    box_id = Column(String(64))
    coverage = Column(String(24), nullable=False)          # complete|has_gaps|uncovered|unknown
    open_gaps = Column(JSONB, default=list)
    affected_segments = Column(JSONB, default=list)
    diagnosis_snapshot = Column(JSONB, default=dict)
    computed_at = Column(DateTime(timezone=True), server_default=func.now())
