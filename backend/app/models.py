"""数据模型。

分层：
  原始接入层  transport_batch / connection_state / raw_envelope / frame
  协议契约层  protocol_contract
  设备关系层  device_relation / clock_anchor / meta_counter
  标准化层    parsed_message / sample / sample_conflict / reading_version / current_reading
  质量发布层  publication / publication_item / consumer / consumption_record
  运行控制    standard_run（标准化/重放运行，提交前校验）
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
