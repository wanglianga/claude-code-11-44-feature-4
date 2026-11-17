# 冷链设备多协议接入与批次温控信号数据服务

把冷链运输中互相矛盾的原始设备载荷，治理成可按药品批次使用的标准温控信号：
实时 JSON 探头（明确时区、摄氏小数）与离线二进制记录仪（长度/校验/定点温度/
启动批次/本地计数）统一**集中解析**，输出带时间误差与归属的标准读数，
并按用途做质量发布，供消费端按游标可靠读取。

- 后端：FastAPI + PostgreSQL（原始接入库 / 协议契约 / 设备关系 / 标准化 / 质量发布）
- 前端：React（批次下钻、字节与换算过程、批次曲线、契约重放、发布与模拟消费）
- 发送器：本地 Python CLI，按可控顺序发送两类协议，不依赖真实硬件

## 一键启动（Docker Compose）

```bash
docker compose up --build
```

| 入口 | 地址 |
| --- | --- |
| 前端界面 | http://localhost:8080 |
| 后端 API | http://localhost:8000 （自带 OpenAPI 文档 /docs） |
| PostgreSQL | localhost:5432（ccdev / ccdev / coldchain） |

启动后装入演示数据并体验完整流程：

```bash
# 方式一：宿主机（需 python3）
python sender/sender.py run all
python sender/sender.py publish
python sender/sender.py consume audit-app batch_audit

# 方式二：容器内发送器
docker compose run --rm sender run all
docker compose run --rm sender publish
```

## 本地开发

```bash
# 1. 数据库（容器已预装 PostgreSQL，库/用户/密码 ccdev）
PGPASSWORD=ccdev psql -h 127.0.0.1 -U ccdev -d postgres -c "CREATE DATABASE coldchain OWNER ccdev;"

# 2. 后端
python3 -m venv .venv
.venv/bin/pip install -r backend/requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
cd backend && ../.venv/bin/uvicorn app.main:app --port 8000
# 首次启动自动建表并写入种子契约/关系/锚点（幂等）

# 3. 前端（另开终端）
cd frontend && npm install && npm run dev     # http://localhost:5173，已配置 /api 代理

# 4. 发送器（另开终端，服务端地址可用 --base 或 SENDER_BASE 覆盖）
cd sender && ../.venv/bin/python sender.py run all
```

## 测试账号

本服务是内部数据治理服务，**未设登录认证，无需账号**；演示用设备、车辆、
货物批次由种子数据提供（PRB-1001 / PRB-2002 / PRB-3003，VH-01/02/03，LOT-A/B/C）。
消费端标识任意起名（如 `audit-app`），用于演示游标与消费记录。

## 验证方式

### 自动化验收（已实际执行）

```bash
cd backend && ../.venv/bin/python -m pytest tests/ -v
```

- `tests/test_framing.py`：组帧器单元测试（半帧、多帧、坏校验不吞帧、噪声、
  截断、连接隔离、绝对偏移）。
- `tests/test_acceptance.py`：端到端验收——独立测试库与端口起真实服务，
  用发送器真实发送两类协议，组合半帧、多帧、坏校验、负温、序号重启、
  断网补传、换车边界、契约修订，核对标准温度（如 80→4.8°C、−56→−3.7°C、
  41°F→5.0°C）、样本总数（17）、时间范围（10:05–13:02 UTC）、归属
  （补传样本挂旧车 VH-01、边界样本保持待归属）与消费端接收结果
  （13 条审计数据、未确认分页重发、确认才推进、无重复样本）。
- 重放提交守卫：安装关系或新信封在重放期间变化时，提交被拒绝。

最近一次执行结果：`9 passed`。

### 手工界面验证（已实际执行）

1. `python sender.py reset && python sender.py seed && python sender.py run all`
   —— 实际发送两类协议的全部场景；
2. `python sender.py publish` —— 构建 `line_monitor`（14 条）与
   `batch_audit`（13 条）发布版本；
3. `python sender.py consume audit-app batch_audit` —— 消费端按游标读完全部
   13 条，分页确认，无重复；
4. 前端「发送批次」页可见各批次的组帧/解析/冲突/待归属数量，可下钻到
   帧字节区间、十六进制、逐条换算步骤与时钟映射；「批次曲线」页查看
   LOT-A/B 温度曲线；「协议契约」页可新建修订、激活、重放；
   「发布与消费」页可交互式拉取/确认分页。

### 关键行为速查

| 行为 | 规则 |
| --- | --- |
| 幂等 | (设备, 启动批次, 样本序号)；同键同载荷去重，同键不同载荷记冲突且保留原值 |
| 新固件 | 无生效契约 → 待解析，不猜单位；契约修订后重放解决 |
| 组帧 | 按连接维护状态；坏校验/截断分别记录；坏帧不吞后续帧；连接间残片不串接 |
| 时钟 | 离线计数只经同启动批次可信锚点映射；缺锚保持待归属 |
| 归属 | 按采样时间±误差与安装区间连接；误差跨换车边界保持待归属 |
| 重放 | 只重放依赖该契约的信封；校验通过才切换当前索引；提交检查关系与信封版本 |
| 发布 | 按用途门槛交付完整快照；消费端游标读取，未确认重发，确认才推进 |

## 目录结构

```
backend/    FastAPI 服务（app/）与验收测试（tests/）
sender/     本地发送器：帧构造、场景库、CLI（不依赖真实硬件）
frontend/   React 界面（Vite 构建，nginx 托管）
docs/       protocol.md —— 完整协议说明与虚构报文
docker-compose.yml
```

详细协议定义（帧布局、CRC、换算、时钟映射、发布消费协议）见
[docs/protocol.md](docs/protocol.md)。

## 说明

- 本服务只治理温控数据：不调度车辆、不认定药品处置、不创建运输业务操作。
- `POST /api/admin/reset` 会清空全部业务数据，仅供本地验收与演示使用。
