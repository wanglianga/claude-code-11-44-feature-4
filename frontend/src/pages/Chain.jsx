import React, { useEffect, useState } from 'react'
import { api } from '../api.js'

const STATUS_CLS = {
  confirmed: 'tag bad', pending: 'tag warn', partial: 'tag warn',
  parse_backlog: 'tag warn', annotated: 'tag', filled: 'tag ok',
  superseded: 'tag',
}
const COVERAGE_CLS = { uncovered: 'tag bad', has_gaps: 'tag warn',
  partial: 'tag warn', covered: 'tag ok', complete: 'tag ok' }

export default function Chain() {
  const [rows, setRows] = useState([])
  const [causes, setCauses] = useState([])
  const [segments, setSegments] = useState([])
  const [scopes, setScopes] = useState([])
  const [conflicts, setConflicts] = useState([])
  const [filters, setFilters] = useState({ cargo_batch_id: '', device_id: '',
    status: '', cause: '', segment: '', include_closed: false })
  const [detail, setDetail] = useState(null)
  const [events, setEvents] = useState(null)
  const [segmentHistory, setSegmentHistory] = useState(null)
  const [msg, setMsg] = useState('')
  const [error, setError] = useState('')
  const [form, setForm] = useState({ operator: 'on-call', evidence: '', scope: '', cause: '' })

  const load = () => {
    const q = []
    Object.entries(filters).forEach(([k, v]) => {
      if (v !== '' && v !== false) q.push(`${k}=${encodeURIComponent(v)}`)
    })
    // 按状态查询已关闭项时要能查到历史结论
    if (filters.status && q.every((x) => !x.startsWith('include_closed'))) {
      q.push('include_closed=true')
    }
    api.get(`/api/chain/gaps${q.length ? '?' + q.join('&') : ''}`)
      .then((d) => { setRows(d.gaps); setCauses(d.causes); setSegments(d.segments) })
      .catch((e) => setError(e.message))
    api.get('/api/chain/scopes').then((d) => setScopes(d.scopes)).catch(() => {})
    api.get('/api/chain/conflicts').then((d) => setConflicts(d.conflicts)).catch(() => {})
  }
  useEffect(load, [])

  const refresh = async () => {
    setError(''); setMsg('')
    try {
      const r = await api.post('/api/chain/refresh', {})
      const created = r.refreshes.reduce((a, x) => a + x.created, 0)
      const filled = r.refreshes.reduce((a, x) => a + x.filled, 0)
      setMsg(`已重算 ${r.refreshes.length} 个区间：新增 ${created}，补齐 ${filled}`)
      load()
    } catch (e) { setError(e.message) }
  }

  const open = async (id) => {
    setError('')
    try {
      const d = await api.get(`/api/chain/gaps/${id}`)
      setDetail(d)
      setSegmentHistory(null)
      setEvents(null)
      const ev = await api.get(
        `/api/chain/events?device_id=${encodeURIComponent(d.device_id)}` +
        `&boot_batch=${encodeURIComponent(d.boot_batch)}`)
      setEvents(ev)
    } catch (e) { setError(e.message) }
  }

  const annotate = async () => {
    if (!detail) return
    setError(''); setMsg('')
    try {
      const r = await api.post(`/api/chain/gaps/${detail.id}/annotate`, {
        operator: form.operator, evidence: form.evidence, scope: form.scope,
        cause: form.cause || null,
      })
      setDetail({ ...detail, ...r })
      setMsg(`已标注缺口 #${detail.id}（版本 v${r.version}，旧判断保留）`)
      load()
    } catch (e) { setError(e.message) }
  }

  const openHistory = async (segmentId) => {
    try {
      setSegmentHistory(await api.get(`/api/chain/segments/${segmentId}/history`))
    } catch (e) { setError(e.message) }
  }

  const setF = (k, v) => setFilters({ ...filters, [k]: v })

  return (
    <div>
      {error && <div className="error">{error}</div>}
      {msg && <div className="ok-msg">{msg}</div>}

      <h2>观测链路缺口诊断</h2>
      <div className="toolbar">
        <label>货物批次 <input value={filters.cargo_batch_id} placeholder="LOT-A"
          onChange={(e) => setF('cargo_batch_id', e.target.value)} size={8} /></label>
        <label>设备 <input value={filters.device_id} placeholder="PRB-2002"
          onChange={(e) => setF('device_id', e.target.value)} size={10} /></label>
        <label>状态
          <select value={filters.status} onChange={(e) => setF('status', e.target.value)}>
            <option value="">全部</option>
            {['confirmed', 'pending', 'partial', 'parse_backlog', 'annotated',
              'filled'].map((s) => <option key={s}>{s}</option>)}
          </select>
        </label>
        <label>原因
          <select value={filters.cause} onChange={(e) => setF('cause', e.target.value)}>
            <option value="">全部</option>
            {causes.map((c) => <option key={c.key} value={c.key}>{c.label}</option>)}
          </select>
        </label>
        <label>环节
          <select value={filters.segment} onChange={(e) => setF('segment', e.target.value)}>
            <option value="">全部</option>
            {segments.map((c) => <option key={c.key} value={c.key}>{c.label}</option>)}
          </select>
        </label>
        <label><input type="checkbox" checked={filters.include_closed}
          onChange={(e) => setF('include_closed', e.target.checked)} /> 包含已关闭</label>
        <button onClick={load}>查询</button>
        <button onClick={refresh}>重算全部区间</button>
      </div>

      <p className="muted">
        结果按 (设备, 启动批次) 增量维护。清单/计划/关系修订后区间失效，
        重算完成前缺口标为滞后；单条“同步成功”不能代替清单完整性。
      </p>

      <table>
        <thead>
          <tr>
            <th>缺口</th><th>批次 / 货箱</th><th>设备 / 启动</th>
            <th>序号范围</th><th>时间范围</th><th>状态</th><th>缺失环节</th>
            <th>原因</th><th>置信</th><th>受影响区段</th><th>版本</th><th>评估时间</th><th></th>
          </tr>
        </thead>
        <tbody>
          {rows.map((g) => (
            <tr key={g.id} className={(detail?.id === g.id) ? 'selected' : ''}>
              <td>#{g.id}</td>
              <td>{g.cargo_batch_id || '—'} / {g.box_id || '—'}</td>
              <td>{g.device_id} / {g.boot_batch}</td>
              <td><code>[{g.seq_from}, {g.seq_to}]</code>
                {g.origin_seq_from != null && (g.origin_seq_from !== g.seq_from ||
                  g.origin_seq_to !== g.seq_to) &&
                  <div className="muted">原 [{g.origin_seq_from}, {g.origin_seq_to}]</div>}
              </td>
              <td className="note">{g.time_from} ~ {g.time_to}</td>
              <td><span className={STATUS_CLS[g.status] || 'tag'}>{g.status_label || g.status}</span>
                {g.reopened && <div><span className="tag warn">已重开</span></div>}
                {g.scope_state?.stale && <div className="warn">结果滞后</div>}
              </td>
              <td>{g.segment_label}</td>
              <td className="note">{g.cause_label}</td>
              <td>{g.confidence}</td>
              <td>{(g.affected_segments || []).map((s) => (
                <div key={s.id}>
                  {s.cargo_batch_id}/{s.box_id}{' '}
                  <span className={COVERAGE_CLS[s.coverage] || 'tag'}>{s.coverage}</span>{' '}
                  {s.observed_count}/{s.expected_count}
                </div>
              ))}</td>
              <td>v{g.version}</td>
              <td className="muted">{g.last_evaluated_at}</td>
              <td><button onClick={() => open(g.id)}>详情</button></td>
            </tr>
          ))}
          {rows.length === 0 && <tr><td colSpan={13} className="empty">没有缺口</td></tr>}
        </tbody>
      </table>

      {scopes.length > 0 && (
        <>
          <h3>区间维护状态</h3>
          <table>
            <thead><tr><th>区间</th><th>状态</th><th>失效原因</th>
              <th>最近评估</th><th>最近重算</th></tr></thead>
            <tbody>
              {scopes.map((s) => (
                <tr key={s.scope_key}>
                  <td>{s.scope_key}</td>
                  <td><span className={s.stale ? 'tag warn' : 'tag ok'}>
                    {s.stale ? '滞后' : '最新'}</span></td>
                  <td className="note">{s.stale_reason}</td>
                  <td className="muted">{s.last_evaluated_at}</td>
                  <td className="muted">{s.last_refresh?.finished_at || '—'}
                    {s.last_refresh && `（${s.last_refresh.status}）`}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}

      {conflicts.length > 0 && (
        <>
          <h3>同身份内容冲突 <span className="muted">（不静默采用任一值）</span></h3>
          <table>
            <thead><tr><th>冲突</th><th>设备/启动/序号</th><th>环节</th>
              <th>内容 A</th><th>内容 B</th><th>状态</th></tr></thead>
            <tbody>
              {conflicts.map((c) => (
                <tr key={c.id}>
                  <td>#{c.id}</td>
                  <td>{c.device_id}/{c.boot_batch}/{c.sample_seq}</td>
                  <td>{c.tier_a} / {c.tier_b}</td>
                  <td><code>{JSON.stringify(c.content_a)}</code></td>
                  <td><code>{JSON.stringify(c.content_b)}</code></td>
                  <td>{c.status}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}

      {detail && (
        <div className="panel">
          <h3>缺口 #{detail.id} · {detail.device_id}/boot-{detail.boot_batch} ·
            序号 [{detail.seq_from}, {detail.seq_to}]</h3>
          <p>
            <span className={STATUS_CLS[detail.status] || 'tag'}>
              {detail.status_label || detail.status}</span>{' '}
            <b>{detail.cause_label}</b>（{detail.segment_label}，置信 {detail.confidence}）
            · 判定依据：{detail.rationale}
          </p>
          <p className="muted">
            采集任务 {detail.collection_task_id ?? '—'} · 预期采样计划 v{detail.plan_version}
            {' · '}上次设备序号 {detail.last_device_seq ?? '—'}
            {' · '}设备序号连续：{String(detail.device_seq_continuous)}
            {' · '}设备事件时间 {detail.device_event_time || '—'}
            {' · '}网关收到 {detail.gateway_received_time || '—'}
            {' · '}平台接收 {detail.platform_received_time || '—'}
          </p>
          <p className="muted">
            来源完整性：
            {Object.entries(detail.scope_completeness || {})
              .filter(([, v]) => v).map(([k, v]) =>
                <span key={k}> {k}={typeof v === 'object' ? JSON.stringify(v) : v}</span>)}
          </p>

          <h4>仍未知的环节</h4>
          {detail.unknown_segments?.length
            ? <ul>{detail.unknown_segments.map((u, i) => <li key={i}>{u}</li>)}</ul>
            : <p className="muted">无（证据已闭合）</p>}

          <h4>支持记录 / 冲突记录</h4>
          <div className="two-col">
            <div>
              <b>支持</b>
              <ul className="evidence">
                {(detail.supporting || []).map((s, i) => (
                  <li key={i}><code>{s.kind}</code> {s.summary}</li>
                ))}
                {!detail.supporting?.length && <li className="muted">无</li>}
              </ul>
            </div>
            <div>
              <b>冲突</b>
              <ul className="evidence">
                {(detail.conflicting || []).map((s, i) => (
                  <li key={i} className="bad"><code>{s.kind}</code> {s.summary}</li>
                ))}
                {!detail.conflicting?.length && <li className="muted">无</li>}
              </ul>
            </div>
          </div>
          <p className="muted">重复到达次数：{JSON.stringify(detail.duplicate_counts || {})}</p>
          <details>
            <summary>来源清单引用（{detail.source_manifest_refs?.length || 0}）</summary>
            <pre>{JSON.stringify(detail.source_manifest_refs, null, 1)}</pre>
          </details>

          <h4>受影响温控区段 <span className="muted">（按设备、有效安装关系与有效时段计算）</span></h4>
          <table>
            <thead><tr><th>区段</th><th>货物 / 货箱 / 车辆</th><th>有效时段</th>
              <th>覆盖</th><th>已观测/应观测</th><th>剩余缺口</th><th>版本</th><th></th></tr></thead>
            <tbody>
              {(detail.affected_segments || []).map((s) => (
                <tr key={s.id}>
                  <td>#{s.id}</td>
                  <td>{s.cargo_batch_id}/{s.box_id}/{s.vehicle_id}</td>
                  <td className="note">{s.effective_from} ~ {s.effective_to}</td>
                  <td><span className={COVERAGE_CLS[s.coverage] || 'tag'}>{s.coverage}</span></td>
                  <td>{s.observed_count}/{s.expected_count}</td>
                  <td className="note">
                    {s.remaining_gap ? `[${s.remaining_gap.seq_from}, ${s.remaining_gap.seq_to}]` : '—'}
                  </td>
                  <td>{s.revision}</td>
                  <td><button onClick={() => openHistory(s.id)}>变化历史</button></td>
                </tr>
              ))}
              {!detail.affected_segments?.length &&
                <tr><td colSpan={8} className="muted">缺口时段不落在任何有效安装区间</td></tr>}
            </tbody>
          </table>
          {segmentHistory && (
            <div>
              <h4>区段 #{segmentHistory.segment.id} 覆盖变化</h4>
              <table>
                <thead><tr><th>版本</th><th>覆盖</th><th>已观测/应观测</th>
                  <th>读数依据</th><th>剩余缺口</th><th>原因</th><th>时间</th></tr></thead>
                <tbody>
                  {segmentHistory.history.map((h) => (
                    <tr key={h.revision}>
                      <td>{h.revision}</td>
                      <td><span className={COVERAGE_CLS[h.coverage] || 'tag'}>{h.coverage}</span></td>
                      <td>{h.observed_count}/{h.expected_count}</td>
                      <td className="note">{(h.readings_basis || []).map((b) =>
                        `${b.sample_id}@${b.sample_time}=${b.std_c}℃`).join('; ') || '—'}</td>
                      <td className="note">{h.remaining_gap ? JSON.stringify(h.remaining_gap.seqs) : '—'}</td>
                      <td>{h.change_reason}</td>
                      <td className="muted">{h.created_at}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          <h4>判断历史（不可变版本）</h4>
          <table>
            <thead><tr><th>版本</th><th>状态</th><th>原因</th><th>序号范围</th>
              <th>变化原因</th><th>触发</th><th>标注</th><th>时间</th></tr></thead>
            <tbody>
              {(detail.versions || []).map((v) => (
                <tr key={v.version}>
                  <td>v{v.version}</td>
                  <td><span className={STATUS_CLS[v.status] || 'tag'}>{v.status}</span></td>
                  <td>{v.cause_label}</td>
                  <td>{v.seq_from}–{v.seq_to}</td>
                  <td className="note">{v.change_reason}</td>
                  <td className="note">{v.triggered_by}</td>
                  <td className="note">{v.annotation
                    ? `${v.annotation.operator}: ${v.annotation.evidence}` : '—'}</td>
                  <td className="muted">{v.created_at}</td>
                </tr>
              ))}
            </tbody>
          </table>

          <h4>人工标注 <span className="muted">（记录操作者、证据与适用范围）</span></h4>
          <div className="form">
            <label>操作者 <input value={form.operator}
              onChange={(e) => setForm({ ...form, operator: e.target.value })} /></label>
            <label>适用范围 <input value={form.scope} placeholder="如 LOT-A/PRB-2002 seq 1020-1027"
              onChange={(e) => setForm({ ...form, scope: e.target.value })} /></label>
            <label>原因（可选）
              <select value={form.cause} onChange={(e) => setForm({ ...form, cause: e.target.value })}>
                <option value="">保持建议原因</option>
                {causes.map((c) => <option key={c.key} value={c.key}>{c.label}</option>)}
              </select>
            </label>
            <label className="grow">证据 <input value={form.evidence}
              onChange={(e) => setForm({ ...form, evidence: e.target.value })} /></label>
            <button onClick={annotate}>提交标注</button>
          </div>

          {events && (
            <details>
              <summary>各环节事件与包对应（统一事件身份后比较，重复只计到达）</summary>
              <h4>事件</h4>
              <table>
                <thead><tr><th>环节</th><th>序号</th><th>事件标识</th><th>内容摘要</th>
                  <th>事件时间</th><th>到达次数</th><th>到达包</th></tr></thead>
                <tbody>
                  {events.events.map((e) => (
                    <tr key={e.id}>
                      <td>{e.tier}</td><td>{e.sample_seq}</td>
                      <td className="note">{(e.event_ids || []).join(', ')}</td>
                      <td><code>{e.content_hash?.slice(0, 10)}</code></td>
                      <td className="note">{e.event_time}</td>
                      <td className={e.arrival_count > 1 ? 'warn' : ''}>{e.arrival_count}</td>
                      <td className="note">{(e.arrivals || []).map((a) =>
                        `包${a.packet_id}@${a.received_at}`).join('; ')}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              <h4>包</h4>
              <table>
                <thead><tr><th>包</th><th>环节</th><th>引用</th><th>设备</th>
                  <th>事件数</th><th>重试/拆分</th><th>接收时间</th></tr></thead>
                <tbody>
                  {events.packets.map((p) => (
                    <tr key={p.id}>
                      <td>#{p.id}</td><td>{p.tier}</td><td>{p.packet_ref}</td>
                      <td>{(p.device_ids || []).join(',')}</td><td>{p.event_count}</td>
                      <td>{p.retry ? '重试' : ''}{p.split_of ? `拆分自 ${p.split_of}` : ''}</td>
                      <td className="note">{p.received_at}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </details>
          )}
          <button onClick={() => { setDetail(null); setSegmentHistory(null); setEvents(null) }}>
            收起
          </button>
        </div>
      )}
    </div>
  )
}
