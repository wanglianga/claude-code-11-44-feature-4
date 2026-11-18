import React, { useEffect, useState } from 'react'
import { api } from '../api.js'
import TempCurve from '../components/TempCurve.jsx'

const COVERAGE_CLS = { uncovered: 'tag bad', has_gaps: 'tag warn',
  covered: 'tag ok' }

export default function Curves() {
  const [cargo, setCargo] = useState('LOT-A')
  const [device, setDevice] = useState('')
  const [series, setSeries] = useState({})
  const [coverage, setCoverage] = useState([])
  const [error, setError] = useState('')

  const load = () => {
    const q = device ? `device_id=${device}` : `cargo_batch_id=${cargo}`
    api.get(`/api/curves?${q}`)
      .then((d) => { setSeries(d.series); setCoverage(d.coverage || []) })
      .catch((e) => setError(e.message))
  }
  useEffect(load, [cargo, device])

  const bands = coverage.flatMap((c) => (c.gaps || [])
    .filter((g) => g.time_from && g.time_to && g.segment_coverage !== 'covered')
    .map((g) => ({ ...g, box: c.box_id })))

  return (
    <div>
      {error && <div className="error">{error}</div>}
      <div className="toolbar">
        <label>货物批次：</label>
        <select value={cargo} onChange={(e) => { setCargo(e.target.value); setDevice('') }}>
          {['LOT-A', 'LOT-B', 'LOT-C'].map((c) => <option key={c}>{c}</option>)}
        </select>
        <label>或设备：</label>
        <select value={device} onChange={(e) => setDevice(e.target.value)}>
          <option value="">（按货物批次）</option>
          {['PRB-1001', 'PRB-2002', 'PRB-3003', 'PRB-3005'].map((d) =>
            <option key={d}>{d}</option>)}
        </select>
      </div>
      <TempCurve series={series} bands={device ? [] : bands} />

      <h3>温控有效覆盖 <span className="muted">
        （空档未被解析前不补造温度，也不推断为正常）</span></h3>
      {coverage.length === 0 && <div className="empty">本地图无诊断缺口记录</div>}
      {coverage.map((c) => (
        <div key={c.box_id} className="panel">
          <h4>货箱 {c.box_id} <span className={COVERAGE_CLS[c.coverage] || 'tag'}>
            {c.coverage}</span>{' '}
            <span className="muted">已观测 {c.observed} / 应观测 {c.expected}</span>
          </h4>
          <table>
            <thead><tr><th>缺口</th><th>设备</th><th>时段</th><th>缺失环节</th>
              <th>原因</th><th>区段覆盖</th><th>剩余缺口</th></tr></thead>
            <tbody>
              {(c.gaps || []).map((g) => (
                <tr key={g.gap_id}>
                  <td>#{g.gap_id}{g.active ? '' : '（已关闭）'}</td>
                  <td>{g.device_id}</td>
                  <td className="note">{g.time_from} ~ {g.time_to}</td>
                  <td>{g.segment_label}</td>
                  <td className="note">{g.cause_label}</td>
                  <td><span className={COVERAGE_CLS[g.segment_coverage] || 'tag'}>
                    {g.segment_coverage}</span> {g.observed_count}/{g.expected_count}</td>
                  <td className="note">{g.remaining_gap
                    ? `[${g.remaining_gap.seq_from}, ${g.remaining_gap.seq_to}]` : '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ))}

      <table>
        <thead>
          <tr><th>序列</th><th>点数</th><th>时间范围</th><th>温度范围</th></tr>
        </thead>
        <tbody>
          {Object.entries(series).map(([name, pts]) => {
            const ts = pts.map((p) => p.t).sort()
            const vs = pts.map((p) => p.std_c)
            return (
              <tr key={name}>
                <td>{name}</td><td>{pts.length}</td>
                <td>{ts[0]} ~ {ts[ts.length - 1]}</td>
                <td>{Math.min(...vs).toFixed(2)} ~ {Math.max(...vs).toFixed(2)} °C</td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}
