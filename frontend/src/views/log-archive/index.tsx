import { useCallback, useEffect, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/log-archive'

type WindowRow = {
  id: number
  window_id: string
  status: string
  record_count: number
  size_bytes: number
  source_numbers: string[]
  manifest_hash: string
  rolled_back_count: number
  created_at: string | null
  committed_at: string | null
}

type Stats = {
  window_total: number
  window_committed: number
  dataset_records: number
  audit_records: number
  contract_records: number
  rolled_back: number
}

type VerifyResult = {
  window_id: string
  ok: boolean
  checks: Record<string, boolean>
  recomputed: Record<string, unknown>
}

type AuditRow = {
  id: number
  audit_no: string
  result: string
  record_count: number
  size_bytes: number
  source_numbers: string[]
  manifest_hash: string
  conflict_count: number
  created_at: string
  rollback_reason?: string
}

type ManifestRow = {
  id: number
  record_key: string
  来源编号: string
  来源版本: string
  记录编号: string
  调用服务: string
  请求时间: string
  source_status: string
  size_bytes: number
  sha256: string
}

type PageResult<T> = { items: T[]; total: number }

async function getJson<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}))
    throw new Error(typeof detail.detail === 'string' ? detail.detail : `接口返回 ${response.status}`)
  }
  return (await response.json()) as T
}

async function postJson(path: string, body: Record<string, unknown>): Promise<{ ok: boolean; message: string; entry?: Record<string, unknown> }> {
  const response = await request(path, { method: 'POST', body: JSON.stringify(body) })
  const payload = (await response.json()) as { ok: boolean; message: string; entry?: Record<string, unknown>; detail?: unknown }
  if (!response.ok || !payload.ok) {
    throw new Error(typeof payload.detail === 'string' ? payload.detail : payload.message || '归档请求失败')
  }
  return payload
}

export default function LogArchiveView() {
  const [windows, setWindows] = useState<WindowRow[]>([])
  const [stats, setStats] = useState<Stats | null>(null)
  const [selected, setSelected] = useState<WindowRow | null>(null)
  const [manifest, setManifest] = useState<ManifestRow[]>([])
  const [audits, setAudits] = useState<AuditRow[]>([])
  const [verify, setVerify] = useState<VerifyResult | null>(null)
  const [message, setMessage] = useState('')
  const [errorMessage, setErrorMessage] = useState('')
  const [sourceNumber, setSourceNumber] = useState('LOG-0003')
  const [windowId, setWindowId] = useState('')

  const reload = useCallback(async () => {
    setErrorMessage('')
    try {
      const [page, stat] = await Promise.all([
        getJson<PageResult<WindowRow>>(`${ENDPOINT}/windows?size=50`),
        getJson<Stats>(`${ENDPOINT}/stats`),
      ])
      setWindows(page.items)
      setStats(stat)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '归档窗口读取失败')
    }
  }, [])

  useEffect(() => {
    void reload()
  }, [reload])

  const loadDetail = useCallback(async (row: WindowRow) => {
    setSelected(row)
    setVerify(null)
    setErrorMessage('')
    try {
      const [manifestPage, auditPage] = await Promise.all([
        getJson<PageResult<ManifestRow>>(`${ENDPOINT}/windows/${row.window_id}/manifest`),
        getJson<PageResult<AuditRow>>(`${ENDPOINT}/windows/${row.window_id}/audits`),
      ])
      setManifest(manifestPage.items)
      setAudits(auditPage.items)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '归档明细读取失败')
    }
  }, [])

  async function runArchive(failureStage: string | null = null) {
    setErrorMessage('')
    setMessage('')
    const body: Record<string, unknown> = {
      source_number: sourceNumber.trim() || null,
    }
    if (windowId.trim()) {
      body.window_id = windowId.trim()
    }
    if (failureStage) {
      body.simulate_failure = failureStage
    }
    try {
      const result = await postJson(`${ENDPOINT}/windows`, body)
      setMessage(result.message)
      await reload()
      const created = result.entry?.window as WindowRow | undefined
      if (created) {
        await loadDetail(created)
      }
    } catch (error) {
      setMessage('')
      setErrorMessage(error instanceof Error ? error.message : '归档执行失败')
      await reload()
    }
  }

  async function runVerify(row: WindowRow) {
    setErrorMessage('')
    try {
      const result = await getJson<VerifyResult>(`${ENDPOINT}/windows/${row.window_id}/verify`)
      setVerify(result)
      setMessage(result.ok ? `窗口 ${row.window_id} 检查点全部复核通过` : `窗口 ${row.window_id} 复核未通过`)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '复核失败')
    }
  }

  const statCards = stats
    ? [
        { label: '归档窗口总数', value: stats.window_total },
        { label: '已提交窗口', value: stats.window_committed },
        { label: '数据集记录数', value: stats.dataset_records },
        { label: '审计记录数', value: stats.audit_records },
        { label: '归档契约数', value: stats.contract_records },
        { label: '回滚窗口数', value: stats.rolled_back },
      ]
    : []

  return (
    <section className="page" data-module="log-archive">
      <header className="page-head">
        <div>
          <h2>调用日志归档</h2>
          <p className="page-desc">
            标准化日志按归档窗口写入可追溯归档数据集，记录数量、来源编号与审计记录同事务提交；
            重复归档幂等去重，事务中断回滚后可从断点重做。
          </p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => void runArchive()}>
            执行归档
          </button>
        </div>
      </header>

      <div className="stat-row">
        {statCards.map((item) => (
          <article key={item.label} className="stat-card">
            <span className="stat-label">{item.label}</span>
            <strong className="stat-value">{item.value}</strong>
          </article>
        ))}
      </div>

      <form
        className="filter-bar"
        onSubmit={(event) => {
          event.preventDefault()
          void runArchive()
        }}
      >
        <label className="filter-item">
          <span>来源编号</span>
          <input value={sourceNumber} placeholder="留空则全量归档" onChange={(event) => setSourceNumber(event.target.value)} />
        </label>
        <label className="filter-item">
          <span>归档窗口编号</span>
          <input value={windowId} placeholder="留空自动生成；回滚窗口填同一编号重做" onChange={(event) => setWindowId(event.target.value)} />
        </label>
        <button className="btn" type="submit">归档</button>
        <button className="btn ghost" type="button" onClick={() => void runArchive('commit')}>
          演练提交中断
        </button>
      </form>

      {message && <p className="page-desc" style={{ color: '#1a7f37' }}>{message}</p>}
      {errorMessage && <p className="error-text">{errorMessage}</p>}

      <table className="data-table">
        <thead>
          <tr>
            <th>归档窗口</th>
            <th>状态</th>
            <th>记录数量</th>
            <th>大小(字节)</th>
            <th>来源编号</th>
            <th>回滚次数</th>
            <th>提交时间</th>
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {windows.map((row) => (
            <tr key={row.window_id}>
              <td>{row.window_id}</td>
              <td>{row.status}</td>
              <td>{row.record_count}</td>
              <td>{row.size_bytes}</td>
              <td>{row.source_numbers.join('、') || '—'}</td>
              <td>{row.rolled_back_count}</td>
              <td>{row.committed_at ?? '—'}</td>
              <td className="row-actions">
                <button className="link" type="button" onClick={() => void loadDetail(row)}>
                  查看清单
                </button>
                {row.status === '已提交' && (
                  <>
                    <button className="link" type="button" onClick={() => void runVerify(row)}>
                      检查点复核
                    </button>
                    <button
                      className="link"
                      type="button"
                      onClick={() => window.open(`${ENDPOINT}/windows/${row.window_id}/export/jsonl`, '_blank')}
                    >
                      导出JSONL
                    </button>
                    <button
                      className="link"
                      type="button"
                      onClick={() => window.open(`${ENDPOINT}/windows/${row.window_id}/package`, '_blank')}
                    >
                      备份打包下载
                    </button>
                  </>
                )}
                {row.status === '已回滚' && (
                  <button className="link" type="button" onClick={() => {
                    setWindowId(row.window_id)
                    void runArchive()
                  }}>
                    断点重做
                  </button>
                )}
              </td>
            </tr>
          ))}
          {windows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={8}>暂无归档窗口，先执行一次归档</td>
            </tr>
          )}
        </tbody>
      </table>

      {selected && (
        <>
          <h3 style={{ marginTop: 24 }}>归档数据集清单 · {selected.window_id}</h3>
          <table className="data-table">
            <thead>
              <tr>
                <th>记录键</th>
                <th>来源编号</th>
                <th>来源版本</th>
                <th>记录编号</th>
                <th>调用服务</th>
                <th>请求时间</th>
                <th>来源状态</th>
                <th>大小(字节)</th>
                <th>记录哈希</th>
              </tr>
            </thead>
            <tbody>
              {manifest.map((row) => (
                <tr key={row.id}>
                  <td>{row.record_key}</td>
                  <td>{row.来源编号}</td>
                  <td>{row.来源版本}</td>
                  <td>{row.记录编号}</td>
                  <td>{row.调用服务}</td>
                  <td>{row.请求时间}</td>
                  <td>{row.source_status}</td>
                  <td>{row.size_bytes}</td>
                  <td title={row.sha256}>{row.sha256.slice(0, 12)}…</td>
                </tr>
              ))}
              {manifest.length === 0 && (
                <tr>
                  <td className="empty-state" colSpan={9}>该窗口没有已提交的归档记录（可能已回滚）</td>
                </tr>
              )}
            </tbody>
          </table>

          <h3 style={{ marginTop: 24 }}>审计记录</h3>
          <table className="data-table">
            <thead>
              <tr>
                <th>审计编号</th>
                <th>结果</th>
                <th>记录数量</th>
                <th>大小(字节)</th>
                <th>来源编号</th>
                <th>冲突数</th>
                <th>时间</th>
                <th>说明/回滚原因</th>
              </tr>
            </thead>
            <tbody>
              {audits.map((row) => (
                <tr key={row.id}>
                  <td>{row.audit_no}</td>
                  <td>{row.result}</td>
                  <td>{row.record_count}</td>
                  <td>{row.size_bytes}</td>
                  <td>{row.source_numbers.join('、') || '—'}</td>
                  <td>{row.conflict_count}</td>
                  <td>{row.created_at}</td>
                  <td>{row.rollback_reason ?? '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}

      {verify && (
        <div className="stat-row" style={{ marginTop: 24 }}>
          {Object.entries(verify.checks).map(([name, passed]) => (
            <article key={name} className="stat-card">
              <span className="stat-label">{name}</span>
              <strong className="stat-value" style={{ color: passed ? '#1a7f37' : '#cf222e' }}>
                {passed ? '通过' : '不通过'}
              </strong>
            </article>
          ))}
        </div>
      )}

      <footer className="page-foot">
        <span>共 {windows.length} 个归档窗口；备份模块经 /api/backup/archive-contracts 读取同一数据快照</span>
      </footer>
    </section>
  )
}
