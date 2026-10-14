import { useCallback, useEffect, useRef, useState, type FormEvent } from "react";
import { createRoot } from "react-dom/client";
import { Activity, Database, Layers, ArrowDownUp, Archive, Settings, RefreshCw, Sun, Moon, Search, ChevronRight, ShieldCheck, CaseSensitive, Hash, Binary, Plus } from "lucide-react";
import { EntryViewer } from "./EntryViewer";
import { CopyButton } from "./CopyButton";
import { base64Size, decodeUtf8, fromBase64, toBase64 } from "./entry-data";
import "./style.css";

type Level = { level: number; tableCount: number; bytes: number };
type Stats = { database: string; mode: string; health: string; fault: string | null; pressure: string; levels: Level[]; activeMemTableBytes: number; immutableMemTableCount: number; immutableMemTableBytes: number; writeOperations: number; userBytesWritten: number; walBytesWritten: number; cacheHits: number; cacheMisses: number; cacheHitRatio: number | null; writeAmplification: number | null; compactionBacklogBytes: number | null; publishedSequence: number | null };
type Info = { statistics: Stats; diskBytes: number; sstFiles: number; walFiles: number };
type Table = { fileId: number; level: number; bytes: number; blocks: number; firstKey: string; lastKey: string };
type Identity = { name: string; instanceId: string; allowWrites: boolean };
type Row = { key: string; value: string };
type Job = { id: string; state: string; error: string | null };
type Page = "Overview" | "Data browser" | "Storage" | "Transfers" | "Backups" | "Maintenance";
type Encoding = "utf8" | "hex" | "base64";
const pages: [Page, typeof Activity][] = [["Overview", Activity], ["Data browser", Database], ["Storage", Layers], ["Transfers", ArrowDownUp], ["Backups", Archive], ["Maintenance", Settings]];
const fragment = new URLSearchParams(location.hash.slice(1));
const supplied = fragment.get("token");
history.replaceState(null, "", location.pathname + location.search);
let token = supplied ?? sessionStorage.getItem("silex.token") ?? "";
if (supplied) sessionStorage.setItem("silex.token", supplied);
const bytes = (value: number) => new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 }).format(value / (value >= 1073741824 ? 1073741824 : value >= 1048576 ? 1048576 : value >= 1024 ? 1024 : 1)) + (value >= 1073741824 ? " GiB" : value >= 1048576 ? " MiB" : value >= 1024 ? " KiB" : " B");
const number = (value: number) => new Intl.NumberFormat().format(value);
function encode(text: string, encoding: Encoding): string {
  if (encoding === "base64") { atob(text); return text; }
  let data: Uint8Array;
  if (encoding === "hex") {
    if (!/^(?:[a-fA-F0-9]{2})*$/.test(text)) throw new Error("Hex input requires complete pairs of hexadecimal digits.");
    data = Uint8Array.from(text.match(/../g) ?? [], pair => parseInt(pair, 16));
  } else data = new TextEncoder().encode(text);
  let raw = "";
  for (let i = 0; i < data.length; i += 8192) raw += String.fromCharCode(...data.subarray(i, i + 8192));
  return btoa(raw);
}
function decode(text: string, encoding: Encoding): string {
  if (encoding === "base64") return text;
  const data = fromBase64(text);
  if (encoding === "hex") return Array.from(data, byte => byte.toString(16).padStart(2, "0")).join("");
  return decodeUtf8(data);
}
async function response(path: string, body?: unknown, signal?: AbortSignal): Promise<Response> {
  let result: Response;
  try {
    result = await fetch("/v1/" + path, { method: body === undefined ? "GET" : "POST", headers: { Authorization: "Bearer " + token, ...(body === undefined ? {} : { "Content-Type": "application/json" }) }, body: body === undefined ? undefined : JSON.stringify(body), signal, redirect: "error" });
  } catch (failure) {
    if (["put", "delete", "compact", "backups"].includes(path)) throw new Error("Connection lost. The mutation outcome is unknown; inspect database state before retrying.");
    throw failure;
  }
  if (!result.ok) {
    if (result.status === 401) { sessionStorage.removeItem("silex.token"); throw new Error("Authentication expired. Relaunch with silex ui --server NAME."); }
    if (result.status === 404 && path === "read") throw new Error("Key not found.");
    const error = await result.json() as { error: string; committedRecords?: number; commitOutcome?: string };
    throw new Error(error.error + (error.committedRecords == null ? "" : ` (${number(error.committedRecords)} confirmed committed records)`) + (error.commitOutcome ? ` Commit outcome: ${error.commitOutcome}` : ""));
  }
  return result;
}
async function json<T>(path: string, body?: unknown, signal?: AbortSignal): Promise<T> { return (await response(path, body, signal)).json() as Promise<T>; }
async function bounded(result: Response, limit: number): Promise<Uint8Array<ArrayBuffer>> {
  if (!result.body) throw new Error("Missing response body.");
  const reader = result.body.getReader(); const parts: Uint8Array[] = []; let length = 0;
  try {
    while (true) { const next = await reader.read(); if (next.done) break; length += next.value.length; if (length > limit) { await reader.cancel(); throw new Error("Browser response limit exceeded. Narrow the scan or use the CLI for large exports."); } parts.push(next.value); }
  } finally { reader.releaseLock(); }
  const output = new Uint8Array(length); let offset = 0; for (const part of parts) { output.set(part, offset); offset += part.length; } return output;
}
function download(data: Uint8Array<ArrayBuffer>, filename: string) {
  const url = URL.createObjectURL(new Blob([data])); const link = document.createElement("a"); link.href = url; link.download = filename; link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
}
function App() {
  const [page, setPage] = useState<Page>("Overview"), [info, setInfo] = useState<Info>(), [identity, setIdentity] = useState<Identity>(), [tables, setTables] = useState<Table[]>([]);
  const [error, setError] = useState(""), [notice, setNotice] = useState(""), [busy, setBusy] = useState(false), [live, setLive] = useState(true), [historyValues, setHistory] = useState<number[]>([]);
  const [encoding, setEncoding] = useState<Encoding>("utf8"), [key, setKey] = useState(""), [from, setFrom] = useState(""), [to, setTo] = useState(""), [limit, setLimit] = useState(100), [reverse, setReverse] = useState(false), [rows, setRows] = useState<Row[]>([]);
  const [entry, setEntry] = useState<Row>(), [editing, setEditing] = useState(false), [creating, setCreating] = useState(false), [selectionId, setSelectionId] = useState(0);
  const [format, setFormat] = useState("binary"), [exportPrefix, setExportPrefix] = useState(""), [liveExport, setLiveExport] = useState(false), [file, setFile] = useState<File>(), [backupName, setBackupName] = useState(""), [jobs, setJobs] = useState<Job[]>([]);
  const [theme, setTheme] = useState(document.documentElement.dataset.theme ?? "light");
  const mounted = useRef(true);
  const writes = identity?.allowWrites === true;
  const refresh = useCallback(async (signal?: AbortSignal) => {
    const id = await json<Identity>("identity", undefined, signal);
    const next = await json<Info>("info", undefined, signal);
    const current = await json<Table[]>("tables", undefined, signal);
    if (!mounted.current) return;
    setIdentity(id); setInfo(next); setTables(current); setHistory(previous => [...previous.slice(-29), next.statistics.activeMemTableBytes]);
  }, []);
  useEffect(() => {
    mounted.current = true; const controller = new AbortController(); let timer: ReturnType<typeof setTimeout>;
    const poll = async () => { try { await refresh(controller.signal); } catch (failure) { if (!controller.signal.aborted) setError(String(failure instanceof Error ? failure.message : failure)); } if (!controller.signal.aborted && live) timer = setTimeout(poll, 5000); };
    if (token) void poll();
    return () => { mounted.current = false; controller.abort(); clearTimeout(timer); };
  }, [refresh, live]);
  useEffect(() => {
    if (!jobs.some(job => job.state === "running")) return;
    const controller = new AbortController();
    const timer = setTimeout(async () => {
      try { const updated = await Promise.all(jobs.map(job => job.state === "running" ? json<Job>("operations/" + job.id, undefined, controller.signal) : job)); if (!controller.signal.aborted) setJobs(updated); }
      catch (failure) { if (!controller.signal.aborted) setError(failure instanceof Error ? failure.message : String(failure)); }
    }, 1000);
    return () => { clearTimeout(timer); controller.abort(); };
  }, [jobs]);
  async function run(work: () => Promise<void>, message = "") {
    if (busy) return false; setBusy(true); setError(""); setNotice("");
    try { await work(); if (message) setNotice(message); return true; }
    catch (failure) { setError(failure instanceof Error ? failure.message : String(failure)); return false; }
    finally { setBusy(false); }
  }
  async function copy(text: string | (() => string)) {
    try {
      if (!navigator.clipboard) throw new Error("Clipboard access is unavailable. Select the displayed text or download the raw bytes.");
      await navigator.clipboard.writeText(typeof text === "function" ? text() : text);
      return true;
    } catch (failure) { setError(failure instanceof Error ? failure.message : String(failure)); return false; }
  }
  function selection() {
    if (key && (from || to)) throw new Error("Use either Key for prefix search or From/To for a range, not both.");
    return { prefix: key ? encode(key, encoding) : null, from: from ? encode(from, encoding) : null, to: to ? encode(to, encoding) : null, reverse };
  }
  async function scan() {
    const result = await response("scan", { ...selection(), limit });
    const text = new TextDecoder().decode(await bounded(result, 16 * 1024 * 1024));
    setRows(text.split("\n").filter(Boolean).map(line => JSON.parse(line) as Row));
  }
  async function exportData() {
    const result = await response("export", { format, prefix: exportPrefix ? encode(exportPrefix, "utf8") : null, live: liveExport && stats?.mode === "lsm" });
    download(await bounded(result, 256 * 1024 * 1024), "silex-export." + (format === "binary" ? "silex" : "jsonl"));
  }
  async function importData(selected: File) {
    let result: Response;
    try {
      result = await fetch("/v1/import?format=auto", { method: "POST", headers: { Authorization: "Bearer " + token }, body: selected, redirect: "error" });
    } catch {
      throw new Error("Connection lost during import. Partial progress and the final mutation outcome are unknown; inspect database state before retrying.");
    }
    if (result.status === 401) {
      sessionStorage.removeItem("silex.token");
      throw new Error("Authentication expired. Relaunch with silex ui --server NAME.");
    }
    let report: { records?: number; batches?: number; error?: string; committedRecords?: number; commitOutcome?: string };
    try { report = await result.json(); }
    catch { throw new Error("Import response could not be read. Partial progress and the final mutation outcome are unknown; inspect database state before retrying."); }
    if (!result.ok) throw new Error(`${report.error ?? "Import failed"}; ${report.committedRecords ?? "unknown"} confirmed records. Commit outcome: ${report.commitOutcome ?? "unknown"}. No automatic retry.`);
    setNotice(`${number(report.records ?? 0)} records imported in ${number(report.batches ?? 0)} batches.`);
    await refresh();
  }
  function changeEncoding(next: Encoding) {
    try {
      const values = [key, from, to].map(text => decode(encode(text, encoding), next));
      setKey(values[0]); setFrom(values[1]); setTo(values[2]); setEncoding(next);
      return true;
    } catch (failure) { setError(failure instanceof Error ? failure.message : String(failure)); return false; }
  }
  async function readEntry() {
    const requestedKey = encode(key, encoding);
    const result = await response("read", { key: requestedKey });
    const data = await bounded(result, 16 * 1024 * 1024);
    selectEntry({ key: requestedKey, value: toBase64(data) }, false, true);
  }
  function selectEntry(next: Row, isNew = false, reload = false) {
    if (!isNew && !reload && entry?.key === next.key) return;
    if (editing && !confirm("Discard the current edit draft and select another entry?")) return;
    setEntry(next); setCreating(isNew); setSelectionId(previous => previous + 1);
  }
  function createEntry() {
    selectEntry({ key: encode(key, encoding), value: "" }, true);
  }
  async function saveEntry(value: string) {
    if (!entry) throw new Error("Select a key before saving.");
    const saved = { key: entry.key, value };
    await response("put", saved);
    setEntry(saved); setCreating(false); setRows(previous => previous.map(row => row.key === saved.key ? saved : row));
  }
  async function deleteEntry() {
    if (!entry) throw new Error("Select a key before deleting.");
    const deleted = entry.key;
    await response("delete", { key: deleted });
    setEditing(false); setRows(previous => previous.filter(row => row.key !== deleted));
    setEntry(undefined); setCreating(false);
  }
  function display(data: string) { try { return decode(data, encoding); } catch { return "[binary; switch to hex or base64]"; } }
  const stats = info?.statistics;
  const maxLevel = Math.max(1, ...(stats?.levels.map(level => level.bytes) ?? [1]));
  const maxHistory = Math.max(1, ...historyValues);
  if (!token) return <main className="login"><Database size={40}/><h1>Silex Studio</h1><p>Open a credentialed session from your terminal.</p><code>silex ui --server NAME</code><p>Access follows the database directory's filesystem permissions.</p></main>;
  return <div className="shell">
    <aside><a className="brand" href="/ui/"><span className="logo"><Database size={22}/></span><div>Silex <b>Studio</b><small>LOCAL DATABASE WORKSPACE</small></div></a>
      <div className="connection"><span className="dot"/><div><span className="copyable">{identity?.name ?? "Connecting"}{identity && <CopyButton label="Copy server name" text={identity.name} copy={copy} disabled={busy}/>}</span><small>{stats?.mode.toUpperCase() ?? "LOCAL"} · loopback only</small></div></div>
      <nav>{pages.map(([name, Icon]) => <button key={name} className={page === name ? "selected" : ""} onClick={() => { if (name !== page && editing && !confirm("Discard the current edit draft and leave this page?")) return; setPage(name); }}><Icon size={18}/>{name}<ChevronRight size={14}/></button>)}</nav>
      <div className="sidebar-foot"><ShieldCheck size={18}/><span>{writes ? "Writes enabled" : "Read-only access"}<small>Authenticated developer session</small></span></div>
    </aside>
    <main><header><div><span className="eyebrow">DATABASE / {page.toUpperCase()}</span><h1>{page}</h1><div className="copyable"><p className="path" title={stats?.database}>{stats?.database ?? "Connecting to local server..."}</p>{stats && <CopyButton label="Copy database path" text={stats.database} copy={copy} disabled={busy}/>}</div></div><div className="toolbar"><label className="check"><input type="checkbox" checked={live} onChange={event => setLive(event.target.checked)}/>Auto-refresh</label><button aria-label="Refresh" disabled={busy} onClick={() => void run(() => refresh())}><RefreshCw size={17}/></button><button aria-label="Toggle theme" onClick={() => { const next = theme === "dark" ? "light" : "dark"; document.documentElement.dataset.theme = next; setTheme(next); }}>{theme === "dark" ? <Sun size={17}/> : <Moon size={17}/>}</button></div></header>
      {error && <div role="alert" className="message error">{error}<button onClick={() => setError("")}>Dismiss</button></div>}{notice && <div role="status" className="message success">{notice}</div>}{busy && <div role="status" className="working">Operation in progress. Mutations are never automatically retried.</div>}
      {!writes && page !== "Overview" && page !== "Storage" && <div className="message">Writes are disabled. Start the host with <code>--allow-writes</code> to enable mutations.</div>}
      {page === "Overview" && <>
        <div className="metrics">{[["Database footprint", bytes(info?.diskBytes ?? 0), `${info?.sstFiles ?? 0} SST files · ${info?.walFiles ?? 0} WAL files`], ["Memtable", bytes(stats?.activeMemTableBytes ?? 0), `${stats?.immutableMemTableCount ?? 0} immutable tables waiting`], ["Cache hit rate", stats?.cacheHitRatio == null ? "—" : (stats.cacheHitRatio * 100).toFixed(1) + "%", `${number(stats?.cacheHits ?? 0)} hits / ${number(stats?.cacheMisses ?? 0)} misses`], ["Write amplification", stats?.writeAmplification == null ? "—" : stats.writeAmplification.toFixed(2) + "×", "Since this server opened"]].map(([label, metric, detail]) => <section className="metric" key={label}><span>{label}</span><strong>{metric}</strong><small>{detail}</small></section>)}</div>
        <div className="columns"><section className="card"><div className="card-title"><h2>Storage distribution</h2><span>Published SST bytes</span></div>{stats?.levels.map(level => <div className="level" key={level.level}><b>L{level.level}</b><div className="bar-track"><div className="bar" style={{ width: `${level.bytes / maxLevel * 100}%` }}/></div><span>{bytes(level.bytes)}</span><small>{level.tableCount} tables</small></div>)}<p className="hint">Physical sizes include shadowed versions and tombstones. Not logical row counts.</p></section>
        <section className="card"><div className="card-title"><h2>Memtable activity</h2><span>Last 30 observations</span></div><svg viewBox="0 0 600 150" className="sparkline" role="img" aria-label="Active memtable bytes over recent observations"><polyline fill="none" stroke="var(--cp-accent)" strokeWidth="3" points={historyValues.map((sample, i) => `${i * 600 / Math.max(1, historyValues.length - 1)},${140 - sample / maxHistory * 125}`).join(" ")}/></svg><div className="split"><span>Peak observed</span><b>{bytes(maxHistory === 1 ? 0 : maxHistory)}</b></div></section></div>
        <section className="card"><div className="card-title"><h2>Engine status</h2><span className="badge">{stats?.health ?? "Connecting"}</span></div><div className="facts">{[["Pressure", stats?.pressure], ["Write operations", number(stats?.writeOperations ?? 0)], ["WAL written", bytes(stats?.walBytesWritten ?? 0)], ["Compaction backlog", stats?.compactionBacklogBytes == null ? "Unavailable" : bytes(stats.compactionBacklogBytes)], ["Immutable bytes", bytes(stats?.immutableMemTableBytes ?? 0)], ["Published MVCC sequence", stats?.publishedSequence ?? "Not applicable"]].map(([name, fact]) => <div key={name}><small>{name}</small><b>{fact}</b></div>)}</div>{stats?.fault && <p className="error">{stats.fault}</p>}</section>
      </>}
      {page === "Data browser" && <>
        <form className="card data-filters" aria-label="Key filters" onSubmit={event => { event.preventDefault(); if (!busy && Number.isInteger(limit) && limit >= 1 && limit <= 1000) void run(scan); }}><div className="card-title"><h2>Filter keys</h2><span>Input: {encoding === "utf8" ? "UTF-8" : encoding === "hex" ? "hex" : "Base64"}</span></div>
          <div className="form-grid"><Field label="Key" value={key} change={setKey}/><Field label="From (inclusive)" value={from} change={setFrom}/><Field label="To (exclusive)" value={to} change={setTo}/><label>Limit<input type="number" min="1" max="1000" autoComplete="off" value={limit} onChange={event => setLimit(Number(event.target.value))}/></label></div>
          <div className="filter-actions"><label className="check"><input type="checkbox" checked={reverse} onChange={event => setReverse(event.target.checked)}/>Reverse order</label>
            <button type="submit" className="primary" disabled={busy || !Number.isInteger(limit) || limit < 1 || limit > 1000}><Search size={16}/>Scan</button>
            <div className="key-lookup">
              <button type="button" className="icon-button" title="Read exact key" aria-label="Read exact key" disabled={busy || !key} onClick={() => void run(readEntry)}><Search size={18}/></button>
              <button type="button" className="icon-button" title="Create entry" aria-label="Create entry" disabled={busy || !writes || !key} onClick={() => void run(async () => createEntry())}><Plus size={18}/></button>
            </div>
            <p className="hint">Scan finds keys starting with Key. Leave Key empty to scan all keys or use a range.</p>
          </div>
        </form>
        <div className="data-workspace">
          <section className="card key-list" aria-label="Keys">
            <div className="key-list-heading"><h2>Keys <span className="hint">{rows.length}</span></h2>
              <div className="icon-group" role="toolbar" aria-label="Key rendering">
                {([["utf8", "UTF-8", CaseSensitive], ["hex", "hex", Hash], ["base64", "Base64", Binary]] as const).map(([mode, label, Icon]) => <button key={mode} className="icon-button" title={`Render keys as ${label}`} aria-label={`Render keys as ${label}`} aria-pressed={encoding === mode} disabled={busy} onClick={() => changeEncoding(mode)}><Icon size={17}/></button>)}
              </div>
            </div>
            <div className="key-items">
              {rows.map(row => <div key={row.key} className={`key-row${entry?.key === row.key ? " selected" : ""}`}><button className={`key-item${entry?.key === row.key ? " selected" : ""}`} aria-label={`Select key ${display(row.key)}`} aria-pressed={entry?.key === row.key} disabled={busy} onClick={() => selectEntry(row)}>
                <span className="mono">{display(row.key).slice(0, 160)}</span><small>{base64Size(row.key).toLocaleString()} B key · {base64Size(row.value).toLocaleString()} B value</small>
              </button><CopyButton label={`Copy key ${display(row.key)}`} text={row.key} copy={value => copy(() => decode(value, encoding))} disabled={busy}/></div>)}
              {!rows.length && <div className="empty">Scan a prefix or range to list keys.</div>}
            </div>
          </section>
          <div className="entry-pane">
          {entry ? <EntryViewer key={selectionId} entry={entry} keyLabel={display(entry.key)} copyKey={() => copy(() => decode(entry.key, encoding))} creating={creating} canEdit={writes} busy={busy}
            editingChanged={setEditing} save={value => run(() => saveEntry(value), "Value saved.")} remove={() => void run(deleteEntry, "Key deleted.")}
            copy={copy} download={download}/> : <section className="card entry-placeholder"><Database size={32}/><h2>Select an entry</h2><p>Choose a key on the left to view or edit its bytes.</p></section>}
          </div>
        </div>
      </>}
      {page === "Storage" && <section className="card table-card"><div className="card-title"><h2>Published SST inventory</h2><span>{tables.length} active tables</span></div><p className="hint">Metadata copied under the engine structure lock. Physical key bounds include MVCC version encoding; keys below are hexadecimal. Old files awaiting reclamation are not listed.</p><table><thead><tr><th>File</th><th>Level / run</th><th>Stored size</th><th>Blocks</th><th>First key</th><th>Last key</th></tr></thead><tbody>{tables.map(table => <tr key={table.fileId}><td className="mono"><span className="copyable">{table.fileId}.sst<CopyButton label="Copy SST filename" text={`${table.fileId}.sst`} copy={copy} disabled={busy}/></span></td><td><span className="badge">L{table.level}</span></td><td>{bytes(table.bytes)}</td><td>{number(table.blocks)}</td><td className="mono" title={decode(table.firstKey, "hex")}><span className="copyable">{decode(table.firstKey, "hex").slice(0, 40)}<CopyButton label="Copy SST first key" text={decode(table.firstKey, "hex")} copy={copy} disabled={busy}/></span></td><td className="mono" title={decode(table.lastKey, "hex")}><span className="copyable">{decode(table.lastKey, "hex").slice(0, 40)}<CopyButton label="Copy SST last key" text={decode(table.lastKey, "hex")} copy={copy} disabled={busy}/></span></td></tr>)}</tbody></table>{!tables.length && <div className="empty">No SSTs are published yet. New writes may still be in memtables.</div>}</section>}
      {page === "Transfers" && <div className="columns">
        <section className="card"><h2>Export logical data</h2><p className="hint">Exports current key/value bytes, not version history or physical SSTs. Browser downloads are limited to 256 MiB; use the CLI for larger transfers.</p>
          <label>Format<select value={format} onChange={event => setFormat(event.target.value)}><option value="binary">Checksummed Silex binary</option><option value="jsonl">Base64 JSONL</option></select></label>
          <Field label="Optional prefix (UTF-8)" value={exportPrefix} change={setExportPrefix}/>
          <label className="check"><input type="checkbox" disabled={stats?.mode !== "lsm"} checked={liveExport} onChange={event => setLiveExport(event.target.checked)}/>Live export (LSM only; no snapshot)</label>
          <button className="primary" disabled={busy} onClick={() => void run(exportData, "Export downloaded.")}>Download export</button>
        </section>
        <section className="card"><h2>Import / upsert</h2><p className="hint">JSONL, binary, and gzip are detected automatically. Commits occur in batches, not atomically for the entire file. Duplicate keys use the last value.</p>
          <label>Data file<input type="file" onChange={event => setFile(event.target.files?.[0])}/></label>{file && <p>{file.name} · {bytes(file.size)}</p>}
          <button className="primary" disabled={busy || !writes || !file} onClick={() => { if (file && confirm(`Import ${file.name}? Existing keys may be overwritten. Partial progress can remain after failure.`)) void run(() => importData(file)); }}>Import file</button>
        </section>
      </div>}
      {page === "Backups" && <section className="card"><h2>Physical backup jobs</h2><p className="hint">Destinations must be new simple names under the host's configured backup root. Backups can run when writes are disabled. Restore requires offline access.</p><form onSubmit={(event: FormEvent) => { event.preventDefault(); void run(async () => { const job = await json<Job>("backups", { name: backupName }); setJobs(previous => [job, ...previous].slice(0, 64)); setBackupName(""); }); }}><Field label="Destination name" value={backupName} change={setBackupName}/><button className="primary" disabled={busy || !/^[a-zA-Z0-9_-]{1,64}$/.test(backupName)}>Create backup</button></form><table><thead><tr><th>Operation</th><th>Status</th><th>Error</th></tr></thead><tbody>{jobs.map(job => <tr key={job.id}><td className="mono"><span className="copyable">{job.id}<CopyButton label="Copy backup operation ID" text={job.id} copy={copy} disabled={busy}/></span></td><td><span className="badge">{job.state}</span></td><td>{job.error && <span className="copyable">{job.error}<CopyButton label="Copy backup error" text={job.error} copy={copy} disabled={busy}/></span>}</td></tr>)}</tbody></table><p className="hint">This session shows submitted jobs; completed jobs remain queryable while the server runs.</p></section>}
      {page === "Maintenance" && <section className="card"><h2>Flush and compact</h2><p>Flush pending writes and run the engine's configured compaction strategy.</p><p className="hint">This may take time and uses the host's mutation gate. The operation is not automatically retried if the connection is lost. Integrity verification and restore remain offline.</p><button className="danger" disabled={busy || !writes} onClick={() => { if (confirm("Flush and compact this database? Writes may wait until maintenance completes.")) void run(async () => { await response("compact", {}); await refresh(); }, "Flush and compaction completed."); }}>Flush &amp; compact</button></section>}
      <footer><ShieldCheck size={14}/> Local-only · Native AOT host · Counters since open · No automatic mutation retries</footer>
    </main>
  </div>;
}
function Field({ label, value, change }: { label: string; value: string; change: (value: string) => void }) { return <label>{label}<input autoComplete="off" spellCheck={false} value={value} onChange={event => change(event.target.value)}/></label>; }
createRoot(document.getElementById("root")!).render(<App/>);
