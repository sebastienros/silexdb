import { useEffect, useMemo, useRef, useState } from "react";
import type { Node } from "jsonc-parser";
import { CopyButton } from "./CopyButton";
import { FileText, Hash, Braces, Binary, Pencil, Save, X, Trash2, Download } from "lucide-react";
import { draftBytes, draftText, formatJson, fromBase64, hexPageBytes, hexRows, inspectBytes, textPreviewLimit, toBase64, type Viewer } from "./entry-data";

type Entry = { key: string; value: string };

const formats: [Viewer, string, typeof FileText][] = [["text", "text", FileText], ["hex", "hex", Hash], ["json", "JSON", Braces], ["base64", "Base64", Binary]];

export function EntryViewer({ entry, keyLabel, copyKey, creating, canEdit, busy, save, remove, editingChanged, copy, download }: {
  entry: Entry;
  keyLabel: string;
  copyKey: () => Promise<boolean>;
  creating: boolean;
  canEdit: boolean;
  busy: boolean;
  save: (value: string) => Promise<boolean>;
  remove: () => void;
  editingChanged: (editing: boolean) => void;
  copy: (text: string) => Promise<boolean>;
  download: (data: Uint8Array<ArrayBuffer>, filename: string) => void;
}) {
  const data = useMemo(() => fromBase64(entry.value), [entry.value]);
  const analysis = useMemo(() => inspectBytes(data), [data]);
  const [viewer, setViewer] = useState<Viewer>(analysis.automatic);
  const [page, setPage] = useState(0);
  const [editing, setEditing] = useState(creating);
  const [draft, setDraft] = useState(() => creating ? draftText(data, analysis.automatic) : "");
  const [baseline, setBaseline] = useState(draft);
  const [source, setSource] = useState(data);
  const [validationError, setValidationError] = useState("");
  const editor = useRef<HTMLTextAreaElement>(null);
  useEffect(() => { editingChanged(editing); }, [editing, editingChanged]);
  useEffect(() => () => editingChanged(false), [editingChanged]);
  useEffect(() => { if (editing) editor.current?.focus(); }, [editing]);
  const active = viewer;
  const rows = useMemo(() => active === "hex" ? hexRows(data, page * hexPageBytes) : [], [active, data, page]);
  const formatted = useMemo(() => active === "json" && analysis.root ? formatJson(analysis.text!, analysis.root) : "", [active, analysis]);
  const error = active === "text" ? analysis.textError : active === "json" ? analysis.jsonError : undefined;
  const displayed = active === "text" ? analysis.text?.slice(0, textPreviewLimit) ?? "" :
    active === "base64" ? entry.value.slice(0, textPreviewLimit) :
    active === "hex" ? rows.map(row => `${row.offset}  ${row.hex.padEnd(48)}  |${row.ascii}|`).join("\n") : formatted;
  const truncated = active === "text" ? (analysis.text?.length ?? 0) > textPreviewLimit : active === "base64" && entry.value.length > textPreviewLimit;
  const pages = Math.max(1, Math.ceil(data.length / hexPageBytes));
  function currentBytes(validate = false) {
    if (draft === baseline) {
      if (validate && active === "json") draftBytes(draft, active);
      return source;
    }
    return draftBytes(draft, active, validate);
  }
  function changeFormat(next: Viewer) {
    try {
      if (editing) {
        const bytes = currentBytes();
        const text = draftText(bytes, next);
        setSource(bytes); setDraft(text); setBaseline(text);
      }
      setViewer(next); setValidationError("");
    } catch (failure) { setValidationError(failure instanceof Error ? failure.message : String(failure)); }
  }
  function beginEdit() {
    const text = draftText(data, active);
    setSource(data); setDraft(text); setBaseline(text); setValidationError(""); setEditing(true);
  }
  async function saveDraft() {
    try { if (await save(toBase64(currentBytes(true)))) { setEditing(false); setValidationError(""); } }
    catch (failure) { setValidationError(failure instanceof Error ? failure.message : String(failure)); }
  }
  return <section className="card entry-inspector" aria-label="Entry inspector">
    <div className="entry-heading"><div><h2>{creating ? "New entry" : editing ? "Edit entry" : "Entry"}</h2><div className="copyable"><div className="entry-key mono" title={keyLabel}>{keyLabel}</div><CopyButton label="Copy entry key" text={keyLabel} copy={copyKey} disabled={busy}/></div></div><span className="badge">{data.length.toLocaleString()} bytes</span></div>
    <div className="viewer-controls" role="toolbar" aria-label="Entry format and actions">
      <div className="icon-group" role="group" aria-label="Value format">
        {formats.map(([mode, label, Icon]) => <button key={mode} className="icon-button" title={`View as ${label}`} aria-label={`View as ${label}`} aria-pressed={active === mode} disabled={busy} onClick={() => changeFormat(mode)}><Icon size={18}/></button>)}
      </div>
      <span className="format-name">{active === "json" ? "JSON" : active === "base64" ? "Base64" : active === "hex" ? "Hex" : "UTF-8 text"}</span>
      <div className="icon-group entry-actions">
        <CopyButton className="icon-button" size={18} label="Copy displayed content" text={editing ? draft : displayed} copy={copy} disabled={busy || (!editing && !!error)}/>
        <button className="icon-button" title="Download raw bytes" aria-label="Download raw bytes" onClick={() => download(data, "silex-entry.bin")}><Download size={18}/></button>
        {editing ? <>
          <button className="icon-button primary" title="Save value" aria-label="Save value" disabled={busy || !canEdit} onClick={() => void saveDraft()}><Save size={18}/></button>
          <button className="icon-button" title="Cancel edit" aria-label="Cancel edit" disabled={busy} onClick={() => { if ((draft !== baseline || source !== data) && !confirm("Discard unsaved changes?")) return; setEditing(false); setValidationError(""); }}><X size={18}/></button>
        </> : <button className="icon-button" title="Edit entry" aria-label="Edit entry" disabled={busy || !canEdit || !!error} onClick={beginEdit}><Pencil size={18}/></button>}
        <button className="icon-button danger" title="Delete key" aria-label="Delete key" disabled={busy || !canEdit || creating} onClick={() => { if (confirm("Delete this key? Unsaved changes will be discarded. This cannot be undone.")) remove(); }}><Trash2 size={18}/></button>
      </div>
    </div>
    {validationError && <p role="alert" className="message error">{validationError}</p>}
    {editing ? <><label className="editor-label">Value ({active === "json" ? "JSON" : active === "text" ? "UTF-8" : active})<textarea ref={editor} aria-label="Value editor" autoComplete="off" spellCheck={false} value={draft} disabled={busy} onChange={event => setDraft(event.target.value)} rows={20}/></label>
      <p className="hint">Saving is explicit. Hex accepts byte pairs and whitespace, not offsets or ASCII. JSON must be valid. Raw downloads always use stored bytes.</p></> :
    error ? <p role="alert" className="message error">{error}</p> :
      active === "hex" ? <>
        <div className="hex-scroll"><table className="hex-view"><thead><tr><th>Offset</th><th>Hex bytes</th><th>ASCII</th></tr></thead><tbody>
          {rows.map(row => <tr key={row.offset}><td>{row.offset}</td><td>{row.hex}</td><td>{row.ascii}</td></tr>)}
        </tbody></table>{!data.length && <p className="hint">Empty value (0 bytes).</p>}</div>
        <div className="toolbar"><button disabled={page === 0} onClick={() => setPage(previous => previous - 1)}>Previous bytes</button>
          <label className="inline-label">Hex page<input type="number" min="1" max={pages} value={page + 1} onChange={event => { const next = Number(event.target.value); if (Number.isInteger(next) && next >= 1 && next <= pages) setPage(next - 1); }}/></label>
          <span>of {pages.toLocaleString()} · 256 bytes per page</span><button disabled={page + 1 >= pages} onClick={() => setPage(previous => previous + 1)}>Next bytes</button>
        </div>
      </> :
      active === "json" && analysis.root ? <div className="json-view"><JsonNode node={analysis.root} source={analysis.text!} copy={copy} busy={busy} root/></div> :
      <pre className="entry-content" aria-label={active === "text" ? "Text content" : "Base64 content"}>{displayed}</pre>}
    {!editing && truncated && <p className="hint">Showing the first 65,536 characters. Copy copies this preview only; download the raw bytes for the complete value.</p>}
    {!editing && active === "hex" && <p className="hint">Copy copies the displayed page, including offsets and ASCII. Non-printable bytes appear as dots.</p>}
  </section>;
}

function JsonNode({ node, source, label, copy, busy, root = false }: { node: Node; source: string; label?: string; copy: (text: string) => Promise<boolean>; busy: boolean; root?: boolean }) {
  const [expanded, setExpanded] = useState(root);
  const [visible, setVisible] = useState(50);
  if (node.type !== "object" && node.type !== "array") return <div className="json-leaf">{label && <span className="json-label">{label}: </span>}<code>{source.slice(node.offset, node.offset + node.length)}</code><CopyButton label={`Copy JSON value${label ? ` ${label}` : ""}`} text={node.type === "string" && typeof node.value === "string" ? node.value : source.slice(node.offset, node.offset + node.length)} copy={copy} disabled={busy}/></div>;
  const children = node.children ?? [];
  return <details open={expanded} onToggle={event => setExpanded(event.currentTarget.open)} className="json-branch">
    <summary>{label && <span className="json-label">{label}: </span>}{node.type === "object" ? "{ }" : "[ ]"} <span className="hint">{children.length} {node.type === "object" ? "properties" : "items"}</span></summary>
    {expanded && <div className="json-children">
      {children.slice(0, visible).map((child, i) => {
        const property = child.type === "property" ? child.children! : undefined;
        return <JsonNode key={child.offset} node={property ? property[1] : child} source={source} copy={copy} busy={busy} label={property ? source.slice(property[0].offset, property[0].offset + property[0].length) : `[${i}]`}/>;
      })}
      {children.length > visible && <button onClick={() => setVisible(previous => previous + 50)}>Show next {Math.min(50, children.length - visible)} items ({children.length - visible} remaining)</button>}
    </div>}
  </details>;
}
