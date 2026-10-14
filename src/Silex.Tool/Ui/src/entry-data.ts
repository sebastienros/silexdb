import { parseTree, printParseErrorCode, type Node, type ParseError } from "jsonc-parser";

export type Viewer = "text" | "hex" | "json" | "base64";
export const jsonPreviewLimit = 256 * 1024;
export const textPreviewLimit = 64 * 1024;
export const hexPageBytes = 256;

export function base64Size(value: string): number {
  return value.length * 3 / 4 - (value.endsWith("==") ? 2 : value.endsWith("=") ? 1 : 0);
}

export function draftText(data: Uint8Array, viewer: Viewer): string {
  if (viewer === "base64") return toBase64(data);
  if (viewer === "hex") {
    const chunks: string[] = [];
    for (let start = 0; start < data.length; start += 8192) {
      let chunk = "";
      for (let i = start; i < Math.min(start + 8192, data.length); i++)
        chunk += (i ? i % 16 === 0 ? "\n" : " " : "") + data[i].toString(16).padStart(2, "0");
      chunks.push(chunk);
    }
    return chunks.join("");
  }
  const text = decodeUtf8(data);
  if (viewer === "text") return text;
  const analysis = inspectBytes(data);
  return analysis.root ? formatJson(text, analysis.root) : text;
}

export function draftBytes(text: string, viewer: Viewer, validateJson = true): Uint8Array<ArrayBuffer> {
  if (viewer === "base64") return fromBase64(text);
  if (viewer === "hex") {
    const hex = text.replace(/\s+/g, "");
    if (!/^(?:[a-fA-F0-9]{2})*$/.test(hex)) throw new Error("Hex input requires complete pairs of hexadecimal digits; do not include offsets or ASCII.");
    const data = new Uint8Array(hex.length / 2);
    for (let i = 0; i < data.length; i++) data[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
    return data;
  }
  const data = new TextEncoder().encode(text);
  if (viewer === "json" && validateJson) {
    const analysis = inspectBytes(data);
    if (!analysis.root) throw new Error(analysis.jsonError);
  }
  return data;
}

export function fromBase64(value: string): Uint8Array<ArrayBuffer> {
  const raw = atob(value);
  return Uint8Array.from(raw, char => char.charCodeAt(0));
}

export function toBase64(data: Uint8Array): string {
  let raw = "";
  for (let i = 0; i < data.length; i += 8192) raw += String.fromCharCode(...data.subarray(i, i + 8192));
  return btoa(raw);
}

export function decodeUtf8(data: Uint8Array): string {
  return new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(data);
}

export function inspectBytes(data: Uint8Array) {
  let text: string;
  try { text = decodeUtf8(data); }
  catch {
    return { automatic: "hex" as Viewer, textError: "These bytes are not valid UTF-8. Use Hex or Base64 to inspect them.", jsonError: "JSON requires valid UTF-8 bytes." };
  }

  let jsonError = "";
  let root: Node | undefined;
  if (data.length > jsonPreviewLimit) {
    jsonError = "JSON previews are limited to 256 KiB. Use Text or download the raw bytes for larger values.";
  } else {
    let depth = 0;
    for (const match of text.matchAll(/"(?:\\[\s\S]|[^"\\])*"|[{}[\]]/g)) {
      if (match[0][0] === '"') continue;
      depth += match[0] === "{" || match[0] === "[" ? 1 : -1;
      if (depth > 64) { jsonError = "JSON nesting exceeds the 64-level preview limit. Use Text or download the raw bytes."; break; }
    }
    if (!jsonError) {
      const errors: ParseError[] = [];
      root = parseTree(text, errors, { disallowComments: true, allowTrailingComma: false, allowEmptyContent: false });
      if (errors.length || !root) {
        const first = errors[0];
        jsonError = first ? `Invalid JSON at character ${first.offset + 1}: ${printParseErrorCode(first.error)}.` : "This value is not JSON.";
        root = undefined;
      }
    }
  }
  return { automatic: (root ? "json" : "text") as Viewer, text, root, jsonError };
}

export function formatJson(source: string, node: Node, depth = 0): string {
  if (node.type === "property") {
    const children = node.children!;
    return `${source.slice(children[0].offset, children[0].offset + children[0].length)}: ${formatJson(source, children[1], depth)}`;
  }
  if (node.type !== "object" && node.type !== "array") return source.slice(node.offset, node.offset + node.length);
  const [open, close] = node.type === "object" ? ["{", "}"] : ["[", "]"];
  const children = node.children ?? [];
  if (!children.length) return open + close;
  const indent = "  ".repeat(depth + 1);
  return open + "\n" + children.map(child => indent + formatJson(source, child, depth + 1)).join(",\n") + "\n" + "  ".repeat(depth) + close;
}

export function hexRows(data: Uint8Array, start: number) {
  const rows: { offset: string; hex: string; ascii: string }[] = [];
  const end = Math.min(data.length, start + hexPageBytes);
  for (let offset = start; offset < end; offset += 16) {
    const chunk = data.subarray(offset, Math.min(offset + 16, end));
    let hex = "", ascii = "";
    for (let i = 0; i < chunk.length; i++) {
      if (i) hex += i === 8 ? "  " : " ";
      hex += chunk[i].toString(16).padStart(2, "0");
      ascii += chunk[i] >= 32 && chunk[i] <= 126 ? String.fromCharCode(chunk[i]) : ".";
    }
    rows.push({ offset: offset.toString(16).padStart(8, "0"), hex, ascii });
  }
  return rows;
}
