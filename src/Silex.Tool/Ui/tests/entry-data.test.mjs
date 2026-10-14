import { test } from "node:test";
import assert from "node:assert/strict";
import { base64Size, decodeUtf8, draftBytes, draftText, formatJson, fromBase64, hexRows, inspectBytes, jsonPreviewLimit, toBase64 } from "../src/entry-data.ts";

const utf8 = text => new TextEncoder().encode(text);

test("key/value sizes are exact without decoding Base64", () => {
  for (const size of [0, 1, 2, 3, 4, 8192]) assert.equal(base64Size(toBase64(new Uint8Array(size))), size);
});

test("inline drafts preserve bytes across text, hex, and Base64", () => {
  const data = utf8("\ufeff café\n\t世界 ");
  for (const mode of ["text", "hex", "base64"]) assert.deepEqual(draftBytes(draftText(data, mode), mode), data);
  const binary = Uint8Array.from({ length: 8193 }, (_, i) => i % 256);
  for (const mode of ["hex", "base64"]) assert.deepEqual(draftBytes(draftText(binary, mode), mode), binary);
});

test("JSON editing is lossless for numbers and rejects invalid JSON before saving", () => {
  const data = utf8('{"n":9007199254740993,"n":1e400}');
  const formatted = draftText(data, "json");
  assert.match(decodeUtf8(draftBytes(formatted, "json")), /9007199254740993/);
  assert.match(decodeUtf8(draftBytes(formatted, "json")), /1e400/);
  assert.throws(() => draftBytes("{bad}", "json"), /Invalid JSON/);
  assert.deepEqual(draftBytes("{bad}", "json", false), utf8("{bad}"));
  assert.throws(() => draftBytes("0 a0", "hex"), /complete pairs/);
  assert.throws(() => draftBytes("zz", "hex"), /complete pairs/);
  assert.deepEqual(draftBytes("00 ff\n80", "hex"), new Uint8Array([0,255,128]));
});

test("binary roundtrips exactly, including empty and chunked values", () => {
  for (const size of [0, 1, 8191, 8192, 8193, 65537]) {
    const data = Uint8Array.from({ length: size }, (_, i) => i % 256);
    assert.deepEqual(fromBase64(toBase64(data)), data);
  }
});

test("automatic selection handles JSON scalars, text, empty values, and invalid UTF-8", () => {
  for (const value of ['{"x":[true,null]}', "false", "0", '"hello"', "[]", "{}"])
    assert.equal(inspectBytes(utf8(value)).automatic, "json");
  for (const value of ["", "hello\nworld", "{invalid}"])
    assert.equal(inspectBytes(utf8(value)).automatic, "text");
  const invalid = inspectBytes(new Uint8Array([0xff, 0x80, 0x00]));
  assert.equal(invalid.automatic, "hex");
  assert.match(invalid.textError, /not valid UTF-8/);
  assert.match(invalid.jsonError, /valid UTF-8/);
});

test("JSON formatting preserves number lexemes, duplicate keys, escapes, and property order", () => {
  const source = '{"n":9007199254740993,"n":1e400,"s":"\\u0061","a":[-0,1.2300]}';
  const analysis = inspectBytes(utf8(source));
  assert.equal(analysis.automatic, "json");
  assert.equal(formatJson(source, analysis.root), '{\n  "n": 9007199254740993,\n  "n": 1e400,\n  "s": "\\u0061",\n  "a": [\n    -0,\n    1.2300\n  ]\n}');
});

test("JSON is strict and never accepts comments, trailing commas, or extra content", () => {
  for (const source of ['{"a":1,}', "[1,]", "/*x*/{}", "{}{}", "\ufeff{}", '{"x":NaN}']) {
    const analysis = inspectBytes(utf8(source));
    assert.equal(analysis.automatic, "text");
    assert.match(analysis.jsonError, /Invalid JSON/);
  }
});

test("UTF-8 text retains whitespace, BOM, null bytes, and non-ASCII characters", () => {
  const text = "\ufeff  café\n\t世界\u0000 ";
  assert.equal(inspectBytes(utf8(text)).text, text);
});

test("UTF-8 edit decoding preserves original bytes including a leading BOM", () => {
  const bytes = utf8("\ufeff{\"x\":1}");
  assert.deepEqual(utf8(decodeUtf8(fromBase64(toBase64(bytes)))), bytes);
  assert.throws(() => decodeUtf8(new Uint8Array([0xff])), TypeError);
});

test("preview limits are explicit and do not mutate the bytes", () => {
  for (const text of ['"'.padEnd(jsonPreviewLimit + 1, "x") + '"', "[".repeat(65) + "0" + "]".repeat(65)]) {
    const data = utf8(text), before = toBase64(data);
    const analysis = inspectBytes(data);
    assert.equal(analysis.automatic, "text");
    assert.match(analysis.jsonError, /preview|previews/);
    assert.equal(toBase64(data), before);
  }
  assert.equal(inspectBytes(utf8('"'.padEnd(jsonPreviewLimit - 1, "x") + '"')).automatic, "json");
  assert.equal(inspectBytes(utf8("[".repeat(64) + "0" + "]".repeat(64))).automatic, "json");
});

test("hex pages have exact offsets, grouped bytes, ASCII, and partial final rows", () => {
  const data = Uint8Array.from({ length: 273 }, (_, i) => i % 256);
  const first = hexRows(data, 0), second = hexRows(data, 256);
  assert.equal(first.length, 16);
  assert.equal(first[0].offset, "00000000");
  assert.equal(first[0].hex, "00 01 02 03 04 05 06 07  08 09 0a 0b 0c 0d 0e 0f");
  assert.equal(first[0].ascii, ".".repeat(16));
  assert.equal(first[2].ascii, ' !"#$%&\'()*+,-./');
  assert.equal(second.length, 2);
  assert.equal(second[0].offset, "00000100");
  assert.equal(second[1].hex, "10");
  assert.deepEqual(hexRows(new Uint8Array(), 0), []);
});
