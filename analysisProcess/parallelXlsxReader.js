/**
 * parallelXlsxReader.js
 * ────────────────────────────────────────────────────────────────
 * Lector XLSX por streaming + helper de elección de estrategia.
 *
 * API:
 *   • readSheetAllColumnsStreaming({ filePath|buffer, sheetIndex|sheetName, onProgress?, reportEveryRows? })
 *       → { headers, rawColumnsData, colStats, totalRows, rowsWithData }
 *
 *   • ensureExcelData(buffer, { fileName?, report?, ioSlice? })
 *       → decide entre lectura convencional (xlsx) o streaming
 *         y devuelve { headers, rawColumnsData, colStats, totalRows, rowsWithData }
 *
 * Requiere:
 *   npm i xlsx-stream-reader xlsx
 */

'use strict';

/* ───────────────────── Imports ───────────────────── */
const fs = require('fs');
const fsp = require('fs/promises');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const XLSX = require('xlsx');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');

/* ───────────────────── Config & logging ───────────────────── */
const DEFAULT_BATCH_ROWS   = Math.max(100, Number(process.env.XLSX_BATCH_ROWS) || 1000);
const DEFAULT_REPORT_EVERY = Number(process.env.XLSX_REPORT_EVERY) || 5000;
const LOG_ENABLED          = process.env.XLSX_DEBUG_LOGS !== '0'; // habilitado por defecto
const PFX = '[PXR]';
const ts  = () => new Date().toISOString().replace('T',' ').replace('Z','');
const log = (...a) => { if (LOG_ENABLED) console.log(ts(), PFX, ...a); };
const warn = (...a) => console.warn(ts(), PFX, ...a);
const err  = (...a) => console.error(ts(), PFX, ...a);

/* ───────────────────── Helpers comunes ───────────────────── */
function indexToColLetter(idx1) { let n = idx1, out=''; while(n){ out = String.fromCharCode(65+((n-1)%26))+out; n = Math.floor((n-1)/26);} return out; }
function unifyDecimalSeparator(strVal) {
  if (typeof strVal !== 'string') return String(strVal ?? '');
  let s = strVal.trim(); if (!s) return s;
  const hasComma = s.includes(','), hasDot = s.includes('.');
  if (hasComma && !hasDot) { s = s.replace(/\./g,''); s = s.replace(',','.'); }
  else if (!hasComma && hasDot) { s = s.replace(/,/g,''); }
  return s.trim();
}
/** “vacío” SIN considerar 0/“0” como vacío */
function isEmptyish(v) {
  if (v == null) return true;
  if (typeof v === 'number') return Number.isNaN(v); // 0 no es vacío
  const s = typeof v === 'string' ? v.trim() : String(v).trim();
  return s === ''; // "0" no es vacío
}
function toText(v) {
  if (v == null) return '';
  if (typeof v === 'string') return v;
  if (typeof v === 'number') return Number.isFinite(v) ? String(v) : '';
  if (v instanceof Date) return v.toISOString();
  if (typeof v === 'boolean') return v ? 'true' : 'false';
  return String(v);
}
function humanMB(b) { return `${(b/(1024*1024)).toFixed(2)} MB`; }

/* Buffer ↔ archivo temp */
async function ensurePathOrTemp(filePath, buffer, wantSize = false) {
  if (filePath) {
    if (!wantSize) return { tmpPath: filePath, cleanup: async () => {}, sizeBytes: undefined, isTemp: false };
    const st = await fsp.stat(filePath);
    return { tmpPath: filePath, cleanup: async () => {}, sizeBytes: st.size, isTemp: false };
  }
  if (!buffer || !Buffer.isBuffer(buffer)) throw new Error('Must provide filePath or buffer');
  const tmpDir = os.tmpdir();
  const name = `xlsx_${crypto.randomUUID ? crypto.randomUUID() : Date.now().toString(36)}.xlsx`;
  const tmpPath = path.join(tmpDir, name);
  await fsp.writeFile(tmpPath, buffer);
  return {
    tmpPath,
    cleanup: async () => { try { await fsp.unlink(tmpPath); } catch {} },
    sizeBytes: wantSize ? buffer.length : undefined,
    isTemp: true
  };
}

/* ════════════════════════════════════════════════════════════════
 *                           WORKER
 * ════════════════════════════════════════════════════════════════ */
if (!isMainThread && workerData && workerData.__pxr === true) {
  (async () => {
    const {
      filePath,
      fileId,
      sheetIndex,
      sheetName,
      reportEveryRows = DEFAULT_REPORT_EVERY,
      batchRows      = DEFAULT_BATCH_ROWS,
    } = workerData;

    try {
      const XlsxStreamReader = require('xlsx-stream-reader');
      const reader = new XlsxStreamReader();

      let targetWsIdx = 0;
      let headers = null;      // string[] (con ::A, ::B…)
      let nCols   = 0;
      let sawHeaderFallback = false;

      let rowsRead = 0;
      let totalRows = 0;
      let rowsWithData = 0;

      let batch = [];          // array<array<string>>
      let batchIndex = 0;

      const debugCounters = { worksheetsSeen: 0, skippedHidden: 0 };

      function emit(type, payload) {
        parentPort.postMessage({ type, fileId, ...payload });
      }

      function buildHeadersFromRow(vals) {
        // vals es 1-based (vals[1] = col A)
        const out = [];
        for (let i1 = 1; i1 < vals.length; i1++) {
          const raw = vals[i1];
          const name = String(raw ?? '').trim() || `col_${i1}`;
          out.push(`${name}::${indexToColLetter(i1)}`);
        }
        return out;
      }

      function flushBatch(force = false) {
        if (!headers || nCols === 0) {
          // Si llegamos aquí con datos sin headers algo anda mal: log explícito.
          if (batch.length && LOG_ENABLED) {
            warn(`[worker:${fileId}] flushBatch() ignorado: no hay headers (rows acumuladas=${batch.length}).`);
          }
          batch = [];
          return;
        }
        if (!batch.length && !force) return;

        // Transponer filas→columnas
        const colsChunk = {};
        for (let c = 0; c < nCols; c++) {
          const hdr = headers[c];
          const out = new Array(batch.length);
          for (let r = 0; r < batch.length; r++) out[r] = batch[r][c];
          colsChunk[hdr] = out;
        }

        emit('batch', {
          batchIndex,
          rowsInBatch: batch.length,
          rowsRead,
          rowsWithData,
          totalRows,
          columnsChunk: colsChunk
        });

        batch = [];
        batchIndex++;
      }

      reader.on('worksheet', ws => {
        debugCounters.worksheetsSeen++;

        const wsName = ws.attributes?.name || ws.name;
        const wsIsTarget =
          (typeof sheetIndex === 'number' && targetWsIdx === sheetIndex) ||
          (typeof sheetName === 'string' && sheetName && wsName === sheetName) ||
          (sheetIndex == null && sheetName == null && targetWsIdx === 0);

        const isHidden = ws.attributes?.state === 'hidden';
        if (!wsIsTarget || isHidden) {
          if (isHidden) debugCounters.skippedHidden++;
          targetWsIdx++;
          ws.skip();
          return;
        }

        emit('meta', { phase: 'sheet', sheetIndex: targetWsIdx, sheetName: wsName });

        ws.on('row', (row) => {
          const vals = row.values || [];
          const rowIdx = row.r || row.attributes?.r; // a veces no viene
          const isHeaderByIdx = (rowIdx === 1);

          if (!headers) {
            if (isHeaderByIdx) {
              headers = buildHeadersFromRow(vals);
              nCols   = headers.length;
              emit('meta', { phase: 'headers', columns: nCols, batchRows });
              log(`[worker:${fileId}] headers detectados por r=1 → nCols=${nCols}`);
              return;
            }

            // Fallback: si no viene r=1 o headers vacíos, usar la **primera fila no vacía** como headers
            const nonEmptyInRow = vals.slice(1).some(v => !isEmptyish(v));
            if (nonEmptyInRow) {
              headers = buildHeadersFromRow(vals);
              nCols   = headers.length;
              sawHeaderFallback = true;
              emit('meta', { phase: 'headers', columns: nCols, batchRows, note: 'fallback-first-row-as-header' });
              warn(`[worker:${fileId}] Fallback de headers (no r=1). nCols=${nCols}`);
              return;
            }

            // Si no hay nada no podemos procesar todavía.
            return;
          }

          // Desde aquí son filas de datos
          totalRows++;
          const rowArray = new Array(nCols);
          let anyReal = false;

          for (let i1 = 1; i1 <= nCols; i1++) {
            let v = vals[i1];
            if (typeof v === 'string') {
              if ((v.indexOf(',') !== -1 || v.indexOf('.') !== -1) && /\d/.test(v) && !/[A-Za-z]/.test(v)) {
                v = unifyDecimalSeparator(v);
              }
            } else if (v == null) { v = ''; }
            else { v = String(v); }

            if (!anyReal && !isEmptyish(v)) anyReal = true;
            rowArray[i1 - 1] = v;
          }

          if (!anyReal) return;  // descarta fila totalmente vacía
          rowsWithData++;
          batch.push(rowArray);
          rowsRead++;

          if (reportEveryRows && rowsRead % reportEveryRows === 0) {
            emit('progress', { rowsRead, rowsWithData, totalRows });
          }

          if (batch.length >= batchRows) flushBatch(false);
        });

        ws.on('end', () => {
          // último flush (aunque esté vacío, forzamos solo si hubo headers)
          if (headers && !batch.length) {
            // Batch vacío inicializador: evita rawColumnsData vacío en el caller
            emit('batch', {
              batchIndex,
              rowsInBatch: 0,
              rowsRead,
              rowsWithData,
              totalRows,
              columnsChunk: Object.fromEntries(headers.map(h => [h, []]))
            });
            log(`[worker:${fileId}] end(): se envió batch vacío inicializador (headers presentes, sin filas).`);
            batchIndex++;
          } else {
            flushBatch(true);
          }

          emit('done', {
            totalRows,
            rowsWithData,
            batches: batchIndex,
            headerFallback: sawHeaderFallback,
            wsSeen: debugCounters.worksheetsSeen,
            wsHiddenSkipped: debugCounters.skippedHidden
          });
        });

        ws.process();
      });

      fs.createReadStream(filePath)
        .on('error', (e) => emit('error', { error: `readStream error: ${e?.message || e}` }))
        .pipe(reader);

      reader.on('error', (e) => emit('error', { error: `xlsx-stream-reader error: ${e?.message || e}` }));
    } catch (e) {
      parentPort.postMessage({ type: 'error', fileId: workerData?.fileId, error: e?.message || String(e) });
    }
  })();

  return; // worker end
}

/* ════════════════════════════════════════════════════════════════
 *                           MAIN API
 * ════════════════════════════════════════════════════════════════ */

/**
 * Lee TODAS las columnas en streaming y devuelve:
 * { headers, rawColumnsData, colStats, totalRows, rowsWithData }
 *
 * @param {Object} opts
 * @param {string} [opts.filePath]
 * @param {Buffer} [opts.buffer]
 * @param {number} [opts.sheetIndex=0]
 * @param {string} [opts.sheetName]
 * @param {number} [opts.reportEveryRows=5000]
 * @param {(p:{phase?:'sheet'|'headers'|'read'|'populate',rowsRead?:number,rowsWithData?:number,totalRows?:number,columns?:number})=>void} [opts.onProgress]
 */
async function readSheetAllColumnsStreaming(opts = {}) {
  const {
    filePath,
    buffer,
    sheetIndex = 0,
    sheetName,
    reportEveryRows = DEFAULT_REPORT_EVERY,
    onProgress
  } = opts;

  const { tmpPath, cleanup, sizeBytes, isTemp } = await ensurePathOrTemp(filePath, buffer, true);
  const fileId = path.basename(tmpPath);

  log(`start readSheetAllColumnsStreaming: size=${humanMB(sizeBytes||0)}${isTemp ? ' (temp copy)': ''} sheet=${sheetName ?? `#${sheetIndex}`}, reportEveryRows=${reportEveryRows}`);

  // Acumuladores finales
  let headers = null;
  let nCols = 0;
  let totalRows = 0;
  let rowsWithData = 0;

  const rawColumnsData = {}; // header → string[]
  const colStats = {};       // header → { nonEmptyCount, distinctCount }

  // Ensamblado ordenado
  let nextToFlush = 0;
  const pending = new Map(); // batchIndex → columnsChunk
  let batches = 0;
  let populatedRows = 0;

  function emitProgress(p) { try { onProgress && onProgress(p); } catch {} }

  function flushOrdered() {
    while (pending.has(nextToFlush)) {
      const chunk = pending.get(nextToFlush);
      const rowsInChunk = headers && chunk[headers[0]] ? chunk[headers[0]].length : 0;

      // Inicialización perezosa de estructuras
      if (!headers) {
        headers = Object.keys(chunk);
        nCols   = headers.length;
        headers.forEach(h => { rawColumnsData[h] = []; colStats[h] = { nonEmptyCount: 0, distinctCount: 0, _distinct: new Set() }; });
        log(`init from first batch: nCols=${nCols}`);
      }

      // Append + stats
      for (let c = 0; c < nCols; c++) {
        const hdr = headers[c];
        const arr = chunk[hdr] || [];
        const st  = colStats[hdr];

        for (let i = 0; i < arr.length; i++) {
          const v = arr[i];
          rawColumnsData[hdr].push(v);
          if (!isEmptyish(v)) {
            st.nonEmptyCount++;
            st._distinct.add(String(v));
          }
        }
      }

      populatedRows += rowsInChunk;
      emitProgress({ phase: 'populate', populatedRows, totalRows });
      pending.delete(nextToFlush);
      nextToFlush++;
    }
  }

  const w = new Worker(__filename, {
    workerData: {
      __pxr: true,
      filePath: tmpPath,
      fileId,
      sheetIndex,
      sheetName,
      reportEveryRows,
      batchRows: DEFAULT_BATCH_ROWS
    }
  });

  const done = new Promise((resolve, reject) => {
    w.on('message', (m) => {
      if (m.type === 'meta') {
        if (m.phase === 'sheet') {
          log(`sheet selected: #${m.sheetIndex} "${m.sheetName}"`);
        } else if (m.phase === 'headers') {
          log(`headers meta: columns=${m.columns}${m.note ? ` (${m.note})` : ''}`);
          emitProgress({ phase: 'headers', columns: m.columns });
        }
        return;
      }
      if (m.type === 'progress') {
        totalRows = Math.max(totalRows, m.totalRows || 0);
        rowsWithData = Math.max(rowsWithData, m.rowsWithData || 0);
        const denom = m.totalRows || 0;
        const pct = denom ? ((m.rowsRead / denom) * 100).toFixed(1) : '?';
        log(`read progress: rowsRead=${m.rowsRead}${denom ? `/${denom}`:''}, withData=${m.rowsWithData ?? 'n/a'} (${pct}%)`);
        emitProgress({ phase: 'read', rowsRead: m.rowsRead, rowsWithData: m.rowsWithData, totalRows: m.totalRows });
        return;
      }
      if (m.type === 'batch') {
        batches++;
        if (!headers && m.columnsChunk) {
          // Si aún no tenemos headers, inicializa desde el chunk (incluye caso de batch vacío)
          headers = Object.keys(m.columnsChunk);
          nCols   = headers.length;
          headers.forEach(h => { rawColumnsData[h] = []; colStats[h] = { nonEmptyCount: 0, distinctCount: 0, _distinct: new Set() }; });
          log(`first batch arrived (rowsInBatch=${m.rowsInBatch}) → init nCols=${nCols}`);
        }
        if (typeof m.totalRows === 'number') totalRows = Math.max(totalRows, m.totalRows);
        if (typeof m.rowsWithData === 'number') rowsWithData = Math.max(rowsWithData, m.rowsWithData);

        pending.set(m.batchIndex, m.columnsChunk);
        flushOrdered();
        return;
      }
      if (m.type === 'done') {
        totalRows = m.totalRows || totalRows;
        rowsWithData = m.rowsWithData || rowsWithData;
        log(`worker done: batches=${m.batches}, rowsWithData=${rowsWithData}/${totalRows}, headerFallback=${!!m.headerFallback}, wsSeen=${m.wsSeen}, wsHiddenSkipped=${m.wsHiddenSkipped}`);
        return;
      }
      if (m.type === 'error') {
        err(`worker error: ${m.error}`);
        reject(new Error(`[${m.fileId}] ${m.error}`));
        try { w.terminate(); } catch {}
        return;
      }
    });

    w.once('error', (e) => { err(`worker crashed: ${e?.message || e}`); reject(e); });
    w.once('exit', (code) => {
      if (code !== 0) { return reject(new Error(`Worker exit ${code}`)); }
      // flush final por si acaso
      flushOrdered();
      resolve();
    });
  });

  const t0 = Date.now();
  try {
    await done;
  } finally {
    await cleanup().catch(() => {});
  }

  // Cerrar stats: compute distinctCount y borrar set interno
  Object.values(colStats).forEach(st => { st.distinctCount = st._distinct.size; delete st._distinct; });

  // Logs diagnósticos para el caso “rawColumnsData vacío”
  const emptyRaw = !headers || headers.length === 0 || (headers.length > 0 && rawColumnsData[headers[0]]?.length === 0);
  if (emptyRaw) {
    warn(`DIAG: rawColumnsData vacío o sin filas. Detalles → headers? ${!!headers} (${headers?.length || 0}), batches=${batches}, totalRows=${totalRows}, rowsWithData=${rowsWithData}`);
    if (!headers) {
      warn('Posibles causas: (1) hoja objetivo no encontrada u oculta, (2) encabezados no detectados y tampoco hubo fallback, (3) archivo corrupto.');
    } else if (batches === 0) {
      warn('No llegó ningún batch desde el worker. Revisa errores previos de streaming/worksheet.');
    } else if ((headers.length > 0) && rawColumnsData[headers[0]].length === 0) {
      warn('Hubo headers pero 0 filas pobladas. Puede ser que todas las filas fueran vacías o se descartaron por isEmptyish (nota: "0" ya NO se considera vacío).');
    }
  }

  const dt = Date.now() - t0;
  log(`completed: cols=${headers?.length || 0}, rowsWithData=${rowsWithData}/${totalRows}, elapsed=${dt}ms`);

  return { headers: headers || [], rawColumnsData, colStats, totalRows, rowsWithData };
}

/* ════════════════════════════════════════════════════════════════
 *                       ensureExcelData
 *   Decide entre lectura convencional (xlsx) o streaming.
 *   Env:
 *     • XLSX_STREAM_MIN_MB  – umbral en MB para usar streaming (def 40)
 *     • XLSX_REPORT_EVERY   – filas por reporte para streaming
 * ════════════════════════════════════════════════════════════════ */
async function ensureExcelData(fileBuffer, {
  fileName = '',
  report = () => {},
  ioSlice = 0.02,
} = {}) {
  const sizeBytes = (fileBuffer && fileBuffer.length) || 0;
  const sizeMB    = sizeBytes / (1024 * 1024);
  const TH_MB     = Number(process.env.XLSX_STREAM_MIN_MB || 20);
  const everyRows = Number(process.env.XLSX_REPORT_EVERY || 5000);

  // Helper local para stats homogéneos
  const buildColStats = (headers, rawColumnsData) => {
    const out = {};
    for (const h of headers) {
      const arr = rawColumnsData[h] || [];
      let nonEmptyCount = 0;
      const distinct = new Set();
      for (const v of arr) {
        const s = (typeof v === 'string' ? v : toText(v)).trim();
        if (s !== '') { nonEmptyCount++; distinct.add(s); }
      }
      out[h] = { nonEmptyCount, distinctCount: distinct.size, total: arr.length };
    }
    return out;
  };

  // Normalizador de colStats si vienen del lector externo
  const normalizeColStats = (headers, rawColumnsData, colStatsMaybe) => {
    const out = {};
    const src = colStatsMaybe || {};
    for (const h of headers) {
      const cs = src[h] || {};
      if (typeof cs.nonEmptyCount === 'number') {
        out[h] = { nonEmptyCount: cs.nonEmptyCount, distinctCount: cs.distinctCount ?? 0, total: cs.total ?? (rawColumnsData[h]?.length || 0) };
      } else if (typeof cs.nonEmpty === 'number') {
        out[h] = { nonEmptyCount: cs.nonEmpty,       distinctCount: cs.distinctCount ?? 0, total: cs.total ?? (rawColumnsData[h]?.length || 0) };
      } else {
        // si no hay stats para esta columna, los construimos
        const arr = rawColumnsData[h] || [];
        let nonEmptyCount = 0;
        const distinct = new Set();
        for (const v of arr) {
          const s = (typeof v === 'string' ? v : toText(v)).trim();
          if (s !== '') { nonEmptyCount++; distinct.add(s); }
        }
        out[h] = { nonEmptyCount, distinctCount: distinct.size, total: arr.length };
      }
    }
    return out;
  };
  
  // 1) Lectura convencional (xlsx) — sin reportes de progreso, enfocada en performance
  const readConventional = () => {
    const wb = XLSX.read(fileBuffer, { type: 'buffer', cellDates: true, cellText: false, cellNF: false });
    if (!wb.SheetNames?.length) throw new Error('El archivo no contiene hojas');

    let ws = null;
    for (const sn of wb.SheetNames) {
      const s = wb.Sheets[sn];
      if (s && s['!ref']) { ws = s; break; }
    }
    if (!ws) throw new Error('No se encontró una hoja con rango de datos (!ref)');

    const range = XLSX.utils.decode_range(ws['!ref']);
    const headerRow = range.s.r;

    const seen = new Map();
    const uniq = (name) => {
      const base = (name && String(name).trim()) || 'col_x';
      const n = seen.get(base) || 0; seen.set(base, n + 1);
      return n === 0 ? base : `${base}#${n + 1}`;
    };

    const colsCount = range.e.c - range.s.c + 1;
    const headers = new Array(colsCount);
    for (let c = range.s.c, i = 0; c <= range.e.c; c++, i++) {
      const addr = XLSX.utils.encode_cell({ r: headerRow, c });
      const cell = ws[addr];
      const txt  = toText(cell ? (cell.w ?? cell.v) : '').trim();
      headers[i] = uniq(txt || `col_${i + 1}`);
    }

    const totalRows = Math.max(0, range.e.r - headerRow);
    const rawColumnsData = Object.fromEntries(headers.map(h => [h, []]));
    let rowsWithData = 0;

    const dataStart = headerRow + 1;
    for (let r = dataStart; r <= range.e.r; r++) {
      let rowHasData = false;
      for (let c = range.s.c, i = 0; c <= range.e.c; c++, i++) {
        const cell = ws[XLSX.utils.encode_cell({ r, c })];
        const txt  = toText(cell ? (cell.w ?? cell.v) : '').trim();
        if (!rowHasData && txt !== '') rowHasData = true;
        rawColumnsData[headers[i]].push(txt);
      }
      if (rowHasData) rowsWithData++;
    }

    const colStats = buildColStats(headers, rawColumnsData);
    return { headers, rawColumnsData, colStats, totalRows, rowsWithData };
  };

  // 2) Lectura streaming (si supera umbral)
  const readStreaming = async () => {
    report(0.001, `Preparando lectura (lector=streaming, size≈${sizeMB.toFixed(2)}MB, umbral=${TH_MB}MB, everyRows=${everyRows})`);
    const t0 = Date.now();

    let lastBucket = -1;
    const res = await readSheetAllColumnsStreaming({
      buffer: fileBuffer,
      reportEveryRows: everyRows,
      onProgress: ({ rowsRead, totalRows }) => {
        if (typeof totalRows === 'number' && totalRows > 0) {
          const progress = Math.min(1, rowsRead / totalRows);
          const bucket   = Math.floor(progress * 5);
          if (bucket >= 1 && bucket <= 4 && bucket !== lastBucket) {
            report(ioSlice * (bucket / 5), `Leyendo filas… ${rowsRead} / ${totalRows}`);
            lastBucket = bucket;
          }
        }
      }
    });

    report(ioSlice, `Lectura por streaming completada en ${Date.now() - t0} ms (${res.rowsWithData} filas, ${res.headers?.length || 0} columnas)`);

    const headers        = Array.isArray(res.headers) ? res.headers : Object.keys(res.rawColumnsData || {});
    const rawColumnsData = res.rawColumnsData || Object.fromEntries(headers.map(h => [h, []]));
    const colStats       = normalizeColStats(headers, rawColumnsData, res.colStats);
    const totalRows      = typeof res.totalRows === 'number' ? res.totalRows : (rawColumnsData[headers[0]]?.length || 0);
    const rowsWithData   = typeof res.rowsWithData === 'number' ? res.rowsWithData : totalRows;

    return { headers, rawColumnsData, colStats, totalRows, rowsWithData };
  };

  const useStreaming = sizeMB >= TH_MB;
  console.log(`[ANALYZE][${fileName}] decide lector → ${useStreaming ? 'streaming' : 'convencional'} (size=${sizeMB.toFixed(2)}MB, thr=${TH_MB}MB)`);

  return useStreaming ? await readStreaming() : readConventional();
}

/* ───────────────────── Exports ───────────────────── */
module.exports = {
  readSheetAllColumnsStreaming,
  ensureExcelData
};

// --- fingerprint del módulo ---
let __mtime = '';
try { __mtime = fs.statSync(__filename).mtime.toISOString(); } catch {}
module.exports.__signature     = 'pxr-rows-batch@2025-08-12';
module.exports.__resolved_path = __filename;
module.exports.__mtime         = __mtime;
console.log('[PXR_BOOT]', __filename, __mtime, module.exports.__signature);