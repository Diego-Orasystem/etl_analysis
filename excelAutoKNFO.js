/* src/xlsxETL/services/excelAutoKNFO.js */
/* eslint-disable no-console */
'use strict';

const path = require('path').posix;
const fs = require('fs').promises;
const http = require('http');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');

/* ───────── DEPENDENCIAS PROPIAS ───────── */
const { sftpSingleton, sftpConfig, CAN_BASE_DIR, CAS_BASE_DIR } = require('./sftpPool');
const { analyzeExcelFile } = require('./analysisProcess/analyzeExcelFile');
const { buildKnfo } = require('./analysisProcess/buildKnfo');
const { buildMeta } = require('./analysisProcess/buildMeta');
const { safeSftpUpload, metaExistsForOriginal, nonEmpty } = require('./analysisHelpers');

/* ssh2-sftp-client opcional cuando SFTP_PER_WORKER=1 */
let SftpClient = null;
try { SftpClient = require('ssh2-sftp-client'); } catch { /* opcional */ }

/* ───────── CONFIG ───────── */
const INTERVAL_MS     = Number(process.env.INTERVAL_MS || 5_000);
const PARALLEL_LIMIT  = Number(process.env.PARALLEL_LIMIT || 5);
// MODO PRUEBA: Solo procesar CAN. Para volver a procesar ambos, cambiar a: 'CAN,CAS'
const CTXS            = (process.env.CTX_LIST || 'CAN').split(',').map(s => s.trim()).filter(Boolean);
// const CTXS            = (process.env.CTX_LIST || 'CAN,CAS').split(',').map(s => s.trim()).filter(Boolean); // ← Descomentar para producción
const SFTP_TIMEOUT_MS = Number(process.env.SFTP_TIMEOUT_MS || 30_000);

/* Umbral consistente con parallelXlsxReader.ensureExcelData */
const TH_MB           = Number(process.env.XLSX_STREAM_MIN_MB || 20);
const MAX_FAILURES    = Number(process.env.MAX_FAILURES || 3); // Número máximo de intentos fallidos antes de saltar un archivo

/* Filtros */
const PATTERNS = [
  'MB51','MB5B','ME5A','S_P99_41000062','ME2L','ZMMR_SQVI_BUS_RAPIDA','ZMMREPO','MRO_IO',
  'KOB1','CJI3','KSB1','ZFIR_STATSLOAD','CN41N','ZRPT_PS_PROJECT','IW49N',
  'LEK2DAT_FORECAST','LEK2DAT_STRUCTURE_EE','LEK2DAT_STRUCTURE_CC','LEK2DAT_STRUCTURE_CCEE','LEK2DAT_STRUCTURE_ACC',
  'PRESU','PATRI'
];

/* ───────── UTILS ───────── */
const ts = () => new Date().toISOString();
const keyOf = (ctx, file) => `${ctx}:${file}`;
const pref  = (f) => f.split('$')[0];

/* HOISTED: usadas por worker y main */
function baseDirOf(ctx) { return ctx === 'CAN' ? CAN_BASE_DIR : CAS_BASE_DIR; }
function ctxPaths(ctx) {
  const baseDir = baseDirOf(ctx);
  return {
    excelDir: path.join(baseDir, 'sap-files', 'excel'),
    metaBase: path.join(baseDir, 'sap-files', 'meta'),
    metaRoot: path.join(baseDir, 'sap-files', 'meta', 'meta'),
    knfoDir : path.join(baseDir, 'sap-files', 'meta', 'knfo'),
  };
}
function withTimeout(promise, ms, label) {
  return Promise.race([
    promise,
    new Promise((_, rej) => setTimeout(() => rej(new Error(`TIMEOUT ${label} (${ms}ms)`)), ms))
  ]);
}
function sftpConfFor(ctx) {
  if (!sftpConfig) return null;
  return sftpConfig[ctx] || sftpConfig.default || sftpConfig;
}
async function getSftpShared() {
  return withTimeout(sftpSingleton.get(), SFTP_TIMEOUT_MS, 'sftpSingleton.get');
}
async function getSftpForWorker(ctx) {
  const perWorker = process.env.SFTP_PER_WORKER === '1';
  if (!perWorker || !SftpClient) return getSftpShared();
  const conf = sftpConfFor(ctx) || {};
  const client = new SftpClient();
  await withTimeout(client.connect(conf), SFTP_TIMEOUT_MS, `sftp.connect(${ctx})`);
  return client;
}
async function endSftpIfLocal(client) {
  if (process.env.SFTP_PER_WORKER === '1' && client && typeof client.end === 'function') {
    try { await client.end(); } catch {}
  }
}

/* ──────────────────────────────────────────────────────────
   WORKER BRANCH (usa funciones hoisted, sin TDZ)
   ────────────────────────────────────────────────────────── */
if (!isMainThread) {
  const send = (m) => { try { parentPort.postMessage(m); } catch {} };
  ['log','info','warn','error'].forEach(level => {
    const orig = console[level].bind(console);
    console[level] = (...a) => {
      const line = a.map(x => (typeof x === 'string' ? x : (()=>{ try{return JSON.stringify(x);}catch{return String(x);} })())).join(' ');
      send({ __type: 'log', level, line });
      try { orig(...a); } catch {}
    };
  });

  (async () => {
    try {
      const { ctx, file } = workerData || {};
      await createKnfoAndMeta({ ctx, file, wlog: console.log });
      send({ ok: true });
    } catch (e) {
      send({ ok: false, err: e?.message || String(e), stack: e?.stack || '' });
    }
  })();

  // no ejecutar rama "main"
  return;
}

/* ───────── STATE (main) ───────── */
const queues      = new Map();  // ctx → [{ name, sizeMB }]
const inFlight    = new Set();  // jobKey
const processedOK = new Set();  // jobKey
const flags       = new Map();  // jobKey → { large: boolean }
const failedFiles = new Map();  // jobKey → failureCount (número de intentos fallidos)

/* Ciclo de escaneo: BOOT → RESCAN → IDLE */
let phase = 'IDLE';
let ticking = false;
let timer = null;

/* ───────── File Monitor JSON helpers ───────── */
async function readFileMonitorJson(jsonPath) {
  // Los archivos JSON del file-monitor están en el servidor SFTP 10.4.0.2
  // Intentar leer desde SFTP primero
  try {
    const sftp = await getSftpShared();
    // La ruta ya es correcta: /home/fits/file-monitor/data/files_*.json
    const sftpPath = jsonPath;
    
    try {
      const content = await withTimeout(sftp.get(sftpPath), SFTP_TIMEOUT_MS, `sftp.get(${sftpPath})`);
      const parsed = JSON.parse(content.toString('utf8'));
      return parsed;
    } catch (sftpError) {
      // Si falla SFTP, intentar lectura local como fallback (por si acaso)
      try {
        const content = await fs.readFile(jsonPath, 'utf8');
        return JSON.parse(content);
      } catch (localError) {
        if (localError.code === 'ENOENT' || sftpError.message?.includes('No such file')) {
          // No mostrar warning si el archivo simplemente no existe (comportamiento normal)
          return {};
        }
        // Solo mostrar error si es un problema real de conexión
        if (!sftpError.message?.includes('No such file')) {
          console.warn(`[${ts()}] Error leyendo JSON desde SFTP ${sftpPath}: ${sftpError.message}`);
        }
        return {};
      }
    }
  } catch (e) {
    // Fallback a lectura local si SFTP no está disponible
    try {
      const content = await fs.readFile(jsonPath, 'utf8');
      return JSON.parse(content);
    } catch (localError) {
      // No mostrar warning si el archivo no existe (comportamiento normal)
      if (localError.code !== 'ENOENT') {
        console.error(`[${ts()}] Error leyendo ${jsonPath}: ${localError.message}`);
      }
      return {};
    }
  }
}

function filterByContext(filesJson, ctx, expectedBaseDir) {
  const filtered = {};
  const ctxLower = ctx.toLowerCase();
  const baseDirLower = expectedBaseDir.toLowerCase();
  
  for (const [rutaCompleta, info] of Object.entries(filesJson)) {
    if (!info || typeof info !== 'object') continue;
    
    const rutaLower = rutaCompleta.toLowerCase();
    const rutaRelativa = (info.ruta_relativa || '').toLowerCase();
    
    // Filtrar por contexto
    if (ctxLower === 'can' && !rutaLower.includes('/can/') && !rutaRelativa.includes('/can/')) continue;
    if (ctxLower === 'cas' && !rutaLower.includes('/cas/') && !rutaRelativa.includes('/cas/')) continue;
    
    // Verificar que esté en lek-files-dev o en el directorio base
    const inLekFiles = rutaLower.includes('/lek-files-dev/');
    const inBaseDir = baseDirLower && rutaLower.includes(baseDirLower);
    
    if (!inLekFiles && !inBaseDir) continue;
    
    filtered[rutaCompleta] = info;
  }
  
  return filtered;
}

function fileMonitorJsonToList(filesJson, extensionFilter) {
  const list = [];
  
  for (const [rutaCompleta, info] of Object.entries(filesJson)) {
    if (!info || typeof info !== 'object') continue;
    
    const nombre = info.nombre || path.basename(rutaCompleta);
    const extension = info.extension || path.extname(nombre);
    
    // Filtrar por extensión
    if (extensionFilter) {
      const extLower = extension.toLowerCase();
      if (extensionFilter === '.xlsx' && extLower !== '.xlsx') continue;
      if (extensionFilter === '.knfo' && extLower !== '.knfo') continue;
      if (extensionFilter === '.meta' && extLower !== '.meta') continue;
    }
    
    list.push({
      name: nombre,
      type: '-',
      size: info.tamaño || 0,
      modifyTime: info.fecha_modificacion || info.fecha_creacion || new Date().toISOString(),
      accessTime: info.fecha_creacion || new Date().toISOString(),
      rights: { user: 'r', group: 'r', other: 'r' },
      owner: null,
      group: null,
      _fileMonitorInfo: info
    });
  }
  
  return list;
}

/* ───────── DISCOVERY ───────── */
async function listAllMetaFiles(sftp, dir) {
  // SIEMPRE usar SFTP directamente con listado recursivo para obtener datos actualizados
  // Los JSON del File Monitor pueden estar desactualizados
  const names = [];
  try {
    // Usar listado recursivo (true = recursivo) para encontrar todos los archivos META en subdirectorios
    const files = await withTimeout(sftp.list(dir, true), SFTP_TIMEOUT_MS, `sftp.list(${dir}, true)`).catch(() => []);
    for (const f of files || []) {
      if (f.type === '-' && f.name.toLowerCase().endsWith('.meta')) {
        // Extraer solo el nombre del archivo (sin la ruta completa)
        const fileName = path.basename(f.name || f.path || '');
        if (fileName) {
          names.push(fileName.toLowerCase());
        }
      }
    }
  } catch (e) {
    console.warn(`[${ts()}] Error listando archivos META desde SFTP: ${e.message}`);
  }
  return names;
}

async function scanCtx(ctx) {
  const { excelDir, metaRoot, knfoDir } = ctxPaths(ctx);
  const baseDir = baseDirOf(ctx);
  
  // SIEMPRE usar SFTP directamente para obtener datos actualizados
  // Los JSON del File Monitor pueden estar desactualizados
  const sftp = await getSftpShared();
  
  let xlsList = [];
  let knfoList = [];
  let metaList = [];
  
  try {
    // Leer directamente desde SFTP para obtener datos en tiempo real
    console.log(`[${ts()}][${ctx}][SCAN] 📂 Leyendo archivos desde SFTP directamente...`);
    xlsList = await withTimeout(sftp.list(excelDir), SFTP_TIMEOUT_MS, `list(${excelDir})`).catch(() => []);
    knfoList = await withTimeout(sftp.list(knfoDir), SFTP_TIMEOUT_MS, `list(${knfoDir})`).catch(() => []);
    const metaNames = await listAllMetaFiles(sftp, metaRoot).catch(() => []);
    metaList = metaNames.map(nombre => ({
      name: nombre,
      type: '-',
      size: 0,
      modifyTime: new Date().toISOString(),
      accessTime: new Date().toISOString(),
      rights: { user: 'r', group: 'r', other: 'r' },
      owner: null,
      group: null
    }));
    
    console.log(`[${ts()}][${ctx}][SCAN] 📊 SFTP: ${xlsList.length} Excel, ${knfoList.length} KNFO, ${metaList.length} META`);
  } catch (e) {
    console.error(`[${ts()}][${ctx}] Error leyendo desde SFTP: ${e.message}`);
    return [];
  }
  
  try {

    const knfoSet = new Set(knfoList.map(f => f.name.toLowerCase()));
    const metaSet = new Set(metaList.map(n => (n.name || n).toLowerCase()));
    const xlsSet = new Set(xlsList.filter(f => f.type === '-' && f.name.toLowerCase().endsWith('.xlsx')).map(f => f.name.toLowerCase()));

    const items = [];
    let skippedByPattern = 0;
    let skippedByComplete = 0;
    let skippedByProcessed = 0;
    let knfoSinMetaCount = 0;
    
    // Primera pasada: procesar archivos Excel
    for (const f of xlsList) {
      if (f.type !== '-' || !f.name.toLowerCase().endsWith('.xlsx')) continue;
      const name = f.name;
      if (!PATTERNS.some(p => name.includes(p))) {
        skippedByPattern++;
        continue; // respeta filtro original
      }

      const baseLower = name.slice(0, -5).toLowerCase();
      const hasKnfo = knfoSet.has(`${baseLower}.knfo`);
      const hasMeta = metaSet.has(`${baseLower}.meta`);
      
      // Solo omitir si tiene AMBOS knfo Y meta
      if (hasKnfo && hasMeta) {
        skippedByComplete++;
        continue; // ya listo
      }

      const k = keyOf(ctx, name);
      // Si tiene KNFO pero no META, procesarlo incluso si ya fue procesado antes
      // (necesita generar el META desde el KNFO existente)
      if (processedOK.has(k) && !(hasKnfo && !hasMeta)) {
        skippedByProcessed++;
        continue;
      }

      items.push({ name, sizeMB: (Number(f.size) || 0) / (1024 * 1024) });
    }
    
    // Segunda pasada: buscar archivos KNFO sin META que no tienen Excel correspondiente
    // (los que tienen Excel ya fueron manejados en la primera pasada)
    for (const knfoFile of knfoList) {
      if (knfoFile.type !== '-' || !knfoFile.name.toLowerCase().endsWith('.knfo')) continue;
      
      const knfoName = knfoFile.name.toLowerCase();
      const baseName = knfoName.replace('.knfo', '');
      const metaName = `${baseName}.meta`;
      
      // Si ya tiene META, saltar
      if (metaSet.has(metaName)) {
        continue;
      }
      
      // Verificar si tiene un Excel correspondiente
      const xlsName = `${baseName}.xlsx`;
      const hasExcel = xlsSet.has(xlsName);
      
      // Si no tiene Excel pero tiene KNFO sin META, no podemos procesarlo
      // porque necesitamos el Excel para la función createKnfoAndMeta
      // (aunque técnicamente podríamos generar META solo desde KNFO, pero por ahora lo omitimos)
      if (!hasExcel) {
        // Log para debugging pero no agregamos porque no tenemos Excel
        knfoSinMetaCount++;
      }
    }
    
    if (items.length > 0 || skippedByPattern > 0 || skippedByComplete > 0 || skippedByProcessed > 0 || knfoSinMetaCount > 0) {
      console.log(`[${ts()}][${ctx}][SCAN] 📊 Resumen: ${items.length} pendientes, ${skippedByPattern} sin patrón, ${skippedByComplete} completos, ${skippedByProcessed} ya procesados, ${knfoSinMetaCount} KNFO sin META detectados`);
    }

    items.sort((a, b) => b.sizeMB - a.sizeMB);
    return items;
  } catch (e) {
    console.error(`[${ts()}][${ctx}] scan error: ${e.message || e}`);
    return [];
  }
}

async function scanAll(tag) {
  const tmpQueues = new Map();
  for (const ctx of CTXS) {
    const items = await scanCtx(ctx);
    tmpQueues.set(ctx, items);
    if (items.length > 0) {
      console.log(`[${ts()}][${tag}][${ctx}] 📋 Archivos detectados sin knfo/meta: ${items.length}`);
      // Mostrar primeros 5 archivos para depuración
      const sample = items.slice(0, 5).map(i => i.name).join(', ');
      if (items.length > 5) {
        console.log(`[${ts()}][${tag}][${ctx}]   Ejemplos: ${sample} ... (+${items.length - 5} más)`);
      } else {
        console.log(`[${ts()}][${tag}][${ctx}]   Archivos: ${sample}`);
      }
    }
  }

  if (inFlight.size === 0) {
    queues.clear();
    for (const ctx of CTXS) queues.set(ctx, (tmpQueues.get(ctx) || []).slice());
  }
  const total = [...queues.values()].reduce((acc, arr) => acc + arr.length, 0);
  console.error(`[${ts()}][${tag}] TH_MB=${TH_MB}MB PARALLEL_LIMIT=${PARALLEL_LIMIT} → pendientes=${total}`);
}

function queuesEmpty() { return ![...queues.values()].some(arr => arr.length); }

/* ───────── PICK ─────────
   - Si existe un archivo >= TH_MB al frente de algún ctx → solo ese (exclusivo).
   - Si no, tomar hasta `max` archivos < TH_MB repartidos por ctx. */
function pickBatch(max) {
  const picks = [];

  // 1) Exclusivo si hay LARGE al frente de algún ctx
  for (const ctx of CTXS) {
    const q = queues.get(ctx) || [];
    if (!q.length) continue;
    if (q[0].sizeMB >= TH_MB) {
      const it = q.shift();
      queues.set(ctx, q);
      picks.push({ ctx, file: it.name, large: true });
      return picks; // exclusivo
    }
  }

  // 2) Solo pequeños, hasta `max`
  for (const ctx of CTXS) {
    const q = queues.get(ctx) || [];
    while (q.length && picks.length < max) {
      if (q[0].sizeMB >= TH_MB) break; // dejamos LARGE para el ciclo exclusivo
      const it = q.shift();
      picks.push({ ctx, file: it.name, large: false });
    }
    queues.set(ctx, q);
    if (picks.length === max) break;
  }
  return picks;
}

/* ───────── WORKER ───────── */
function runWorker(ctx, file) {
  return new Promise((resolve, reject) => {
    const worker = new Worker(__filename, { workerData: { ctx, file } });
    let settled = false;

    worker.on('message', (m) => {
      if (m && m.__type === 'log') {
        const lvl = m.level || 'log', line = m.line || '';
        if (lvl === 'error' || lvl === 'warn') console.error(`[W:${ctx}:${file}] ${line}`);
        else                                     console.log(`[W:${ctx}:${file}] ${line}`);
        return;
      }
      if (m && Object.prototype.hasOwnProperty.call(m, 'ok')) {
        settled = true; m.ok ? resolve() : reject(new Error(m.err || 'worker failed'));
      }
    });

    worker.on('error', (e) => { if (!settled) { settled = true; reject(e); } });
    worker.on('exit',  (c) => { if (!settled) { settled = true; c === 0 ? resolve() : reject(new Error(`worker exit ${c}`)); } });
  });
}

async function processBatch(batch) {
  // Usar Promise.allSettled para que no bloquee si un archivo falla o se cuelga
  const results = await Promise.allSettled(batch.map(async ({ ctx, file, large }) => {
    const k = keyOf(ctx, file);
    inFlight.add(k); flags.set(k, { large });

    console.log(`[${ts()}][${ctx}] → procesa ${file}${large ? ' [LARGE]' : ''}`);
    const t0 = Date.now();

    try {
      // Verificar si el archivo ha fallado demasiadas veces
      const failureCount = failedFiles.get(k) || 0;
      if (failureCount >= MAX_FAILURES) {
        console.warn(`[${ts()}][${ctx}] ⏭️  ${file} → Saltando (${failureCount} intentos fallidos previos)`);
        processedOK.add(k); // Marcarlo como procesado para evitar reintentos infinitos
        return; // No lanzar error, solo saltar
      }
      
      // Timeout por archivo: 2 minutos para archivos normales, 10 minutos para LARGE
      const FILE_TIMEOUT_MS = large ? 10 * 60 * 1000 : 2 * 60 * 1000;
      
      // Los archivos están en SFTP, no en el sistema de archivos local
      // El worker manejará los errores de SFTP si el archivo no existe
      await withTimeout(
        runWorker(ctx, file),
        FILE_TIMEOUT_MS,
        `runWorker(${ctx}, ${file})`
      );
      processedOK.add(k);
      failedFiles.delete(k); // Limpiar contador si tuvo éxito
      console.log(`[${ts()}][${ctx}] ✔ ok ${file} (${((Date.now() - t0) / 60000).toFixed(1)} min)`);
    } catch (e) {
      const errorMsg = e?.message || String(e);
      const failureCount = (failedFiles.get(k) || 0) + 1;
      failedFiles.set(k, failureCount);
      
      if (errorMsg.includes('TIMEOUT')) {
        console.error(`[${ts()}][${ctx}] ⏱️  ${file} → Timeout después de ${large ? '10' : '2'} minutos (intento ${failureCount}/${MAX_FAILURES})`);
        if (failureCount >= MAX_FAILURES) {
          console.error(`[${ts()}][${ctx}] 🚫 ${file} → Archivo marcado como problemático, se saltará en futuros intentos`);
          processedOK.add(k); // Marcarlo para evitar reintentos infinitos
          return; // No lanzar error después del último intento
        }
      } else {
        console.error(`[${ts()}][${ctx}] ✖ ${file} → ${errorMsg} (intento ${failureCount}/${MAX_FAILURES})`);
      }
      throw e; // Re-lanzar para que Promise.allSettled capture el error
    } finally {
      inFlight.delete(k); flags.delete(k);
      console.log(`[${ts()}][${ctx}] ← fin ${file}`);
    }
  }));
  
  // Log de resultados
  const failed = results.filter(r => r.status === 'rejected');
  if (failed.length > 0) {
    console.warn(`[${ts()}][TICK] ⚠️  ${failed.length} archivo(s) fallaron en el lote: ${failed.map(f => f.reason?.message || 'Error desconocido').join(', ')}`);
  }
}

/* ───────── TICK ───────── */
async function tick() {
  if (ticking) {
    console.log(`[${ts()}][TICK] ⏸️  Tick ya en ejecución, omitiendo... (inFlight=${inFlight.size}, queuesEmpty=${queuesEmpty()}, phase=${phase})`);
    return;
  }
  ticking = true;
  try {
    console.log(`[${ts()}][TICK] 🚀 Iniciando tick() - queuesEmpty=${queuesEmpty()}, inFlight=${inFlight.size}, phase=${phase}`);
    
    // Barrera: si hay un LARGE en curso, no se lanzan pequeños
    if ([...flags.values()].some(v => v.large)) {
      console.log(`[${ts()}][TICK] ⏸️  Hay un archivo LARGE en curso, omitiendo...`);
      return;
    }

    // Escaneo masivo controlado por fases
    if (queuesEmpty() && inFlight.size === 0) {
      if (phase === 'IDLE')      { 
        console.log(`[${ts()}][TICK] 📊 Ejecutando scanAll(BOOT)...`); 
        await scanAll('BOOT');   
        // Después del scan, verificar si hay archivos para procesar
        if (!queuesEmpty()) {
          console.log(`[${ts()}][TICK] 📦 Archivos encontrados después del scan, continuando procesamiento...`);
          phase = 'BOOTED';
          // No hacer return, continuar con el procesamiento
        } else {
          console.log(`[${ts()}][TICK] ⏸️  No se encontraron archivos después del scan`);
          phase = 'BOOTED';
          // No retornar, permitir que continúe con el siguiente escaneo
        }
      }
      else if (phase === 'BOOTED')    { 
        console.log(`[${ts()}][TICK] 📊 Ejecutando scanAll(RESCAN)...`); 
        await scanAll('RESCAN'); 
        // Después del rescan, verificar si hay archivos para procesar
        if (!queuesEmpty()) {
          console.log(`[${ts()}][TICK] 📦 Archivos encontrados después del rescan, continuando procesamiento...`);
          phase = 'RESCANNED';
          // No hacer return, continuar con el procesamiento
        } else {
          console.log(`[${ts()}][TICK] ⏸️  No se encontraron archivos después del rescan`);
          phase = 'RESCANNED';
          // No retornar, permitir que continúe con el siguiente escaneo
        }
      }
      else if (phase === 'RESCANNED') { 
        // Volver a IDLE para permitir nuevo ciclo de escaneo
        console.log(`[${ts()}][TICK] 🔄 Fase RESCANNED completada, volviendo a IDLE para nuevo ciclo`); 
        phase = 'IDLE';
        // No retornar, permitir que continúe con el siguiente escaneo
      }
    }

    const avail = Math.max(0, PARALLEL_LIMIT - inFlight.size);
    if (avail <= 0) {
      console.log(`[${ts()}][TICK] ⏸️  No hay capacidad disponible (avail=${avail}, inFlight=${inFlight.size})`);
      return;
    }

    const batch = pickBatch(avail);
    if (!batch.length) {
      console.log(`[${ts()}][TICK] ⏸️  No hay archivos en cola para procesar (queuesEmpty=${queuesEmpty()}, inFlight=${inFlight.size})`);
      return;
    }

    console.log(`[${ts()}][PICK] 📦 Procesando lote de ${batch.length} archivo(s): ${batch.map(b => `${b.ctx}/${path.basename(b.file)}`).join(' , ')}`);
    
    // Procesar el lote sin bloquear el tick si hay archivos colgados
    // Usar Promise.race con un timeout para evitar bloqueos indefinidos
    const PROCESS_TIMEOUT = 30 * 60 * 1000; // 30 minutos máximo por lote
    try {
      await Promise.race([
        processBatch(batch),
        new Promise((_, reject) => 
          setTimeout(() => reject(new Error('Timeout: proceso de lote excedió 30 minutos')), PROCESS_TIMEOUT)
        )
      ]);
      console.log(`[${ts()}][TICK] ✅ Tick completado exitosamente`);
    } catch (e) {
      if (e.message.includes('Timeout')) {
        console.error(`[${ts()}][TICK] ⚠️  Timeout en processBatch: ${e.message}`);
        console.error(`[${ts()}][TICK] ⚠️  Archivos en inFlight: ${inFlight.size}, continuando...`);
        // No relanzar el error, permitir que el tick termine
      } else {
        throw e; // Relanzar otros errores
      }
    }
  } catch (e) {
    console.error(`[${ts()}][TICK] ❌ Error en tick: ${e.message}`);
    console.error(`[${ts()}][TICK] Stack: ${e.stack}`);
  } finally {
    ticking = false;
    console.log(`[${ts()}][TICK] 🔓 Tick liberado (ticking=false)`);
  }
}

/* ───────── START ───────── */
function start() {
  if (global.__excelAutoKNFO_started) return;
  global.__excelAutoKNFO_started = true;
  CTXS.forEach(c => queues.set(c, []));
  console.log(`[${ts()}] Scheduler KNFO activo (modo WebSocket - sin scans automáticos) (TH_MB=${TH_MB}MB, PARALLEL_LIMIT=${PARALLEL_LIMIT})`);
  (async () => {
    // NO hacer scan automático al inicio ni con setInterval
    // Solo se ejecutará cuando llegue señal por WebSocket/HTTP
    
    // Crear servidor HTTP simple para recibir señales de scan
    const SCAN_PORT = Number(process.env.ETL_ANALYSIS_SCAN_PORT || 3002);
    const BIND_HOST = process.env.ETL_ANALYSIS_BIND_HOST || '0.0.0.0';
    const server = http.createServer(async (req, res) => {
      if ((req.method === 'POST' || req.method === 'GET') && req.url === '/run') {
        // Endpoint para ejecución manual
        console.log(`[${ts()}] 🚀 Ejecución manual solicitada`);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, message: 'Procesamiento iniciado' }));
        
        // Limpiar estado para permitir reprocesar todos los archivos
        processedOK.clear();
        failedFiles.clear(); // Limpiar también los archivos fallidos para darles otra oportunidad
        queues.clear();
        inFlight.clear();
        flags.clear();
        phase = 'IDLE';
        console.log(`[${ts()}] 🧹 Estado limpiado para procesamiento manual`);
        
        // Ejecutar tick en bucle hasta que no haya más archivos pendientes
        (async function runUntilComplete() {
          let iterations = 0;
          const MAX_ITERATIONS = 1000; // Límite de seguridad
          let consecutiveEmptyTicks = 0;
          const MAX_CONSECUTIVE_EMPTY = 3; // 3 ticks vacíos consecutivos = terminado
          
          while (iterations < MAX_ITERATIONS) {
            iterations++;
            
            // Esperar a que el tick anterior termine antes de ejecutar uno nuevo
            let waitCount = 0;
            while (ticking && waitCount < 30) {
              await new Promise(resolve => setTimeout(resolve, 1000));
              waitCount++;
            }
            
            if (ticking) {
              console.warn(`[${ts()}] ⚠️  Tick bloqueado después de ${waitCount} segundos, forzando continuación...`);
              ticking = false; // Forzar liberación si está bloqueado
            }
            
            // Ejecutar tick
            try {
              await tick();
            } catch (e) {
              console.error(`[${ts()}] ❌ Error en tick manual (iteración ${iterations}): ${e.message}`);
              console.error(`[${ts()}] Stack: ${e.stack}`);
            }
            
            // Verificar si hay archivos pendientes o en procesamiento
            const hasPending = !queuesEmpty() || inFlight.size > 0;
            
            if (!hasPending) {
              consecutiveEmptyTicks++;
              console.log(`[${ts()}] 📊 Tick ${iterations}: Sin archivos pendientes (consecutivos vacíos: ${consecutiveEmptyTicks}/${MAX_CONSECUTIVE_EMPTY})`);
              
              // Si la fase no es IDLE, resetear para permitir nuevo escaneo
              if (phase !== 'IDLE') {
                console.log(`[${ts()}] 🔄 Reseteando fase a IDLE para permitir nuevo escaneo`);
                phase = 'IDLE';
                consecutiveEmptyTicks = 0; // Resetear contador al cambiar fase
              } else if (consecutiveEmptyTicks >= MAX_CONSECUTIVE_EMPTY) {
                // Hacer un último escaneo para asegurar que no hay más archivos
                console.log(`[${ts()}] 🔍 Última verificación de archivos pendientes...`);
                await scanAll('FINAL_CHECK');
                
                if (queuesEmpty() && inFlight.size === 0) {
                  console.log(`[${ts()}] ✅ Procesamiento manual completado (${iterations} iteraciones, ${processedOK.size} archivos procesados)`);
                  break;
                } else {
                  console.log(`[${ts()}] 📦 Se encontraron más archivos, continuando procesamiento...`);
                  consecutiveEmptyTicks = 0;
                }
              }
            } else {
              consecutiveEmptyTicks = 0;
              const totalPending = [...queues.values()].reduce((acc, arr) => acc + arr.length, 0);
              console.log(`[${ts()}] 📊 Tick ${iterations}: ${totalPending} archivos en cola, ${inFlight.size} en procesamiento`);
            }
            
            // Esperar un poco antes de la siguiente iteración para no saturar
            await new Promise(resolve => setTimeout(resolve, 2000));
          }
          
          if (iterations >= MAX_ITERATIONS) {
            console.warn(`[${ts()}] ⚠️  Procesamiento manual alcanzó el límite de iteraciones (${MAX_ITERATIONS})`);
            console.warn(`[${ts()}] ⚠️  Estado final: ${[...queues.values()].reduce((acc, arr) => acc + arr.length, 0)} pendientes, ${inFlight.size} en procesamiento`);
          }
        })().catch(e => {
          console.error(`[${ts()}] ❌ Error en bucle de procesamiento manual: ${e.message}`);
          console.error(`[${ts()}] Stack: ${e.stack}`);
        });
      } else if ((req.method === 'GET' || req.method === 'POST') && req.url === '/status') {
        // Endpoint para obtener estado y progreso
        const totalPending = [...queues.values()].reduce((acc, arr) => acc + arr.length, 0);
        
        const byContext = {};
        for (const ctx of CTXS) {
          const arr = queues.get(ctx) || [];
          byContext[ctx] = arr.length;
        }
        
        // Obtener información de archivos en procesamiento
        const inFlightInfo = [];
        for (const key of inFlight) {
          const [ctx, file] = key.split(':');
          const flag = flags.get(key) || {};
          inFlightInfo.push({
            ctx,
            file,
            large: flag.large || false
          });
        }
        
        // Calcular capacidad disponible
        const avail = Math.max(0, PARALLEL_LIMIT - inFlight.size);
        
        // Calcular progreso total (archivos procesados en esta sesión)
        const totalProcessed = processedOK.size;
        
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          success: true,
          totalPending,
          inFlight: inFlight.size,
          inFlightFiles: inFlightInfo,
          processed: totalProcessed,
          byContext,
          phase,
          ticking,
          capacity: {
            available: avail,
            limit: PARALLEL_LIMIT,
            used: inFlight.size
          },
          config: {
            thMB: TH_MB,
            parallelLimit: PARALLEL_LIMIT
          }
        }));
      } else if (req.method === 'GET' && req.url.startsWith('/track/')) {
        // Endpoint para obtener el estado de seguimiento de un archivo específico
        const filename = decodeURIComponent(req.url.replace('/track/', ''));
        const fileBase = filename.replace(/\.xlsx$/i, '');
        
        console.log(`[${ts()}] [TRACK] Consultando estado de archivo: ${filename}`);
        
        // Verificar estado del archivo en todos los contextos
        let isProcessed = false;
        let isInFlight = false;
        let isInQueue = false;
        let foundContext = null;
        
        for (const ctx of CTXS) {
          const k = keyOf(ctx, filename);
          if (processedOK.has(k)) isProcessed = true;
          if (inFlight.has(k)) {
            isInFlight = true;
            foundContext = ctx;
          }
          const arr = queues.get(ctx) || [];
          if (arr.some(item => item.name === filename)) {
            isInQueue = true;
            foundContext = ctx;
          }
          if (foundContext) break;
        }
        
        // Verificar existencia de KNFO y META en SFTP (buscar en todos los contextos)
        let hasKnfo = false;
        let hasMeta = false;
        let knfoPath = null;
        let metaPath = null;
        let metaContext = null;
        let sftpError = null;
        
        try {
          const sftp = await getSftpShared();
          
          // Buscar en todos los contextos
          for (const ctx of CTXS) {
            const { knfoDir, metaRoot } = ctxPaths(ctx);
            const knfoFullPath = path.join(knfoDir, `${fileBase}.knfo`);
            
            try {
              const ctxHasKnfo = await withTimeout(nonEmpty(sftp, knfoFullPath), SFTP_TIMEOUT_MS, `nonEmpty(${knfoFullPath})`).catch(() => false);
              if (ctxHasKnfo) {
                hasKnfo = true;
                knfoPath = knfoFullPath;
                console.log(`[${ts()}] [TRACK] KNFO encontrado en ${ctx}: ${knfoFullPath}`);
              }
            } catch (e) {
              console.warn(`[${ts()}] [TRACK] Error verificando KNFO en ${ctx}: ${e.message}`);
            }
            
            try {
              const ctxHasMeta = await withTimeout(metaExistsForOriginal(sftp, metaRoot, fileBase), SFTP_TIMEOUT_MS, `metaExistsForOriginal(${metaRoot}, ${fileBase})`).catch(() => false);
              if (ctxHasMeta) {
                hasMeta = true;
                metaContext = ctx;
                console.log(`[${ts()}] [TRACK] META encontrado en ${ctx}`);
                // Intentar encontrar la ruta exacta del META
                try {
                  const metaFiles = await withTimeout(sftp.list(metaRoot, true), SFTP_TIMEOUT_MS, `sftp.list(${metaRoot})`);
                  for (const f of metaFiles || []) {
                    if (f.type === '-' && (f.name || f.path) && (f.name || f.path).includes(`${fileBase}.meta`)) {
                      metaPath = f.path || f.name;
                      break;
                    }
                  }
                } catch (e) {
                  console.warn(`[${ts()}] [TRACK] Error listando META: ${e.message}`);
                }
                break; // Encontrado, no buscar en otros contextos
              }
            } catch (e) {
              console.warn(`[${ts()}] [TRACK] Error verificando META en ${ctx}: ${e.message}`);
            }
          }
        } catch (e) {
          sftpError = e.message;
          console.error(`[${ts()}] [TRACK] Error general verificando archivo ${filename}: ${e.message}`);
          console.error(`[${ts()}] [TRACK] Stack: ${e.stack}`);
        }
        
        const response = {
          success: true,
          filename,
          fileBase,
          context: foundContext || metaContext || CTXS[0],
          status: {
            processed: isProcessed,
            inFlight: isInFlight,
            inQueue: isInQueue,
            hasKnfo,
            hasMeta,
            knfoPath,
            metaPath
          },
          pipeline: {
            step1_knfo: hasKnfo ? 'completed' : (isInFlight || isInQueue ? 'processing' : 'pending'),
            step2_meta: hasMeta ? 'completed' : (hasKnfo && (isInFlight || isInQueue) ? 'processing' : (hasKnfo ? 'pending' : 'waiting'))
          }
        };
        
        if (sftpError) {
          response.error = sftpError;
        }
        
        console.log(`[${ts()}] [TRACK] Respuesta para ${filename}: hasKnfo=${hasKnfo}, hasMeta=${hasMeta}`);
        
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(response));
      } else if (req.method === 'POST' && req.url === '/scan') {
        const clientIP = req.socket.remoteAddress || 'unknown';
        const timestamp = new Date().toISOString();
        
        // Leer body si existe (puede contener información sobre qué archivos cambiaron)
        let body = '';
        req.on('data', chunk => { body += chunk.toString(); });
        req.on('end', () => {
          let bodyData = null;
          try {
            if (body) bodyData = JSON.parse(body);
          } catch (e) {
            // Ignorar si no es JSON válido
          }
          
          console.log(`[${ts()}] ✅ Señal de scan recibida por HTTP desde ${clientIP}`);
          if (bodyData) {
            console.log(`[${ts()}] 📦 Datos de la señal: ${JSON.stringify(bodyData)}`);
          }
          console.log(`[${ts()}] 📊 Estado actual: queuesEmpty=${queuesEmpty()}, inFlight=${inFlight.size}, phase=${phase}`);
          
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: true, message: 'Scan iniciado', timestamp }));
          
          // Ejecutar tick cuando se recibe la señal
          console.log(`[${ts()}] 🔄 Ejecutando tick() para procesar cambios...`);
          tick().catch(e => {
            console.error(`[${ts()}] ❌ Error en tick: ${e.message}`);
            console.error(`[${ts()}] Stack: ${e.stack}`);
          });
        });
      } else {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false, error: 'Not found' }));
      }
    });
    
    server.listen(SCAN_PORT, BIND_HOST, () => {
      console.log(`[${ts()}] Servidor HTTP de scan escuchando en http://${BIND_HOST}:${SCAN_PORT}/scan`);
    });
    
    server.on('error', (err) => {
      console.error(`[${ts()}] Error en servidor HTTP: ${err.message}`);
    });
  })();
}

function removeEmptyAttributes(obj) {
  if (Array.isArray(obj)) {
    return obj.map(removeEmptyAttributes)
              .filter(v => v !== null && v !== undefined && v !== '');
  }
  if (obj && typeof obj === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(obj)) {
      const clean = removeEmptyAttributes(v);
      if (clean !== null && clean !== undefined && clean !== '') out[k] = clean;
    }
    return out;
  }
  return obj;
}

/* ───────── KNFO + META (ejecuta el worker) ───────── */
async function createKnfoAndMeta({ ctx, file, wlog = console.log }) {
  const { excelDir, metaBase, metaRoot, knfoDir } = ctxPaths(ctx);
  const fileBase = file.replace(/\.xlsx$/i, '');
  const knfoPath = path.join(knfoDir, `${fileBase}.knfo`);

  // ===== Helpers de depuración / formato =====
  const HOTFIX_KEEP_META = process.env.KNFO_KEEP_META !== '0'; // default ON
  const label = (s) => `[KNFO-DIAG][${ctx}][${file}] ${s}`;

  function preview(v, max = 240) {
    try { const j = typeof v === 'string' ? v : JSON.stringify(v); return j.length > max ? j.slice(0,max)+'…' : j; }
    catch { return String(v); }
  }

  function metaSnapshot(obj) {
    const own = !!obj && Object.prototype.hasOwnProperty.call(obj, '__meta__');
    const enumerable = !!obj && Object.prototype.propertyIsEnumerable.call(obj, '__meta__');
    const desc = !!obj && Object.getOwnPropertyDescriptor(obj, '__meta__');
    const type = own ? (Array.isArray(obj.__meta__) ? 'array' : typeof obj.__meta__) : 'n/a';
    return {
      hasOwn: own, enumerable, type,
      desc: desc ? { enumerable: !!desc.enumerable, configurable: !!desc.configurable, writable: !!desc.writable } : null,
      valuePreview: own ? preview(obj.__meta__, 160) : 'n/a'
    };
  }

  // Clon JSON-safe: hace todas las props enumerables y evita toJSON
  function jsonPlainify(src, seen = new WeakMap()) {
    if (src === null || src === undefined) return src;
    if (typeof src !== 'object') return src;
    if (src instanceof Date) return src.toISOString();

    if (Array.isArray(src)) {
      if (seen.has(src)) return seen.get(src);
      const arr = new Array(src.length);
      seen.set(src, arr);
      for (let i = 0; i < src.length; i++) arr[i] = jsonPlainify(src[i], seen);
      return arr;
    }

    if (seen.has(src)) return seen.get(src);
    const dst = Object.create(null);
    seen.set(src, dst);

    for (const k of Reflect.ownKeys(src)) {
      if (k === 'toJSON') continue;
      try {
        const v = jsonPlainify(src[k], seen);
        Object.defineProperty(dst, k, { value: v, enumerable: true, writable: true, configurable: true });
      } catch { /* getter problemático → omitir */ }
    }
    return dst;
  }

  // Ordena para que __meta__ quede primero (solo nivel top)
  function orderMetaFirst(src) {
    if (!src || typeof src !== 'object') return src;
    const out = Object.create(null);
    const has = Object.prototype.hasOwnProperty.call(src, '__meta__');
    if (has) Object.defineProperty(out, '__meta__', { value: src.__meta__, enumerable: true, writable: true, configurable: true });
    for (const k of Object.keys(src)) {
      if (k === '__meta__') continue;
      Object.defineProperty(out, k, { value: src[k], enumerable: true, writable: true, configurable: true });
    }
    return out;
  }

  // Podar recursivamente TODAS las claves "frequencySingles"
  function pruneFrequencySinglesDeep(input, stats = { removed: 0 }, seen = new WeakMap()) {
    if (input === null || input === undefined) return input;
    const t = typeof input;
    if (t !== 'object') return input;

    if (Array.isArray(input)) {
      if (seen.has(input)) return seen.get(input);
      const arr = new Array(input.length);
      seen.set(input, arr);
      for (let i = 0; i < input.length; i++) arr[i] = pruneFrequencySinglesDeep(input[i], stats, seen);
      return arr;
    }

    if (seen.has(input)) return seen.get(input);
    const out = Object.create(null);
    seen.set(input, out);

    for (const k of Object.keys(input)) {
      if (k === 'frequencySingles') { stats.removed++; continue; }
      const v = pruneFrequencySinglesDeep(input[k], stats, seen);
      Object.defineProperty(out, k, { value: v, enumerable: true, writable: true, configurable: true });
    }
    return out;
  }

  // removeEmptyAttributes local (ya presente en tu archivo)
  function removeEmptyAttributes(obj) {
    if (Array.isArray(obj)) {
      return obj.map(removeEmptyAttributes)
                .filter(v => v !== null && v !== undefined && v !== '');
    }
    if (obj && typeof obj === 'object') {
      const out = {};
      for (const [k, v] of Object.entries(obj)) {
        const clean = removeEmptyAttributes(v);
        if (clean !== null && clean !== undefined && clean !== '') out[k] = clean;
      }
      return out;
    }
    return obj;
  }

  const perWorker = process.env.SFTP_PER_WORKER === '1';
  wlog(`[WSTEP][${ctx}] ${file} → acquiring SFTP (perWorker=${perWorker ? 'yes' : 'no'})`);
  const sftp = await getSftpForWorker(ctx);
  wlog(`[WSTEP][${ctx}] ${file} → SFTP ready`);

  try {
    const hasKnfo = await withTimeout(nonEmpty(sftp, knfoPath), SFTP_TIMEOUT_MS, 'nonEmpty(knfo)').catch(() => false);
    const hasMeta = await withTimeout(metaExistsForOriginal(sftp, metaRoot, fileBase), SFTP_TIMEOUT_MS, 'metaExistsForOriginal').catch(() => false);

    wlog(`[${ts()}][${ctx}] decision ${file} → hasKnfo=${hasKnfo} hasMeta=${hasMeta} → ${!hasKnfo ? 'ANALYZE+KNFO+META' : 'KNFO->META_ONLY'}`);
    if (hasKnfo && hasMeta) {
      wlog(`[${ts()}][${ctx}]   • ${file} → knfo/meta OK (skip)`);
      return;
    }

    if (!hasKnfo) {
      // ===== Construcción KNFO + META desde XLSX =====
      const tGet = Date.now();
      const xlsxBuf = await withTimeout(sftp.get(path.join(excelDir, file)), SFTP_TIMEOUT_MS * 2, 'sftp.get(xlsx)');
      wlog(`[WSTEP][${ctx}] ${file} → xlsx ${xlsxBuf?.length || 0}B in ${Date.now() - tGet}ms`);

      const tAna = Date.now();
      const analysis = await analyzeExcelFile(xlsxBuf, { fileName: fileBase, context: ctx });
      wlog(`[WSTEP][${ctx}] ${file} → analyze done in ${Date.now() - tAna}ms`);

      const knfoObj   = buildKnfo(analysis, analysis);
      wlog(label(`after buildKnfo → ${JSON.stringify(metaSnapshot(knfoObj))}`));

      const knfoPlain = jsonPlainify(knfoObj);
      wlog(label(`after jsonPlainify → ${JSON.stringify(metaSnapshot(knfoPlain))}`));

      // Limpieza general (sin borrar info útil)
      const tClean = Date.now();
      let knfoClean = removeEmptyAttributes(knfoPlain);
      wlog(label(`removeEmptyAttributes took ${Date.now() - tClean}ms; snapshot=${JSON.stringify(metaSnapshot(knfoClean))}`));

      // Si __meta__ se perdió, reinyectar (debería ya no ocurrir, pero por seguridad)
      if (Object.prototype.hasOwnProperty.call(knfoPlain, '__meta__') &&
          !Object.prototype.hasOwnProperty.call(knfoClean, '__meta__') &&
          HOTFIX_KEEP_META) {
        knfoClean.__meta__ = knfoPlain.__meta__;
        wlog(label(`💊 [HOTFIX] __meta__ reinyectado tras limpieza`));
      }

      // Poner __meta__ al comienzo (nivel top)
      const ordered = orderMetaFirst(knfoClean);
      const isFirstMeta = Object.keys(ordered)[0] === '__meta__';
      wlog(label(`orderMetaFirst → __meta__ is first? ${isFirstMeta}`));

      // Podar TODOS los frequencySingles (en cualquier nivel)
      const stats = { removed: 0 };
      const knfoPersist = pruneFrequencySinglesDeep(ordered, stats);
      wlog(label(`pruneFrequencySinglesDeep → eliminados=${stats.removed}`));

      // Serializar y subir
      const jsonStr = JSON.stringify(knfoPersist, null, 2);
      wlog(label(`final JSON size=${jsonStr.length} bytes, includes "__meta__"? ${jsonStr.includes('"__meta__"')}`));

      await withTimeout(sftp.mkdir(knfoDir, true).catch(() => {}), SFTP_TIMEOUT_MS, 'sftp.mkdir(knfoDir)');
      await withTimeout(
        safeSftpUpload(sftp, knfoPath, Buffer.from(jsonStr)),
        SFTP_TIMEOUT_MS * 2,
        'upload(knfo)'
      );

      // Construir META desde la misma versión que persistimos (sin frequencySingles)
      wlog(`[META-GEN][${ctx}] ${file} → Iniciando generación de META junto con KNFO nuevo...`);
      const tBuildMeta = Date.now();
      let metaObj;
      try {
        metaObj = await buildMeta(knfoPersist, { sftpConfig, dirMeta: metaBase });
        wlog(`[META-GEN][${ctx}] ${file} → META construido exitosamente en ${Date.now() - tBuildMeta}ms`);
        wlog(`[META-GEN][${ctx}] ${file} → dataRepo: ${metaObj.dataRepo}, TCODE: ${metaObj.tcode}, columnas: ${metaObj.totalColumns || 0}`);
      } catch (e) {
        wlog(`[META-GEN][${ctx}] ${file} → ❌ ERROR construyendo META: ${e.message}`);
        wlog(`[META-GEN][${ctx}] ${file} → Stack: ${e.stack}`);
        throw e;
      }
      
      const metaRepoDir = path.join(metaBase, 'meta', metaObj.dataRepo);
      wlog(`[META-GEN][${ctx}] ${file} → Creando directorio META: ${metaRepoDir}`);
      const tMkdir = Date.now();
      try {
        await withTimeout(sftp.mkdir(metaRepoDir, true).catch(() => {}), SFTP_TIMEOUT_MS, 'sftp.mkdir(metaRepoDir)');
        wlog(`[META-GEN][${ctx}] ${file} → Directorio META creado/verificado en ${Date.now() - tMkdir}ms`);
      } catch (e) {
        wlog(`[META-GEN][${ctx}] ${file} → ❌ ERROR creando directorio META: ${e.message}`);
        throw e;
      }
      
      const metaPath = path.join(metaRepoDir, `${fileBase}.meta`);
      wlog(`[META-GEN][${ctx}] ${file} → Serializando objeto META...`);
      const tSerialize = Date.now();
      let metaJson;
      try {
        metaJson = JSON.stringify(metaObj, null, 2);
        wlog(`[META-GEN][${ctx}] ${file} → META serializado: ${metaJson.length} bytes en ${Date.now() - tSerialize}ms`);
      } catch (e) {
        wlog(`[META-GEN][${ctx}] ${file} → ❌ ERROR serializando META: ${e.message}`);
        throw e;
      }
      
      wlog(`[META-GEN][${ctx}] ${file} → Subiendo archivo META a: ${metaPath}`);
      const tUpload = Date.now();
      try {
        await withTimeout(
          safeSftpUpload(sftp, metaPath, Buffer.from(metaJson)),
          SFTP_TIMEOUT_MS * 2,
          'upload(meta)'
        );
        wlog(`[META-GEN][${ctx}] ${file} → ✅ META subido exitosamente en ${Date.now() - tUpload}ms`);
        wlog(`[META-GEN][${ctx}] ${file} → ✅ Proceso completo de generación de META finalizado`);
      } catch (e) {
        wlog(`[META-GEN][${ctx}] ${file} → ❌ ERROR subiendo META: ${e.message}`);
        wlog(`[META-GEN][${ctx}] ${file} → Stack: ${e.stack}`);
        throw e;
      }
      return;
    }

    // ===== KNFO ya existe → generar solo META =====
    wlog(`[META-GEN][${ctx}] ${file} → Iniciando generación de META desde KNFO existente...`);
    const tReadKnfo = Date.now();
    const knfoRaw = await withTimeout(sftp.get(knfoPath), SFTP_TIMEOUT_MS, 'sftp.get(knfo)');
    wlog(`[WSTEP][${ctx}] ${file} → knfo read ${knfoRaw?.length || 0}B in ${Date.now() - tReadKnfo}ms`);

    const knfoObj = JSON.parse(Buffer.isBuffer(knfoRaw) ? knfoRaw.toString('utf8') : String(knfoRaw));
    wlog(`[META-GEN][${ctx}] ${file} → KNFO parseado correctamente, TCODE: ${knfoObj.__meta__?.tcode || 'UNKNOWN'}`);
    
    // Para coherencia con el formato nuevo, calculamos META sobre una vista sin frequencySingles
    const stats = { removed: 0 };
    const knfoForMeta = pruneFrequencySinglesDeep(knfoObj, stats);
    wlog(label(`META from existing KNFO → frequencySingles eliminados=${stats.removed}`));

    wlog(`[META-GEN][${ctx}] ${file} → Construyendo objeto META...`);
    const tBuildMeta = Date.now();
    let metaObj;
    try {
      metaObj = await buildMeta(knfoForMeta, { sftpConfig, dirMeta: metaBase });
      wlog(`[META-GEN][${ctx}] ${file} → META construido exitosamente en ${Date.now() - tBuildMeta}ms`);
      wlog(`[META-GEN][${ctx}] ${file} → dataRepo: ${metaObj.dataRepo}, TCODE: ${metaObj.tcode}, columnas: ${metaObj.totalColumns || 0}`);
    } catch (e) {
      wlog(`[META-GEN][${ctx}] ${file} → ❌ ERROR construyendo META: ${e.message}`);
      wlog(`[META-GEN][${ctx}] ${file} → Stack: ${e.stack}`);
      throw e;
    }
    
    const metaRepoDir = path.join(metaBase, 'meta', metaObj.dataRepo);
    wlog(`[META-GEN][${ctx}] ${file} → Creando directorio META: ${metaRepoDir}`);
    const tMkdir = Date.now();
    try {
      await withTimeout(sftp.mkdir(metaRepoDir, true).catch(() => {}), SFTP_TIMEOUT_MS, 'sftp.mkdir(metaRepoDir)');
      wlog(`[META-GEN][${ctx}] ${file} → Directorio META creado/verificado en ${Date.now() - tMkdir}ms`);
    } catch (e) {
      wlog(`[META-GEN][${ctx}] ${file} → ❌ ERROR creando directorio META: ${e.message}`);
      throw e;
    }
    
    const metaPath = path.join(metaRepoDir, `${fileBase}.meta`);
    wlog(`[META-GEN][${ctx}] ${file} → Serializando objeto META...`);
    const tSerialize = Date.now();
    let metaJson;
    try {
      metaJson = JSON.stringify(metaObj, null, 2);
      wlog(`[META-GEN][${ctx}] ${file} → META serializado: ${metaJson.length} bytes en ${Date.now() - tSerialize}ms`);
    } catch (e) {
      wlog(`[META-GEN][${ctx}] ${file} → ❌ ERROR serializando META: ${e.message}`);
      throw e;
    }
    
    wlog(`[META-GEN][${ctx}] ${file} → Subiendo archivo META a: ${metaPath}`);
    const tUpload = Date.now();
    try {
      await withTimeout(
        safeSftpUpload(sftp, metaPath, Buffer.from(metaJson)),
        SFTP_TIMEOUT_MS * 2,
        'upload(meta-from-knfo)'
      );
      wlog(`[META-GEN][${ctx}] ${file} → ✅ META subido exitosamente en ${Date.now() - tUpload}ms`);
      wlog(`[META-GEN][${ctx}] ${file} → ✅ Proceso completo de generación de META finalizado`);
    } catch (e) {
      wlog(`[META-GEN][${ctx}] ${file} → ❌ ERROR subiendo META: ${e.message}`);
      wlog(`[META-GEN][${ctx}] ${file} → Stack: ${e.stack}`);
      throw e;
    }
  } finally {
    await endSftpIfLocal(sftp);
  }
}

module.exports = { start, tick };
if (require.main === module) start();