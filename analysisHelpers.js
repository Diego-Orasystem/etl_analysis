/********************************************************************
 * analysisHelpers.js  –  versión simplificada y acotada
 * ---------------------------------------------------------------
 *  ▸ Sin dependencias externas salvo ssh2-sftp-client y crypto.
 *  ▸ Sólo exporta las funciones realmente llamadas desde:
 *        • src/xlsxETL/services/excelAutoKNFO.js
 *        • src/analysisProcess/buildMeta.js
 *        • src/analysisProcess/analyzeExcelFile.js
 *******************************************************************/
const crypto      = require('crypto');
const posix       = require('path').posix;


/* ══════════════ 1.  SFTP utils ══════════════ */


/**
 * Subida segura usando un cliente YA conectado.
 * Sube a un archivo temporal y luego renombra de forma atómica. 
 * @param {string} remotePath
 * @param {Buffer} buf
 */
async function safeSftpUpload(sftp, remotePath, buf) {
  const dir = posix.dirname(remotePath);
  const tmp = `${remotePath}.tmp-${crypto.randomUUID()}`;

  await sftp.mkdir(dir, true).catch(() => {});
  try {
    await sftp.put(buf, tmp);
    await sftp.delete(remotePath).catch(() => {});
    await sftp.rename(tmp, remotePath);
  } catch (e) {
    await sftp.delete(tmp).catch(() => {});
    throw e;
  }
}

/** Stat > 2 bytes → se considera “existe” */
async function nonEmpty(sftp, remotePath) {
  try { return (await sftp.stat(remotePath)).size > 2; }
  catch { return false; }
}

async function metaExistsForOriginal(sftp, metaRoot, originalBase) {
  const target = `${originalBase}.meta`;

  let rootList;
  try {
    rootList = await sftp.list(metaRoot);
  } catch (e) {
    console.error(`[metaExistsForOriginal] list(${metaRoot}) error: ${e?.message || e}`);
    return false;
  }

  const dirs = rootList.filter(e => e && e.type === 'd' && typeof e.name === 'string');

  for (const d of dirs) {
    const candidate = posix.join(metaRoot, d.name, target);
    try {
      const st = await sftp.stat(candidate);
      if (st && Number(st.size) > 2) {
        console.log(`[metaExistsForOriginal] found: ${candidate} (${st.size}B)`);
        return true;
      }
    } catch (e) {
      const msg = e?.message || String(e);
      // No spam por "no existe"; pero sí logueamos otros problemas
      if (!/no such file|does not exist|not exist|ENOENT/i.test(msg)) {
        console.warn(`[metaExistsForOriginal] stat(${candidate}) error: ${msg}`);
      }
    }
  }

  console.log(`[metaExistsForOriginal] not found under ${metaRoot}/*/${target}`);
  return false;
}

/* ══════════════ 2.  Limpieza de JSON ══════════════ */


/* ══════════════ 3.  Normalización de encabezados ══════════════ */
function _normalizeBase(txt) {
  return String(txt)
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/\.+$/, '')
    .replace(/\s+/g, '_')
    .replace(/[^a-zA-Z0-9_]/g, '')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '')
    .toLowerCase();
}

function normHeader(raw, dupSet) {
  const base = _normalizeBase(raw);
  const suf  = (String(raw).match(/::([A-Za-z]+)$/) || [])[1];
  return suf && dupSet && dupSet.has(base) ? `${base}_$${suf.toLowerCase()}` : (base || 'col_x');
}

/* analysisHelpers.js  ─ sección 3  ▸ Sustituye createNormFn completa */
function createNormFn(headers = []) {
  /* 1 ▸ obtener base normalizado sin el sufijo ::Letra */
  const baseOf = (h) =>
    _normalizeBase(String(h).replace(/::[A-Za-z]+$/u, '')) || 'col_x';

  /* 2 ▸ agrupar por base conservando el orden de aparición            */
  const groups = Object.create(null);                 // base → [{h,idx}, …]
  headers.forEach((h, idx) => {
    const b = baseOf(h);
    (groups[b] ||= []).push({ h, idx });
  });

  /* 3 ▸ construir mapeo raw → norm                                             
         • único  → “base”                                                        
         • dúp    → “base#n”  (n = 1‑based, orden de aparición en header)        */
  const map = new Map();
  for (const [base, arr] of Object.entries(groups)) {
    if (arr.length === 1) {
      map.set(arr[0].h, base);
    } else {
      arr.forEach(({ h }, i) => map.set(h, `${base}#${i + 1}`));
    }
  }

  /* 4 ▸ normalizador cerrado (lookup O(1)) */
  const fn = (raw) => map.get(raw) ?? baseOf(raw);
  fn.dupSet = new Set(                                // ← por compatibilidad
    Object.keys(groups).filter((b) => groups[b].length > 1)
  );
  return fn;
}

/* ══════════════ 4.  Hash utilitario ══════════════ */
const hash8 = (s) =>
  crypto.createHash('sha256').update(String(s)).digest('base64url').slice(0, 8);

/* ══════════════ 5.  EXPORTS ══════════════ */
module.exports = {
  safeSftpUpload,
  nonEmpty,
  metaExistsForOriginal,  
  createNormFn,
  hash8,
  _normalizeBase,
};