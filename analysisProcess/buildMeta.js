const crypto = require('crypto');
const path   = require('path').posix;
const { sftpSingleton } = require('../sftpPool'); 
const { createNormFn, _normalizeBase, hash8 } = require('../analysisHelpers');

/* ───── configuración inyectada ───── */
let CFG;
const _sftp = { async get() { return sftpSingleton.get(); } };
const _disconnect = async () => {};

/* helper: directorio contenedor del .index para cada tcode */
const idxDir = (tcode) => path.join(CFG.dirMeta,'meta', tcode);

/* ───── registro central en RAM ───── */
const REGISTRY = new Map();                // key = dirMeta:tcode
const memKey   = (t) => `${CFG.dirMeta}:${t}`;

/* carga (o crea vacío) el diccionario de claves para un tcode */
async function loadIndex(tcode) {
  const cacheKey = memKey(tcode);
  if (REGISTRY.has(cacheKey)) return REGISTRY.get(cacheKey);
  
  const sftp = await sftpSingleton.get();
  const remote = path.join(CFG.dirMeta,'meta', `${tcode}.index`);

  let map = new Map();
  try {
    const buf = await sftp.get(remote);          // lo intenta leer
    map = new Map(Object.entries(JSON.parse(buf.toString())));
  } catch {
    /* no existe todavía → se devuelve mapa vacío */
  }

  REGISTRY.set(cacheKey, map);
  return map;
}

/* persiste en disco el diccionario actualizado */
async function flushIndex(tcode) {
  const map = REGISTRY.get(memKey(tcode));
  if (!map) return;                              // nada que guardar

  const sftp = await sftpSingleton.get();
  const dir  = path.join(CFG.dirMeta,'meta');
  await sftp.mkdir(dir, true).catch(() => {});   // aseguro carpeta
  
  const remote = path.join(dir, `${tcode}.index`);
  const buf    = Buffer.from(JSON.stringify(Object.fromEntries(map)));

  await sftp.put(buf, remote);
}

/* ───── abreviaturas SQL → 2 letras ───── */
const SQL2ABBR = { INT: 'IN', BIGINT: 'BI', 'TINYINT(1)': 'TI', 'CHAR(1)': 'CH', 'DECIMAL(38,10)': 'DE', DATETIME: 'DT' };
const typeAbbr = (sql = '') =>
  (sql.match(/^VARCHAR\((\d+)\)$/i)?.[1] && `VC${RegExp.$1}`) || SQL2ABBR[sql.toUpperCase()] || 'UN';


/* genera una sigla de longitud `len` (default 6)
 *  • reparte los cupos entre las palabras en orden      (p.e. 2-2-2 ó 3-3)
 *  • 1º intento = solo letras  • 2º intento = letras+dígitos
 */
function makeSigla(words, len = 6, includeDigits = false) {
  const out = [];
  const isOk = (ch) =>
    (ch >= 'a' && ch <= 'z') || (includeDigits && ch >= '0' && ch <= '9');

  /* cuota base por palabra + sobrante a las primeras */
  const base = Math.floor(len / words.length);
  let extra  = len - base * words.length;             // 0 ≤ extra < words.length

  for (const w of words) {
    let quota = base + (extra > 0 ? 1 : 0);
    if (extra > 0) extra--;

    for (let i = 0; i < w.length && quota > 0; i++) {
      const ch = w[i];
      if (isOk(ch)) {
        out.push(ch);
        quota--;
        if (out.length === len) break;                // sigla completa
      }
    }
    if (out.length === len) break;
  }

  /* si faltan caracteres → segunda pasada sobre las palabras */
  if (out.length < len) {
    for (const w of words) {
      for (const ch of w) {
        if (isOk(ch)) {
          out.push(ch);
          if (out.length === len) break;
        }
      }
      if (out.length === len) break;
    }
  }

  while (out.length < len) out.push('x');             // relleno
  return out.join('').slice(0, len);
}

function buildKeyMap(headers, normHdr, registry) {
  const map        = new Map();                     // raw ⇒ sigla
  const used       = new Set([...registry.values()]); // siglas ya tomadas
  const base2sigla = new Map();                     // alias base ⇒ sigla principal
  const ABC        = 'abcdefghijklmnopqrstuvwxyz';
  const clean      = (s) => s.replace(/[^a-z0-9]/gi, '').toLowerCase();

  const genUniqueSigla = (alias, raw) => {
    const words = alias.split('_').filter(Boolean);
    let sig = makeSigla(words, 6, false);           // intento 1 (letras)
    if (used.has(sig)) sig = makeSigla(words, 6, true);     // intento 2 (letras+num)
    if (used.has(sig)) {                                     // intento 3 (hash)
      const h = clean(hash8(raw));                           // ≥8 chars
      for (let i = 0; i <= h.length - 6 && used.has(sig); i++)
        sig = h.slice(i, i + 6);
      let idx = 0;
      while (used.has(sig)) sig = sig.slice(0, 5) + ABC[idx++ % 26];
    }
    return sig;
  };

  for (const raw of headers) {
    const normFull          = normHdr(raw);        // «posting_row» o «posting_row#2»
    const [base, numTxt]    = normFull.split('#'); // base alias y nº (si hay)
    let mainSigla           = base2sigla.get(base) || registry.get(base);

    /* ─── aparición principal (#1 implícito) ─── */
    if (numTxt === undefined || numTxt === '1') {
      if (!mainSigla) {                            // no existía en registro
        mainSigla = genUniqueSigla(base, raw);
        registry.set(base, mainSigla);             // se guarda sólo por alias
        used.add(mainSigla);
      }
      map.set(raw, mainSigla);
      base2sigla.set(base, mainSigla);
      continue;
    }

    /* ─── duplicados dentro del mismo archivo (#2, #3, …) ─── */
    if (!mainSigla) {                              // aún sin sigla principal
      mainSigla = genUniqueSigla(base, raw);
      registry.set(base, mainSigla);
      used.add(mainSigla);
      base2sigla.set(base, mainSigla);
    }

    const digits   = numTxt;                       // «2», «10», …
    let   siglaDup = mainSigla.slice(0, mainSigla.length - digits.length) + digits;

    if (used.has(siglaDup)) {                      // colisión poco común
      let idx = 0;
      const core = mainSigla.slice(0, mainSigla.length - digits.length - 1);
      while (used.has(siglaDup))
        siglaDup = core + ABC[idx++ % 26] + digits;
    }

    used.add(siglaDup);
    map.set(raw, siglaDup);
  }

  return map;
}

/* ───── helpers strip ───── */
const stripLengthGroups = (cols) =>
  Object.fromEntries(Object.entries(cols).map(([h, c]) => {
    const { lengthGroups, ...rest } = c; return [h, rest];
  }));

function stripDollarSuffix(colsObj) {
  const out = {};
  for (const [h, meta] of Object.entries(colsObj)) {
    const m = h.match(/^(.*?)(_\$[a-z]{1,3})$/i);
    if (m) {
      const base = m[1];                // nombre sin sufijo
      if (!(base in out)) {             // primera aparición ⇒ usa el base
        out[base] = meta;
      } else {                          // ya existe ⇒ mantén el nombre entero
        out[h] = meta;
      }
    } else {                            // sin sufijo ⇒ copia tal cual
      out[h] = meta;
    }
  }
  return out;
}

function groupByDataType(cols) {
  const out = {};
  for (const [h, m] of Object.entries(cols)) (out[m.DataType || 'Unknown'] ||= []).push(h);
  return out;
}

/* ───── buildSignature ───── */
function buildSignature(cols, keyMap, normHdr, keyColsNorm, short) {
  const order = [...keyColsNorm.filter(c => cols[c]), ...Object.keys(cols).filter(c => !keyColsNorm.includes(c)).sort()];
  const seen = {};
  const parts = order.map(raw => {
    const base = short ? keyMap.get(raw) : normHdr(raw);
    const typ  = cols[raw].DataType || 'UN';
    const tok0 = keyColsNorm.includes(normHdr(raw)) ? `PK$${base}` : base;
    let tok = `${tok0}$${short ? typeAbbr(typ) : typ}`;
    const n = (seen[tok] = (seen[tok] || 0) + 1);
    if (n > 1) tok += `#${n}`;
    return tok;
  });
  return `${parts.length}>${parts.join('-')}`;
}



async function buildMeta(knfoObj,{sftpConfig,dirMeta}){
  const tcode = knfoObj.__meta__?.tcode || 'UNKNOWN';
  console.log(`[buildMeta][${tcode}] Iniciando construcción de META...`);
  
  if(!knfoObj?.columns) {
    console.error(`[buildMeta][${tcode}] ❌ ERROR: knfoObj inválido - sin columnas`);
    throw new Error('invalid knfoObj');
  }
  CFG={sftpConfig,dirMeta};

  /* 1 ◇ depurar columnas ------------------------------------------ */
  console.log(`[buildMeta][${tcode}] Paso 1: Depurando columnas (total inicial: ${Object.keys(knfoObj.columns || {}).length})`);
  const slim1 = stripLengthGroups(knfoObj.columns||{});
  const slim2 = Object.fromEntries(
                 Object.entries(slim1).filter(([,m])=>m.DataType&&m.DataType!=='Unknown'));
  const slim3 = stripDollarSuffix(slim2);
  console.log(`[buildMeta][${tcode}] Paso 1 completado: ${Object.keys(slim3).length} columnas después de depuración`);

  const headers = Object.keys(slim3);
  const normHdr = createNormFn(headers);

  /* 2 ◇ key-map de 6 chars ---------------------------------------- */
  console.log(`[buildMeta][${tcode}] Paso 2: Cargando registro y construyendo key-map...`);
  const registry = await loadIndex(knfoObj.__meta__.tcode);
  const keyMap   = buildKeyMap(headers,normHdr,registry);
  await flushIndex(knfoObj.__meta__.tcode);
  console.log(`[buildMeta][${tcode}] Paso 2 completado: ${keyMap.size} columnas mapeadas`);

  /* 3 ◇ normalización definitiva (manejo duplicados Excel::AA) ---- */
  const xlIdx = s=>s.split('').reduce((n,ch)=>n*26+(ch.charCodeAt(0)-64),0)||Infinity;
  const groups=new Map();                                          // base → [{raw,ord}]
  for(const raw of headers){
    const [,suf=''] = raw.match(/::([A-Za-z]+)$/)||[];
    const base=_normalizeBase(raw.replace(/::[A-Za-z]+$/,''));
    (groups.get(base)||groups.set(base,[])&&groups.get(base)).push({raw,ord:xlIdx(suf)});
  }
  const normByCol={};
  for(const [base,arr] of groups){
    if(arr.length===1) normByCol[arr[0].raw]=base||'col_x';
    else{
      arr.sort((a,b)=>a.ord-b.ord);
      arr.forEach((o,i)=>{ normByCol[o.raw]=`${base}#${i+1}`; });
    }
  }

  /* 4 ◇ claves lógicas y campos de segmento normalizados ---------- */
  const keyColsNorm = (knfoObj.__meta__.keyColumns||[]).map(normHdr);

/* 4 ◇ segmentFields (ahora funciona con objeto o arreglo) ---------- */
const segObj         = knfoObj.__meta__.segmentFields || {};
const segFieldsRaw   = Array.isArray(segObj) ? segObj               // ya venía como lista
                                             : Object.keys(segObj); // era objeto → extrae llaves

const segFieldsNorm  = [...new Set(segFieldsRaw.map(normHdr))]
                         .filter(c => c && !keyColsNorm.includes(c)); // evita repetir PK

  /* 5 ◇ firmas ---------------------------------------------------- */
  console.log(`[buildMeta][${tcode}] Paso 5: Construyendo firmas...`);
  const signature      = buildSignature(slim3,keyMap,normHdr,keyColsNorm,false);
  const shortSignature = buildSignature(slim3,keyMap,normHdr,keyColsNorm,true);
  const dataRepo       = `${knfoObj.__meta__.tcode}$${hash8(shortSignature)}`;
  console.log(`[buildMeta][${tcode}] Paso 5 completado: dataRepo=${dataRepo}`);

  /* 6 ◇ salida ---------------------------------------------------- */
  console.log(`[buildMeta][${tcode}] Paso 6: Preparando salida final...`);
  const columnsByType = groupByDataType(slim3); delete columnsByType.Unknown;

  const result = {
    tcode         : knfoObj.__meta__.tcode,
    keyColumns    : knfoObj.__meta__.keyColumns,
    segmentFields      : segObj,          // mantiene pares clave-valor originales
    segmentFieldsNorm  : segFieldsNorm,   // nombres normalizados
    dtStructCode  : knfoObj.__meta__.dtStructCode,
    signature, shortSignature, dataRepo,

    repKeyByColumn : Object.fromEntries(keyMap),
    normHdrByColumn: normByCol,

    columnsByDataType : columnsByType,
    columns           : slim3,              // columnas originales depuradas
    totalRows         : knfoObj.totalRows,
    rowsWithData      : knfoObj.rowsWithData,
    totalColumns      : headers.length
  };
  
  console.log(`[buildMeta][${tcode}] ✅ META construido exitosamente: dataRepo=${result.dataRepo}, columnas=${result.totalColumns}, filas=${result.totalRows}`);
  return result;
}

process.on('exit', _disconnect);
module.exports = { buildMeta };