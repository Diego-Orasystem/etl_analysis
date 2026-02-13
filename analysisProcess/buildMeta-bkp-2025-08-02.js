/* analysisProcess/buildMeta.js
 * ───────────────────────────────────────────────────────────────
 *   Construye el objeto `.meta` a partir de un `.knfo`
 *   ▸ repKeyByColumn  : claves únicas de 4 chars (homogéneas + createNormFn)
 *   ▸ signature       : columnas PK primero  (formato  PK$base$TYPE)
 *   ▸ shortSignature  : usando repKeyByColumn y abrevs de tipo
 *   ▸ dataRepo        :  "<tcode>$HiJMpdbx"   (hash fijo)
 *   ▸ columns         : copia SIN lengthGroups
 * ───────────────────────────────────────────────────────────────*/
const crypto               = require('crypto');
const { createNormFn, hash8 }     = require('../analysisHelpers');   // normalizador global

/* ═════════════════════════ util básicos ════════════════════════ */
const SQL2ABBR = { INT:'IN', BIGINT:'BI', 'TINYINT(1)':'TI',
                   'CHAR(1)':'CH', 'DECIMAL(38,10)':'DE', DATETIME:'DT' };


const typeAbbr = sql => {
  if (typeof sql !== 'string' || !sql.trim()) return 'UN';
  const m = sql.match(/^VARCHAR\((\d+)\)$/i);
  return m ? `VC${m[1]}` : (SQL2ABBR[sql.toUpperCase()] || 'UN');
};

/* ═══════════════════ helper: 4-chars homogéneos ═════════════════ */
function homog4(orderedWords) {
  const chars = [];
  let depth   = 0;
  while (chars.length < 4) {
    let pushed = false;
    for (const w of orderedWords) {
      if (chars.length === 4) break;
      if (w.length > depth) { chars.push(w[depth]); pushed = true; }
    }
    if (!pushed) break;              // palabras demasiado cortas
    depth++;
  }
  while (chars.length < 4) chars.push('x');   // relleno improbable
  return chars.join('');
}

/* ═══════════════════ repKeyByColumn (único) ════════════════════ */
function buildKeyMap(headers, normHdr) {
  const keyMap = new Map();
  const used   = new Set();

  for (const raw of headers) {
    const words = normHdr(raw).split('_').filter(Boolean).sort(); // alfabético
    let key     = homog4(words);

    /* colisión: recorre hash8 hasta encontrar slot libre */
    if (used.has(key)) {
      const base = hash8(raw);
      for (let i = 0; i < base.length - 3 && used.has(key); i++) {
        key = base.slice(i, i + 4);
      }
      /* aún chocan ⇒ sufijo numérico */
      let n = 1;
      while (used.has(key)) key = key.slice(0, 3) + (n++ % 10);
    }
    keyMap.set(raw, key);
    used.add(key);
  }
  return keyMap;
}

/* ═══════════════════ firmas (larga / corta) ════════════════════ */
function buildSignature(cols, keyMap, normHdr, keyColsNorm, short = false) {

  const order = [
    ...keyColsNorm.filter(c => cols[c]),                     // PK primero
    ...Object.keys(cols).filter(c => !keyColsNorm.includes(c)).sort(),
  ];

  const seen = Object.create(null);      // para sufijo #n
  const parts = order.map(rawHdr => {
    const base  = short ? keyMap.get(rawHdr) : normHdr(rawHdr);
    const typ   = cols[rawHdr].DataType || 'UN';
    const tail  = short ? typeAbbr(typ)     : typ;
    const head  = keyColsNorm.includes(normHdr(rawHdr)) ? `PK$${base}` : base;
    let   tok   = `${head}$${tail}`;

    const n = (seen[tok] = (seen[tok] || 0) + 1);
    if (n > 1) tok += `#${n}`;
    return tok;
  });

  return `${parts.length}>${parts.join('-')}`;
}

/* ═══════════════════ stripLengthGroups ═════════════════════════ */
const stripLengthGroups = cols => Object.fromEntries(
  Object.entries(cols).map(([h, c]) => {
    const { lengthGroups, ...rest } = c;
    return [h, rest];
  })
);

/* ═══════════════════ buildMeta principal ═══════════════════════ */
function buildMeta(knfoObj) {
  if (!knfoObj?.columns) throw new Error('invalid knfoObj');

  /* 1 ▸ columnas ligeras */
  const slimCols = stripLengthGroups(knfoObj.columns);

  /* 2 ▸ normalizador + claves */
  const headers  = Object.keys(slimCols);
  const normHdr  = createNormFn(headers);                 // ← requisito nuevo
  const keyMap   = buildKeyMap(headers, normHdr);

  /* 3 ▸ columnas clave normalizadas */
  const keyColsNorm = (knfoObj.__meta__.keyColumns || []).map(normHdr);

  /* 4 ▸ firmas */
  const signature      = buildSignature(slimCols, keyMap, normHdr, keyColsNorm, false);
  const shortSignature = buildSignature(slimCols, keyMap, normHdr, keyColsNorm, true);

  /* 5 ▸ dataRepo fijo */
  const dataRepo = `${knfoObj.__meta__.tcode}$${hash8(shortSignature)}`;

  /* 6 ▸ meta final */
  return {
    ...knfoObj.__meta__,
    repKeyByColumn : Object.fromEntries(keyMap.entries()),
    signature,
    shortSignature,
    dataRepo,
    columns        : slimCols,
  };
}

module.exports = { buildMeta };