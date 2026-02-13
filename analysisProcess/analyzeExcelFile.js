/* src/analysisProcess/analyzeExcelFile.js
 * ──────────────────────────────────────────────────────────────────────────
 *  ✔ Registra avance en excel_processing_state mediante saveStage()
 *  ✔ Sin dependencias de socket
 *  ✔ Compatible con llamadas antiguas (filename solo)             
 *  · Se invoca así:
 *        await analyzeExcelFile(buf, {
 *          fileName : baseName,     // sin “.xlsx”
 *          context  : 'CAN' | 'CAS',
 *          startPct : 10,           // rango opcional 0-100 reservado al caller
 *          endPct   : 90
 *        });
 *    Si no se pasa options, simplemente no escribe progreso.
 * ─────────────────────────────────────────────────────────────────────────*/
const crypto  = require('crypto');
const aq = require('arquero');
const XLSX = require('xlsx');

const determinePossibleUseForGroup  = require('./determinePossibleUses');
const buildCorrelations             = require('./buildCorrelations');
const { detectKeyColumns }             = require('./detectKeyColumns');
const { hash8 } = require('../analysisHelpers');

const {
  normalizeByUsage,
  inferDateOrder,
  parseAmbiguousWithOrder,
  parseFlexibleDate,            // ← se mantiene por compat en otras partes
  parseDateStrict,
  parseDateTimeStrict,
  parseTimeStrict,
  isReasonableDateUTC
} = require('./datetimeUtil');
const { createNumberNormalizer } = require('./numberUtil');

const PXR = require('./parallelXlsxReader');
console.log('[ANALYZE_BOOT] PXR path=', PXR.__resolved_path, 'sig=', PXR.__signature, 'mtime=', PXR.__mtime);
if (!PXR.__signature?.startsWith('pxr-rows-batch')) {
  throw new Error('Wrong parallelXlsxReader loaded from: ' + (PXR.__resolved_path || '?'));
}
const { readSheetAllColumnsStreaming, ensureExcelData } = PXR;

/* ────────────────────────────────────────────────────────────────
 * 1.  Helpers genéricos (nuevos)
 * ────────────────────────────────────────────────────────────────*/
const cloneDeep        = o => JSON.parse(JSON.stringify(o));
const b64url10         = s => crypto.createHash('sha256').update(String(s)).digest('base64url').slice(0, 10);
const isEmptyUse       = u => u === 'Empty' || u === 'ZerosOnly';
const USE_ABB          = { Empty:'EP', StandardText:'ST', StandardDescription:'ST',
                           AlphaNumericCode:'AC', NumericCode:'NC', TextCode:'TC',
                           FreeText:'FT', IntegerValue:'IV', DecimalValue:'DV',
                           DateTime:'DT', Date:'DA', Time:'TI', Boolean:'BO',
                           Email:'EM', CodeWithoutSpaces:'CW', ZerosOnly:'ZP' };
const NUM_SET          = new Set(['IntegerValue','DecimalValue','NumericCode']);

/* longitud máxima de un colObj ----------------------------------*/
const maxCharLengthOf = col =>
  [
    ...(col.lengthGroups || []),
    ...(col.ungroupable  || [])
  ].reduce((m, g) => Math.max(m, g.charLength || 0), 0);

/* content-type completo (FullContentType) -----------------------*/
function buildFullCT(possibleUses, maxLen) {
  const uses      = possibleUses.filter(u => !isEmptyUse(u));
  const onlyCode  = uses.length && uses.every(u => u.endsWith('Code'));
  const onlyNum   = uses.length && uses.every(u => NUM_SET.has(u));

  if (!uses.length)                           return 'ND';
  if (uses.includes('FreeText'))              return `FT[${maxLen}]`;
  if (onlyNum  && uses.includes('DecimalValue')) return `DV[${maxLen}]`;
  if (onlyCode && uses.length > 1)            return 'MC';
  if (uses.includes('StandardText'))          return `ST[${maxLen}]`;
  if (uses.length === 1)                      return `${USE_ABB[uses[0]]}[${maxLen}]`;
  return `MIX[${maxLen}]`;
}

// helpers -------------------------------------------------------
const NUMERIC_SET   = new Set(['DecimalValue', 'IntegerValue', 'NumericCode']);
const DATETIME_SET  = new Set(['DateTime', 'Date', 'Time']);
const TEXTCODE_SET  = new Set(['TextCode', 'AlphaNumericCode', 'Email']);
const FREETEXT_SET  = new Set(['FreeText', 'StandardText']);

const V = [5, 20, 50, 100, 150, 250];          // buckets
const SLACK = 0.25; /* factor de holgura (25 % por defecto) */

const SQL2ABBR = {
  INT: 'IN',
  BIGINT: 'BI',
  'TINYINT(1)': 'TI',
  'CHAR(1)': 'CH',
  'DECIMAL(38,10)': 'DE',
  DATETIME: 'DT',
  DATE: 'DA',
  TIME: 'TM',
  TIMESTAMP: 'TS'
};
const ABBR2SQL = Object.fromEntries(Object.entries(SQL2ABBR).map(([k, v]) => [v, k]));

// Heurísticas por nombre de columna
const DATE_HEADER_HINTS = [
  'date','fecha','fech','fch','fec',
  'posting date','document date','doc date','postingdate','documentdate',
  'value date','due date','creation date','entry date'
];

const TIME_HEADER_HINTS = [
  'time','hora','hr','posting time','document time','creation time'
];
const DATETIME_HEADER_HINTS = [
  'datetime','timestamp','fecha hora','fecha/hora','time stamp'
];

function headerLooksLikeDate(h) {
  const s = String(h || '').toLowerCase();
  return DATE_HEADER_HINTS.some(tok => s.includes(tok));
}
function headerLooksLikeTime(h) {
  const s = String(h || '').toLowerCase();
  return TIME_HEADER_HINTS.some(tok => s.includes(tok));
}
function headerLooksLikeDateTime(h) {
  const s = String(h || '').toLowerCase();
  return DATETIME_HEADER_HINTS.some(tok => s.includes(tok));
}

function hasTimePart(text) {
  return /(?:\d{1,2}:\d{2})(?::\d{2})?/.test(String(text||''));
}

// ¿Predomina formato serial Excel o fracción de día?
function excelSerialHint(arr) {
  let ser = 0, frac = 0, tot = 0;
  for (const v of arr) {
    const s = String(v).trim();
    if (!/^-?\d+(?:\.\d+)?$/.test(s)) continue;
    const n = Number(s);
    if (!Number.isFinite(n)) continue;
    tot++;
    if (n >= 0 && n < 1) frac++;
    else if (n > 20000 && n < 80000) ser++;
  }
  if (!tot) return null;
  const ratio = (ser + frac) / tot;
  if (ratio >= 0.8) return ser > 0 ? 'Date' : 'Time';
  return null;
}
// Decide el orden DMY/MDY/YMD por columna con muestra no ambigua
function decideDateOrderForColumn(values, prefer = 'DMY') {
  const samples = [];
  for (const v of values) {
    const s = String(v || '').trim();
    if (!s) continue;
    if (/[\/\-.]/.test(s) || /[A-Za-z]{3}/.test(s)) samples.push(s);
    if (samples.length >= 1000) break;
  }
  const { order, confidence } = inferDateOrder(samples, { prefer, maxScan: 1000 });
  return { order, confidence: Number(confidence || 0) };
}

function buildNumericHelperForColumn(rawValues, header, fileName) {
  const sample = [];
  for (const v of rawValues || []) {
    const s = toText(v);
    if (!s) continue;
    // Sólo muestreamos si la cadena completa es “numérica pura”
    if (/[A-Za-z]/.test(s)) continue;
    if (/[^0-9.,()\-+ %€£¥₱$]/.test(s)) continue;
    if (!/[0-9]/.test(s)) continue;
    sample.push(s);
    if (sample.length >= 2000) break;
  }

  const norm = createNumberNormalizer(sample, {
    alias: header,
    fileBase: fileName,
    preferThousandsOnly: true
  });

  const hasFrac = (canon) => /\./.test(canon) && !/\.0+$/.test(canon);

  return {
    normalizeInt     : (txt) => norm(txt, 'INT',     { alias: header, fileBase: fileName }),
    normalizeDecimal : (txt) => norm(txt, 'DECIMAL', { alias: header, fileBase: fileName }),
    looksInteger     : (txt) => {
      const d = norm(txt, 'DECIMAL', { alias: header, fileBase: fileName });
      return typeof d === 'string' && d !== '' && (!hasFrac(d));
    },
    looksDecimal     : (txt) => {
      const d = norm(txt, 'DECIMAL', { alias: header, fileBase: fileName });
      return typeof d === 'string' && d !== '' && hasFrac(d);
    }
  };
}

function inferDataType(col, slack = SLACK) {
  // Filtra usos válidos (descarta vacíos)
  const uses = (col.possibleUses || []).filter(
    (u) => u && u !== 'Empty' && u !== 'ZerosOnly'
  );
  if (!uses.length) return null;

  // ───────────────────────────────────────────────────────────────
  // 1) Fecha/Hora con defensa contra "null"/"N/A" singleton
  // ───────────────────────────────────────────────────────────────
  if (uses.some((u) => DATETIME_SET.has(u))) {
    const allGroups = [
      ...(col.lengthGroups || []),
      ...(col.ungroupable || []),
    ];

    // Grupos NO-fecha/hora
    const nonDateGroups = allGroups.filter((g) => {
      const u = g?.possibleUses?.[0];
      return u && !['Empty', 'ZerosOnly'].includes(u) && !DATETIME_SET.has(u);
    });

    // ¿Lo no-fecha son sólo tokens "nulos" (singleton)?
    const onlyNullishSingletons =
      nonDateGroups.length > 0 &&
      nonDateGroups.every((g) => {
        const vals = Array.isArray(g.valueSingles) ? g.valueSingles : [];
        if (g.totalCount !== 1 || vals.length !== 1) return false;
        const v = String(vals[0] ?? '').trim();
        return v === '' || NULLISH_RX.test(v);
      });

    if (onlyNullishSingletons || uses.every((u) => DATETIME_SET.has(u))) {
      if (uses.includes('DateTime')) return 'DATETIME';
      if (uses.includes('Time') && !uses.includes('Date')) return 'TIME';
      return 'DATE';
    }
  }

  // ───────────────────────────────────────────────────────────────
  // 2) Solo numéricos
  // ───────────────────────────────────────────────────────────────
  if (uses.every((u) => NUMERIC_SET.has(u))) {
    if (uses.includes('DecimalValue')) return 'DECIMAL(38,10)';

    const groups = [
      ...(col.lengthGroups || []),
      ...(col.ungroupable || []),
    ];
    // longitud efectiva: descuenta el posible signo negativo
    const maxEff = Math.max(
      0,
      ...groups.map(
        (g) => (g?.charLength || 0) - (g?.charLenFromNegSign ? 1 : 0)
      )
    );
    return maxEff <= 6 ? 'INT' : 'BIGINT';
  }

  // ───────────────────────────────────────────────────────────────
  // 3) Boolean
  // ───────────────────────────────────────────────────────────────
  if (uses.length === 1 && uses[0] === 'Boolean') {
    const allGroups = [
      ...(col.lengthGroups || []),
      ...(col.ungroupable || []),
    ].filter((g) => g?.possibleUses?.[0] === 'Boolean');

    const values = allGroups.flatMap((g) =>
      Array.isArray(g.valueSingles) ? g.valueSingles : []
    );

    // Si todos los boolean vienen como 0/1 → INT
    const numericOnly =
      values.length &&
      values.every((v) => /^\d+$/.test(String(v).trim()));
    if (numericOnly) return 'INT';

    // Si no, elegir CHAR(n) si homogéneo y corto; si no, VARCHAR(n)
    const maxLen = Math.max(1, ...allGroups.map((g) => g?.charLength || 0));
    const sameLen = allGroups.every((g) => (g?.charLength || 0) === maxLen);
    return sameLen && maxLen <= 10 ? `CHAR(${maxLen})` : `VARCHAR(${maxLen})`;
  }

  // ───────────────────────────────────────────────────────────────
  // 4) Solo códigos (sin texto libre)
  // ───────────────────────────────────────────────────────────────
  if (uses.every((u) => TEXTCODE_SET.has(u))) return varcharFor(col);

  // ───────────────────────────────────────────────────────────────
  // 5) Solo texto (libre/estándar)
  // ───────────────────────────────────────────────────────────────
  if (uses.every((u) => FREETEXT_SET.has(u))) return varcharFor(col);

  // ───────────────────────────────────────────────────────────────
  // 6) Mezcla → VARCHAR por seguridad
  // ───────────────────────────────────────────────────────────────
  return varcharFor(col);
}

/* ---------------- helper VARCHAR(bucket) --------------------- */
function varcharFor(col) {
  const maxLen = col.maxCharLength || 0;
  // escogemos el primer tramo ≥ maxLen, o el último si ninguno lo cubre
  const size = V.find(bucket => maxLen <= bucket) || V[V.length - 1];
  return `VARCHAR(${size})`;
}

/* ───────── 6) inferir DataType (y dtStructCode) ───────── */
function inferAndTagDataTypes(columns, keyColumns) {
  const freq = {}; let cCount = 0;
  const pkSet = new Set(keyColumns || []);

  Object.entries(columns).forEach(([hdr, col]) => {
    const beforeUses = (col.possibleUses || []).slice();
    const dt = inferDataType(col);
    if (!dt) return;
    col.DataType = dt;
    cCount++;

    // Traza final por columna objetivo (o global si TRACE_DTTYPES=1)
    _traceType(hdr, 'final-datatype', {
      chosen: dt,
      uses: beforeUses,
      hasAlpha: [...(col.lengthGroups||[]), ...(col.ungroupable||[])]
        .some(g => (g.valueSingles||[]).some(v => /[A-Za-z]/.test(String(v))))
    });

    const tok = pkSet.has(hdr) ? `PK$${dt}` : dt;
    freq[tok] = (freq[tok] || 0) + 1;
  });

  const ordered = Object.keys(freq)
    .sort((a, b) => {
      const pkA = a.startsWith('PK$'), pkB = b.startsWith('PK$');
      if (pkA !== pkB) return pkA ? -1 : 1;
      return a.localeCompare(b, undefined, { sensitivity: 'base' });
    })
    .map(t => (freq[t] > 1 ? `${t}::[${freq[t]}]` : t));

  return `${cCount}>${ordered.join('-')}`;
}

/* ───────────────────────────────────────────────────────────────
 * abrevia dtStructCode → { withDash, noDash }
 * ───────────────────────────────────────────────────────────────*/
function abbrevDtStruct(code) {
  if (!code || typeof code !== 'string') {
    return { withDash: '', noDash: '' };
  }
  // Quita prefijo "N>" si viene (p.ej. "12>...")
  const body = code.replace(/^\d+>/, '');

  const parts = body
    .split('-')
    .filter(Boolean)
    .map((raw) => {
      let tok = String(raw);
      // PK
      const isPk = tok.startsWith('PK$');
      if (isPk) tok = tok.slice(3);

      // Repetición "::[n]"
      const repMatch = tok.match(/::\[(\d+)]$/);
      const rep = repMatch ? parseInt(repMatch[1], 10) : 1;
      if (repMatch) tok = tok.slice(0, tok.indexOf('::'));

      // Abreviación:
      // 1) intenta concordancia exacta (p.ej. "DECIMAL(38,10)")
      // 2) si falla, toma la base y el tamaño (p.ej. "VARCHAR(150)" -> "VC150")
      let abbr = SQL2ABBR[tok];
      if (!abbr) {
        const m = tok.match(/^([A-Z]+)(?:\(([\d,]+)\))?$/);
        const base = (m && m[1]) || '';
        const size = (m && m[2]) || '';
        abbr = base === 'VARCHAR' ? `VC${size}` : (SQL2ABBR[base] || 'UN');
      }

      return `${isPk ? 'K' : ''}${rep}${abbr}`;
    });

  const withDash = parts.join('-');
  return { withDash, noDash: withDash.replace(/-/g, '') };
}

function unifyDecimalSeparator(strVal) {
  let s = strVal.trim();
  if (!s) return s;
  const hasComma = s.includes(',');
  const hasDot = s.includes('.');
  if (hasComma && !hasDot) {
    s = s.replace(/\./g, '');
    s = s.replace(',', '.');
  } else if (!hasComma && hasDot) {
    s = s.replace(/,/g, '');
  }
  return s.trim();
}


// ----------------------------------------------------------------
// Identificación de tipo de valor básico (agrupación).
// ----------------------------------------------------------------
function getValueType(strVal) {
  const trimmed = strVal.trim();
  if (!trimmed) return 'EMPTY';
  const rutRegex = /^[0-9]{1,8}-[0-9Kk]$/;
  if (rutRegex.test(trimmed)) return 'RUT';
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (emailRegex.test(trimmed)) return 'EMAIL';
  if (/^[0-9]+$/.test(trimmed)) return 'NUMERIC';
  const alphaRegex = /^[A-Za-z._-]+$/;
  if (alphaRegex.test(trimmed) && /[A-Za-z]/.test(trimmed) && !/\d/.test(trimmed)) {
    return 'ALPHA';
  }
  const alphaNumRegex = /^[A-Za-z0-9._-]+$/;
  if (alphaNumRegex.test(trimmed)) return 'ALPHANUM';
  return 'PHRASE';
}


function computeGroupStatsAfterUsage(groupValues, usage, order, numHelper) {
  const USAGES_WITH_STATS = new Set([
    'NumericCode',
    'IntegerValue',
    'DecimalValue',
    'DateTime',
    'Date',
    'Time'
  ]);
  if (!USAGES_WITH_STATS.has(usage)) return {};

  // ── numéricos ─────────────────────────────────────────────
  if (['NumericCode', 'DecimalValue', 'IntegerValue'].includes(usage)) {
    const numericVals = [];
    for (const v of groupValues) {
      const t = String(v ?? '').trim();
      if (!t) continue;

      let canon = null;
      if (usage === 'DecimalValue') {
        canon = numHelper?.normalizeDecimal ? numHelper.normalizeDecimal(t) : null;
      } else {
        // IntegerValue / NumericCode → preferimos INT; si no hay, aceptamos DEC sin fracción
        canon = numHelper?.normalizeInt ? numHelper.normalizeInt(t) : null;
        if (!canon && numHelper?.normalizeDecimal) {
          const d = numHelper.normalizeDecimal(t);
          if (d && !/\./.test(d)) canon = d;
        }
      }
      if (!canon) continue;

      const n = Number(canon);
      if (Number.isFinite(n)) numericVals.push(n);
    }
    if (!numericVals.length) return {};
    numericVals.sort((a, b) => a - b);
    return { min: numericVals[0], max: numericVals[numericVals.length - 1] };
  }

  // ── fechas/horas (estricto; igual que clasificación) ─────
  const dates = [];
  for (const v of groupValues) {
    const s = String(v ?? '').trim();
    if (!s) continue;

    let d = null;
    if (usage === 'Date')       d = parseDateStrict(s, { order });
    else if (usage === 'Time')  d = parseTimeStrict(s);
    else                        d = parseDateTimeStrict(s, { order });

    if (d && (usage === 'Time' || isReasonableDateUTC(d))) dates.push(d);
  }
  if (!dates.length) return {};

  dates.sort((a, b) => a.getTime() - b.getTime());
  return { min: dates[0], max: dates[dates.length - 1] };
}

function representValueByUsage(value, usage) {
  // Devuelve SIEMPRE string canónica para Date/Time/DateTime
  if (['DateTime', 'Date', 'Time'].includes(usage)) {
    return String(normalizeByUsage(value, usage));
  }
  if (['NumericCode', 'DecimalValue', 'IntegerValue'].includes(usage)) {
    return String(value);
  }
  return String(value);
}

// ----------------------------------------------------------------
// homogeneidad
// ----------------------------------------------------------------
function computePenaltyFactor(distinctLengthsArr) {
  if (distinctLengthsArr.length === 1 && distinctLengthsArr[0] === 1) return 0;
  const has1 = distinctLengthsArr.includes(1);
  const has2 = distinctLengthsArr.includes(2);
  const has3 = distinctLengthsArr.includes(3);
  if (has1) return 0.4;
  if (has2) return 0.2;
  if (has3) return 0.1;
  return 0;
}
function computeHomogeneityRate(charLengthCount, sumCount) {
  const distinctLengthsArr = Object.keys(charLengthCount)
    .map(x => parseInt(x, 10))
    .sort((a, b) => a - b);

  if (distinctLengthsArr.length < 2 || sumCount <= 0) return 0;

  let entropy = 0;
  for (const len of distinctLengthsArr) {
    const p = charLengthCount[len] / sumCount;
    if (p > 0) {
      entropy -= p * Math.log2(p);
    }
  }
  const maxEntropy = Math.log2(distinctLengthsArr.length);
  const normalizedEntropy = maxEntropy > 0 ? entropy / maxEntropy : 0;

  const MAX_EXPECTED = 10;
  const adjustmentFactor = (distinctLengthsArr.length - 1) / (MAX_EXPECTED - 1);
  const baseIndex = normalizedEntropy * adjustmentFactor;

  const penaltyFactor = computePenaltyFactor(distinctLengthsArr);
  return baseIndex + penaltyFactor;
}

function buildGroupsForColumn(header, distinctValues, dateOrder, numHelper) {
  const byKey = Object.create(null);
  for (const raw of distinctValues || []) {
    const s = typeof raw === 'string' ? raw : toText(raw);
    const trimmed   = String(s ?? '').trim();
    const basicType = getValueType(trimmed);
    const groupKey  = `${trimmed.length}::${basicType}`;
    (byKey[groupKey] ||= []).push(trimmed);
  }

  const groupsArr = [];
  const ungroupableArr = [];

  const statFor = (values, usage, order) =>
    computeGroupStatsAfterUsage(values, usage, order, numHelper);

  for (const key of Object.keys(byKey)) {
    const vals = byKey[key];
    const [lenStr] = key.split('::');
    const groupCharLength = parseInt(lenStr, 10) || 0;

    const numericOnly = vals.every((v) => /^-?\d+$/.test(v));
    const charLenFromNegSign =
      numericOnly && vals.some((v) => v.startsWith('-') && v.length === groupCharLength);

    const usage =
      groupCharLength === 0
        ? 'Empty'
        : determinePossibleUseForGroup(
            vals,
            { valueSingles: vals.slice(), dateOrder },
            { order: dateOrder, num: numHelper }     // ← inyectamos plan numérico
          );

    const subStats = statFor(vals, usage, dateOrder);

    const gObj = {
      charLength: groupCharLength,
      charLenFromNegSign,
      totalCount: vals.length,
      possibleUses: [usage],
      numericOnly,
      valueSingles: vals.slice(),
    };
    if (subStats && Object.keys(subStats).length) gObj.ValuesStats = subStats;

    if (vals.length === 1) {
      delete gObj.ValuesStats;
      ungroupableArr.push(gObj);
    } else {
      groupsArr.push(gObj);
    }
  }

  const validGroups = groupsArr.filter(
    (g) => !['Empty', 'ZerosOnly'].includes(g.possibleUses[0])
  );
  const allAreCode =
    validGroups.length > 0 &&
    validGroups.every((g) => g.possibleUses[0].endsWith('Code'));

  if (allAreCode) {
    for (const u of ungroupableArr) {
      const curUsage = u.possibleUses[0];
      if (!['Empty', 'ZerosOnly'].includes(curUsage)) {
        const singleVal = (u.valueSingles && u.valueSingles[0]) || '';
        const vt = getValueType(String(singleVal).trim());
        let forcedCode = 'TextCode';
        switch (vt) {
          case 'ALPHANUM': forcedCode = 'AlphaNumericCode'; break;
          case 'NUMERIC':  forcedCode = 'NumericCode';      break;
          default:         forcedCode = 'TextCode';         break;
        }
        u.possibleUses = [forcedCode];
      }
    }
  }

  return { groupsArr, ungroupableArr };
}

const TRACE_COLUMNS = new Set(['movement type','movement_type','movementtype']);
const _lower = s => String(s || '').toLowerCase();

function _traceType(hdr, stage, obj) {
  if (TRACE_COLUMNS.has(_lower(hdr)) || process.env.TRACE_DTTYPES === '1') {
    try { console.warn(`[TYPE-TRACE][${hdr}] ${stage} ${JSON.stringify(obj)}`); } catch {}
  }
}

function possiblyForceNumericUsage(header, groupsArr, columnsData, dateOrder, numHelper) {
  const cloned = groupsArr.map(g => ({
    ...g,
    possibleUses: [...(g.possibleUses || [])]
  }));

  // Si ya parece fecha/hora → no tocar
  const alreadyDateLike = cloned.some(g => ['Date','DateTime','Time'].includes(g.possibleUses?.[0]));
  if (alreadyDateLike) return cloned;

  const rows = (columnsData[header] || []).map(o => String(o.value ?? ''));
  let nonEmpty = 0, numericCnt = 0, decimalSeen = false, dtOk = 0, alphaCnt = 0;

  // ESCANEA TODAS LAS FILAS (sin límite)
  for (const raw of rows) {
    const s = raw.trim();
    if (!s) continue;
    nonEmpty++;

    if (/[A-Za-z]/.test(s)) alphaCnt++;

    if (numHelper) {
      const d = numHelper.normalizeDecimal(s);
      if (d) {
        numericCnt++;
        if (/\./.test(d) && !/\.0+$/.test(d)) decimalSeen = true;
      } else {
        const i = numHelper.normalizeInt(s);
        if (i) numericCnt++;
      }
    } else {
      const num = Number(s.replace(/\./g, '').replace(',', '.'));
      if (Number.isFinite(num)) {
        numericCnt++;
        if (Math.abs(num % 1) > 1e-12) decimalSeen = true;
      }
    }

    const d = dateOrder ? parseAmbiguousWithOrder(s, dateOrder) : parseFlexibleDate(s);
    if (d && !isNaN(d)) dtOk++;
  }

  const shareNumeric = nonEmpty ? numericCnt / nonEmpty : 0;
  const shareDateOk  = nonEmpty ? dtOk         / nonEmpty : 0;

  // Métrica de homogeneidad por longitudes (solo grupos válidos)
  const validGroups = cloned.filter(g => !['Empty','ZerosOnly'].includes(g.possibleUses?.[0]));
  const charLengthCount = {};
  let sumCount = 0;
  for (const g of validGroups) {
    charLengthCount[g.charLength] = (charLengthCount[g.charLength] || 0) + g.totalCount;
    sumCount += g.totalCount;
  }
  const homogeneityRate = computeHomogeneityRate(charLengthCount, sumCount);

  // Traza previa
  _traceType(header, 'pre-force-metrics', {
    nonEmpty, numericCnt, alphaCnt, shareNumeric: +shareNumeric.toFixed(4),
    shareDateOk: +shareDateOk.toFixed(4), homogeneityRate: +homogeneityRate.toFixed(4),
    hasAlphaSamples: rows.some(v => /^[A-Za-z]/.test(v)),
    sampleAlpha: rows.filter(v => /[A-Za-z]/.test(v)).slice(0, 10)
  });

  // NUEVA REGLA: si existe cualquier valor con letras → jamás forzar a numérico
  if (alphaCnt > 0) {
    _traceType(header, 'skip-force_alpha-present', { alphaCnt });
    return cloned;
  }

  // Si no hay evidencia fuerte de fecha/hora y domina lo numérico de forma homogénea…
  if (homogeneityRate > 0.3 && shareNumeric >= 0.95 && shareDateOk <= 0.20) {
    const forced = decimalSeen ? 'DecimalValue' : 'IntegerValue';
    // Importante: SOLO tocar grupos que ya son numéricos o numéricos puros; NO sobrescribir *Code
    for (const g of validGroups) {
      const u = g.possibleUses?.[0] || '';
      const looksNumeric = (u === 'NumericCode' || u === 'IntegerValue' || u === 'DecimalValue' || g.numericOnly === true);
      if (looksNumeric) g.possibleUses = [forced];
    }
    _traceType(header, 'forced-numeric', { forced, decimalSeen });
  } else {
    _traceType(header, 'no-force', { shareNumeric: +shareNumeric.toFixed(4), homogeneityRate: +homogeneityRate.toFixed(4) });
  }

  return cloned;
}

/*********************************************************************
*  finalizeColumnStats (nueva versión)                               *
*  ▸ NO muta el objeto recibido; devuelve una copia actualizada      *
*********************************************************************/
function finalizeColumnStats(columnObj) {
  const cloned = {
    ...columnObj,
    lengthGroups: (columnObj.lengthGroups || []).map(g => ({ ...g })),
    ungroupable:  (columnObj.ungroupable || []).map(u => ({ ...u }))
  };

  const validGroups = cloned.lengthGroups.filter(g => {
    const u = g.possibleUses?.[0];
    return u && u !== 'Empty' && u !== 'ZerosOnly';
  });
  if (!validGroups.length) return cloned;

  const { charLength: commonLen, possibleUses: [commonUsage] } = validGroups[0];

  const uniformChar  = validGroups.every(g => g.charLength      === commonLen);
  const uniformUsage = validGroups.every(g => g.possibleUses[0] === commonUsage);

  const ALLOWED = new Set([
    'NumericCode', 'DecimalValue', 'IntegerValue',
    'DateTime', 'Date', 'Time'
  ]);

  if (uniformChar && uniformUsage && ALLOWED.has(commonUsage)) {
    let globalMin = null, globalMax = null;

    validGroups.forEach(g => {
      if (!g.ValuesStats) return;
      const { min, max } = g.ValuesStats;
      if (min !== null && (globalMin === null || min < globalMin)) globalMin = min;
      if (max !== null && (globalMax === null || max > globalMax)) globalMax = max;
    });

    if (globalMin !== null && globalMax !== null) {
      cloned.ValuesStats = {
        min: representValueByUsage(globalMin, commonUsage),
        max: representValueByUsage(globalMax, commonUsage),
        charLength: commonLen
      };
    }
  }

  return cloned;
}


/**
 *  unifyNumericUngroupable
 *  ────────────────────────────────────────────────────────────────
 *  Si **TODOS** los grupos (lengthGroups + ungroupable) comparten
 *  el mismo possibleUse y este es un tipo numérico permitido
 *  ('NumericCode' | 'DecimalValue' | 'IntegerValue'), entonces
 *  se fuerza a los ungroupable para que adopten ese mismo uso.
 *
 *  No muta el objeto original: devuelve una copia profunda a
 *  un nivel de arrays/objetos.
 * ----------------------------------------------------------------*/
function unifyNumericUngroupable(columnObj) {
  const NUMERIC_USES = new Set(['NumericCode', 'DecimalValue', 'IntegerValue']);

  // ------- 1. Clonar (superficial + arrays) -------
  const cloned = {
    ...columnObj,
    lengthGroups: (columnObj.lengthGroups || []).map(g => ({ ...g })),
    ungroupable : (columnObj.ungroupable   || []).map(u => ({ ...u }))
  };

  if (!cloned.lengthGroups.length || !cloned.ungroupable.length) {
    return cloned;                                // nada que unificar
  }

  // ------- 2. Evaluar los possibleUses en TODOS los grupos -------
  const allGroups = [...cloned.lengthGroups, ...cloned.ungroupable];
  const firstNumericGrp = allGroups.find(g => NUMERIC_USES.has(g.possibleUses?.[0]));

  // Si no hay ningún grupo numérico → nada que hacer.
  if (!firstNumericGrp) return cloned;

  const referenceUse = firstNumericGrp.possibleUses[0];

  const allSameNumeric = allGroups.every(
    g => g.possibleUses?.[0] === referenceUse && NUMERIC_USES.has(referenceUse)
  );

  // ------- 3. Forzar unificación si corresponde -------
  if (allSameNumeric) {
    cloned.ungroupable.forEach(u => { u.possibleUses = [referenceUse]; });
  }

  return cloned;
}

/**********************************************************************
*  detectOutliers (versión pura)                                      *
*  ▸ Devuelve copia; añade col.outliers sin tocar el original         *
**********************************************************************/
function detectOutliers(analysis) {
  const cloned = {
    ...analysis,
    columns: Object.fromEntries(
      Object.entries(analysis.columns).map(([k, v]) => [k, { ...v }])
    )
  };

  for (const [colName, colObj] of Object.entries(cloned.columns)) {
    const outlierArr = [];

    (colObj.lengthGroups || []).forEach((gr, idx) => {
      const vs = gr.ValuesStats;
      if (vs && typeof vs.min === 'number' && typeof vs.max === 'number') {
        const range = vs.max - vs.min;
        if (range > 100000) {
          outlierArr.push({
            groupIndex: idx,
            range,
            usage: gr.possibleUses[0]
          });
        }
      }
    });

    if (outlierArr.length) {
      cloned.columns[colName] = { ...colObj, outliers: outlierArr };
    }
  }

  return cloned;
}

/* --------------------------------------------------------------- *
 *  top-10 más frecuentes de un array de strings                   *
 *    → [{ v:'valor', p:0.27 }, … ]  (p = frecuencia relativa)     *
 * --------------------------------------------------------------- */
function calcTop10Freq(values = []) {
  const freq = Object.create(null);
  let total  = 0;
  for (const vRaw of values) {
    const v = String(vRaw ?? '').trim();        // homogeneidad
    if (v === '') continue;                     // ignora vacíos
    freq[v] = (freq[v] || 0) + 1;
    total++;
  }
  return total === 0
    ? []
    : Object.entries(freq)
        .sort(([,a],[,b]) => b - a)             // desc  por frecuencia
        .slice(0, 10)
        .map(([v,c]) => ({ v, p: c / total }));
}

function enrichColumn(
  colName,
  groupsArr,
  ungroupArr,
  columnsData,
  correlationsMap
) {
  /* 2.1 fusionar info de grupos + ungroupable */
  const col = {
    lengthGroups : cloneDeep(groupsArr),
    ungroupable  : cloneDeep(ungroupArr)
  };

  /* 2.2 posibleUses + maxLen */
  const uses = new Set();
  [...col.lengthGroups, ...col.ungroupable].forEach(g => {
    const u = g.possibleUses?.[0];
    if (u) uses.add(u);
  });
  col.possibleUses  = [...uses];
  col.maxCharLength = maxCharLengthOf(col);

  /* 2.3 FullContentType / ContentType */
  col.FullContentType = buildFullCT(col.possibleUses, col.maxCharLength);
  col.ContentType     = b64url10(col.FullContentType);

  /* 2.4 top-10 frecuencias */
  col.top10freq = calcTop10Freq(columnsData[colName].map(o => o.value));

  /* 2.5 CorrelationChain (solo para *Code) */
  if (col.possibleUses.some(u => u.endsWith('Code'))) {
    const chain = new Set();
    const allGr = [...col.lengthGroups, ...col.ungroupable];
    allGr.forEach((g, idx) => {
      const ref = `${colName}::lengthGroups[${idx}]`;
      Object.values(correlationsMap).flat()
        .filter(o => o.xGroup === ref)
        .flatMap(o => o.correlated || [])
        .forEach(o => chain.add(o.yGroup.split('::')[0]));
    });
    col.CorrelationChain = [...chain].sort().join('>');
  } else {
    col.CorrelationChain = '';
  }

  /* Differences & CorrelationDifferences se rellenarán en la
     fase global, así que van inicializadas vacías           */
  col.Differences            = [];
  col.CorrelationDifferences = [];
  col.b64urlCode             = '';           // se calcula después
  return col;
}

// ───────────────────────────────────────────────────────────────
// Breakpoint si una columna "parece" Date/Time pero no fue tipada así
// ───────────────────────────────────────────────────────────────
function _dtEvidenceFor(values, order) {
  let nonEmpty = 0, dateOnly = 0, timeOnly = 0, dateTime = 0, timeToken = 0;

  for (const v of values || []) {
    const s = toText(v).trim();
    if (!s) continue;
    nonEmpty++;

    // Preferimos detectar DateTime primero, luego Date y Time
    if (parseDateTimeStrict(s, { order })) {
      dateTime++;
    } else {
      if (parseDateStrict(s, { order })) dateOnly++;
      if (parseTimeStrict(s))           timeOnly++;
    }
    if (hasTimePart(s)) timeToken++;
  }

  const serialHint = excelSerialHint(values.map(toText)) || null;

  const ratioDate      = nonEmpty ? (dateOnly + dateTime) / nonEmpty : 0;
  const ratioTime      = nonEmpty ? (timeOnly) / nonEmpty : 0;
  const ratioTimeToken = nonEmpty ? (timeToken) / nonEmpty : 0;

  const strongDate = ratioDate >= 0.60 || serialHint === 'Date';
  const strongTime = ratioTime >= 0.60 || ratioTimeToken >= 0.60 || serialHint === 'Time';

  // Si hay evidencia fuerte de ambos, tratamos como Date/DateTime
  let expected = null;
  if (strongTime && !strongDate) expected = 'TIME';
  else if (strongDate) expected = 'DATE_OR_DATETIME';

  return {
    nonEmpty, ratioDate, ratioTime, ratioTimeToken, serialHint, expected
  };
}

function _maybeDebugDateTimeMismatch(hdr, dataType, rawValues, order) {
  const dt = String(dataType || '').toUpperCase();
  const ev = _dtEvidenceFor(rawValues, order);

  // Esperábamos TIME y tiparon otra cosa
  if (ev.expected === 'TIME' && dt !== 'TIME') {
    console.warn(
      `[DT-MISMATCH][${hdr}] esperado=TIME  detectado=${dt} `
      + `(ratioTime=${ev.ratioTime.toFixed(2)}, tokens=${ev.ratioTimeToken.toFixed(2)}, serial=${ev.serialHint||'-'})`
    );
    if (process.env.BREAK_ON_DT_MISMATCH === '1') debugger;
  }

  // Esperábamos DATE/DATETIME y tiparon algo fuera de esa familia (TIME no vale)
  const isDate = (dt === 'DATE' || dt === 'DATETIME' || dt === 'TIMESTAMP');
  if (ev.expected === 'DATE_OR_DATETIME' && !isDate) {
    console.warn(
      `[DT-MISMATCH][${hdr}] esperado=DATE|DATETIME  detectado=${dt} `
      + `(ratioDate=${ev.ratioDate.toFixed(2)}, serial=${ev.serialHint||'-'})`
    );
    if (process.env.BREAK_ON_DT_MISMATCH === '1') debugger;
  }
}

/* ────────────────────────────────────────────────────────────────
 * 3.  postProcessGlobal(analysis) – **sustituye** el antiguo
 *     bloque “post-proceso global”
 * ────────────────────────────────────────────────────────────────*/
function postProcessGlobal(analysis) {
  /* 3.1  agrupar por FullContentType para detectar diferencias */
  const byFCT = {};
  Object.entries(analysis.columns).forEach(([name,col]) => {
    if (!col.FullContentType || col.FullContentType === 'ND') return;
    (byFCT[col.FullContentType] ||= []).push(name);
  });

  Object.values(byFCT).forEach(list => {
    if (list.length < 2) return;
    const refCol = analysis.columns[list[0]];       // referencia
    list.forEach(cn => {
      const c = analysis.columns[cn];
      /* Differences */
      const valsRef = new Set(refCol.top10freq.map(o=>o.v));
      c.Differences = c.top10freq.map(o=>o.v).filter(v=>!valsRef.has(v));
      /* CorrelationDifferences */
      const refChain = new Set(refCol.CorrelationChain.split('>').filter(Boolean));
      c.CorrelationDifferences =
        c.CorrelationChain.split('>').filter(x => x && !refChain.has(x));
    });
  });

  /* 3.2  b64urlCode por columna */
  Object.values(analysis.columns).forEach(col => {
    col.b64urlCode = `${col.ContentType}$${b64url10(col.Differences.join('|'))}`;
  });

  /* 3.3  columnas agrupadas por DataType */  
  const byDt = {};
  Object.entries(analysis.columns).forEach(([n,c]) => {
    const dt = c.DataType || 'Unknown';
    (byDt[dt] ||= []).push(n);
  });
  Object.values(byDt).forEach(a => a.sort());
  analysis.__meta__.columnsByDataType = byDt;
  analysis.__meta__.totalColumns      = Object.keys(analysis.columns).length;

  /* 3.4  structCode + b64urlCode global */
  const pkSet = new Set(analysis.__meta__.keyColumns || []);
  const freq  = {};
  Object.entries(analysis.columns).forEach(([h,c]) => {
    if (!c.FullContentType || c.FullContentType === 'ND') return;
    const tok = pkSet.has(h) ? `PK$${c.FullContentType}` : c.FullContentType;
    freq[tok] = (freq[tok] || 0) + 1;
  });
  const fmt = t => freq[t] > 1 ? `${t}::[${freq[t]}]` : t;
  const pk  = Object.keys(freq).filter(t=>t.startsWith('PK$')).sort().map(fmt);
  const oth = Object.keys(freq).filter(t=>!t.startsWith('PK$')).sort().map(fmt);
  analysis.__meta__.structCode = [...pk,...oth].join('-');
  analysis.__meta__.b64urlCode = b64url10(analysis.__meta__.structCode);
}

// ── normalización de tokens "vacíos" escritos como texto ───────────
const NULLISH_RX = /^(?:null|n\/a)$/i;   // añade más si te conviene: ^(?:null|n\/a|none|s\/f|n\/d)$
function normalizeStringCell(s) {
  const t = String(s ?? '').trim();
  return NULLISH_RX.test(t) ? '' : t;
}

const toText = (v) => {
  if (v === null || v === undefined) return '';
  if (typeof v === 'string') return normalizeStringCell(v);           // ← ¡clave!
  if (v instanceof Date)      return normalizeByUsage(v, 'DateTime'); // canónico UTC
  if (typeof v === 'boolean') return v ? 'true' : 'false';
  if (typeof v === 'number')  return Number.isFinite(v) ? String(v) : '';
  return String(v);
};

async function analyzeExcelFile(
  fileBuffer,
  { fileName = '', context = '', startPct = 0, endPct = 100 } = {}
) {
  console.log(`[ANALYZE][${fileName}] 0.0 %  Iniciando análisis (ctx=${context || '-'})`);

  const pctSpan  = endPct - startPct || 100;
  const clampPct = (p) => Math.max(startPct, Math.min(endPct, p));
  const report   = (relPct, msg) =>
    console.log(`[ANALYZE][${fileName}] ${clampPct(startPct + relPct * pctSpan).toFixed(1)} %  ${msg}`);

  // 1) Lectura + extracción (convencional o streaming)
  const IO_PROGRESS_SLICE = 0.02; // 2% del rango reservado a I/O
  const {
    headers,
    rawColumnsData,
    colStats,
    totalRows,
    rowsWithData
  } = await ensureExcelData(fileBuffer, { fileName, report, ioSlice: IO_PROGRESS_SLICE });

  // 2) columnsData (value siempre string; fila Excel = i+2)
  const columnsData = Object.fromEntries(
    headers.map(h => [h, (rawColumnsData[h] || []).map((v, i) => ({ value: toText(v), row: i + 2 }))])
  );

  // 3) Estructura base
  const analysis = {
    columns: {},
    __meta__: {
      processingTimes: {},
      totalRows,
      rowsWithData,
      totalColumns: headers.length,
      keyColumns: [],
      dateOrderByColumn: {} // nuevo: orden DMY/MDY/YMD por columna
    }
  };

  // 4) Análisis por columna (90% del rango)
  const totalCols  = headers.length || 1;
  const colPctStep = 0.90 / totalCols;
  let processedCols = 0;

  for (const hdr of headers) {
    const tCol = Date.now();

    // 4.1) Inferir orden de fecha por columna a partir de la materia prima
    const rawValsForOrder = (rawColumnsData[hdr] || []).map(v =>
      (typeof v === 'string' ? v : toText(v))
    );
    const { order: colOrder, confidence } = decideDateOrderForColumn(rawValsForOrder, 'DMY');
    analysis.__meta__.dateOrderByColumn[hdr] = { order: colOrder || null, confidence };

    // if( hdr.toLowerCase()=== "item of requisition")
    //   debugger;

    // 4.2) Construir grupos usando el orden inferido    
    const distinctByHeader = Object.fromEntries(
      headers.map(h => {
        const src = rawColumnsData[h] || [];
        const set = new Set(src.map(v => toText(v)));  // ← normaliza "null" → ''
        return [h, Array.from(set)];
      })
    );

    const numHelper = buildNumericHelperForColumn(rawColumnsData[hdr] || [], hdr, fileName);
    const { groupsArr, ungroupableArr } =
      buildGroupsForColumn(hdr, distinctByHeader[hdr], colOrder, numHelper);

    let colObj = {
      lengthGroups: possiblyForceNumericUsage(hdr, groupsArr, columnsData, colOrder, numHelper)
    };

    if (ungroupableArr.length) colObj.ungroupable = ungroupableArr;
    colObj = unifyNumericUngroupable(finalizeColumnStats(colObj));

    analysis.columns[hdr] = enrichColumn(
      hdr,
      colObj.lengthGroups,
      colObj.ungroupable || [],
      columnsData,
      analysis.correlations || {}
    );
    analysis.__meta__.processingTimes[hdr] = { totalMs: Date.now() - tCol };

    processedCols++;
    if (processedCols === totalCols || processedCols % Math.ceil(totalCols / 10) === 0) {
      report(0.10 + processedCols * colPctStep, `Columnas procesadas: ${processedCols}/${totalCols}`);
    }
  }

  // 5) Fases globales
  detectKeyColumns(analysis, rawColumnsData, colStats);
  report(0.93, 'Key columns detectadas');

  detectOutliers(analysis);
  report(0.95, 'Outliers detectados');

  analysis.correlations           = buildCorrelations(analysis);
  analysis.columnCharacterization = {};
  analysis.__meta__.dtStructCode  = inferAndTagDataTypes(analysis.columns, analysis.__meta__.keyColumns);

  // ── Chequeo tardío de desajustes Date/Time con breakpoint opcional
  for (const hdr of Object.keys(analysis.columns)) {
    const col = analysis.columns[hdr] || {};
    const rawVals = rawColumnsData[hdr] || [];
    const ordInfo = analysis.__meta__.dateOrderByColumn[hdr];
    const order   = ordInfo && ordInfo.order || null;
    _maybeDebugDateTimeMismatch(hdr, col.DataType, rawVals, order);
  }

  postProcessGlobal(analysis);
  report(0.98, 'Post-proceso global aplicado');

  // 6) SegmentFields
  const segCandidates = { cat_datasource: context };
  const rx = /@\+([A-Za-z0-9]+)=([A-Za-z0-9.]+)/g;
  let m; while ((m = rx.exec(fileName)) !== null) segCandidates[m[1]] = m[2];

  const canon = (s) => String(s).toLowerCase().replace(/::.*$/, '').trim();
  const segmentFields = {};
  for (const [key, val] of Object.entries(segCandidates)) {
    const match = headers.find(h => canon(h) === canon(key));
    if (match) {
      const distinct = new Set((rawColumnsData[match] || []).map(v => (typeof v === 'string' ? v : toText(v)).trim()).filter(v => v !== ''));
      if (distinct.size === 1 && distinct.has(val)) continue;
    }
    segmentFields[key] = val;
  }
  analysis.__meta__.segmentFields = segmentFields;

  // 7) DataRepo
  const base               = fileName.replace(/\.xlsx$/i, '');
  const [, classInfo = ''] = base.split('-');
  const [tcode = '']       = classInfo.split(/[#@]/);
  analysis.__meta__.tcode       = tcode;
  analysis.__meta__.abrDtStruct = abbrevDtStruct(analysis.__meta__.dtStructCode);
  analysis.__meta__.dataRepo    = `${tcode}$${hash8(analysis.__meta__.abrDtStruct.noDash)}`;

  // 8) Limpieza de payload pesado
  Object.values(analysis.columns).forEach(col => {
    (col.lengthGroups || []).forEach(g => (g.candidateDetails = null));
    (col.ungroupable  || []).forEach(g => (g.candidateDetails = null));
  });

  report(1, 'Análisis completado');
  return analysis;
}

module.exports = { analyzeExcelFile, abbrevDtStruct, postProcessGlobal };