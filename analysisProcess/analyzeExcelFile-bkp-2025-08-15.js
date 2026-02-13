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

const { computeBothRanges     }         = require('./compressRanges');
const determinePossibleUseForGroup  = require('./determinePossibleUses');
const buildCorrelations             = require('./buildCorrelations');
const { detectKeyColumns }             = require('./detectKeyColumns');
const { hash8 } = require('../analysisHelpers');

const {
  normalizeByUsage,
  parseFlexibleDate,
  excelSerialToDateUTC,
  inferDateOrder,
  parseAmbiguousWithOrder,
} = require('./datetimeUtil');

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

function inferDataType(col, slack = SLACK) {
  const uses = (col.possibleUses || [])
    .filter(u => !['Empty', 'ZerosOnly'].includes(u));

  if (!uses.length) return null;

  // 1. Solo numérico
  if (uses.every(u => NUMERIC_SET.has(u))) {
    if (uses.includes('DecimalValue')) return 'DECIMAL(38,10)';
    const maxEff = Math.max(
      0,
      ...[...(col.lengthGroups || []), ...(col.ungroupable || [])]
        .map(g => (g.charLength || 0) - (g.charLenFromNegSign ? 1 : 0))
    );
    return maxEff <= 6 ? 'INT' : 'BIGINT';
  }

  // 2. Solo fecha/hora (distingue DATE/TIME/DATETIME)
  if (uses.every(u => DATETIME_SET.has(u))) {
    if (uses.includes('DateTime')) return 'DATETIME';
    if (uses.includes('Time') && !uses.includes('Date')) return 'TIME';
    if (uses.includes('Date') && !uses.includes('Time')) return 'DATE';
    return 'DATETIME';
  }

  // 3. Solo Boolean
  if (uses.length === 1 && uses[0] === 'Boolean') {
    const allGroups = [...(col.lengthGroups || []), ...(col.ungroupable || [])]
      .filter(g => g.possibleUse === 'Boolean');
    const values = allGroups.flatMap(g =>
      (Array.isArray(g.values) ? g.values : []).map(v => v.split('::[')[0])
    );
    const numericOnly = values.length && values.every(v => /^\d+$/.test(v));
    if (numericOnly) return 'INT';

    const maxLen = Math.max(1, ...allGroups.map(g => g.charLength || 0));
    const sameLen = allGroups.every(g => g.charLength === maxLen);
    return sameLen && maxLen <= 10 ? `CHAR(${maxLen})` : `VARCHAR(${maxLen})`;
  }

  // 4. Solo códigos
  if (uses.every(u => TEXTCODE_SET.has(u))) return varcharFor(col);

  // 5. Solo texto
  if (uses.every(u => FREETEXT_SET.has(u))) return varcharFor(col);

  // 6. Mezcla → VARCHAR
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

  const pkSet   = new Set(keyColumns || []);

  Object.entries(columns).forEach(([hdr, col]) => {
    const dt = inferDataType(col);
    if (!dt) return;
    col.DataType = dt;
    cCount++;
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
  const body = code.replace(/^\d+>/, '');
  const parts = body.split('-').map((tok) => {
    const pk = tok.startsWith('PK$');
    if (pk) tok = tok.slice(3);

    const mRep = tok.match(/::\[(\d+)]$/);
    const rep  = mRep ? +mRep[1] : 1;
    if (mRep) tok = tok.slice(0, tok.indexOf('::'));

    //const [, base = '', size = ''] = tok.match(/^([A-Z]+)(?:\((\d+)\))?$/) || [];
    const [, base = '', size = ''] = tok.match(/^([A-Z]+)(?:\(([\d,]+)\))?$/) || [];

    let ab = SQL2ABBR[tok];           // ← intenta concordancia exacta primero
    if (!ab) {
      const [, base = '', size = ''] = tok.match(/^([A-Z]+)(?:\(([\d,]+)\))?$/) || [];
      ab = base === 'VARCHAR' ? `VC${size}` : SQL2ABBR[base] || 'UN';
    }
    //const ab = base === 'VARCHAR' ? `VC${size}` : SQL2ABBR[base] || 'UN';

    return `${pk ? 'K' : ''}${rep}${ab}`;
  });
  return { withDash: parts.join('-'), noDash: parts.join('') };
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


function computeGroupStatsAfterUsage(groupValues, usage, order) {
  const USAGES_WITH_STATS = new Set([
    'NumericCode',
    'IntegerValue',
    'DecimalValue',
    'DateTime',
    'Date',
    'Time'
  ]);
  if (!USAGES_WITH_STATS.has(usage)) return {};

  const numberRegex = /^-?[\d]+(?:[.,]\d+)?$/;

  // numérico
  if (['NumericCode', 'DecimalValue', 'IntegerValue'].includes(usage)) {
    const numericVals = [];
    for (const v of groupValues) {
      if (typeof v !== 'string') continue;
      const t = v.trim();
      if (numberRegex.test(t)) {
        const n = parseFloat(unifyDecimalSeparator(t));
        if (!isNaN(n)) numericVals.push(n);
      }
    }
    if (!numericVals.length) return {};
    numericVals.sort((a, b) => a - b);
    return { min: numericVals[0], max: numericVals[numericVals.length - 1] };
  }

  // fecha/hora
  if (['Date', 'Time', 'DateTime'].includes(usage)) {
    const arr = [];
    for (const v of groupValues) {
      if (v == null) continue;
      const s = String(v).trim();
      if (!s) continue;

      // 1) numérico en texto → Excel serial / fracción / epoch
      if (numberRegex.test(s)) {
        const f = parseFloat(unifyDecimalSeparator(s));
        if (isFinite(f)) {
          if (f >= 0 && f < 1) {
            const d = new Date(Date.UTC(1970,0,1) + Math.round(f * 24 * 3600 * 1000));
            if (!isNaN(d)) { arr.push(d); continue; }
          }
          if (f > 20000 && f < 80000) {
            const d = excelSerialToDateUTC(f);
            if (!isNaN(d)) { arr.push(d); continue; }
          }
          if (f > 1e11 && f < 1e14) { const d = new Date(f); if (!isNaN(d)) { arr.push(d); continue; } }
          if (f > 1e9 && f < 1e11)  { const d = new Date(f * 1000); if (!isNaN(d)) { arr.push(d); continue; } }
        }
      }

      // 2) string ambiguo → usa orden por columna si existe
      if (order) {
        const d = parseAmbiguousWithOrder(s, order);
        if (d && !isNaN(d)) { arr.push(d); continue; }
      }

      // 3) último intento flexible (ISO / no ambiguo)
      const d2 = parseFlexibleDate(s);
      if (d2 && !isNaN(d2)) arr.push(d2);
    }
    if (!arr.length) return {};
    arr.sort((a, b) => a.getTime() - b.getTime());
    return { min: arr[0], max: arr[arr.length - 1] };
  }

  return {};
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

/**
 * Dado un array de números, devuelve un array de rangos continuos
 * ["3-6","5203-5203","6001-6003"] en lugar de [3,4,5,6,5203,6001,6002,6003]
 */
function compressRanges(numbers) {
  if (!Array.isArray(numbers) || numbers.length === 0) return [];
  const sorted = Array.from(new Set(numbers)).sort((a, b) => a - b);
  const ranges = [];
  let start = sorted[0];
  let end = sorted[0];
  for (let i = 1; i < sorted.length; i++) {
    const n = sorted[i];
    if (n === end + 1) {
      end = n;
    } else {
      ranges.push(start === end ? `${start}` : `${start}-${end}`);
      start = n;
      end = n;
    }
  }
  ranges.push(start === end ? `${start}` : `${start}-${end}`);
  return ranges;
}

function buildGroupsForColumn(header, colData, dateOrder) {
  // 1) Agregar celdas al diccionario byKey: "<length>::<valueType>"
  const byKey = {};
  colData.forEach((cell) => {
    const trimmed   = cell.value.trim();
    const basicType = getValueType(cell.value); // p.e. 'NUMERIC','ALPHA','PHRASE'...
    const groupKey  = `${trimmed.length}::${basicType}`;
    (byKey[groupKey] ??= []).push(cell);
  });

  const groupsArr      = [];
  const ungroupableArr = [];

  // 2) Procesar cada agrupación
  for (const key in byKey) {
    const groupCells = byKey[key];
    const [lenStr] = key.split('::');
    const groupCharLength = parseInt(lenStr, 10) || 0;

    // 2.1) Detección numérica
    const numericOnly = groupCells.every((gc) => /^-?\d+$/.test(gc.value.trim()));
    const charLenFromNegSign =
      numericOnly &&
      groupCells.some((gc) => {
        const t = gc.value.trim();
        return t.startsWith('-') && t.length === groupCharLength;
      });

    // 2.2) Frecuencias y candidateDetails
    const freqMap = {};
    const candidateDetails = {};
    groupCells.forEach((gc) => {
      freqMap[gc.value] = (freqMap[gc.value] || 0) + 1;
      (candidateDetails[gc.value] ??= []).push(gc.row);
    });
    Object.keys(candidateDetails).forEach((val) => {
      candidateDetails[val] = compressRanges(candidateDetails[val]);
    });

    let frequencyRanges  = [];
    let frequencySingles = {};
    let valueRanges      = [];
    let valueSingles     = {};

    // 2.3) Rangos si es numérico
    if (numericOnly) {
      const numericFreq = {};
      groupCells.forEach((gc) => {
        numericFreq[gc.value] = (numericFreq[gc.value] || 0) + 1;
      });
      const numericKeys = Object.keys(numericFreq)
        .map((k) => +k)
        .sort((a, b) => a - b);
      const ranges = computeBothRanges(numericKeys, numericFreq);
      ['frequencyRanges', 'valueRanges'].forEach((prop) => {
        if (ranges[prop]?.length) {
          ranges[prop] = ranges[prop].map((r) => {
            const { charLength, ...rest } = r;
            if (rest.stats) {
              rest.stats = { min: rest.stats.min, max: rest.stats.max };
            }
            return rest;
          });
        }
      });
      frequencyRanges  = ranges.frequencyRanges  || [];
      frequencySingles = ranges.frequencySingles || {};
      valueRanges      = ranges.valueRanges      || [];
      valueSingles     = ranges.valueSingles     || {};
    } else {
      frequencySingles = { ...freqMap };
    }

    // 2.4) Determinar usage principal
    const rawValues = groupCells.map((gc) => gc.value);
    let usage =
      groupCharLength === 0
        ? 'Empty'
        : determinePossibleUseForGroup(rawValues, {
            header,
            frequencyRanges,
            frequencySingles,
            valueRanges,
            valueSingles,
          });

    // 2.4.b) “Patch” si el detector no marcó fecha/hora pero hay evidencia
    if (!['Date','Time','DateTime'].includes(usage)) {
      const serialGuess = excelSerialHint(rawValues); // 'Date' | 'Time' | null
      const headerSuggestsDT =
        headerLooksLikeDate(header) || headerLooksLikeTime(header) || headerLooksLikeDateTime(header);

      if (serialGuess && headerSuggestsDT) {
        usage = serialGuess;
      } else if (dateOrder || headerSuggestsDT) {
        let ok = 0, tot = 0, timeSeen = 0;
        for (const rv of rawValues) {
          const s = String(rv || '').trim(); if (!s) continue;
          const d = dateOrder ? parseAmbiguousWithOrder(s, dateOrder) : parseFlexibleDate(s);
          if (d && !isNaN(d)) {
            ok++; tot++;
            if (hasTimePart(s) || d.getUTCHours()+d.getUTCMinutes()+d.getUTCSeconds()>0) timeSeen++;
          } else {
            tot++;
          }
        }
        if (tot >= 5 && ok / tot >= 0.7) {
          usage = (timeSeen || headerLooksLikeDateTime(header)) ? 'DateTime' : 'Date';
        } else if (headerLooksLikeTime(header) && serialGuess === 'Time') {
          usage = 'Time';
        }
      }
    }

    // 2.5) Sub-stats para numeric / date (con orden)
    const subStats = computeGroupStatsAfterUsage(rawValues, usage, dateOrder);

    // 2.6) Construir objeto de grupo
    const gObj = {
      charLength: groupCharLength,
      charLenFromNegSign,
      totalCount: groupCells.length,
      possibleUses: [usage],
      candidateDetails,
      numericOnly,
    };
    if (frequencyRanges.length) gObj.frequencyRanges = frequencyRanges;
    if (Object.keys(frequencySingles).length) gObj.frequencySingles = frequencySingles;
    if (valueRanges.length) gObj.valueRanges = valueRanges;
    if (Object.keys(valueSingles).length) gObj.valueSingles = valueSingles;
    if (subStats && Object.keys(subStats).length) {
      gObj.ValuesStats = subStats;
    }

    // Grupo de 1 -> ungroupable
    if (groupCells.length === 1) {
      delete gObj.valueSingles;
      delete gObj.ValuesStats;
      ungroupableArr.push(gObj);
    } else {
      groupsArr.push(gObj);
    }
  }

  // 3) Forzado a *Code en ungroupable si todos los válidos acaban en Code
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
        const singleValKey = Object.keys(u.candidateDetails || {})[0] || '';
        const vt = getValueType(singleValKey.trim());
        let forcedCode = 'TextCode';
        switch (vt) {
          case 'ALPHANUM': forcedCode = 'AlphaNumericCode'; break;
          case 'NUMERIC':  forcedCode = 'NumericCode'; break;
          case 'RUT':
          case 'EMAIL':
          case 'ALPHA':
          default:         forcedCode = 'TextCode'; break;
        }
        u.possibleUses = [forcedCode];
      }
    }
  }

  return { groupsArr, ungroupableArr };
}

function possiblyForceNumericUsage(header, groupsArr, columnsData) {
  // Clon superficial: suficiente porque solo se cambia possibleUses
  const cloned = groupsArr.map(g => ({
    ...g,
    possibleUses: [...(g.possibleUses || [])]
  }));

  // 0) Si ya está marcada como fecha/hora, no tocar
  const alreadyDateLike = cloned.some(g => ['Date','DateTime','Time'].includes(g.possibleUses?.[0]));
  if (alreadyDateLike) return cloned;

  // 1) Si la cabecera sugiere fecha/hora, no forzar a numérico
  if (headerLooksLikeDate(header) || headerLooksLikeTime(header) || headerLooksLikeDateTime(header)) {
    return cloned;
  }

  // 2) Si la muestra cuadra como serial Excel o fracción de día, no forzar
  const serialGuess = excelSerialHint((columnsData[header] || []).map(o => o.value));
  if (serialGuess) return cloned; // 'Date' o 'Time' detectado por heurística

  // 3) Lógica original de “forzar numérico” (sin cambios)
  let allNumeric    = true;
  const distinct    = new Set();
  let foundDecimal  = false;

  for (const { value } of columnsData[header]) {
    const s = value.trim();
    if (!s) continue;
    const num = Number(unifyDecimalSeparator(s));
    if (isNaN(num)) {
      allNumeric = false;
      break;
    }
    distinct.add(num);
    if (Math.abs(num % 1) > 1e-12) foundDecimal = true;
  }

  if (!allNumeric || distinct.size <= 2) return cloned;

  const validGroups       = cloned.filter(g => !['Empty', 'ZerosOnly'].includes(g.possibleUses[0]));
  const nonCodeGroupCount = cloned.filter(g => !/Code$/.test(g.possibleUses[0])).length;
  if (!validGroups.length || nonCodeGroupCount === 0) return cloned;

  const charLengthCount = {};
  let sumCount = 0;
  for (const g of validGroups) {
    charLengthCount[g.charLength] = (charLengthCount[g.charLength] || 0) + g.totalCount;
    sumCount += g.totalCount;
  }
  const homogeneityRate = computeHomogeneityRate(charLengthCount, sumCount);

  if (homogeneityRate > 0.3) {
    const forced = foundDecimal ? 'DecimalValue' : 'IntegerValue';
    validGroups.forEach(g => { g.possibleUses = [forced]; });
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

const toText = (v) => {
  if (v === null || v === undefined) return '';
  if (v instanceof Date) return normalizeByUsage(v, 'DateTime'); // canónico UTC
  if (typeof v === 'boolean') return v ? 'true' : 'false';
  if (typeof v === 'number') return Number.isFinite(v) ? String(v) : '';
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
    headers.map(h => [h, (rawColumnsData[h] || []).map((v, i) => ({ value: (typeof v === 'string' ? v : toText(v)), row: i + 2 }))])
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

    // 4.2) Construir grupos usando el orden inferido
    const { groupsArr, ungroupableArr } = buildGroupsForColumn(hdr, columnsData[hdr], colOrder);

    let colObj = { lengthGroups: possiblyForceNumericUsage(hdr, groupsArr, columnsData) };
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