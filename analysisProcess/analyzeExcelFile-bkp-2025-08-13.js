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

  
/* =================================================== *
 *  inferDataType  —  compacta + buckets VARCHAR
 * =================================================== */
// helpers -------------------------------------------------------
const NUMERIC_SET   = new Set(['DecimalValue', 'IntegerValue', 'NumericCode']);
const DATETIME_SET  = new Set(['DateTime', 'Date', 'Time']);
const TEXTCODE_SET  = new Set(['TextCode', 'AlphaNumericCode', 'Email']);
const FREETEXT_SET  = new Set(['FreeText', 'StandardText']);

const V = [5, 20, 50, 100, 150, 250];          // buckets
const SLACK = 0.25; /* factor de holgura (25 % por defecto) */

const SQL2ABBR = { INT:'IN', BIGINT:'BI', 'TINYINT(1)':'TI',
                   'CHAR(1)':'CH', 'DECIMAL(38,10)':'DE', DATETIME:'DT' };
const ABBR2SQL = Object.fromEntries(Object.entries(SQL2ABBR).map(([k, v]) => [v, k]));

// ――――――――――――――― inferDataType ―――――――――――――――
function inferDataType(col, slack = SLACK) {
  const uses = (col.possibleUses || [])
    .filter(u => !['Empty', 'ZerosOnly'].includes(u));

  if (!uses.length) return null;

  /* 1. Compatibilidad numérica */
  if (uses.every(u => NUMERIC_SET.has(u))) {
    if (uses.includes('DecimalValue')) return 'DECIMAL(38,10)';

    const maxEff = Math.max(
      0,
      ...[...(col.lengthGroups || []), ...(col.ungroupable || [])]
        .map(g => (g.charLength || 0) - (g.charLenFromNegSign ? 1 : 0))
    );
    return maxEff <= 6 ? 'INT' : 'BIGINT';
  }

  /* 2. Compatibilidad datetime */
  if (uses.every(u => DATETIME_SET.has(u))) {
    return 'DATETIME';
  }

  /* 3. Sólo Boolean (valores dicotómicos, formato variable) */
  if (uses.length === 1 && uses[0] === 'Boolean') {
    const allGroups = [...(col.lengthGroups || []), ...(col.ungroupable || [])]
      .filter(g => g.possibleUse === 'Boolean');

    const values   = allGroups.flatMap(g =>
      (Array.isArray(g.values) ? g.values : []).map(v => v.split('::[')[0])
    );

    const numericOnly = values.length && values.every(v => /^\d+$/.test(v));
    if (numericOnly) return 'INT';

    // tomar el largo máximo observado en los grupos booleanos
    const maxLen = Math.max(1, ...allGroups.map(g => g.charLength || 0));

    // valores texto → longitud fija o variable según dispersión
    const sameLen = allGroups.every(g => g.charLength === maxLen);
    return sameLen && maxLen <= 10              // p.ej. «Verdadero» (9)
      ? `CHAR(${maxLen})`
      : `VARCHAR(${maxLen})`;
  }

  /* 4. Sólo códigos alfanuméricos / email */
  if (uses.every(u => TEXTCODE_SET.has(u))) {
    return varcharFor(col);
  }

  /* 5. Sólo texto libre / estándar */
  if (uses.every(u => FREETEXT_SET.has(u))) {
    return varcharFor(col);
  }

  /* 6. Mezcla incompatible → VARCHAR genérico */
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

function excelDateToJSDate(num) {
  const baseTime = Date.UTC(1899, 11, 31);
  const dayMs = 24 * 3600 * 1000;
  return new Date(baseTime + num * dayMs);
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


// ----------------------------------------------------------------
// Stats en caso numérico o fecha/hora
// ----------------------------------------------------------------
function computeGroupStatsAfterUsage(groupValues, usage) {
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
    return {
      min: numericVals[0],
      max: numericVals[numericVals.length - 1]
    };
  }

  // fecha/hora
  if (['Date', 'Time', 'DateTime'].includes(usage)) {
    const arr = [];
    for (const v of groupValues) {
      if (typeof v !== 'string') continue;
      const trimmed = v.trim();
      const numReg = /^-?[\d]+(?:[.,]\d+)?$/;
      if (numReg.test(trimmed)) {
        const f = parseFloat(unifyDecimalSeparator(trimmed));
        if (f >= 0) {
          const d = excelDateToJSDate(f);
          if (!isNaN(d.getTime())) {
            arr.push(d);
            continue;
          }
        }
      }
      // const p = chrono.parseDate(trimmed);
      // if (p instanceof Date && !isNaN(p.getTime())) {
      //   arr.push(p);
      // }
    }
    if (!arr.length) return {};
    arr.sort((a, b) => a.getTime() - b.getTime());
    return {
      min: arr[0],
      max: arr[arr.length - 1]
    };
  }

  return {};
}

function representValueByUsage(value, usage) {
  if (['NumericCode', 'DecimalValue', 'IntegerValue'].includes(usage)) {
    return String(value);
  }
  if (['DateTime', 'Date', 'Time'].includes(usage)) {
    return value.toISOString();
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

/* ----------------------------------------------------------------------
 * buildGroupsForColumn
 *  - Crea agrupaciones por <charLength>::<valueType> (p.ej. "10::NUMERIC")
 *  - Retorna {groupsArr, ungroupableArr}.
 *  - Usa determinePossibleUseForGroup(...) para asignar "usage" válido:
 *      'ZerosOnly', 'Date', 'Time', 'DateTime', 'Boolean', 'Email',
 *      'DecimalValue', 'IntegerValue', 'NumericCode', 'TextCode',
 *      'AlphaNumericCode', 'FreeText', 'StandardText'.
 *
 *  - Ajuste: si TODOS los grupos “válidos” (usage != 'Empty'/'ZerosOnly')
 *    terminan en "...Code", se fuerza a los ungroupableArr a también ser
 *    "*Code", mapeando su ValueType a:
 *      ALPHA     -> 'TextCode'
 *      ALPHANUM  -> 'AlphaNumericCode'
 *      NUMERIC   -> 'NumericCode'
 *      RUT       -> 'TextCode'
 *      EMAIL     -> 'TextCode'
 *      (fallback)-> 'TextCode'
 * ----------------------------------------------------------------------*/
function buildGroupsForColumn(header, colData) {
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
    const [lenStr, basicType] = key.split('::');
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

    // Acumula todas las filas para cada valor
    groupCells.forEach((gc) => {
      freqMap[gc.value] = (freqMap[gc.value] || 0) + 1;
      (candidateDetails[gc.value] ??= []).push(gc.row);
    });

    // Comprime cada array de filas en rangos continuos
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

      // Eliminar charLength en frequencyRanges / valueRanges
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
      // Frecuencias directas
      frequencySingles = { ...freqMap };
    }

    // 2.4) Determinar usage principal
    const rawValues = groupCells.map((gc) => gc.value);
    const usage =
      groupCharLength === 0
        ? 'Empty'
        : determinePossibleUseForGroup(rawValues, {
            header,
            frequencyRanges,
            frequencySingles,
            valueRanges,
            valueSingles,
          });

    // 2.5) Sub-stats para numeric / date
    const subStats = computeGroupStatsAfterUsage(rawValues, usage);

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

  
  /* ------------------------------------------------------------------
   * 3) Si todos los grupos válidos acaban en "...Code", forzamos
   *    ungroupableArr a "*Code" según ValueType principal:
   *      ALPHA -> 'TextCode'
   *      ALPHANUM -> 'AlphaNumericCode'
   *      NUMERIC -> 'NumericCode'
   *      RUT -> 'TextCode'
   *      EMAIL -> 'TextCode'
   *      fallback -> 'TextCode'
   * ------------------------------------------------------------------*/
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
        // ungroupable -> "*Code"
        const singleValKey = Object.keys(u.candidateDetails || {})[0] || '';
        const vt = getValueType(singleValKey.trim());
        let forcedCode = 'TextCode';

        switch (vt) {
          case 'ALPHANUM':
            forcedCode = 'AlphaNumericCode';
            break;
          case 'NUMERIC':
            forcedCode = 'NumericCode';
            break;
          case 'RUT':
          case 'EMAIL':
          case 'ALPHA':
            forcedCode = 'TextCode';
            break;
          default:
            // PHRASE, etc.
            forcedCode = 'TextCode';
            break;
        }
        u.possibleUses = [forcedCode];
      }
    }
  }

  // 4) Retornar
  return { groupsArr, ungroupableArr };
}

/********************************************************************
 *  Nuevo possiblyForceNumericUsage                                  *
 *  ▸ NO muta groupsArr; devuelve una copia (profundidad 1)          *
 *  ▸ Si no corresponde forzar uso numérico => retorna copia intacta *
 ********************************************************************/
function possiblyForceNumericUsage(header, groupsArr, columnsData) {
  // Clon superficial: suficiente porque solo se cambia possibleUses
  const cloned = groupsArr.map(g => ({
    ...g,
    possibleUses: [...(g.possibleUses || [])]
  }));

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

  // homogeneidad
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
  if (v instanceof Date) return v.toISOString();
  if (typeof v === 'boolean') return v ? 'true' : 'false';
  if (typeof v === 'number') return Number.isFinite(v) ? String(v) : '';
  return String(v);
};

// ─────────────────────────────────────────────────────────────────────────
// analyzeExcelFile  –  ahora usa ensureExcelData()
// ─────────────────────────────────────────────────────────────────────────
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
      keyColumns: []
    }
  };

  // 4) Análisis por columna (90% del rango)
  const totalCols  = headers.length || 1;
  const colPctStep = 0.90 / totalCols;
  let processedCols = 0;

  for (const hdr of headers) {
    const tCol = Date.now();

    const { groupsArr, ungroupableArr } = buildGroupsForColumn(hdr, columnsData[hdr]);

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