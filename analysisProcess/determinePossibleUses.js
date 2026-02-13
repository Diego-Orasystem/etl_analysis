'use strict';

const { checkDateOrDateTime, detectUniformFormat } = require('./datetimeUtil');
// Back-compat: si algún módulo viejito usa detectUniformFormat sin importarlo:
if (typeof global !== 'undefined' && typeof global.detectUniformFormat !== 'function') {
  global.detectUniformFormat = detectUniformFormat;
}

/** Pares booleanos válidos (insensitive) */
function isBooleanGroup(values) {
  if (!Array.isArray(values) || values.length === 0) return false;
  const pairs = [
    ['1', '0'], ['1', ''],
    ['yes', 'no'], ['y', 'n'],
    ['true', 'false'], ['t', 'f'],
    ['si', 'no'], ['s', 'n'],
    ['verdadero', 'falso'], ['v', 'f'],
    ['x', ''] // caso especial 1-valor
  ];
  const norm = values.map(v => String(v ?? '').trim().toLowerCase());
  const uniq = [...new Set(norm)];
  if (uniq.length > 2) return false;
  if (uniq.length === 1) return uniq[0] === 'x';
  const sortedUnique = uniq.slice().sort();
  return pairs.some(p => {
    if (p.length !== 2) return false;
    const sp = p.slice().map(x => x.toLowerCase()).sort();
    return sp[0] === sortedUnique[0] && sp[1] === sortedUnique[1];
  });
}

function isAllEmails(values) {
  if (!Array.isArray(values) || !values.length) return false;
  const rx = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return values.every(v => rx.test(String(v).trim()));
}

function isCodeWithoutSpaces(values) {
  if (!Array.isArray(values) || !values.length) return false;
  const rx = /^[A-Za-z0-9_.-]+$/;
  return values.every(v => rx.test(String(v).trim()));
}



function isAllLetters(values) {
  if (!Array.isArray(values) || !values.length) return false;
  const rx = /^[A-Za-z._-]+$/;
  return values.every(v => {
    const s = String(v).trim();
    return rx.test(s) && !/\d/.test(s);
  });
}

function isAlphaNumeric(values) {
  if (!Array.isArray(values) || !values.length) return false;
  const rx = /^[A-Za-z0-9_.-]+$/;
  const hasLetter = (s) => /[A-Za-z]/.test(s);
  const hasDigit  = (s) => /\d/.test(s);
  return values.every(v => rx.test(String(v).trim())) &&
         values.some(v => hasLetter(String(v))) &&
         values.some(v => hasDigit(String(v)));
}

function codeDiversityCheck(values) {
  const trimmed = values.map(s => String(s ?? '').trim());
  const distinct = new Set(trimmed);
  if (!trimmed.length) return false;
  const sampleLen = trimmed[0].length;
  return sampleLen > 1 ? (distinct.size >= 2) : (distinct.size >= 3);
}

function isMostlyNumeric(values, threshold = 0.9, numHelper) {
  if (!Array.isArray(values) || !values.length) return false;
  if (values.some(v => /[A-Za-z]/.test(String(v ?? '').trim()))) return false;
  let cnt = 0, tot = 0;
  for (const v of values) {
    const t = String(v ?? '').trim();
    if (!t) continue;
    tot++;
    if (numHelper) {
      const d = numHelper.normalizeDecimal(t);
      if (d && (!/\./.test(d) || /\.0+$/.test(d))) { cnt++; continue; }
      const i = numHelper.normalizeInt(t);
      if (i) { cnt++; continue; }
    } else {
      if (!isNaN(+t) && Number.isInteger(+t)) cnt++;
    }
  }
  return tot > 0 && (cnt / tot) >= threshold;
}

function isMostlyDecimal(values, threshold = 0.9, numHelper) {
  if (!Array.isArray(values) || !values.length) return false;
  let floatCount = 0, fracSeen = false, tot = 0;
  for (const v of values) {
    const t = String(v ?? '').trim();
    if (!t) continue;
    tot++;
    if (numHelper) {
      const d = numHelper.normalizeDecimal(t);
      if (d) {
        floatCount++;
        if (/\./.test(d) && !/\.0+$/.test(d)) fracSeen = true;
      }
    } else {
      const num = Number(t.replace(/\./g, '').replace(',', '.'));
      if (Number.isFinite(num)) {
        floatCount++;
        if (Math.abs(num % 1) > 1e-12) fracSeen = true;
      }
    }
  }
  return tot > 0 && (floatCount / tot) >= threshold && fracSeen;
}

function containsNegativeInteger(values = [], numHelper) {
  return values.some(v => {
    const t = String(v ?? '').trim();
    if (!t) return false;
    if (numHelper) {
      const d = numHelper.normalizeDecimal(t);
      if (d && Number(d) < 0 && (!/\./.test(d) || /\.0+$/.test(d))) return true;
      const i = numHelper.normalizeInt(t);
      return !!(i && Number(i) < 0);
    }
    return /^-?\d+$/.test(t) && Number(t) < 0;
  });
}

function isAllDigits(values) {
  if (!Array.isArray(values) || !values.length) return false;
  return values.every(v => /^\d+$/.test(String(v ?? '').trim()));
}

function determinePossibleUseForGroup(groupValues, groupFreqData = {}, opts = {}) {
  const values = Array.isArray(groupValues) ? groupValues : [];
  if (!values.length) return 'Empty';

  // ZerosOnly (todos los no-vacíos son 0)
  if (values.every(s => {
    const t = String(s).trim();
    return t !== '' && Number(t) === 0;
  })) return 'ZerosOnly';

  // Fecha / Hora (uniforme)
  const orderFromArgs = opts.order || groupFreqData.order || groupFreqData.dateOrder || null;
  const dtKind = checkDateOrDateTime(values, orderFromArgs ? { order: orderFromArgs, requireUniformFormat: true }
                                                           : { requireUniformFormat: true });
  if (dtKind === 'Date' || dtKind === 'Time' || dtKind === 'DateTime') return dtKind;

  // Boolean / Email
  if (isBooleanGroup(values)) return 'Boolean';
  if (isAllEmails(values))    return 'Email';

  // Métricas numéricas
  if (isMostlyDecimal(values, 0.9, opts.num))          return 'DecimalValue';
  if (containsNegativeInteger(values, opts.num))       return 'IntegerValue';

  const anyAlpha = values.some(v => /[A-Za-z]/.test(String(v ?? '').trim()));

  // Conjunto "code-like" (solo [A-Za-z0-9_.-]) con diversidad
  if (isCodeWithoutSpaces(values) && codeDiversityCheck(values)) {
    if (isAllDigits(values))     return 'NumericCode';     // ← FIX: antes devolvía FreeText
    if (isAllLetters(values))    return 'TextCode';
    if (isAlphaNumeric(values))  return 'AlphaNumericCode';
    return 'FreeText';
  }

  // Otros patrones de código
  if (isAllLetters(values))    return 'TextCode';
  if (isAlphaNumeric(values))  return 'AlphaNumericCode';

  // Mayormente numérico, sin letras → NumericCode
  if (!anyAlpha && isMostlyNumeric(values, 0.9, opts.num)) return 'NumericCode';

  // Fallback según frecuencia
  const freq = groupFreqData.frequencySingles || {};
  const anyRepeated = Object.keys(freq).some(k => (parseInt(freq[k] || '0', 10)) > 1);
  return anyRepeated ? 'StandardText' : 'FreeText';
}

module.exports = determinePossibleUseForGroup;