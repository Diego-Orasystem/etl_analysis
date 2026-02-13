'use strict';

// numberUtil.js — Inferencia de separadores (miles/decimal) con trazabilidad.
// Reglas:
//  1) Si algún número de la muestra tiene ≥2 ocurrencias del mismo símbolo ('.' o ','),
//     ese símbolo es de miles. Si el último grupo tras ese separador queda con <3 dígitos,
//     se rellenan ceros a la derecha (solo en esos casos).
//  2) Si NO existe (1), se analiza si HAY constancia de 3 dígitos tras el separador;
//     si se cumple, se asume separador de miles; de lo contrario, se asume separador decimal.

const { logNormalizationFailure } = require('./repoInsertLiteTracer');

/* util */
const _occ = (s, ch) => (s ? (s.split(ch).length - 1) : 0);
const _onlyDigits = s => (s.match(/\d+/g) || []).join('');
const _sanitize = raw =>
  (raw == null ? '' : String(raw).trim())
    .replace(/\s+/g, '')
    .replace(/[^0-9.,()\-]/g, '');
const _lastRunLenAfter = (s, sep) => {
  const i = s.lastIndexOf(sep);
  if (i < 0) return null;
  let k = 0;
  for (let j = i + 1; j < s.length; j++) {
    const c = s[j]; if (c >= '0' && c <= '9') k++; else break;
  }
  return k;
};
const _allThree = arr => (arr.length > 0 && arr.every(n => n === 3));

function _extractSign(s) {
  let negative = false, core = s;
  const m = core.match(/^\((.*)\)$/);
  if (m) { negative = true; core = m[1]; }
  if (core.startsWith('-')) { negative = true; core = core.slice(1); }
  return { negative, core };
}
const _applySign = (txt, negative) =>
  (negative && txt && txt !== '0' && txt !== '0.0') ? ('-' + txt) : txt;

function _fmtSet(st) { return `{${[...st].join('')}}`; }


/** Traza del plan elegido (por columna) */
function tracePlan(plan, opts, sample) {
  const alias = opts?.alias || '?';
  const file  = opts?.fileBase || '?';
  const ex    = (sample || []).map(_sanitize).filter(Boolean).slice(0, 6);
  const dot   = plan.stats['.'], com = plan.stats[','];
  console.debug(
    `[TRACE][NUM_PLAN] alias=${alias} file=${file} ` +
    `mode=${plan.mode} reason=${plan.reason} thousand=${_fmtSet(plan.thousandSymbols)} ` +
    `decimal=${plan.decimalSymbol ?? '∅'} ` +
    `dot.present=${dot.present} dot.multi=${dot.multi} dot.tailLens=${JSON.stringify(dot.tailLens.slice(0,6))} ` +
    `comma.present=${com.present} comma.multi=${com.multi} comma.tailLens=${JSON.stringify(com.tailLens.slice(0,6))} ` +
    `sample=${ex.join('|')}${ex.length < (sample || []).length ? '|…' : ''}`
  );
}

// --- numberUtil.js ---

function inferSeparatorsFromSample(sample, opts = {}) {
  // 1) Sanitiza y descarta valores sin ',' ni '.'
  const rows = (Array.isArray(sample) ? sample : [])
    .map(_sanitize)
    .filter(Boolean)
    .filter(s => /[.,]/.test(s)); // solo muestras con separadores

  // Caso base: no hay separadores → asumir miles-solo
  if (rows.length === 0) {
    const stats = {
      '.': { present: 0, multi: 0, tailLens: [] },
      ',': { present: 0, multi: 0, tailLens: [] }
    };
    return {
      mode: 'thousands_only',
      thousandSymbols: new Set(['.', ',']),
      decimalSymbol: null,
      reason: 'no_sep_in_samples',
      stats,
      _usedRows: rows
    };
  }

  const sym = ['.', ','];
  const stats = {
    '.': { present: 0, multi: 0, tailLens: [] },
    ',': { present: 0, multi: 0, tailLens: [] }
  };
  const rowsBoth = [];

  // Recolecta métricas por separador y detecta filas con ambos
  for (const s of rows) {
    const hasDot = s.includes('.');
    const hasCom = s.includes(',');
    if (hasDot && hasCom) rowsBoth.push(s);
    for (const sep of sym) {
      const cnt = _occ(s, sep);
      if (cnt > 0) {
        stats[sep].present++;
        if (cnt >= 2) stats[sep].multi++;
        const L = _lastRunLenAfter(s, sep);
        if (L != null) stats[sep].tailLens.push(L);
      }
    }
  }

  // ───────── Regla 0: si hay ambos separadores en algún valor,
  // el decimal es el último separador que aparece ─────────
  if (rowsBoth.length > 0) {
    let lastDotWins = 0, lastComWins = 0;
    for (const s of rowsBoth) {
      const iDot = s.lastIndexOf('.');
      const iCom = s.lastIndexOf(',');
      if (iDot > iCom) lastDotWins++;
      else if (iCom > iDot) lastComWins++;
    }
    if (lastDotWins !== lastComWins) {
      const decimalSymbol = (lastDotWins > lastComWins) ? '.' : ',';
      return {
        mode: 'decimal_present',
        thousandSymbols: new Set(sym.filter(ch => ch !== decimalSymbol)),
        decimalSymbol,
        reason: 'both_separators_last_wins',
        stats,
        _usedRows: rows
      };
    }
    // Si hay empate perfecto, continuamos con las reglas 1 y 2.
  }

  const thousandSymbols = new Set();
  let decimalSymbol = null;
  let reason = 'fallback_decimal';

  // ───────── Regla 1: si un símbolo aparece ≥2 veces en algún valor → miles ─────────
  for (const sep of sym) {
    if (stats[sep].multi > 0) thousandSymbols.add(sep);
  }
  if (thousandSymbols.size > 0) {
    const other = sym.find(ch => !thousandSymbols.has(ch));
    if (other && stats[other].present > 0 && !_allThree(stats[other].tailLens)) {
      decimalSymbol = other;
      reason = 'thousands_plus_decimal';
    } else {
      reason = 'rule1_multi_sep';
    }
  }

  // ───────── Regla 2: constancia de grupos de 3 tras el separador → miles ─────────
  if (thousandSymbols.size === 0) {
    for (const sep of sym) {
      if (stats[sep].present > 0 && _allThree(stats[sep].tailLens)) thousandSymbols.add(sep);
    }
    if (thousandSymbols.size > 0) {
      reason = 'rule2_const_3';
    }
  }

  // ───────── Fallback: decidir decimal por “evidencia negativa”/frecuencia ─────────
  if (thousandSymbols.size === 0) {
    const non3Dot = stats['.'].tailLens.filter(n => n !== 3).length;
    const non3Com = stats[','].tailLens.filter(n => n !== 3).length;
    if (non3Dot > non3Com) {
      decimalSymbol = '.';
    } else if (non3Com > non3Dot) {
      decimalSymbol = ',';
    } else {
      if (stats['.'].present > stats[','].present) decimalSymbol = '.';
      else if (stats[','].present > stats['.'].present) decimalSymbol = ',';
      else decimalSymbol = ','; // empate
    }
  } else if (!decimalSymbol) {
    // Hay miles identificados pero no decimal claro
    decimalSymbol = null;
  }

  return {
    mode: decimalSymbol ? 'decimal_present' : 'thousands_only',
    thousandSymbols: thousandSymbols.size
      ? thousandSymbols
      : new Set(sym.filter(ch => ch !== decimalSymbol)),
    decimalSymbol,
    reason,
    stats,
    _usedRows: rows
  };
}

function createNumberNormalizer(sample, opts = {}) {
  const plan = inferSeparatorsFromSample(sample, opts);
  // Importante: trazar usando SOLO las muestras con separadores
  tracePlan(plan, opts, plan._usedRows || sample);
  return function normalizeSqlNumberLike(raw, kind, ctx = {}) {
    const k = (kind || 'DECIMAL').toUpperCase();
    const KK = (k === 'INT' || k === 'BIGINT' || k === 'TINYINT') ? k : 'DECIMAL';
    return normalizeWithPlan(raw, KK, plan, ctx);
  };
}

/**
 * Normaliza un valor según plan; con trazas por caso “interesante”.
 */
function normalizeWithPlan(raw, kind, plan, ctx = {}) {
  if (raw == null || raw === '') return null;

  const alias = ctx.alias || '?';
  const file  = ctx.fileBase || '?';
  const row   = ctx.rowNum ?? '?';

  let s0 = _sanitize(raw);
  if (s0 === '') return null;

  const { negative, core } = _extractSign(s0);
  let s = core;

  const hasDot = s.includes('.'), hasCom = s.includes(',');
 

  // Salvaguarda por valor (NUEVO): si este valor tiene ambos separadores,
  // forzamos "decimal = último separador", aunque el plan diga thousands_only.
  if (plan.mode === 'thousands_only' && hasDot && hasCom) {
    const dec = (s.lastIndexOf('.') > s.lastIndexOf(',')) ? '.' : ',';
    const idx = s.lastIndexOf(dec);
    let intPart  = idx >= 0 ? s.slice(0, idx) : s;
    let fracPart = idx >= 0 ? s.slice(idx + 1) : '';
    const rmMiles = txt => txt.replace(/[.,]/g, '');
    intPart  = rmMiles(intPart);
    fracPart = rmMiles(fracPart);
    let out = intPart || '0';
    if (fracPart) out += '.' + fracPart;
    if (!/^\d+(\.\d+)?$/.test(out)) {
      logNormalizationFailure(alias, raw, file, row, 'number_parse_failed_decimal_fallback');
      return null;
    }
    if (kind === 'INT' || kind === 'BIGINT' || kind === 'TINYINT') out = out.replace(/\..*$/, '');
    console.debug(`[TRACE][NUM_NORM] alias=${alias} file=${file} row=${row} mode=decimal_present(dec_fallback) dec=${dec} raw=${JSON.stringify(raw)} → ${JSON.stringify(_applySign(out, negative))}`);
    return _applySign(out, negative);
  }

  if (plan.mode === 'thousands_only') {
    // Relleno (Regla 1) si este valor concreto tiene ≥2 ocurrencias del MISMO sep de miles
    for (const sep of plan.thousandSymbols) {
      const cnt = _occ(s, sep);
      if (cnt >= 2) {
        const L = _lastRunLenAfter(s, sep) ?? 0;
        if (L > 0 && L < 3) {
          const add = 3 - L;
          const before = s;
          s = s + '0'.repeat(add);
          console.warn(`[TRACE][NUM_PAD] alias=${alias} file=${file} row=${row} sep=${sep} added=${add} raw=${JSON.stringify(before)} → ${JSON.stringify(s)}`);
        }
      }
    }
    // Eliminar TODOS los separadores (ambos cuentan como miles en este modo)
    s = s.replace(/[.,]/g, '');
    if (!/^\d+$/.test(s)) {
      logNormalizationFailure(alias, raw, file, row, 'number_parse_failed_thousands_only');
      return null;
    }
    const out = (kind === 'INT' || kind === 'BIGINT' || kind === 'TINYINT')
      ? s
      : s;

    if (hasDot && hasCom) {
      // ya no deberíamos caer aquí por el guard de arriba, pero mantenemos la traza
      console.debug(`[TRACE][NUM_NORM] alias=${alias} file=${file} row=${row} mode=thousands_only raw=${JSON.stringify(raw)} → ${JSON.stringify(_applySign(out, negative))}`);
    }
     return _applySign(out, negative);
  }
 
  // decimal_present
  const dec = plan.decimalSymbol; // '.' o ','
  const idx = dec ? s.lastIndexOf(dec) : -1;

  let intPart = idx >= 0 ? s.slice(0, idx) : s;
  let fracPart = idx >= 0 ? s.slice(idx + 1) : '';

  // Quitar separadores de miles en ambas partes (todo lo que no sea el decimal elegido)
  const rmMiles = txt => txt.replace(/[.,]/g, '');
  intPart = rmMiles(intPart);
  fracPart = rmMiles(fracPart);

  let out = intPart || '0';
  if (fracPart) out += '.' + fracPart;

  if (!/^\d+(\.\d+)?$/.test(out)) {
    logNormalizationFailure(alias, raw, file, row, 'number_parse_failed_decimal_present');
    return null;
  }
  if (kind === 'INT' || kind === 'BIGINT' || kind === 'TINYINT') out = out.replace(/\..*$/, '');

  if (hasDot && hasCom) {
    console.debug(`[TRACE][NUM_NORM] alias=${alias} file=${file} row=${row} mode=decimal_present dec=${dec} raw=${JSON.stringify(raw)} → ${JSON.stringify(_applySign(out, negative))}`);
  }
  return _applySign(out, negative);
}


/**
 * normalizeSqlNumberLike (versión “rápida” sin plan de muestra)
 * — REESCRITA para usar el mismo motor de inferencia que el plan por muestra,
 *   evitando bugs como reemplazar solo la PRIMERA coma.
 * Acepta: kind 'DECIMAL' | 'INT' | 'BIGINT' | 'TINYINT'
 */
function normalizeSqlNumberLike(raw, kind /* 'DECIMAL' | 'INT' | 'BIGINT' | 'TINYINT' */, ctx = {}) {
  if (raw == null) return null;
  if (typeof raw === 'number') {
    const s = String(raw);
    if ((kind || '').toUpperCase() === 'INT' ||
        (kind || '').toUpperCase() === 'BIGINT' ||
        (kind || '').toUpperCase() === 'TINYINT') {
      return s.replace(/\..*$/, '');
    }
    return s;
  }

  // Generar un plan ad hoc a partir del propio valor.
  // Esto unifica comportamiento con createNumberNormalizer/normalizeWithPlan.
  const plan = inferSeparatorsFromSample([raw], { reason: 'single_value_inference' });
  const k = (kind || 'DECIMAL').toUpperCase();
  const KK = (k === 'INT' || k === 'BIGINT' || k === 'TINYINT') ? k : 'DECIMAL';
  return normalizeWithPlan(raw, KK, plan, ctx);
}

module.exports = {
  inferSeparatorsFromSample,
  createNumberNormalizer,
  normalizeWithPlan,
  normalizeSqlNumberLike
};