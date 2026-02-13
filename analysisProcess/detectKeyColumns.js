//------------------------------------------------------------------
// detectKeyColumns  –  estrategia por muestreo incremental
//------------------------------------------------------------------
const aq  = require('arquero');
const SEP = '\u0001';
const SAMPLE_SIZE = 1_000;
const MAX_WIDTH   = 5;

/* ══════ CONFIG DEBUG ══════ */
const DEBUG_TARGET = ['Purchasing Document::B', 'Item::C']
  .sort()
  .join('|');
/*═══════════════════════════*/

/* ─────────────── NUEVO: reglas de selección de uso ─────────────── */

// Usos bloqueados para clave: si una columna sólo cae en estos, se descarta.
const BLOCKED = new Set(['ZerosOnly', 'Empty', 'DecimalValue']);

// Ranking de "genericidad" (valor más alto = más genérico).
// Ajusta/añade según tu taxonomía real.
const GENERICITY_RANK = {
  FreeText:             100,
  StandardText:         95,
  StandardDescription:  93,

  TextCode:             90,
  CodeWithoutSpaces:    85,
  AlphaNumericCode:     80,

  // Entre valores numéricos, DecimalValue es más genérico (admite enteros y decimales),
  // aunque esté bloqueado para claves; se usa si no hay otra cosa.
  DecimalValue:         75,
  IntegerValue:         70,

  NumericCode:          60,
  Boolean:              50,

  DateTime:             40,
  Date:                 38,
  Time:                 36,

  Email:                30,

  ZerosOnly:            10,
  Empty:                 5
};

// Dedup que preserva orden
function dedupeKeepOrder(arr) {
  const seen = new Set();
  const out  = [];
  for (const x of arr) {
    const k = String(x || '');
    if (!k || seen.has(k)) continue;
    seen.add(k);
    out.push(k);
  }
  return out;
}

// Recoge todos los possibleUses de la columna preservando orden
function collectPossibleUses(col = {}) {
  const uses = [];

  if (Array.isArray(col.possibleUses) && col.possibleUses.length) {
    uses.push(...col.possibleUses);
  }

  if (Array.isArray(col.lengthGroups)) {
    for (const g of col.lengthGroups) {
      if (!g) continue;
      if (Array.isArray(g.possibleUses) && g.possibleUses.length) {
        uses.push(...g.possibleUses);
      } else if (g.possibleUses) {
        uses.push(g.possibleUses);
      }
    }
  }

  if (Array.isArray(col.ungroupable)) {
    for (const g of col.ungroupable) {
      if (!g) continue;
      if (Array.isArray(g.possibleUses) && g.possibleUses.length) {
        uses.push(...g.possibleUses);
      } else if (g.possibleUses) {
        uses.push(g.possibleUses);
      }
    }
  }

  return dedupeKeepOrder(uses);
}

/**
 * mainUsage(col):
 * 1) preselecciona usos NO bloqueados (si hay); si no, los bloqueados presentes;
 * 2) elige el más genérico según GENERICITY_RANK (ties → el primero por orden de aparición).
 */
function mainUsage(col = {}) {
  const all = collectPossibleUses(col);
  if (!all.length) return '';

  const nonBlocked = all.filter(u => !BLOCKED.has(u));
  const pool = nonBlocked.length ? nonBlocked : all.filter(u => BLOCKED.has(u));
  if (!pool.length) return '';

  let winner = pool[0];
  let best   = GENERICITY_RANK[winner] ?? 0;

  for (let i = 1; i < pool.length; i++) {
    const u   = pool[i];
    const rank= GENERICITY_RANK[u] ?? 0;
    if (rank > best) {
      winner = u;
      best   = rank;
    }
  }
  return winner;
}

/* ─────────────────────────────────────────────────────────────── */

function detectKeyColumns(analysis, columnsData, colStats) {

  // Categorías para ordenar columnas candidatas dentro de findKey()
  const category = (u) =>
        u.endsWith('Code')                                   ? 0
      : (u === 'IntegerValue' || u === 'DecimalValue')       ? 1
      : (u === 'Boolean')                                    ? 2
      : (u === 'Date' || u === 'Time' || u === 'DateTime')   ? 3
      : 4;

  /* ---------- 1) columnas candidatas (sin ZerosOnly, Empty, DecimalValue) ---------- */
  const headers = Object.keys(columnsData)
    .filter(h => !BLOCKED.has(mainUsage(analysis.columns[h] || {})))
    .filter(h => (colStats[h]?.nonEmptyCount || 0) > 0);

  if (!headers.length) {
    analysis.__meta__.keyColumns = [];
    return analysis;
  }

  /* ---------- 1-bis) eliminar filas idénticas -------------------- */
  const fullSig = r =>
    headers.map(c => String(columnsData[c][r] ?? '').trim()).join(SEP);

  const sigFirstIdx = new Map();
  const dupSig      = new Map();

  for (let r = 0, m = columnsData[headers[0]].length; r < m; r++) {
    const sig = fullSig(r);
    if (sigFirstIdx.has(sig)) {
      (dupSig.get(sig) || dupSig.set(sig, []).get(sig)).push(r);
    } else {
      sigFirstIdx.set(sig, r);
    }
  }

  const duplicates = [...dupSig.values()].flat().length;
  if (duplicates) {
    const sample = [...dupSig.keys()].slice(0, 5);
    console.warn(
      `[detectKeyColumns] ⚠ ${duplicates} filas idénticas descartadas `
      + `(ejemplos: ${sample.map(s => `"${s}"`).join('; ')})`
    );
  }

  const baseIdx = [...sigFirstIdx.values()];

  /* ---------- 2) trazador de colisiones -------------------------- */
  function debugDuplicates(cols, rows) {
    const id = cols.slice().sort().join('|');
    if (id !== DEBUG_TARGET) return;

    const map = new Map();
    for (const r of rows) {
      const k = cols.map(c => String(columnsData[c][r] ?? '').trim()).join(SEP);
      (map.get(k) || map.set(k, []).get(k)).push(r);
    }
    console.debug('\n🟡 [DEBUG-keyColumns] Combinación analizada:', cols);
    [...map].filter(([, arr]) => arr.length > 1)
      .slice(0, 10)
      .forEach(([k, arr]) =>
        console.debug(`   • clave duplicada "${k}" en filas [${arr.join(', ')}]`)
      );
    if (![...map.values()].some(a => a.length > 1))
      console.debug('   ✓ Sin duplicados (sería válida)');
    console.debug('───────────────────────────────────────────────\n');
  }

  /* ---------- 3) funciones de unicidad --------------------------- */
  function rowHasValue(cols, r) {
    for (const c of cols) {
      const s = String(columnsData[c][r] ?? '').trim();
      if (s !== '' && s !== '0') return true;
    }
    return false;
  }

  function isUnique(cols, rows, seen = new Set()) {
    let unique = true;
    for (const r of rows) {
      if (!rowHasValue(cols, r)) continue;
      const k = cols.map(c => String(columnsData[c][r] ?? '').trim()).join(SEP);
      if (seen.has(k)) { unique = false; break; }
      seen.add(k);
    }
    if (!unique) debugDuplicates(cols, rows);
    return unique;
  }

  function* combos(arr, k, start = 0, prev = []) {
    if (k === 0) { yield prev; return; }
    for (let i = start; i <= arr.length - k; i++) {
      yield* combos(arr, k - 1, i + 1, [...prev, arr[i]]);
    }
  }

  function findKey(rows) {
    const levels = [...new Set(headers.map(h => colStats[h].nonEmptyCount))]
      .sort((a, b) => b - a);

    for (const lvl of levels) {
      const cand = headers.filter(h => colStats[h].nonEmptyCount >= lvl);
      if (!cand.length) continue;

      const ordered = cand
        .map((h, i) => ({
          h,
          i,
          cat: category(mainUsage(analysis.columns[h] || {})),
        }))
        .sort((a, b) => (a.cat !== b.cat ? a.cat - b.cat : a.i - b.i))
        .map(o => o.h);

      // (A) clave simple
      for (const h of ordered) {
        if (isUnique([h], rows)) return [h];
      }

      // (B) combinaciones 2..MAX_WIDTH
      const maxK = Math.min(MAX_WIDTH, ordered.length);
      for (let k = 2; k <= maxK; k++) {
        for (const combo of combos(ordered, k)) {
          if (isUnique(combo, rows)) return combo;
        }
      }
    }
    return null;
  }

  /* ---------- 4) muestreo --------------------------------------- */
  const rowIdx = baseIdx.slice();

  for (let i = 0; i < rowIdx.length; i++) {
    // no-op; si quieres barajar, usa Fisher-Yates aquí (opcional)
  }

  const groups = [];
  for (let i = 0; i < rowIdx.length; i += SAMPLE_SIZE)
    groups.push(rowIdx.slice(i, i + SAMPLE_SIZE));

  /* ---------- 5) búsqueda incremental --------------------------- */
  let accumulatedRows = groups.shift() ?? [];
  let keyCols         = findKey(accumulatedRows);

  if (!keyCols) { analysis.__meta__.keyColumns = []; return analysis; }

  let seen = new Set();
  isUnique(keyCols, accumulatedRows, seen);

  for (const grp of groups) {
    if (isUnique(keyCols, grp, new Set(seen))) {
      grp.forEach(r => {
        if (rowHasValue(keyCols, r)) {
          const k = keyCols.map(c => String(columnsData[c][r] ?? '').trim()).join(SEP);
          seen.add(k);
        }
      });
      accumulatedRows = accumulatedRows.concat(grp);
      continue;
    }

    accumulatedRows = accumulatedRows.concat(grp);
    keyCols = findKey(accumulatedRows);
    if (!keyCols) { 
      analysis.__meta__.keyColumns = []; 
      return analysis; 
    }

    seen = new Set();
    isUnique(keyCols, accumulatedRows, seen);
  }

  analysis.__meta__.keyColumns = keyCols;
  return analysis;
}

module.exports = { detectKeyColumns };