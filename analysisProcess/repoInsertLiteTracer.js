'use strict';

/* repoInsertLiteTracer.js
   ─────────────────────────────────────────────
   Funciones de ayuda para depurar problemas de
   normalización/inserción de datos (fechas, horas,
   números, etc.) en tablas repo/KNFO.
   ============================================= */

/**
 * Log detallado cuando un valor esperado de fecha/hora queda en NULL.
 * @param {string} alias   Alias/columna en .repo (ej: 'posting_date').
 * @param {any} rawValue   Valor crudo leído del Excel.
 * @param {string} fileBase Nombre base del archivo Excel procesado.
 * @param {number} rowNum  Nº de fila de Excel (1-based, incluyendo cabecera).
 * @param {string} [order] Orden inferido 'DMY'|'MDY'|'YMD' (si aplica).
 */
function logNullDate(alias, rawValue, fileBase, rowNum, order = '?') {
  console.error(
    `[DIAG][NULL_DATE] alias=${alias} file=${fileBase} row=${rowNum} ` +
    `order=${order} rawValue=${JSON.stringify(rawValue)}`
  );
}

/**
 * Log genérico cuando cualquier valor no se puede normalizar.
 * @param {string} alias
 * @param {any} rawValue
 * @param {string} fileBase
 * @param {number} rowNum
 * @param {string} reason
 */
function logNormalizationFailure(alias, rawValue, fileBase, rowNum, reason = 'unknown') {
  // console.error(
  //   `[DIAG][NORMALIZE_FAIL] alias=${alias} file=${fileBase} row=${rowNum} ` +
  //   `reason=${reason} rawValue=${JSON.stringify(rawValue)}`
  // );
}

/**
 * Log de diagnóstico al insertar valores NULL en columnas críticas
 * (numéricas, claves o control).
 * @param {string} colName
 * @param {any} val
 * @param {string} table
 * @param {string} fileBase
 * @param {number} rowNum
 */
function logCriticalNull(colName, val, table, fileBase, rowNum) {
  console.error(
    `[DIAG][CRITICAL_NULL] table=${table} col=${colName} file=${fileBase} row=${rowNum} ` +
    `value=${JSON.stringify(val)}`
  );
}

/**
 * Log con dump parcial de la fila completa para inspección rápida.
 * @param {object} rowObj  Objeto con todas las columnas ya procesadas.
 * @param {string} fileBase
 * @param {number} rowNum
 */
function logRowDump(rowObj, fileBase, rowNum) {
  const preview = Object.entries(rowObj)
    .slice(0, 10) // máx 10 cols para no saturar logs
    .map(([k,v]) => `${k}=${JSON.stringify(v)}`)
    .join(' ');
  console.error(`[DIAG][ROW_DUMP] file=${fileBase} row=${rowNum} preview= ${preview} ...`);
}

/**
 * Log de decisión/razón sobre qué pasa con una fila objetivo.
 * @param {string} action  'DROP_AT_PREPARE' | 'SKIP_WITHOUT_KEY' | 'COALESCE_DUP_IN_BATCH'
 *                         | 'WILL_INSERT_NEW_HASH' | 'WILL_UPDATE_EXISTING_HASH' | 'UPDATED_EXISTING'
 * @param {object} ctx     { table, fileBase, rowNum, excelRows, hash, keys, reason, sample }
 */
function logRowDecision(action, ctx = {}) {
  const rows = Array.isArray(ctx.excelRows) ? ctx.excelRows.join(',') : (ctx.rowNum ?? '?');
  console.error(
    `[TRACE][ROW_DECISION] action=${action} table=${ctx.table ?? '?'} file=${ctx.fileBase ?? '?'} ` +
    `rows=${rows} hash=${ctx.hash ?? '?'} keys=${ctx.keys ? JSON.stringify(ctx.keys) : '{}'} ` +
    `reason=${ctx.reason ?? '-'}`
  );
  if (ctx.sample) {
    console.error(`[TRACE][ROW_DECISION][sample] ${ctx.sample}`);
  }
}

module.exports = {
  logNullDate,
  logNormalizationFailure,
  logCriticalNull,
  logRowDump,
  logRowDecision
};