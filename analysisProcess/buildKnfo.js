// src/xlsxETL/backend/analysisProcess/buildKnfo.js

const crypto = require('crypto');
const EXTENSION_KNFO = '.knfo';

/**
 * Deep‐clone un objeto JSON‐serializable.
 */
const cloneDeep = o => JSON.parse(JSON.stringify(o));

/**
 * Crea un hash SHA-256 en base64url y lo trunca a `len` caracteres.
 */
function b64urlHash(str, len = 10) {
  return crypto
    .createHash('sha256')
    .update(String(str))
    .digest('base64url')
    .slice(0, len);
}

/**
 * Une un array de cadenas con un separador, devolviendo cadena vacía si no hay elementos.
 */
function safeJoin(arr, sep = ',') {
  return Array.isArray(arr) && arr.length ? arr.join(sep) : '';
}

/**
 * Construye el objeto KNFO a partir de la salida de analyzeExcelFile.
 *
 * @param {object} baseEgjs        El EGJS base (ya serializado)
 * @param {object} detailAnalysis  Resultado completo de analyzeExcelFile()
 * @returns {object}               Nuevo objeto listo para volcar a .knfo
 */
function buildKnfo(baseEgjs, detailAnalysis) {
  const knfo = cloneDeep(baseEgjs);

  // 1) Meta y extensión
  knfo.__meta__ = knfo.__meta__ || {};
  knfo.__meta__.extension     = EXTENSION_KNFO;
  // Copiamos los campos principales de detalle

  const srcMeta = detailAnalysis.__meta__ || {};
  knfo.__meta__.tcode            = srcMeta.tcode            ?? '';
  knfo.__meta__.totalRows       = srcMeta.totalRows      ?? 0;
  knfo.__meta__.rowsWithData    = srcMeta.rowsWithData   ?? 0;
  knfo.__meta__.totalColumns    = srcMeta.totalColumns   ?? 0;
  knfo.__meta__.keyColumns      = srcMeta.keyColumns     || [];
  knfo.__meta__.columnsByDataType = srcMeta.columnsByDataType || {};

  // 2) Columnas, correlaciones y caracterización
  knfo.columns = detailAnalysis.columns || {};
  if (detailAnalysis.correlations)         knfo.correlations           = detailAnalysis.correlations;
  if (detailAnalysis.columnCharacterization) knfo.columnCharacterization = detailAnalysis.columnCharacterization;

  // 3) StructCode / b64urlCode global sobre FullContentType
  const freq = {};
  const pkSet = new Set(knfo.__meta__.keyColumns);
  for (const [colName, col] of Object.entries(knfo.columns)) {
    const fct = col.FullContentType;
    if (!fct || fct === 'ND') continue;
    // Ignorar columnas sin usos reales
    const realUses = (col.possibleUses || []).filter(u => u !== 'Empty' && u !== 'ZerosOnly');
    if (realUses.length === 0) continue;
    const token = pkSet.has(colName) ? `PK$${fct}` : fct;
    freq[token] = (freq[token] || 0) + 1;
  }

  const pkTokens  = Object.keys(freq).filter(t => t.startsWith('PK$')).sort();
  const othTokens = Object.keys(freq).filter(t => !t.startsWith('PK$')).sort();
  const fmt = t => freq[t] > 1 ? `${t}::[${freq[t]}]` : t;

  const structTokens = [...pkTokens.map(fmt), ...othTokens.map(fmt)];
  knfo.__meta__.structCode  = safeJoin(structTokens, '-');
  knfo.__meta__.b64urlCode  = b64urlHash(knfo.__meta__.structCode);

  return knfo;
}

module.exports = { buildKnfo };