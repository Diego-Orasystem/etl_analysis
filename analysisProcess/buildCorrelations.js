const candidateIsContained = (xCandidate, xCharLength, yCandidate) => {
  // Caso especial: si x es un único carácter (charLength === 1), comprobamos
  // inicio/fin o rodeado por no alfanuméricos.
  if (xCharLength === 1) {
    // Ej.: para 'A', la regex busca "(^|[^A-Za-z0-9])A($|[^A-Za-z0-9])".
    const pattern = new RegExp(`(^|[^A-Za-z0-9])${xCandidate}($|[^A-Za-z0-9])`);
    return pattern.test(yCandidate);
  }
  // Caso general
  return yCandidate.includes(xCandidate);
};

/** Verifica si dos conjuntos de filas son idénticos. */
function rowSetsAreIdentical(rowsX, rowsY) {
  if (rowsX.length !== rowsY.length) return false;
  const setX = new Set(rowsX);
  for (const r of rowsY) {
    if (!setX.has(r)) return false;
  }
  return true;
}

/** Extrae detalles de candidatos (candidateDetails). */
function getCandidateDetails(group) {
  if (!group.candidateDetails) return [];
  const out = [];
  for (const candidate in group.candidateDetails) {
    out.push({ candidate, rows: group.candidateDetails[candidate] });
  }
  return out;
}

// /**
//  * Construye correlaciones "strict" y "contained" en una sola pasada.
//  * Retorna { strict, contained } con sus respectivas correlaciones.
//  *
//  * - xItems, yItems: Arrays de "items" (cada item es { header, index, group, category })
//  * - skipSymmetrical: Si es true, se descartan casos donde (headerY < headerX) o
//  *   (headerY == headerX e indexY <= indexX), evitando duplicados en code->code.
//  */
// function buildCorrelationXtoY(xItems, yItems, skipSymmetrical = false) {
//   const strict = [];
//   const contained = [];

//   for (const xItem of xItems) {
//     for (const yItem of yItems) {
//       // Evitar recursiones infinitas / duplicados en code->code
//       if (skipSymmetrical) {
//         // Si el header Y es "menor" lexicográficamente, saltar
//         if (yItem.header < xItem.header) continue;
//         // Si son iguales, pero yItem.index <= xItem.index, saltar
//         if (yItem.header === xItem.header && yItem.index <= xItem.index) continue;
//       }

//       const xDetails = getCandidateDetails(xItem.group);
//       const yDetails = getCandidateDetails(yItem.group);
//       if (!xDetails.length || !yDetails.length) continue;

//       const matchMapStrict = {};
//       const matchMapContained = {};

//       xDetails.forEach(xd => {
//         yDetails.forEach(yd => {
//           // "strict": si tienen exactamente las mismas filas
//           const sameRows =
//             xd.rows.length === yd.rows.length && rowSetsAreIdentical(xd.rows, yd.rows);
//           if (!sameRows) return;

//           // Registramos un match en "strict"
//           const keyStrict = xd.candidate + '||' + yd.candidate;
//           matchMapStrict[keyStrict] = {
//             x: xd.candidate,
//             y: yd.candidate,
//             rows: xd.rows
//           };

//           // "contained": si yd.candidate contiene (con la lógica especial) a xd.candidate
//           if (candidateIsContained(xd.candidate, xItem.group.charLength, yd.candidate)) {
//             const keyContained = xd.candidate + '||' + yd.candidate;
//             matchMapContained[keyContained] = {
//               x: xd.candidate,
//               y: yd.candidate,
//               rows: xd.rows
//             };
//           }
//         });
//       });

//       const strictMatches = Object.values(matchMapStrict);
//       if (strictMatches.length) {
//         strict.push({
//           xGroup: `${xItem.header}::lengthGroups[${xItem.index}]`,
//           yGroup: `${yItem.header}::lengthGroups[${yItem.index}]`,
//           matches: strictMatches
//         });
//       }

//       const containedMatches = Object.values(matchMapContained);
//       if (containedMatches.length) {
//         contained.push({
//           xGroup: `${xItem.header}::lengthGroups[${xItem.index}]`,
//           yGroup: `${yItem.header}::lengthGroups[${yItem.index}]`,
//           matches: containedMatches
//         });
//       }
//     }
//   }

//   // Agrupar resultados por xGroup
//   function groupByXGroup(detailsArr) {
//     const mapXG = {};
//     detailsArr.forEach(obj => {
//       if (!mapXG[obj.xGroup]) mapXG[obj.xGroup] = [];
//       mapXG[obj.xGroup].push({ yGroup: obj.yGroup, matches: obj.matches });
//     });
//     return Object.keys(mapXG).map(xGroup => ({
//       xGroup,
//       correlated: mapXG[xGroup]
//     }));
//   }

//   return {
//     strict: groupByXGroup(strict),
//     contained: groupByXGroup(contained)
//   };
// }
  
/* =================================================================== *
 *  Nuevo buildCorrelationXtoY  –  O(N log r + Σ|y|)                   *
 *  ­· “strict”   : hash-join por filas                                *
 *  ­· “contained”: Aho-Corasick + regex para candidatos de 1 carácter *
 * =================================================================== */

const AhoCorasick = require('aho-corasick');   //  npm i aho-corasick
const xxhash32    = require('xxhashjs').h32;    //  npm i xxhashjs

const PRIME_SEED = 0xDEADBEEF;

/* ---------- utilidades básicas ----------------------------------- */

const buildRowKey = rows =>
  xxhash32( rows.length + ':' + rows.join(','), PRIME_SEED ).toString(16);

function groupByXGroup(pairs) {
  const map = new Map();      // Map<xGroup, {yGroup, matches}[]>
  for (const p of pairs) {
    if (!map.has(p.xGroup)) map.set(p.xGroup, []);
    map.get(p.xGroup).push({ yGroup: p.yGroup, matches: p.matches });
  }
  return Array.from(map, ([xGroup, correlated]) => ({ xGroup, correlated }));
}

/* ---------- núcleo optimizado ------------------------------------ */

function buildCorrelationXtoY(xItems, yItems, skipSym = false) {
  /* 1) aplanar la información de candidatos ----------------------- */
  const xDetails = [];
  const yDetails = [];

  function pushDetails(dst, itm) {
    const { header, index, group } = itm;
    const rowKeyCache = new Map();   // evita re-hash si un candidato se repite

    for (const { candidate, rows } of getCandidateDetails(group)) {
      const rk = rowKeyCache.get(rows) || buildRowKey(rows);
      rowKeyCache.set(rows, rk);
      dst.push({
        item:        itm,
        candidate,
        rows,
        rowKey : rk,
        charLen: group.charLength
      });
    }
  }

  xItems.forEach(it => pushDetails(xDetails, it));
  yItems.forEach(it => pushDetails(yDetails, it));

  if (!xDetails.length || !yDetails.length)
    return { strict: [], contained: [] };

  /* 2) STRICT  –  hash-join mediante rowKey ------------------------ */
  const xByRow = new Map();    // Map<rowKey, details[]>
  const yByRow = new Map();

  xDetails.forEach(d => { if (!xByRow.has(d.rowKey)) xByRow.set(d.rowKey, []); xByRow.get(d.rowKey).push(d); });
  yDetails.forEach(d => { if (!yByRow.has(d.rowKey)) yByRow.set(d.rowKey, []); yByRow.get(d.rowKey).push(d); });

  const strictPairs = [];

  for (const [rowKey, xArr] of xByRow) {
    const yArr = yByRow.get(rowKey);
    if (!yArr) continue;

    for (const x of xArr) {
      for (const y of yArr) {
        if (x.item === y.item && x.candidate === y.candidate) continue;   // mismo objeto
        if (skipSym) {
          const a = x.item, b = y.item;
          if (b.header < a.header) continue;
          if (b.header === a.header && b.index <= a.index) continue;
        }
        strictPairs.push({
          xGroup : `${x.item.header}::lengthGroups[${x.item.index}]`,
          yGroup : `${y.item.header}::lengthGroups[${y.item.index}]`,
          matches: [{ x: x.candidate, y: y.candidate, rows: x.rows }]
        });
      }
    }
  }

  /* 3) CONTAINED  –  Aho-Corasick + caso especial charLen === 1 ---- */
  const patterns      = [];                       // candidatos X con long. > 1
  const pat2xDetails  = new Map();                // Map<pattern, details[]>
  const char1XDetails = [];                       // candidatos X de 1 carácter

  xDetails.forEach(d => {
    if (d.charLen === 1) {
      char1XDetails.push(d);
    } else {
      patterns.push(d.candidate);
      if (!pat2xDetails.has(d.candidate)) pat2xDetails.set(d.candidate, []);
      pat2xDetails.get(d.candidate).push(d);
    }
  });

  const automaton = patterns.length ? new AhoCorasick(patterns) : null;
  const containedPairs = [];

  function maybePushContained(xd, yd) {
    if (xd.rows.length !== yd.rows.length) return;
    if (!rowSetsAreIdentical(xd.rows, yd.rows)) return;
    if (skipSym) {
      const a = xd.item, b = yd.item;
      if (b.header < a.header) return;
      if (b.header === a.header && b.index <= a.index) return;
    }
    containedPairs.push({
      xGroup : `${xd.item.header}::lengthGroups[${xd.item.index}]`,
      yGroup : `${yd.item.header}::lengthGroups[${yd.item.index}]`,
      matches: [{ x: xd.candidate, y: yd.candidate, rows: xd.rows }]
    });
  }

  // 3-A) patrones de longitud > 1 mediante AC
  if (automaton) {
    for (const yd of yDetails) {
      const hits = automaton.search(yd.candidate);   // [[pos, pattern], ...]
      if (!hits.length) continue;

      hits.forEach(([, pat]) => {
        pat2xDetails.get(pat).forEach(xd => maybePushContained(xd, yd));
      });
    }
  }

  // 3-B) patrones de 1 carácter vía RegExp optimizada
  if (char1XDetails.length) {
    for (const yd of yDetails) {
      char1XDetails.forEach(xd => {
        if (candidateIsContained(xd.candidate, 1, yd.candidate))
          maybePushContained(xd, yd);
      });
    }
  }

  /* 4) compactar resultados --------------------------------------- */
  return {
    strict   : groupByXGroup(strictPairs),
    contained: groupByXGroup(containedPairs)
  };
}

/** 
 * Construye las correlaciones globales para:
 *   - code->standard
 *   - code->free
 *   - code->code (nuevo).
 */
function buildCorrelations(analysis) {
  const groupItems = [];

  // 1) Recopilar items relevantes
  for (const header in analysis.columns) {
    const colObj = analysis.columns[header];
    if (!colObj.lengthGroups) continue;

    colObj.lengthGroups.forEach((group, index) => {
      if (!group.possibleUses || !group.possibleUses.length) return;
      const usage = group.possibleUses[0];

      if (['NumericCode', 'TextCode', 'AlphaNumericCode'].includes(usage)) {
        groupItems.push({ header, index, group, category: 'code' });
      } else if (usage === 'FreeText') {
        groupItems.push({ header, index, group, category: 'freeText' });
      } else if (usage === 'StandardText') {
        groupItems.push({ header, index, group, category: 'standard' });
      }
    });
  }

  // 2) Filtrar las distintas categorías
  const codeItems = groupItems.filter(i => i.category === 'code');
  const stdItems = groupItems.filter(i => i.category === 'standard');
  const freeItems = groupItems.filter(i => i.category === 'freeText');

  // 3) Realizar las correlaciones code->standard, code->free
  const { strict: codeStdStrict, contained: codeStdContained } =
    buildCorrelationXtoY(codeItems, stdItems);
  const { strict: codeFreeStrict, contained: codeFreeContained } =
    buildCorrelationXtoY(codeItems, freeItems);

  // 4) Nueva correlación code->code.  Se pasa skipSymmetrical=true 
  //    para evitar duplicados y ciclos del tipo X->Y, Y->X.
  const { strict: codeCodeStrict, contained: codeCodeContained } =
    buildCorrelationXtoY(codeItems, codeItems, true);

  // 5) Retornar los resultados compilados
  return {
    codeStdStrict,
    codeStdContained,
    codeFreeStrict,
    codeFreeContained,
    codeCodeStrict,
    codeCodeContained
  };
}

module.exports = buildCorrelations;