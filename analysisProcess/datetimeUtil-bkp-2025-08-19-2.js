'use strict';

/* ============================================================
   datetimeUtil.js  —  Utilidades consistentes para fechas/horas
   ------------------------------------------------------------
   Reglas de CLASIFICACIÓN (estrictas):
     • Para reconocer "Date" se exige año+mes+día explícitos (YMD) y hora = 00:00:00.
     • Para "Time" se exige al menos HH:mm (opcional :ss) sin parte de fecha.
     • Para "DateTime" se exige YMD + al menos HH:mm.
     • Todas las fechas/fechas-hora deben caer en un rango razonable [1995-01-01, 2099-12-31] (UTC).
     • NUNCA se clasifica por números puros (serial Excel, época en ms/seg, fracción de día).

   Salida canónica (UTC):
     • DATETIME/TIMESTAMP -> 'YYYY-MM-DD HH:MM:SS'
     • DATE               -> 'YYYY-MM-DD'
     • TIME               -> 'HH:MM:SS'

   Entradas aceptadas para *normalizar*:
     • ISO con T/Z u offset
     • ISO sin milis / sin zona (naive → se asume UTC)
     • Serial Excel (1900 por defecto; opcional 1904)
     • YYYY-MM-DD[ HH:MM[:SS]]
     • YYYY/MM/DD[ HH:MM[:SS]]
     • YYYY.MM.DD[ HH:MM[:SS]]
     • YYYYMMDD[ HHMM[SS]]
     • DD/MM/YYYY, DD-MM-YYYY, DD.MM.YYYY
     • DD/MM/YY,  MM/DD/YY (requiere order)
     • DD-MMM-YYYY (mes abreviado en ENG/ESP, año 2 o 4 dígitos)
     • 'HH:mm[:ss]' y 'hh:mm[:ss] AM/PM'

   Detección por CONJUNTO:
     • detectUniformFormat(values) evalúa una lista cerrada de formatos (KNOWN_FORMATS)
       y devuelve el formato único que explica TODOS los valores no vacíos.
     • checkDateOrDateTime() ahora intenta primero formato uniforme (requerido por defecto).

   ============================================================ */

const DAY_MS  = 24 * 60 * 60 * 1000;
const TWO_DIGIT_YEAR_PIVOT = 50; // 00–49 => 2000–2049, 50–99 => 1950–1999

// Rango razonable (UTC) para fechas y datetime
const DATE_MIN_YEAR = 1990;
const DATE_MAX_YEAR = 2150;
const DATE_MIN_UTC  = Date.UTC(DATE_MIN_YEAR, 0, 1);
const DATE_MAX_UTC  = Date.UTC(DATE_MAX_YEAR, 11, 31, 23, 59, 59, 999);
function isReasonableDateUTC(d) {
  if (!(d instanceof Date) || isNaN(d)) return false;
  const t = d.getTime();
  return t >= DATE_MIN_UTC && t <= DATE_MAX_UTC;
}

/* ------------------------- helpers básicos ------------------------- */
const _pad2 = n => String(n).padStart(2, '0');

function _fmtDateTimeUTC(d) {
  const y  = d.getUTCFullYear();
  const m  = _pad2(d.getUTCMonth() + 1);
  const da = _pad2(d.getUTCDate());
  const hh = _pad2(d.getUTCHours());
  const mm = _pad2(d.getUTCMinutes());
  const ss = _pad2(d.getUTCSeconds());
  return `${y}-${m}-${da} ${hh}:${mm}:${ss}`;
}
function _fmtDateUTC(d) {
  const y  = d.getUTCFullYear();
  const m  = _pad2(d.getUTCMonth() + 1);
  const da = _pad2(d.getUTCDate());
  return `${y}-${m}-${da}`;
}
function _fmtTimeUTC(d) {
  const hh = _pad2(d.getUTCHours());
  const mm = _pad2(d.getUTCMinutes());
  const ss = _pad2(d.getUTCSeconds());
  return `${hh}:${mm}:${ss}`;
}

/* ------------------------ normalización numérica ------------------- */
function _toNumberLocaleAware(s) {
  if (typeof s !== 'string') return Number(s);
  const raw = s.trim();
  if (!raw) return NaN;
  const hasDot = raw.includes('.');
  const hasCom = raw.includes(',');

  let norm = raw;
  if (hasDot && hasCom) {
    norm = norm.replace(/\./g, '').replace(',', '.');       // 1.234,56 -> 1234.56
  } else if (hasCom && !hasDot) {
    norm = norm.replace(',', '.');                           // 12,5 -> 12.5
  } else {
    norm = norm.replace(/(?<=\d)\s+(?=\d)/g, '');            // 1 234 -> 1234
  }
  return Number(norm);
}

/* -------------------- Excel serial (1900 / 1904) ------------------- */
function excelSerialToDateUTC(n, opts = {}) {
  const use1904 = !!opts.excel1904;
  const base = use1904 ? Date.UTC(1904, 0, 1) : Date.UTC(1899, 11, 30);
  const ms   = Math.round(n * DAY_MS);
  return new Date(base + ms);
}

/* ------------------- tablas/ayudas para meses texto ---------------- */
const _MONTH_TXT = {
  JAN:0, FEB:1, MAR:2, APR:3, MAY:4, JUN:5, JUL:6, AUG:7, SEP:8, OCT:9, NOV:10, DEC:11,
  ENE:0, FEBR:1, MARZ:2, ABR:3, MAYO:4, JUN:5, JUL:6, AGO:7, SEPT:8, OCT:9, NOV:10, DIC:11
};
const _isMonthName = s => _MONTH_TXT.hasOwnProperty(String(s||'').slice(0,3).toUpperCase());
const _monthFromName = s => {
  const k3 = String(s||'').slice(0,3).toUpperCase();
  return _MONTH_TXT.hasOwnProperty(k3) ? _MONTH_TXT[k3] : null;
};

/* ----------------------- validadores de fecha ---------------------- */
function _expandYY(yy, pivot = TWO_DIGIT_YEAR_PIVOT) {
  if (yy >= 0 && yy <= 99) return yy >= pivot ? (1900 + yy) : (2000 + yy);
  return yy;
}
function _daysInMonth(y, m1) { return new Date(Date.UTC(y, m1, 0)).getUTCDate(); }
function _validY(y){ return y >= DATE_MIN_YEAR && y <= DATE_MAX_YEAR; }
function _validM(m){ return m >= 1 && m <= 12; }
function _validD(d){ return d >= 1 && d <= 31; }

/* ------------------------ parseo de tiempo ------------------------- */
const _AMPM_RX = /\s*(AM|PM|A\.?M\.?|P\.?M\.?)\s*$/i;
function _parseTimeParts(txt) {
  const s = String(txt || '').trim();
  const ampmM = s.match(_AMPM_RX);
  const ampm  = ampmM ? ampmM[1].toUpperCase().replace(/\./g,'') : null;
  const core  = ampm ? s.replace(_AMPM_RX, '').trim() : s;

  const m = core.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!m) return null;

  let hh = +m[1], mi = +m[2], ss = +(m[3] || 0);

  if (ampm) {
    if (hh === 12) hh = 0;
    if (ampm.startsWith('P')) hh += 12;
  }

  if (hh === 24 && mi === 0 && ss === 0) { hh = 0; } // 24:00 -> 00:00

  if (!(hh >= 0 && hh <= 23) || !(mi >= 0 && mi <= 59) || !(ss >= 0 && ss <= 59)) {
    return null;
  }

  return { hh, mi, ss };
}

/**
 * Intenta convertir a Date (UTC). Devuelve Date o null.
 * Acepta opts = { order:'DMY'|'MDY'|'YMD', excel1904:boolean, yyPivot:number }.
 * Nota: admite serial Excel / epoch / fracción de día SOLO para normalizar, no para clasificar.
 */
function parseFlexibleDate(val, opts = {}) {
  if (val == null || val === '') return null;
  const { order = null, excel1904 = false, yyPivot = TWO_DIGIT_YEAR_PIVOT } = opts;

  if (val instanceof Date && !isNaN(val)) return val;

  // numérico o string-numérico → Excel serial / epoch / fracción de día
  if (typeof val === 'number' || (typeof val === 'string' && /^[\s\d.,+-]+$/.test(val))) {
    const num = typeof val === 'number' ? val : _toNumberLocaleAware(val);
    if (Number.isFinite(num)) {
      if (num > 20000 && num < 80000) return excelSerialToDateUTC(num, { excel1904 }); // Excel serial
      if (num > 1e11 && num < 1e14)   return new Date(num);                              // epoch ms
      if (num > 1e9  && num < 1e11)   return new Date(num * 1000);                      // epoch s
      if (num >= 0 && num < 1)        return new Date(Date.UTC(1970,0,1) + Math.round(num * DAY_MS)); // fracción día
    }
  }

  const s0 = String(val).trim();
  if (!s0) return null;

  // Con orden forzado (cubre YY y ambigüedades)
  if (order) {
    const d = parseAmbiguousWithOrder(s0, order, yyPivot, { excel1904 });
    if (d) return d;
  }

  // ISO con T (con/sin zona)
  if (/^\d{4}-\d{2}-\d{2}T/.test(s0)) {
    const d = new Date(s0);
    return isNaN(d) ? null : d;
  }

  // YYYYMMDD[ HHMM[SS]]
  let m = s0.match(/^(\d{4})(\d{2})(\d{2})(?:[ T]?(\d{2})(\d{2})(?:(\d{2}))?)?$/);
  if (m) {
    const Y=+m[1], Mo=+m[2], D=+m[3];
    const hh=+(m[4]||0), mi=+(m[5]||0), ss=+(m[6]||0);
    if (_validY(Y) && _validM(Mo) && _validD(D) && D <= _daysInMonth(Y, Mo)) {
      return new Date(Date.UTC(Y, Mo-1, D, hh, mi, ss));
    }
  }

  // YYYY-MM-DD[ HH:mm[:ss][ AM/PM]]
  m = s0.match(/^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{1,2}:\d{2}(?::\d{2})?(?:\s*(?:AM|PM|A\.?M\.?|P\.?M\.?)\s*)?))?$/i);
  if (m) {
    const [ , Y, Mo, D, tpart] = m;
    if (tpart) {
      const tp = _parseTimeParts(tpart);
      if (tp) return new Date(Date.UTC(+Y, +Mo - 1, +D, tp.hh, tp.mi, tp.ss));
    }
    return new Date(Date.UTC(+Y, +Mo - 1, +D, 0, 0, 0));
  }

  // YYYY/MM/DD[ ...]
  m = s0.match(/^(\d{4})\/(\d{1,2})\/(\d{1,2})(?:[ T](\d{1,2}:\d{2}(?::\d{2})?(?:\s*(?:AM|PM|A\.?M\.?|P\.?M\.?)\s*)?))?$/i);
  if (m) {
    const [ , Y, Mo, D, tpart] = m;
    if (tpart) {
      const tp = _parseTimeParts(tpart);
      if (tp) return new Date(Date.UTC(+Y, +Mo - 1, +D, tp.hh, tp.mi, tp.ss));
    }
    return new Date(Date.UTC(+Y, +Mo - 1, +D, 0, 0, 0));
  }

  // YYYY.MM.DD[ ...]
  m = s0.match(/^(\d{4})\.(\d{1,2})\.(\d{1,2})(?:[ T](\d{1,2}:\d{2}(?::\d{2})?(?:\s*(?:AM|PM|A\.?M\.?|P\.?M\.?)\s*)?))?$/i);
  if (m) {
    const [ , Y, Mo, D, tpart] = m;
    if (tpart) {
      const tp = _parseTimeParts(tpart);
      if (tp) return new Date(Date.UTC(+Y, +Mo - 1, +D, tp.hh, tp.mi, tp.ss));
    }
    return new Date(Date.UTC(+Y, +Mo - 1, +D, 0, 0, 0));
  }

  // ────────────────────────────────────────────────────────────────
  // NUEVO: DD/MM/YY(YY?) y variantes con '-' y '.' sin order global
  //        Intenta DMY y luego MDY con expansión de año YY (pivot).
  //        Soporta hora opcional.
  // ────────────────────────────────────────────────────────────────
  const tryDayMonthYear = (datePart, sep, tpart) => {
    const mm = datePart.match(new RegExp(`^(\\d{1,2})\\${sep}(\\d{1,2})\\${sep}(\\d{2,4})$`));
    if (!mm) return null;
    const d = +mm[1], mo = +mm[2], yRaw = +mm[3];
    const Y = _expandYY(yRaw, yyPivot);
    const tp = tpart ? _parseTimeParts(tpart) : null;

    // DMY
    if (_validD(d) && _validM(mo) && _validY(Y) && d <= _daysInMonth(Y, mo)) {
      return new Date(Date.UTC(Y, mo - 1, d, tp ? tp.hh : 0, tp ? tp.mi : 0, tp ? tp.ss : 0));
    }
    // MDY
    if (_validM(d) && _validD(mo) && _validY(Y) && mo <= _daysInMonth(Y, d)) {
      return new Date(Date.UTC(Y, d - 1, mo, tp ? tp.hh : 0, tp ? tp.mi : 0, tp ? tp.ss : 0));
    }
    return null;
  };

  // DD/MM/YY(YY?) con hora opcional
  if (/^\d{1,2}\/\d{1,2}\/\d{2,4}/.test(s0)) {
    const [datePart, timePart=''] = s0.split(/[ T]/);
    const d = tryDayMonthYear(datePart, '/', timePart);
    if (d) return d;
  }

  // DD-MM-YY(YY?) con hora opcional
  if (/^\d{1,2}-\d{1,2}-\d{2,4}/.test(s0)) {
    const [datePart, timePart=''] = s0.split(/[ T]/);
    const d = tryDayMonthYear(datePart, '-', timePart);
    if (d) return d;
  }

  // DD.MM.YY(YY?) con hora opcional
  if (/^\d{1,2}\.\d{1,2}\.\d{2,4}/.test(s0)) {
    const [datePart, timePart=''] = s0.split(/[ T]/);
    const d = tryDayMonthYear(datePart, '\\.', timePart);
    if (d) return d;
  }

  // DD-MMM-YYYY / YY con hora opcional
  m = s0.match(/^(\d{1,2})-([A-Za-z]{3,})-(\d{2,4})(?:[ T](\d{1,2}:\d{2}(?::\d{2})?(?:\s*(?:AM|PM|A\.?M\.?|P\.?M\.?)\s*)?))?$/i);
  if (m) {
    let [, d, mon, y, tpart] = m;
    const mmn = _monthFromName(mon);
    const Y  = _expandYY(Number(y), yyPivot);
    const tp = tpart ? _parseTimeParts(tpart) : null;
    if (mmn != null && _validY(Y) && _validD(+d) && (+d) <= _daysInMonth(Y, mmn+1)) {
      return new Date(Date.UTC(Y, mmn, +d, tp ? tp.hh : 0, tp ? tp.mi : 0, tp ? tp.ss : 0));
    }
  }

  // Solo tiempo → 1970-01-01
  const tpOnly = _parseTimeParts(s0);
  if (tpOnly) return new Date(Date.UTC(1970,0,1, tpOnly.hh, tpOnly.mi, tpOnly.ss));

  const d = new Date(s0); // intento nativo
  return isNaN(d) ? null : d;
}

function parseAmbiguousWithOrder(s, order='DMY', yyPivot=TWO_DIGIT_YEAR_PIVOT, opts = {}) {
  if (s == null) return null;
  if (typeof s === 'number' && isFinite(s)) return parseFlexibleDate(s, opts);
  const txt = String(s).trim();
  if (!txt) return null;

  // si opts.sep está dado, forzamos ese separador en el patrón
  const rawSep = opts.sep || null;
  const esc = (ch) => ch === '.' ? '\\.' : ch === '-' ? '\\-' : ch === '/' ? '/' : '[\\/\\-.]';
  const SEP = esc(rawSep || '');

  const rx = new RegExp(
    `^(\\d{1,4}|[A-Za-z]{3,})${SEP}(\\d{1,2}|[A-Za-z]{3,})${SEP}(\\d{1,4})(?:[ T](\\d{1,2}:\\d{2}(?::\\d{2})?(?:\\s*(?:AM|PM|A\\.?M\\.?|P\\.?M\\.?)\\s*)?))?$`,
    'i'
  );
  const m = txt.match(rx);
  if (!m) return null;

  let [ , a, b, c, tpart] = m;
  const tp = tpart ? _parseTimeParts(tpart) : { hh:0, mi:0, ss:0 };

  const token = (t) => _isMonthName(t)
    ? { kind:'MNAME', v:_monthFromName(t)+1 }
    : { kind:'NUM', v:Number(t) };

  const tok = [a,b,c].map(token);

  let d, M, Y;
  if (order === 'YMD') {
    const y = tok[0].kind === 'NUM' ? _expandYY(tok[0].v, yyPivot) : NaN;
    const m2= tok[1].v;
    const dd= tok[2].v;
    Y=y; M=m2; d=dd;
  } else if (order === 'DMY') {
    const dd= tok[0].v;
    const m2= tok[1].v;
    const y = tok[2].kind === 'NUM' ? _expandYY(tok[2].v, yyPivot) : NaN;
    d=dd; M=m2; Y=y;
  } else { // MDY
    const m2= tok[0].v;
    const dd= tok[1].v;
    const y = tok[2].kind === 'NUM' ? _expandYY(tok[2].v, yyPivot) : NaN;
    M=m2; d=dd; Y=y;
  }

  if (!_validY(Y) || !_validM(M) || !_validD(d) || d > _daysInMonth(Y, M)) return null;
  const dt = new Date(Date.UTC(Y, M-1, d, tp.hh, tp.mi, tp.ss));
  return isNaN(dt) ? null : dt;
}


function inferDateOrder(samples = [], opts = {}) {
  const prefer  = opts.prefer || 'DMY';
  const maxScan = opts.maxScan || 5000;
  const yyPivot = opts.yyPivot ?? TWO_DIGIT_YEAR_PIVOT;

  const seenSamples = [];
  const fits = { MDY:0, DMY:0, YMD:0 };
  const daysOk = { MDY:0, DMY:0, YMD:0 };
  const decisive = { MDY:0, DMY:0, YMD:0 };
  const ex = { MDY:[], DMY:[], YMD:[], BOTH_MDY_DMY:[] };

  const pushEx = (arr, v, cap=8) => { if (arr.length < cap) arr.push(v); };

  const asNum = (t) => {
    if (_isMonthName(t)) return _monthFromName(t) + 1;
    const n = Number(t);
    return Number.isFinite(n) ? n : null;
  };

  const norm = (s) => {
    const m = String(s||'').trim()
      .match(/^(\d{1,4}|[A-Za-z]{3,})[\/\-.](\d{1,2}|[A-Za-z]{3,})[\/\-.](\d{1,4})(?:\s+|$)/);
    if (!m) return null;
    return { a:m[1], b:m[2], c:m[3] };
  };

  const expandYear = (y) => _expandYY(Number(y), yyPivot);

  const chkMDY = (a,b,c) => {
    const M = _isMonthName(a) ? _monthFromName(a)+1 : Number(a);
    const D = Number(b);
    const Y = expandYear(c);
    if (!_validM(M) || !_validD(D) || !_validY(Y)) return { fit:false, ok:false };
    return { fit:true, ok: D <= _daysInMonth(Y, M) };
  };
  const chkDMY = (a,b,c) => {
    const D = Number(a);
    const M = _isMonthName(b) ? _monthFromName(b)+1 : Number(b);
    const Y = expandYear(c);
    if (!_validM(M) || !_validD(D) || !_validY(Y)) return { fit:false, ok:false };
    return { fit:true, ok: D <= _daysInMonth(Y, M) };
  };
  const chkYMD = (a,b,c) => {
    const Y = expandYear(a);
    const M = _isMonthName(b) ? _monthFromName(b)+1 : Number(b);
    const D = Number(c);
    if (!_validM(M) || !_validD(D) || !_validY(Y)) return { fit:false, ok:false };
    return { fit:true, ok: D <= _daysInMonth(Y, M) };
  };

  let scanned = 0;

  for (const s of samples) {
    if (scanned >= maxScan) break;
    const tok = norm(s);
    if (!tok) continue;

    const mdy = chkMDY(tok.a, tok.b, tok.c);
    const dmy = chkDMY(tok.a, tok.b, tok.c);
    const ymd = chkYMD(tok.a, tok.b, tok.c);

    if (mdy.fit) { fits.MDY++; if (mdy.ok) daysOk.MDY++; }
    if (dmy.fit) { fits.DMY++; if (dmy.ok) daysOk.DMY++; }
    if (ymd.fit) { fits.YMD++; if (ymd.ok) daysOk.YMD++; }

    const fitOrders = ['MDY','DMY','YMD'].filter(k => ({MDY:mdy,DMY:dmy,YMD:ymd}[k].fit));
    if (fitOrders.length === 1) {
      decisive[fitOrders[0]]++;
      pushEx(ex[fitOrders[0]], String(s).trim());
    } else if (fitOrders.length === 2 && fitOrders.includes('MDY') && fitOrders.includes('DMY')) {
      pushEx(ex.BOTH_MDY_DMY, String(s).trim());
    }

    pushEx(seenSamples, String(s).trim(), 12);
    scanned++;
  }

  // 1) Regla fuerte: si hay decisivas de un orden y del otro no, gana ese orden
  if (decisive.MDY > 0 && decisive.DMY === 0) {
    return {
      order: 'MDY',
      confidence: Math.min(1, 0.7 + decisive.MDY / Math.max(1, scanned)),
      counts: { fits, daysOk, decisive },
      seen: scanned,
      examples: ex,
      sample: seenSamples
    };
  }
  if (decisive.DMY > 0 && decisive.MDY === 0) {
    return {
      order: 'DMY',
      confidence: Math.min(1, 0.7 + decisive.DMY / Math.max(1, scanned)),
      counts: { fits, daysOk, decisive },
      seen: scanned,
      examples: ex,
      sample: seenSamples
    };
  }

  // 2) Si ambos tienen decisivas, gana el de mayor decisivas (empate → prefer)
  if (decisive.MDY > 0 || decisive.DMY > 0) {
    const order = (decisive.MDY === decisive.DMY) ? prefer : (decisive.MDY > decisive.DMY ? 'MDY' : 'DMY');
    const conf  = 0.55 + Math.abs(decisive.MDY - decisive.DMY) / Math.max(2, decisive.MDY + decisive.DMY);
    return {
      order, confidence: Math.min(1, conf),
      counts: { fits, daysOk, decisive }, seen: scanned,
      examples: ex, sample: seenSamples
    };
  }

  // 3) Sin decisivas → usa daysOk, luego fits, luego prefer
  const score = {
    MDY: 4*daysOk.MDY + 2*fits.MDY,
    DMY: 4*daysOk.DMY + 2*fits.DMY,
    YMD: 4*daysOk.YMD + 2*fits.YMD
  };
  let order = prefer;
  ['MDY','DMY','YMD'].forEach(k => { if (score[k] > score[order]) order = k; });

  const totalScore = score.MDY + score.DMY + score.YMD || 1;
  const confidence = Math.max(0, Math.min(1, 0.5*(score[order]/totalScore) + 0.5*((fits[order]||0)/Math.max(1, scanned))));

  return {
    order, confidence,
    counts: { fits, daysOk, decisive },
    seen: scanned,
    examples: ex,
    sample: seenSamples
  };
}

/* ------------------- detectores de tipo SQL (meta) ------------------ */
const isDateTimeType = (t) => /^(?:K)?(?:DT|DATETIME(?:\(\d+\))?|TIMESTAMP(?:\(\d+\))?)$/i.test(String(t||''));
const isDateType     = (t) => /^(?:K)?(?:DA|DATE)$/i.test(String(t||''));
const isTimeType     = (t) => /^(?:K)?(?:TM|TIME)$/i.test(String(t||''));

/* ---------------- lista de formatos esperados (uniformes) ----------- */
/** Cada entrada: { key, kind:'Date'|'Time'|'DateTime', order?:'DMY'|'MDY'|'YMD'|null, re:RegExp } */
const KNOWN_FORMATS = [
  // --- Date (YMD) ---
  { key:'YYYY-MM-DD', kind:'Date', order:'YMD', re:/^\d{4}-\d{2}-\d{2}$/ },
  { key:'YYYY/MM/DD', kind:'Date', order:'YMD', re:/^\d{4}\/\d{1,2}\/\d{1,2}$/ },
  { key:'YYYY.MM.DD', kind:'Date', order:'YMD', re:/^\d{4}\.\d{1,2}\.\d{1,2}$/ },
  { key:'YYYYMMDD',   kind:'Date', order:'YMD', re:/^\d{8}$/ },

  // --- Date (DMY) ---
  { key:'DD/MM/YYYY', kind:'Date', order:'DMY', re:/^\d{1,2}\/\d{1,2}\/\d{4}$/ },
  { key:'DD-MM-YYYY', kind:'Date', order:'DMY', re:/^\d{1,2}-\d{1,2}-\d{4}$/ },
  { key:'DD.MM.YYYY', kind:'Date', order:'DMY', re:/^\d{1,2}\.\d{1,2}\.\d{4}$/ },
  { key:'DD/MM/YY',   kind:'Date', order:'DMY', re:/^\d{1,2}\/\d{1,2}\/\d{2}$/ },
  { key:'DD-MM-YY',   kind:'Date', order:'DMY', re:/^\d{1,2}-\d{1,2}-\d{2}$/ },
  { key:'DD-MMM-YYYY',kind:'Date', order:'DMY', re:/^\d{1,2}-[A-Za-z]{3,}-\d{4}$/ },
  { key:'DD-MMM-YY',  kind:'Date', order:'DMY', re:/^\d{1,2}-[A-Za-z]{3,}-\d{2}$/ },

  // --- Date (MDY) ---
  { key:'MM/DD/YYYY', kind:'Date', order:'MDY', re:/^\d{1,2}\/\d{1,2}\/\d{4}$/ },
  { key:'MM-DD-YYYY', kind:'Date', order:'MDY', re:/^\d{1,2}-\d{1,2}-\d{4}$/ },
  { key:'MM.DD.YYYY', kind:'Date', order:'MDY', re:/^\d{1,2}\.\d{1,2}\.\d{4}$/ },
  { key:'MM/DD/YY',   kind:'Date', order:'MDY', re:/^\d{1,2}\/\d{1,2}\/\d{2}$/ },
  { key:'MM-DD-YY',   kind:'Date', order:'MDY', re:/^\d{1,2}-\d{1,2}-\d{2}$/ },

  // --- Time ---
  { key:'HH:mm',      kind:'Time', order:null,  re:/^\d{1,2}:\d{2}$/ },
  { key:'HH:mm:ss',   kind:'Time', order:null,  re:/^\d{1,2}:\d{2}:\d{2}$/ },
  { key:'hh:mm AM/PM',kind:'Time', order:null,  re:/^\d{1,2}:\d{2}\s*(?:AM|PM|A\.?M\.?|P\.?M\.?)$/i },
  { key:'hh:mm:ss AM/PM',kind:'Time',order:null,re:/^\d{1,2}:\d{2}:\d{2}\s*(?:AM|PM|A\.?M\.?|P\.?M\.?)$/i },

  // --- DateTime YMD ---
  { key:'YYYY-MM-DD HH:mm',    kind:'DateTime', order:'YMD', re:/^\d{4}-\d{2}-\d{2}[ T]\d{1,2}:\d{2}$/ },
  { key:'YYYY-MM-DD HH:mm:ss', kind:'DateTime', order:'YMD', re:/^\d{4}-\d{2}-\d{2}[ T]\d{1,2}:\d{2}:\d{2}$/ },
  { key:'YYYY/MM/DD HH:mm',    kind:'DateTime', order:'YMD', re:/^\d{4}\/\d{1,2}\/\d{1,2}[ T]\d{1,2}:\d{2}$/ },
  { key:'YYYY/MM/DD HH:mm:ss', kind:'DateTime', order:'YMD', re:/^\d{4}\/\d{1,2}\/\d{1,2}[ T]\d{1,2}:\d{2}:\d{2}$/ },
  { key:'YYYYMMDD HHmm',       kind:'DateTime', order:'YMD', re:/^\d{8}[ T]\d{4}$/ },
  { key:'YYYYMMDD HHmmss',     kind:'DateTime', order:'YMD', re:/^\d{8}[ T]\d{6}$/ },
  { key:'YYYYMMDDHHmm',        kind:'DateTime', order:'YMD', re:/^\d{12}$/ },
  { key:'YYYYMMDDHHmmss',      kind:'DateTime', order:'YMD', re:/^\d{14}$/ },  
  { key:'ISO-8601',            kind:'DateTime', order:'YMD', re:/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2})?(?:\.\d+)?(?:Z|[+\-]\d{2}:\d{2})?$/ },

  // --- DateTime DMY ---
  { key:'DD/MM/YYYY HH:mm',    kind:'DateTime', order:'DMY', re:/^\d{1,2}\/\d{1,2}\/\d{4}[ T]\d{1,2}:\d{2}$/ },
  { key:'DD/MM/YYYY HH:mm:ss', kind:'DateTime', order:'DMY', re:/^\d{1,2}\/\d{1,2}\/\d{4}[ T]\d{1,2}:\d{2}:\d{2}$/ },
  { key:'DD-MM-YYYY HH:mm',    kind:'DateTime', order:'DMY', re:/^\d{1,2}-\d{1,2}-\d{4}[ T]\d{1,2}:\d{2}$/ },
  { key:'DD-MM-YYYY HH:mm:ss', kind:'DateTime', order:'DMY', re:/^\d{1,2}-\d{1,2}-\d{4}[ T]\d{1,2}:\d{2}:\d{2}$/ },
  { key:'DD-MMM-YYYY HH:mm',   kind:'DateTime', order:'DMY', re:/^\d{1,2}-[A-Za-z]{3,}-\d{4}[ T]\d{1,2}:\d{2}$/ },
  { key:'DD-MMM-YYYY HH:mm:ss',kind:'DateTime', order:'DMY', re:/^\d{1,2}-[A-Za-z]{3,}-\d{4}[ T]\d{1,2}:\d{2}:\d{2}$/ },

  // --- DateTime MDY ---
  { key:'MM/DD/YYYY HH:mm',    kind:'DateTime', order:'MDY', re:/^\d{1,2}\/\d{1,2}\/\d{4}[ T]\d{1,2}:\d{2}$/ },
  { key:'MM/DD/YYYY HH:mm:ss', kind:'DateTime', order:'MDY', re:/^\d{1,2}\/\d{1,2}\/\d{4}[ T]\d{1,2}:\d{2}:\d{2}$/ },
  { key:'MM-DD-YYYY HH:mm',    kind:'DateTime', order:'MDY', re:/^\d{1,2}-\d{1,2}-\d{4}[ T]\d{1,2}:\d{2}$/ },
  { key:'MM-DD-YYYY HH:mm:ss', kind:'DateTime', order:'MDY', re:/^\d{1,2}-\d{1,2}-\d{4}[ T]\d{1,2}:\d{2}:\d{2}$/ },
];

/** Devuelve sólo el listado de cadenas "key" para mostrar o logs. */
function listKnownFormats() {
  return KNOWN_FORMATS.map(f => f.key);
}



/* ------------------ PARSERS ESTRICTOS para CLASIFICAR --------------- */
function _hasYMDInText(s) {
  const txt = String(s || '').trim();
  return (
    /^\d{4}[\/\-.]\d{1,2}[\/\-.]\d{1,2}$/.test(txt) ||     // YYYY-MM-DD / YYYY/MM/DD / YYYY.MM.DD
    /^\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{4}$/.test(txt) ||      // DD/MM/YYYY, etc.
    /^\d{1,2}-[A-Za-z]{3,}-\d{2,4}$/.test(txt) ||           // DD-MMM-YYYY
    /^\d{4}-\d{2}-\d{2}T/.test(txt) ||                      // ISO con T
    /^\d{8}$/.test(txt)                                     // YYYYMMDD
  );
}

/** Devuelve Date(UTC 1970-01-01 HH:mm:ss) o null para hh:mm[:ss][AM/PM] */
function parseTimeStrict(text) {
  const s = String(text || '').trim();
  if (!s) return null;
  if (/[\/\-.]/.test(s)) return null;           // si trae separadores de fecha, no es “solo hora”
  const tp = _parseTimeParts(s);
  if (!tp) return null;
  return new Date(Date.UTC(1970, 0, 1, tp.hh, tp.mi, tp.ss));
}

/** Devuelve Date(UTC Y-M-D 00:00:00) o null; permite YY si viene order forzado */
function parseDateStrict(text, opts = {}) {
  const s = String(text || '').trim();
  if (!s) return null;
  if (/\d{1,2}:\d{2}/.test(s)) return null;

  const order   = opts.order || null;
  const yyPivot = opts.yyPivot ?? TWO_DIGIT_YEAR_PIVOT;
  const sep     = opts.sep || null;

  if (order) {
    const d = parseAmbiguousWithOrder(s, order, yyPivot, { excel1904: !!opts.excel1904, sep });
    if (!d || isNaN(d)) return null;
    if (d.getUTCHours() !== 0 || d.getUTCMinutes() !== 0 || d.getUTCSeconds() !== 0) return null;
    return isReasonableDateUTC(d) ? d : null;
  }

  if (!_hasYMDInText(s)) return null;

  // YYYYMMDD sin hora
  if (/^\d{8}$/.test(s)) {
    const Y=+s.slice(0,4), M=+s.slice(4,6), D=+s.slice(6,8);
    if (_validY(Y) && _validM(M) && _validD(D) && D <= _daysInMonth(Y,M)) {
      const d = new Date(Date.UTC(Y, M-1, D, 0,0,0));
      return isReasonableDateUTC(d) ? d : null;
    }
    return null;
  }

  const d = parseFlexibleDate(s, opts);
  if (!d || isNaN(d)) return null;
  if (d.getUTCHours() !== 0 || d.getUTCMinutes() !== 0 || d.getUTCSeconds() !== 0) return null;
  return isReasonableDateUTC(d) ? d : null;
}

/** Devuelve Date(UTC Y-M-D HH:mm[:ss]) o null; permite YY si viene order forzado */
function parseDateTimeStrict(text, opts = {}) {
  const s = String(text || '').trim();
  if (!s) return null;

  if (!/\d{1,2}:\d{2}/.test(s)) {
    if (!/^\d{8}\s*\d{4,6}$/.test(s)) return null;
  }

  const order   = opts.order || null;
  const yyPivot = opts.yyPivot ?? TWO_DIGIT_YEAR_PIVOT;
  const sep     = opts.sep || null;

  if (order) {
    const d = parseAmbiguousWithOrder(s, order, yyPivot, { excel1904: !!opts.excel1904, sep });
    if (d && isReasonableDateUTC(d)) return d;
  }

  // YYYYMMDD HHmm[ss]
  const m = s.match(/^(\d{4})(\d{2})(\d{2})\s*(\d{2})(\d{2})(\d{2})?$/);
  if (m) {
    const Y=+m[1], Mo=+m[2], D=+m[3], hh=+m[4], mi=+m[5], ss=+(m[6]||0);
    if (_validY(Y) && _validM(Mo) && _validD(D) && D <= _daysInMonth(Y,Mo)) {
      const d = new Date(Date.UTC(Y, Mo-1, D, hh, mi, ss));
      return isReasonableDateUTC(d) ? d : null;
    }
  }

  const d = parseFlexibleDate(s, opts);
  if (!d || isNaN(d)) return null;
  return isReasonableDateUTC(d) ? d : null;
}

function detectUniformFormat(values = [], opts = {}) {
  const prefer = opts.prefer || 'DMY';
  const requireUniformSep = opts.requireUniformSep !== false; // por defecto, true

  const nonEmpty = values
    .map(v => (v == null ? '' : String(v).trim()))
    .filter(s => s.length > 0);

  if (!nonEmpty.length) return null;

  // 0) si todos son horas
  const allTime = nonEmpty.every(s => /^\d{1,2}:\d{2}(:\d{2})?(\s*(AM|PM|A\.?M\.?|P\.?M\.?))?$/i.test(s));
  if (allTime) {
    const anySeconds = nonEmpty.some(s => /^\d{1,2}:\d{2}:\d{2}/.test(s));
    return {
      key: anySeconds ? 'HH:mm:ss' : 'HH:mm',
      kind: 'Time',
      order: null,
      sep: null,
      re: new RegExp(anySeconds ? '^\\d{1,2}:\\d{2}:\\d{2}$' : '^\\d{1,2}:\\d{2}$')
    };
  }

  // 1) separador usado en el conjunto (primera ocurrencia de / - . en cada string)
  const seps = new Set(
    nonEmpty.map(s => {
      const m = s.match(/[\/\-.]/);
      return m ? m[0] : null;
    }).filter(Boolean)
  );

  // Si exigimos separador uniforme y hay más de uno, no hay formato uniforme
  if (requireUniformSep && seps.size > 1) return null;

  const uniqueSep = [...seps][0] || null;

  // shortcuts para compactos YYYYMMDD...
  const all8  = nonEmpty.every(s => /^\d{8}$/.test(s));
  const all12 = nonEmpty.every(s => /^\d{12}$/.test(s));
  const all14 = nonEmpty.every(s => /^\d{14}$/.test(s));
  if (all8)  return { key:'YYYYMMDD', kind:'Date', order:'YMD', sep: null, re:/^\d{8}$/ };
  if (all12) return { key:'YYYYMMDDHHmm', kind:'DateTime', order:'YMD', sep: null, re:/^\d{12}$/ };
  if (all14) return { key:'YYYYMMDDHHmmss', kind:'DateTime', order:'YMD', sep: null, re:/^\d{14}$/ };

  // 2) familias con separador uniforme: usamos el separador concreto si existe
  const esc = (ch) => ch === '.' ? '\\.' : ch === '-' ? '\\-' : ch === '/' ? '/' : '[\\/\\-.]';
  const SEP = esc(uniqueSep || '');

  const fam = {
    'DD?MM?YYYY':           { kind:'Date',     order:'DMY', re: new RegExp(`^(\\d{1,2})${SEP}(\\d{1,2})${SEP}(\\d{4})$`) },
    'DD?MM?YY':             { kind:'Date',     order:'DMY', re: new RegExp(`^(\\d{1,2})${SEP}(\\d{1,2})${SEP}(\\d{2})$`) },
    'MM?DD?YYYY':           { kind:'Date',     order:'MDY', re: new RegExp(`^(\\d{1,2})${SEP}(\\d{1,2})${SEP}(\\d{4})$`) },
    'MM?DD?YY':             { kind:'Date',     order:'MDY', re: new RegExp(`^(\\d{1,2})${SEP}(\\d{1,2})${SEP}(\\d{2})$`) },
    'YYYY?MM?DD':           { kind:'Date',     order:'YMD', re: new RegExp(`^(\\d{4})${SEP}(\\d{1,2})${SEP}(\\d{1,2})$`) },

    'DD?MON?YYYY':          { kind:'Date',     order:'DMY', re: new RegExp(`^(\\d{1,2})${SEP}([A-Za-z]{3,})${SEP}(\\d{4})$`) },
    'DD?MON?YY':            { kind:'Date',     order:'DMY', re: new RegExp(`^(\\d{1,2})${SEP}([A-Za-z]{3,})${SEP}(\\d{2})$`) },
    'MON?DD?YYYY':          { kind:'Date',     order:'MDY', re: new RegExp(`^([A-Za-z]{3,})${SEP}(\\d{1,2})${SEP}(\\d{4})$`) },
    'MON?DD?YY':            { kind:'Date',     order:'MDY', re: new RegExp(`^([A-Za-z]{3,})${SEP}(\\d{1,2})${SEP}(\\d{2})$`) },

    'DD?MM?YYYY HH:mm':     { kind:'DateTime', order:'DMY', re: new RegExp(`^(\\d{1,2})${SEP}(\\d{1,2})${SEP}(\\d{4})[ T]\\d{1,2}:\\d{2}$`) },
    'DD?MM?YYYY HH:mm:ss':  { kind:'DateTime', order:'DMY', re: new RegExp(`^(\\d{1,2})${SEP}(\\d{1,2})${SEP}(\\d{4})[ T]\\d{1,2}:\\d{2}:\\d{2}$`) },
    'MM?DD?YYYY HH:mm':     { kind:'DateTime', order:'MDY', re: new RegExp(`^(\\d{1,2})${SEP}(\\d{1,2})${SEP}(\\d{4})[ T]\\d{1,2}:\\d{2}$`) },
    'MM?DD?YYYY HH:mm:ss':  { kind:'DateTime', order:'MDY', re: new RegExp(`^(\\d{1,2})${SEP}(\\d{1,2})${SEP}(\\d{4})[ T]\\d{1,2}:\\d{2}:\\d{2}$`) },
    'YYYY?MM?DD HH:mm':     { kind:'DateTime', order:'YMD', re: new RegExp(`^(\\d{4})${SEP}(\\d{1,2})${SEP}(\\d{1,2})[ T]\\d{1,2}:\\d{2}$`) },
    'YYYY?MM?DD HH:mm:ss':  { kind:'DateTime', order:'YMD', re: new RegExp(`^(\\d{4})${SEP}(\\d{1,2})${SEP}(\\d{1,2})[ T]\\d{1,2}:\\d{2}:\\d{2}$`) },
  };

  const familyCandidates = Object.entries(fam)
    .filter(([_, spec]) => nonEmpty.every(s => spec.re.test(s)))
    .map(([key, spec]) => ({ key, ...spec, sep: uniqueSep }));

  if (familyCandidates.length) {
    const inferred = inferDateOrder(nonEmpty, { prefer });
    const filtered = inferred.order
      ? familyCandidates.filter(f => f.order === inferred.order)
      : familyCandidates;
    const chosen = (filtered.length ? filtered : familyCandidates)[0];
    return chosen || null;
  }

  // 3) ISO-8601 puro
  const allISO = nonEmpty.every(s => /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2})?(?:\.\d+)?(?:Z|[+\-]\d{2}:\d{2})?$/.test(s));
  if (allISO) return { key:'ISO-8601', kind:'DateTime', order:'YMD', sep: null, re:/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2})?(?:\.\d+)?(?:Z|[+\-]\d{2}:\d{2})?$/ };

  // 4) Lista declarativa original, filtrando por orden inferida (el separador se ignora aquí)
  const inferred = inferDateOrder(nonEmpty, { prefer });
  const listCandidates = KNOWN_FORMATS.filter(fmt => {
    if ((fmt.kind === 'Date' || fmt.kind === 'DateTime') && fmt.order && inferred.order && fmt.order !== inferred.order) {
      return false;
    }
    return nonEmpty.every(s => fmt.re.test(s));
  });
  if (listCandidates.length === 1) return { ...listCandidates[0], sep: uniqueSep || null };
  if (listCandidates.length > 1) {
    const ranked = listCandidates.sort((a, b) => {
      const aYY = /YY(?!Y)/.test(a.key) ? 0 : 1;
      const bYY = /YY(?!Y)/.test(b.key) ? 0 : 1;
      if (aYY !== bYY) return bYY - aYY;
      const aDT = a.kind === 'DateTime' ? 1 : 0;
      const bDT = b.kind === 'DateTime' ? 1 : 0;
      if (aDT !== bDT) return bDT - aDT;
      if (inferred.order) {
        if (a.order === inferred.order && b.order !== inferred.order) return -1;
        if (b.order === inferred.order && a.order !== inferred.order) return 1;
      }
      return a.key.length > b.key.length ? -1 : 1;
    });
    return { ...ranked[0], sep: uniqueSep || null };
  }

  return null;
}

function checkDateOrDateTime(values = [], opts = {}) {
  if (!Array.isArray(values) || values.length === 0) return null;

  const requireUniform = opts.requireUniformFormat !== false; // por defecto, true
  const order = opts.order || null;

  if (requireUniform) {
    const fmt = detectUniformFormat(values, { prefer: order || 'DMY' });
    if (!fmt) return null;

    // Validación estricta por regex + parseo con funciones estrictas
    const parseFn = fmt.kind === 'Time'
      ? (s) => parseTimeStrict(s)
      : fmt.kind === 'Date'
        ? (s) => parseDateStrict(s, { order: fmt.order || order })
        : (s) => parseDateTimeStrict(s, { order: fmt.order || order });

    for (const v of values) {
      const s = String(v ?? '').trim();
      if (!s || !fmt.re.test(s)) return null;
      const d = parseFn(s);
      if (!d) return null;
      if (fmt.kind !== 'Time' && !isReasonableDateUTC(d)) return null;
    }
    return fmt.kind;
  }

  // Fallback no uniforme (más permisivo, evita números puros)
  const kinds = new Set();
  const parsed = [];

  for (const v of values) {
    const s = String(v ?? '').trim();
    if (!s) return null;

    let d = parseDateTimeStrict(s, { order });
    if (d) { kinds.add('DateTime'); parsed.push({ kind: 'DateTime', d }); continue; }

    d = parseDateStrict(s, { order });
    if (d) { kinds.add('Date'); parsed.push({ kind: 'Date', d }); continue; }

    d = parseTimeStrict(s);
    if (d) { kinds.add('Time'); parsed.push({ kind: 'Time', d }); continue; }

    return null; // algún valor no encaja
  }

  if (kinds.size === 1) return [...kinds][0];

  // Si mezcla Date y DateTime con una hora fija 00:00:00, colapsa a Date
  if (kinds.has('Date') && kinds.has('DateTime')) {
    const dtOnly = parsed.filter(p => p.kind === 'DateTime').map(p => p.d);
    if (dtOnly.length) {
      const r = dtOnly[0];
      const sameTime = dtOnly.every(d =>
        d.getUTCHours() === r.getUTCHours() &&
        d.getUTCMinutes() === r.getUTCMinutes() &&
        d.getUTCSeconds() === r.getUTCSeconds()
      );
      if (sameTime && r.getUTCMinutes() === 0 && r.getUTCSeconds() === 0) return 'Date';
    }
  }
  return 'DateTime';
}

/* ----------------------- normalizadores canónicos ------------------- */
function normalizeSqlDateTime(v, opts={}) {
  if (typeof v === 'number' && isFinite(v)) {
    return _fmtDateTimeUTC(excelSerialToDateUTC(v, { excel1904: !!opts.excel1904 }));
  }
  if (opts.order) {
    const d = parseAmbiguousWithOrder(v, opts.order, opts.yyPivot ?? TWO_DIGIT_YEAR_PIVOT, { excel1904: !!opts.excel1904 });
    if (d) return _fmtDateTimeUTC(d);
  }
  const d = parseFlexibleDate(v, opts);
  return d ? _fmtDateTimeUTC(d) : v;
}

function normalizeSqlDate(v, opts={}) {
  if (typeof v === 'number' && isFinite(v)) {
    return _fmtDateUTC(excelSerialToDateUTC(v, { excel1904: !!opts.excel1904 }));
  }
  if (opts.order) {
    const d = parseAmbiguousWithOrder(v, opts.order, opts.yyPivot ?? TWO_DIGIT_YEAR_PIVOT, { excel1904: !!opts.excel1904 });
    if (d) return _fmtDateUTC(d);
  }
  // aceptar YYYYMMDD
  const s = typeof v === 'string' ? v.trim() : null;
  if (s && /^\d{8}$/.test(s)) {
    const Y=+s.slice(0,4), M=+s.slice(4,6), D=+s.slice(6,8);
    if (_validY(Y) && _validM(M) && _validD(D) && D <= _daysInMonth(Y,M)) {
      return `${Y}-${_pad2(M)}-${_pad2(D)}`;
    }
  }
  const d = parseFlexibleDate(v, opts);
  return d ? _fmtDateUTC(d) : v;
}

function normalizeSqlTime(v, opts={}) {
  if (typeof v === 'number' && v >= 0 && v < 1) {
    const d = new Date(Date.UTC(1970,0,1) + Math.round(v * DAY_MS));
    return _fmtTimeUTC(d);
  }
  const s  = typeof v === 'string' ? v.trim() : null;
  const tp = s ? _parseTimeParts(s) : null;
  if (tp) return `${_pad2(tp.hh)}:${_pad2(tp.mi)}:${_pad2(tp.ss)}`;
  const d = parseFlexibleDate(v, opts);
  return d ? _fmtTimeUTC(d) : v;
}

/** Normaliza por tipo SQL (.meta). */
function normalizeBySqlType(value, sqlType, opts={}) {
  const T = String(sqlType || '').toUpperCase();
  if (isDateTimeType(T)) return normalizeSqlDateTime(value, opts);
  if (isDateType(T))     return normalizeSqlDate(value, opts);
  if (isTimeType(T))     return normalizeSqlTime(value, opts);
  return value;
}

/** Normaliza por "usage" del análisis ('DateTime'|'Date'|'Time'). */
function normalizeByUsage(value, usage, opts={}) {
  const u = String(usage || '').toLowerCase();
  if (u === 'datetime') return normalizeSqlDateTime(value, opts);
  if (u === 'date')     return normalizeSqlDate(value, opts);
  if (u === 'time')     return normalizeSqlTime(value, opts);
  return value;
}

/* ------------------------------ exports ---------------------------- */
module.exports = {
  // límites y validadores de rango
  DATE_MIN_YEAR,
  DATE_MAX_YEAR,
  isReasonableDateUTC,

  // listado de formatos y detección uniforme
  KNOWN_FORMATS,
  listKnownFormats,
  detectUniformFormat,

  // formateadores/normalizadores
  normalizeSqlDateTime,
  normalizeSqlDate,
  normalizeSqlTime,
  normalizeBySqlType,
  normalizeByUsage,

  // parsers/identificadores
  parseFlexibleDate,
  parseAmbiguousWithOrder,
  excelSerialToDateUTC,
  isDateTimeType,
  isDateType,
  isTimeType,

  // parsers estrictos y clasificación por conjunto
  parseTimeStrict,
  parseDateStrict,
  parseDateTimeStrict,
  checkDateOrDateTime,

  // inferencia de orden
  inferDateOrder,
};