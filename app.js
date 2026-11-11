/* Lumen: sesi 3 dari 3. Feed Binance, mesin quant deterministik (tanpa angka acak), TTFM, GXT, renderer canvas. */
(() => {
'use strict';
const $ = (s, r = document) => r.querySelector(s);
const $$ = (s, r = document) => [...r.querySelectorAll(s)];
const bind = (k, v) => $$(`[data-bind="${k}"]`).forEach(e => { e.textContent = v; });
const dirOf = v => (v > 0 ? 'up' : v < 0 ? 'down' : 'flat');
const setDir = (k, v) => $$(`[data-bind="${k}"]`).forEach(e => { e.dataset.dir = dirOf(v); });
const clamp = (x, a, b) => Math.min(b, Math.max(a, x));
const mean = a => a.reduce((s, x) => s + x, 0) / (a.length || 1);
const std = a => { const m = mean(a); return Math.sqrt(mean(a.map(x => (x - m) ** 2))); };
const sgn = Math.sign;
const TF = { '1m': 6e4, '5m': 3e5, '15m': 9e5, '1h': 36e5, '4h': 144e5, '1d': 864e5 };
const HTF = { '1m': '15m', '5m': '1h', '15m': '4h', '1h': '1d', '4h': '1w', '1d': '1M' };
const REST = '';
const RESTS = ['https://data-api.binance.vision/api/v3', 'https://api.binance.com/api/v3', 'https://api1.binance.com/api/v3', 'https://api-gcp.binance.com/api/v3'];
const WSS = ['wss://data-stream.binance.vision/stream?streams=', 'wss://stream.binance.com:9443/stream?streams=', 'wss://stream.binance.com/stream?streams='];
let hostI = 0, wsI = 0;
const HOST = { spot: { rest: RESTS, ws: WSS }, fut: { rest: ['https://fapi.binance.com/fapi/v1'], ws: ['wss://fstream.binance.com/stream?streams=', 'wss://fstream.binance.com:443/stream?streams='] } };
const SMT_AUTO = { BTCUSDT: 'ETHUSDT', ETHUSDT: 'BTCUSDT', SOLUSDT: 'ETHUSDT', BNBUSDT: 'ETHUSDT', XRPUSDT: 'BTCUSDT', XAUUSDT: 'XAGUSDT', XAGUSDT: 'XAUUSDT', QQQUSDT: 'SPYUSDT', SPYUSDT: 'QQQUSDT', EURUSDT: 'GBPUSDT', GBPUSDT: 'EURUSDT', AUDUSDT: 'EURUSDT', CLUSDT: 'BZUSDT', BZUSDT: 'CLUSDT' };
const CATS = [['crypto', 'Kripto'], ['com', 'Komoditas'], ['idx', 'Indeks dan saham'], ['fx', 'Forex']];
const row = (s, n, c, v = 'spot') => ({ s, n, c, v });
const CATALOG = [
  row('BTCUSDT', 'Bitcoin', 'crypto'), row('ETHUSDT', 'Ethereum', 'crypto'), row('SOLUSDT', 'Solana', 'crypto'), row('BNBUSDT', 'BNB', 'crypto'), row('XRPUSDT', 'XRP', 'crypto'), row('DOGEUSDT', 'Dogecoin', 'crypto'),
  row('ADAUSDT', 'Cardano', 'crypto'), row('AVAXUSDT', 'Avalanche', 'crypto'), row('LINKUSDT', 'Chainlink', 'crypto'), row('TRXUSDT', 'TRON', 'crypto'), row('DOTUSDT', 'Polkadot', 'crypto'), row('LTCUSDT', 'Litecoin', 'crypto'),
  row('SUIUSDT', 'Sui', 'crypto'), row('NEARUSDT', 'NEAR', 'crypto'), row('PEPEUSDT', 'Pepe', 'crypto'), row('TONUSDT', 'Toncoin', 'crypto'),
  row('XAUUSDT', 'Emas (XAU/USD)', 'com', 'fut'), row('XAGUSDT', 'Perak (XAG/USD)', 'com', 'fut'),
  row('QQQUSDT', 'Nasdaq 100 (QQQ)', 'idx', 'fut'), row('SPYUSDT', 'S&P 500 (SPY)', 'idx', 'fut'),
  row('EURUSDT', 'Euro (EUR/USD)', 'fx'), row('GBPUSDT', 'Poundsterling (GBP/USD)', 'fx'), row('AUDUSDT', 'Dolar Australia (AUD/USD)', 'fx')
];
const NAMES = { CLUSDT: ['Minyak WTI', 'com'], BZUSDT: ['Minyak Brent', 'com'], NATGASUSDT: ['Gas alam', 'com'], XPTUSDT: ['Platinum', 'com'], XPDUSDT: ['Paladium', 'com'], COPPERUSDT: ['Tembaga', 'com'], DIAUSDT: ['Dow Jones (DIA)', 'idx'], IWMUSDT: ['Russell 2000 (IWM)', 'idx'], AAPLUSDT: ['Apple', 'idx'], TSLAUSDT: ['Tesla', 'idx'], NVDAUSDT: ['Nvidia', 'idx'], MSFTUSDT: ['Microsoft', 'idx'] };
const FX_RE = /^(EUR|GBP|AUD|NZD|CAD|CHF|JPY|CNH|SGD|IDR|MXN|TRY|ZAR|KRW|INR)USDT$/;
const pairOk = () => CATALOG.some(i => i.s === pairSym() && i.v === cfg.venue);
const ALPHA = 0.08, BETA = 0.9, SHRINK = 0.5, NU = 5;

/* Pengaturan */
const DEF = { symbol: 'BTCUSDT', venue: 'spot', tf: '1h', horizon: 24, context: 512, lines: 19, source: 'quant', endpoint: '', drift: 'kalman', dist: 'student', b50: true, b80: true, b95: true, wq: 40, wt: 30, wg: 30, smt: 'auto', layers: { forecast: true, liquidity: true, fvg: true, htf: false, sessions: false } };
let cfg = { ...DEF, layers: { ...DEF.layers } };
try { Object.assign(cfg, JSON.parse(localStorage.getItem('lumen.cfg') || '{}')); } catch { /* abaikan */ }
const save = () => { try { localStorage.setItem('lumen.cfg', JSON.stringify(cfg)); } catch { /* abaikan */ } };
const S = { candles: [], pair: [], an: null, ext: null, bt: null, btKey: '', view: { bars: 90, off: 0 }, feed: { ws: null, tries: 0, last: 0, lat: 0, skew: 0, ticks: [], reconnects: 0, gaps: 0, gen: 0 }, mouse: null, dirty: true, hud: true, timer: 0, px: 0, live: { hit: 0, n: 0, pend: null }, reveal: -1e9, stage: false, lastTrade: 0, qDirty: false };

/* Format */
const fmt = (p, d) => { const x = Math.abs(p), n = d ?? (x >= 100 ? 2 : x >= 10 ? 3 : 5); return p.toLocaleString('en-US', { minimumFractionDigits: n, maximumFractionDigits: n }); };
const pct = (x, d = 2) => `${x >= 0 ? '+' : ''}${(x * 100).toFixed(d)}%`;
const hms = ms => { const s = Math.max(0, Math.floor(ms / 1000)), h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60); return `${h ? h + ':' : ''}${String(m).padStart(2, '0')}:${String(s % 60).padStart(2, '0')}`; };
const log = (msg, level = 'ok') => { const ol = $('#engine-log'), li = document.createElement('li'); li.dataset.level = level; li.textContent = `${new Date().toISOString().slice(11, 19)} ${msg}`; ol.prepend(li); while (ol.children.length > 40) ol.lastChild.remove(); };

/* Statistik: invers CDF normal (Acklam), kuantil Student-t via Cornish-Fisher, berskala varians satu */
const zInv = p => {
  const a = [-39.69683028665376, 220.9460984245205, -275.9285104469687, 138.357751867269, -30.66479806614716, 2.506628277459239];
  const b = [-54.47609879822406, 161.5858368580409, -155.6989798598866, 66.80131188771972, -13.28068155288572];
  const c = [-0.007784894002430293, -0.3223964580411365, -2.400758277161838, -2.549732539343734, 4.374664141464968, 2.938163982698783];
  const d = [0.007784695709041462, 0.3224671290700398, 2.445134137142996, 3.754408661907416];
  const tail = q => (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1);
  if (p < 0.02425) return tail(Math.sqrt(-2 * Math.log(p)));
  if (p > 0.97575) return -tail(Math.sqrt(-2 * Math.log(1 - p)));
  const q = p - 0.5, r = q * q;
  return ((((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q) / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1);
};
const qInv = p => {
  const z = zInv(p);
  if (cfg.dist === 'normal') return z;
  const t = z + (z ** 3 + z) / (4 * NU) + (5 * z ** 5 + 16 * z ** 3 + 3 * z) / (96 * NU ** 2) + (3 * z ** 7 + 19 * z ** 5 + 17 * z ** 3 - 15 * z) / (384 * NU ** 3);
  return t * Math.sqrt((NU - 2) / NU);
};
const solve = (A, b) => {
  const n = b.length, M = A.map((r, i) => [...r, b[i]]);
  for (let i = 0; i < n; i++) {
    let p = i; for (let j = i + 1; j < n; j++) if (Math.abs(M[j][i]) > Math.abs(M[p][i])) p = j;
    [M[i], M[p]] = [M[p], M[i]];
    for (let j = i + 1; j < n; j++) { const f = M[j][i] / M[i][i]; for (let k = i; k <= n; k++) M[j][k] -= f * M[i][k]; }
  }
  const x = Array(n).fill(0);
  for (let i = n - 1; i >= 0; i--) { let s = M[i][n]; for (let j = i + 1; j < n; j++) s -= M[i][j] * x[j]; x[i] = s / M[i][i]; }
  return x;
};
const arFit = (r, p = 4, lam = 0.3) => {
  const A = Array.from({ length: p }, () => Array(p).fill(0)), b = Array(p).fill(0);
  for (let t = p; t < r.length; t++) for (let i = 0; i < p; i++) { b[i] += r[t - 1 - i] * r[t]; for (let j = 0; j < p; j++) A[i][j] += r[t - 1 - i] * r[t - 1 - j]; }
  const sc = mean(A.map((row, i) => row[i])) || 1;
  for (let i = 0; i < p; i++) A[i][i] += lam * sc;
  return solve(A, b);
};
/* Filter Kalman local linear trend pada log harga. Parameter noise diambil dari 100 return pertama saja (kausal). */
const kalman = (ly, s2) => {
  const n = ly.length, R = s2, q1 = 0.1 * s2, q2 = 0.002 * s2, slope = new Array(n).fill(0);
  let l = ly[0], b = 0, P00 = s2, P01 = 0, P11 = s2 * 0.01;
  for (let t = 1; t < n; t++) {
    const l_ = l + b, A00 = P00 + 2 * P01 + P11 + q1, A01 = P01 + P11, A11 = P11 + q2;
    const K0 = A00 / (A00 + R), K1 = A01 / (A00 + R), e = ly[t] - l_;
    l = l_ + K0 * e; b += K1 * e;
    P00 = (1 - K0) * A00; P01 = (1 - K0) * A01; P11 = A11 - K1 * A01;
    slope[t] = b;
  }
  return { slope };
};
/* Model: return log, GARCH(1,1) dengan variance targeting, Kalman, AR ridge */
function fit(cs) {
  const n = cs.length, ly = cs.map(c => Math.log(c.c)), r = ly.map((v, i) => (i ? v - ly[i - 1] : 0));
  const V = Math.pow(std(r.slice(1, 101)), 2) || 1e-8, om = V * (1 - ALPHA - BETA), hv = new Array(n);
  hv[0] = V; for (let t = 1; t < n; t++) hv[t] = om + ALPHA * r[t] ** 2 + BETA * hv[t - 1];
  return { n, ly, r, hv, V, kal: kalman(ly, V), w: cfg.drift === 'ar' ? arFit(r.slice(1, 201)) : [] };
}
function muPath(M, t, H) {
  const out = new Array(H).fill(0);
  if (cfg.drift === 'zero') return out;
  if (cfg.drift === 'ar') {
    const h = M.w.map((_, i) => M.r[t - i] || 0); let c = 0;
    for (let k = 0; k < H; k++) { const y = M.w.reduce((s, w, i) => s + w * h[i], 0); h.unshift(y); h.pop(); out[k] = c += y; }
    return out;
  }
  const cap = 0.25 * Math.sqrt(M.hv[t]), d = clamp(M.kal.slope[t], -cap, cap) * SHRINK;
  for (let k = 0; k < H; k++) out[k] = d * (k + 1);
  return out;
}
function varPath(M, t, H) {
  const rho = ALPHA + BETA, v1 = M.hv[t];
  return Array.from({ length: H }, (_, i) => { const k = i + 1; return k * M.V + (v1 - M.V) * (1 - rho ** k) / (1 - rho); });
}
function quantFc(M, H) {
  const t = M.n - 1, mu = muPath(M, t, H), vp = varPath(M, t, H), P0 = Math.exp(M.ly[t]);
  const Q = (p, k) => P0 * Math.exp(mu[k - 1] + Math.sqrt(vp[k - 1]) * qInv(p));
  const up = k => { let lo = 1e-6, hi = 1 - 1e-6; const tg = -mu[k - 1] / Math.sqrt(vp[k - 1]); for (let i = 0; i < 40; i++) { const m = (lo + hi) / 2; if (qInv(m) < tg) lo = m; else hi = m; } return 1 - lo; };
  return { P0, Q, up, H, name: 'Quant ensemble', state: 'Deterministik, tanpa angka acak' };
}
function extFc(ls, P0, H) {
  const Q = (p, k) => { const i = ls.findIndex(l => l.p >= p); if (i < 0) return ls.at(-1).v[k - 1]; if (i === 0) return ls[0].v[k - 1]; const a = ls[i - 1], b = ls[i], w = (p - a.p) / (b.p - a.p); return a.v[k - 1] + w * (b.v[k - 1] - a.v[k - 1]); };
  const up = k => { let lo = 0, hi = 1; for (let i = 0; i < 30; i++) { const m = (lo + hi) / 2; if (Q(m, k) < P0) lo = m; else hi = m; } return 1 - lo; };
  return { P0, Q, up, H, name: 'Model eksternal', state: 'Memakai endpoint eksternal' };
}
/* Uji walk-forward: setiap titik uji memakai filter kausal, hanya data sebelum titik itu */
function backtest(M, H) {
  const z = qInv(0.9), t0 = Math.max(210, M.n - H - 400); let n = 0, hn = 0, hit = 0, cov = 0, em = 0, en = 0;
  for (let t = t0; t < M.n - H; t++) {
    const mu = muPath(M, t, H)[H - 1], sd = Math.sqrt(varPath(M, t, H)[H - 1]), y = M.ly[t + H] - M.ly[t];
    n++; em += Math.abs(y - mu); en += Math.abs(y);
    if (Math.abs(y - mu) <= z * sd) cov++;
    if (mu) { hn++; if (sgn(mu) === sgn(y)) hit++; }
  }
  return { n, hit: hn ? hit / hn : NaN, cov: n ? cov / n : NaN, skill: en ? 1 - em / en : NaN };
}
/* Quant: tren Kalman, momentum deret waktu, mean reversion, volatilitas GARCH, rezim, eksponen Hurst */
function quant(M, cs) {
  const t = M.n - 1, sig = Math.sqrt(M.hv[t]), trendZ = (M.kal.slope[t] * Math.sqrt(24)) / sig;
  const ts = mean([12, 48, 168].filter(L => L < t).map(L => Math.tanh((M.ly[t] - M.ly[t - L]) / (sig * Math.sqrt(L)) / 1.5)));
  const w = cs.slice(-20).map(c => c.c), zmr = (w[w.length - 1] - mean(w)) / (std(w) || 1);
  const pts = [1, 2, 4, 8, 16].map(k => { const d = []; for (let i = k; i < M.n; i++) d.push(M.ly[i] - M.ly[i - k]); return [Math.log(k), Math.log(std(d) ** 2 || 1e-12)]; });
  const mx = mean(pts.map(p => p[0])), my = mean(pts.map(p => p[1]));
  const hurst = clamp(pts.reduce((s, p) => s + (p[0] - mx) * (p[1] - my), 0) / pts.reduce((s, p) => s + (p[0] - mx) ** 2, 0) / 2, 0, 1);
  const hist = M.hv.slice(-250).map(Math.sqrt), pc = hist.filter(x => x <= sig).length / hist.length;
  const regime = pc < 0.25 ? 'Rendah' : pc < 0.75 ? 'Normal' : pc < 0.95 ? 'Tinggi' : 'Ekstrem';
  const score = clamp(100 * (0.5 * Math.tanh(trendZ) + 0.4 * ts - 0.1 * Math.tanh(zmr / 2)), -100, 100);
  return { trendZ, score, regime, text: {
    trend: `${(M.kal.slope[t] * 1e4).toFixed(2)} bps per candle (z ${trendZ.toFixed(2)})`, tsmom: ts.toFixed(2), zscore: zmr.toFixed(2),
    vol: `${(sig * 100).toFixed(3)}% per candle`, regime, hurst: `${hurst.toFixed(2)} (${hurst > 0.55 ? 'cenderung tren' : hurst < 0.45 ? 'cenderung berbalik' : 'mendekati acak'})` } };
}
/* TTFM (TTrades Fractal Model): urutan C1, C2, C3 pada HTF, sweep, T-spot (midpoint logaritmik), FVG */
const bucket = (t, tf) => {
  const d = new Date(t);
  if (tf === '1M') return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1);
  if (tf === '1w') return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() - ((d.getUTCDay() + 6) % 7));
  return Math.floor(t / TF[tf]) * TF[tf];
};
const resample = (cs, tf) => {
  const out = [];
  for (const c of cs) { const b = bucket(c.t, tf), o = out[out.length - 1]; if (o && o.t === b) { o.h = Math.max(o.h, c.h); o.l = Math.min(o.l, c.l); o.c = c.c; o.end = c.t; } else out.push({ t: b, o: c.o, h: c.h, l: c.l, c: c.c, end: c.t }); }
  return out;
};
function ttfm(cs) {
  const name = HTF[cfg.tf], h = resample(cs, name), none = { score: 0, htf: name, seq: 'Data HTF belum cukup', sweep: 'Tidak ada', tspot: null, htfc: [] };
  if (h.length < 4) return none;
  const [c1, c2, c3] = h.slice(-3), px = cs[cs.length - 1].c; let score = 0, seq, sweep = 'Tidak ada';
  if (c2.l < c1.l && c2.c > c1.l) { seq = 'C2 menyapu low C1 lalu menutup di atasnya, C3 berjalan'; sweep = 'Low C1 (bullish)'; score = 60; }
  else if (c2.h > c1.h && c2.c < c1.h) { seq = 'C2 menyapu high C1 lalu menutup di bawahnya, C3 berjalan'; sweep = 'High C1 (bearish)'; score = -60; }
  else if (c2.c > c1.h) { seq = 'C2 menutup di atas high C1 (kontinuasi naik)'; score = 25; }
  else if (c2.c < c1.l) { seq = 'C2 menutup di bawah low C1 (kontinuasi turun)'; score = -25; }
  else seq = 'C2 inside bar C1 (konsolidasi)';
  if (score) score = sgn(score) * (Math.abs(score) + (sgn(score) * (px - c2.c) > 0 ? 25 : -20));
  const lo = Math.log(c2.o), lc = Math.log(c2.c), lh = Math.log(c2.h), ll = Math.log(c2.l), up = lh - Math.max(lo, lc), dn = Math.min(lo, lc) - ll;
  const mid = Math.max(up, dn) > Math.abs(lc - lo) ? (up >= dn ? (lh + Math.max(lo, lc)) / 2 : (Math.min(lo, lc) + ll) / 2) : (lh + ll) / 2;
  return { score: clamp(score, -100, 100), htf: name, seq, sweep, tspot: Math.exp(mid), htfc: [c1, c2, c3] };
}
function fvgs(cs) {
  const out = [];
  for (let i = Math.max(2, cs.length - 300); i < cs.length; i++) {
    const a = cs[i - 2], c = cs[i];
    if (c.l > a.h) out.push({ i: i - 1, lo: a.h, hi: c.l, dir: 1 });
    else if (c.h < a.l) out.push({ i: i - 1, lo: c.h, hi: a.l, dir: -1 });
  }
  return out.filter(g => { for (let j = g.i + 2; j < cs.length; j++) if (g.dir > 0 ? cs[j].c < g.lo : cs[j].c > g.hi) return false; return true; }).slice(-8);
}
/* GXT: swing, ERL/IRL, premium dan discount, displacement, divergensi SMT */
const pairSym = () => (cfg.smt !== 'auto' && cfg.smt !== cfg.symbol ? cfg.smt : SMT_AUTO[cfg.symbol] || 'ETHUSDT');
function gxt(cs, pair) {
  const n = cs.length, px = cs[n - 1].c, hi = [], lo = [], K = 3, p = pairSym();
  for (let i = K; i < n - K; i++) {
    let h = true, l = true;
    for (let j = 1; j <= K; j++) { if (cs[i].h <= cs[i - j].h || cs[i].h <= cs[i + j].h) h = false; if (cs[i].l >= cs[i - j].l || cs[i].l >= cs[i + j].l) l = false; }
    if (h) hi.push(i); if (l) lo.push(i);
  }
  if (!hi.length || !lo.length) return { score: 0, pair: p, erl: null, irl: null, zone: 'Netral', smt: 'Belum ada swing', disp: 'Tidak ada' };
  const erl = cs[hi.at(-1)].h, irl = cs[lo.at(-1)].l, pos = erl > irl ? (px - irl) / (erl - irl) : 0.5, zs = clamp((0.5 - pos) * 2, -1, 1);
  const zone = pos > 1 ? 'Premium (di atas ERL)' : pos < 0 ? 'Discount (di bawah IRL)' : pos >= 0.5 ? `Premium (${(pos * 100).toFixed(0)}% range)` : `Discount (${(pos * 100).toFixed(0)}% range)`;
  const atr = mean(cs.slice(-15).map((c, i, a) => (i ? Math.max(c.h - c.l, Math.abs(c.h - a[i - 1].c), Math.abs(c.l - a[i - 1].c)) : c.h - c.l)).slice(1));
  let dd = 0, ago = 0;
  for (let i = n - 1; i >= n - 8; i--) if (Math.abs(cs[i].c - cs[i].o) > 1.5 * atr) { dd = sgn(cs[i].c - cs[i].o); ago = n - 1 - i; break; }
  let smt = 0, smtText = `Tidak ada divergensi dengan ${p}`;
  const pm = new Map(pair.map(c => [c.t, c])), a = cs.slice(-20).map(c => [c, pm.get(c.t)]).filter(x => x[1]);
  if (a.length >= 18) {
    const pv = a.slice(0, -3), rc = a.slice(-3), mx = (r, k, f) => Math[f](...r.map(x => x[k].h ?? 0)), hiOf = (r, k) => Math.max(...r.map(x => x[k].h)), loOf = (r, k) => Math.min(...r.map(x => x[k].l));
    const nh = [hiOf(rc, 0) > hiOf(pv, 0), hiOf(rc, 1) > hiOf(pv, 1)], nl = [loOf(rc, 0) < loOf(pv, 0), loOf(rc, 1) < loOf(pv, 1)];
    if (nh[0] !== nh[1]) { smt = -1; smtText = `Bearish: ${nh[0] ? cfg.symbol : p} cetak high baru, satunya tidak ikut`; }
    else if (nl[0] !== nl[1]) { smt = 1; smtText = `Bullish: ${nl[0] ? cfg.symbol : p} cetak low baru, satunya tidak ikut`; }
    void mx;
  }
  return { score: clamp(40 * smt + 25 * dd + 35 * zs, -100, 100), pair: p, erl, irl, zone, smt: smtText, disp: dd ? `${dd > 0 ? 'Naik' : 'Turun'}, ${ago} candle lalu` : 'Tidak ada' };
}
/* Orkestrasi analisis */
const labelOf = s => (s >= 50 ? 'Naik kuat' : s >= 15 ? 'Naik' : s > -15 ? 'Netral' : s > -50 ? 'Turun' : 'Turun kuat');
function analyze() { try { analyze0(); } catch (e) { console.error(e); log(`Analisis gagal: ${e.message}`, 'error'); } }
function analyze0() {
  S.timer = 0;
  if (S.candles.length < 260) return;
  const all = S.candles, cs = all.slice(-Math.max(260, cfg.context)), M = fit(cs), H = cfg.horizon;
  const fc = cfg.source === 'external' && S.ext && S.ext.H === H ? S.ext.fc : quantFc(M, H);
  const q = quant(M, cs), t = ttfm(all), g = gxt(all, S.pair), fv = fvgs(all), px = all[all.length - 1].c;
  const W = [cfg.wq, cfg.wt, cfg.wg], score = clamp((W[0] * q.score + W[1] * t.score + W[2] * g.score) / (W[0] + W[1] + W[2] || 1), -100, 100);
  const key = `${all[all.length - 2].t}|${H}|${cfg.drift}|${cfg.dist}|${cfg.context}`;
  const paths = fc.name === 'Quant ensemble' ? scenarios(M, H, 100) : null;
  if (S.btKey !== key) {
    S.bt = backtest(M, H); S.btKey = key; S.reveal = performance.now(); S.stage = true;
    const prev = S.live.pend, done = prev && all.find(c => c.t === prev.t && c.t < all[all.length - 1].t);
    if (done) { S.live.n++; if (sgn(done.c - prev.p0) === prev.dir) S.live.hit++; }
    S.live.pend = { t: all[all.length - 1].t, p0: fc.P0, dir: sgn(fc.Q(0.5, 1) - fc.P0) };
    log(`median ${fmt(fc.Q(0.5, H))}, p10 ${fmt(fc.Q(0.1, H))}, p90 ${fmt(fc.Q(0.9, H))}`);
    log(paths ? `sebar 100 skenario Filtered Historical Simulation (blok residu berurutan, tanpa angka acak), horizon ${H}` : `skenario dari model eksternal, horizon ${H}`);
    log(`fit Kalman local linear trend dan GARCH(1,1) pada ${cs.length} candle`);
    log(`GET klines ${cfg.symbol} ${cfg.tf}, ${all.length} candle di memori`);
  }
  S.an = { fc, q, t, g, fv, score, label: labelOf(score), H, px, paths, upN: paths ? paths.filter(p => p[H] >= fc.P0).length : 0 };
  publish(S.an); S.dirty = true;
}
function publish(A) {
  const { q, t, g, fc, H, score } = A, dir = score >= 15 ? 'up' : score <= -15 ? 'down' : 'flat';
  Object.entries(q.text).forEach(([k, v]) => bind(`q.${k}`, v));
  const near = A.fv.length ? A.fv.reduce((a, b) => (Math.abs((b.lo + b.hi) / 2 - A.px) < Math.abs((a.lo + a.hi) / 2 - A.px) ? b : a)) : null;
  Object.entries({ htf: t.htf, sequence: t.seq, sweep: t.sweep, tspot: t.tspot ? fmt(t.tspot) : 'Belum ada', fvg: near ? `${near.dir > 0 ? 'Bullish' : 'Bearish'} ${fmt(near.lo)} sampai ${fmt(near.hi)}` : 'Tidak ada', bias: labelOf(t.score) }).forEach(([k, v]) => bind(`t.${k}`, v));
  Object.entries({ pair: g.pair, smt: g.smt, erl: g.erl ? fmt(g.erl) : '-', irl: g.irl ? fmt(g.irl) : '-', zone: g.zone, displacement: g.disp, bias: labelOf(g.score) }).forEach(([k, v]) => bind(`g.${k}`, v));
  setDir('t.bias', t.score); setDir('g.bias', g.score);
  [['quant', q.score], ['ttfm', t.score], ['gxt', g.score]].forEach(([k, s]) => { const li = $(`[data-engine="${k}"]`); li.dataset.dir = dirOf(s); $('meter', li).value = s; bind(`vote.${k}`, s.toFixed(0)); setDir(`vote.${k}`, s); });
  const gauge = $('#gauge'); gauge.style.setProperty('--score', score.toFixed(1)); gauge.dataset.dir = dir;
  bind('bias.label', A.label); bind('bias.score', `${score > 0 ? '+' : ''}${score.toFixed(0)}`);
  bind('model.name', fc.name); if (revealP() >= 1) bind('model.state', fc.state);
  const hs = [...new Set([1, Math.ceil(H / 4), Math.ceil(H / 2), H])].sort((a, b) => a - b);
  $('#fc-rows').innerHTML = hs.map(k => { const u = fc.up(k); return `<tr><td>+${k} candle</td><td>${fmt(fc.Q(0.1, k))}</td><td>${fmt(fc.Q(0.5, k))}</td><td>${fmt(fc.Q(0.9, k))}</td><td data-dir="${u >= 0.5 ? 'up' : 'down'}">${(u * 100).toFixed(0)}%</td></tr>`; }).join('');
  const b = S.bt || {};
  bind('bt.n', b.n ?? '-'); bind('bt.hit', Number.isFinite(b.hit) ? `${(b.hit * 100).toFixed(1)}%` : '-');
  bind('bt.cov', Number.isFinite(b.cov) ? `${(b.cov * 100).toFixed(1)}% (target 80%)` : '-');
  bind('bt.skill', Number.isFinite(b.skill) ? `${(b.skill * 100).toFixed(1)}%` : '-'); setDir('bt.skill', (b.skill || 0) - 0.002);
  bind('sc.median', fmt(fc.Q(0.5, H))); setDir('sc.median', fc.Q(0.5, H) - fc.P0);
  bind('sc.up', A.paths ? `${A.upN}/${A.paths.length} jalur naik` : `${(fc.up(H) * 100).toFixed(0)}% peluang naik`);
  bind('sc.spread', fmt(fc.Q(0.9, H) - fc.Q(0.1, H)));
  bind('sc.hit', S.live.n ? `${S.live.hit}/${S.live.n} (${((S.live.hit / S.live.n) * 100).toFixed(0)}%)` : 'Menunggu candle ditutup');
  const m = fc.Q(0.5, H) / fc.P0 - 1, lo = fc.Q(0.1, H) / fc.P0 - 1, hi = fc.Q(0.9, H) / fc.P0 - 1;
  bind('narrative', `${cfg.symbol} ${cfg.tf}: bias gabungan ${A.label.toLowerCase()} (${score > 0 ? '+' : ''}${score.toFixed(0)}). Quant membaca tren ${q.trendZ > 0.3 ? 'naik' : q.trendZ < -0.3 ? 'turun' : 'datar'} dengan volatilitas ${q.regime.toLowerCase()}. TTFM: ${t.seq.toLowerCase()}. GXT: harga di zona ${g.zone.toLowerCase()}, ${g.smt.toLowerCase()}. Skenario dasar ${H} candle: median ${pct(m)}, rentang 80% ${pct(lo)} sampai ${pct(hi)}, peluang naik ${(fc.up(H) * 100).toFixed(0)}%. Ini estimasi statistik, bukan kepastian.`);
}
const queue = now => { if (now) { clearTimeout(S.timer); analyze(); refreshExternal(); } else if (!S.timer) S.timer = setTimeout(analyze, 250); };
async function refreshExternal() {
  if (cfg.source !== 'external' || !cfg.endpoint || S.candles.length < 260) { S.ext = null; return; }
  const key = `${cfg.symbol}${cfg.tf}${cfg.horizon}${S.candles.at(-1).t}`;
  if (S.ext?.key === key) return;
  try {
    const res = await fetch(cfg.endpoint, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ symbol: cfg.symbol, interval: cfg.tf, horizon: cfg.horizon, candles: S.candles.slice(-cfg.context).map(c => [c.t, c.o, c.h, c.l, c.c, c.v]) }) });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const ls = (await res.json()).lines.map(l => ({ p: +l.p, v: l.prices.map(Number) })).sort((a, b) => a.p - b.p);
    S.ext = { key, H: cfg.horizon, fc: extFc(ls, S.candles.at(-1).c, cfg.horizon) }; log('Prediksi model eksternal diterima'); queue();
  } catch (e) { S.ext = null; log(`Model eksternal gagal (${e.message}), memakai mesin quant`, 'warn'); }
}

/* Feed realtime: REST untuk riwayat, WebSocket kline untuk tick, tambal celah saat sambung ulang */
const jget = async (path, venue = cfg.venue) => { const L = HOST[venue].rest; let err; for (let n = 0; n < L.length; n++) { const i = (hostI + n) % L.length; try { const r = await fetch(L[i] + path); if (!r.ok) throw new Error(`HTTP ${r.status}`); hostI = i; return await r.json(); } catch (e) { err = e; } } throw err; };
const parseK = k => ({ t: +k[0], o: +k[1], h: +k[2], l: +k[3], c: +k[4], v: +k[5] });
function upsert(arr, k) {
  const l = arr[arr.length - 1];
  if (!l || k.t > l.t) arr.push(k); else if (k.t === l.t) arr[arr.length - 1] = k; else { const i = arr.findIndex(c => c.t === k.t); if (i >= 0) arr[i] = k; }
  if (arr.length > 1500) arr.shift();
}
const FEED = { connecting: 'Menghubungkan', live: 'Langsung', stale: 'Tertunda', offline: 'Terputus' };
const setFeed = s => { $('#feed-chip').dataset.state = s; bind('feed.state', FEED[s]); };
const showEmpty = v => { $('#chart-empty').hidden = !v; };
const showError = (v, msg) => { $('#chart-error').hidden = !v; if (msg) bind('error.message', msg); };
function closeWs() { const w = S.feed.ws; if (w) { w.onclose = null; w.onmessage = null; try { w.close(); } catch { /* abaikan */ } } S.feed.ws = null; }
async function load() {
  const gen = ++S.feed.gen; closeWs(); S.candles = []; S.pair = []; S.an = null; S.ext = null; S.btKey = ''; S.view.off = 0; S.live = { hit: 0, n: 0, pend: null };
  showEmpty(true); showError(false); setFeed('connecting'); bind('symbol', cfg.symbol); bind('venue', cfg.venue === 'fut' ? 'Binance futures' : 'Binance spot'); bind('model.state', 'Menunggu data');
  try {
    const t0 = Date.now(), [ks, ps, tm] = await Promise.all([jget(`${REST}/klines?symbol=${cfg.symbol}&interval=${cfg.tf}&limit=1000`), (pairOk() ? jget(`${REST}/klines?symbol=${pairSym()}&interval=${cfg.tf}&limit=300`).catch(() => []) : Promise.resolve([])), jget(`${REST}/time`)]);
    if (gen !== S.feed.gen) return;
    S.feed.skew = tm.serverTime - (t0 + Date.now()) / 2; S.candles = ks.map(parseK); S.pair = ps.map(parseK);
    log(`Riwayat ${S.candles.length} candle ${cfg.symbol} ${cfg.tf} dimuat`); showEmpty(false); updateQuote(); connect(gen); queue(true);
  } catch (e) { if (gen !== S.feed.gen) return; setFeed('offline'); showEmpty(false); showError(true, `Gagal memuat data Binance (${e.message}).`); log(`Gagal memuat riwayat: ${e.message}`, 'error'); }
}
function connect(gen) {
  const s = cfg.symbol.toLowerCase(), p = pairSym().toLowerCase(), W = HOST[cfg.venue].ws, ws = new WebSocket(`${W[wsI % W.length]}${s}@kline_${cfg.tf}/${s}@aggTrade/${s}@miniTicker${pairOk() ? `/${p}@kline_${cfg.tf}` : ''}`);
  S.feed.ws = ws;
  ws.onopen = () => { S.feed.tries = 0; setFeed('live'); showError(false); log('Terhubung ke stream Binance'); };
  ws.onmessage = e => onMsg(JSON.parse(e.data).data);
  ws.onerror = () => ws.close();
  ws.onclose = () => {
    if (gen !== S.feed.gen) return;
    const d = Math.min(15000, 500 * 2 ** S.feed.tries++); setFeed('offline'); S.feed.reconnects++; wsI++; bind('feed.reconnects', S.feed.reconnects);
    log(`Koneksi putus, mencoba lagi dalam ${(d / 1000).toFixed(1)} dtk`, 'warn');
    setTimeout(async () => { if (gen !== S.feed.gen) return; await backfill(); if (gen === S.feed.gen) connect(gen); }, d);
  };
}
async function backfill() {
  try {
    const last = S.candles.at(-1); if (!last) return;
    const ks = (await jget(`${REST}/klines?symbol=${cfg.symbol}&interval=${cfg.tf}&startTime=${last.t}&limit=1000`)).map(parseK);
    ks.forEach(k => upsert(S.candles, k)); const miss = Math.max(0, ks.length - 1);
    if (miss) { S.feed.gaps += miss; bind('feed.gaps', S.feed.gaps); log(`${miss} candle yang terlewat ditambal dari REST`, 'warn'); }
    queue(true);
  } catch (e) { log(`Penambalan celah gagal: ${e.message}`, 'warn'); }
}
function onMsg(d) {
  if (d.e === '24hrMiniTicker') { if (d.s === cfg.symbol) { const ch = (+d.c - +d.o) / +d.o; bind('change24h', pct(ch)); setDir('change24h', ch); } return; }
  if (d.e === 'aggTrade') {
    if (d.s !== cfg.symbol || !S.candles.length) return;
    const f = S.feed, now = Date.now(), p = +d.p, ms = TF[cfg.tf], t0 = Math.floor(d.T / ms) * ms; let c = S.candles[S.candles.length - 1];
    f.lat = Math.max(0, now + f.skew - d.E); f.last = now; f.ticks.push(now); S.lastTrade = d.E;
    if (t0 > c.t) { c = { t: t0, o: p, h: p, l: p, c: p, v: 0 }; S.candles.push(c); }
    else if (t0 === c.t) { c.c = p; if (p > c.h) c.h = p; if (p < c.l) c.l = p; }
    else return;
    S.qDirty = true; S.dirty = true; queue(); return;
  }
  if (d.e !== 'kline') return;
  const f = S.feed, now = Date.now(), k = parseK([d.k.t, d.k.o, d.k.h, d.k.l, d.k.c, d.k.v]);
  f.lat = Math.max(0, now + f.skew - d.E); f.last = now; f.ticks.push(now);
  if (d.s !== cfg.symbol) { upsert(S.pair, k); return; }
  const cur = S.candles[S.candles.length - 1];
  if (!d.k.x && cur && cur.t === k.t && S.lastTrade > d.E) { k.c = cur.c; k.h = Math.max(k.h, cur.h); k.l = Math.min(k.l, cur.l); }
  upsert(S.candles, k); S.qDirty = true; S.dirty = true; queue(d.k.x);
}
function updateQuote() {
  const c = S.candles.at(-1); if (!c) return;
  bind('price', fmt(c.c)); setDir('price', c.c - (S.px || c.c)); S.px = c.c;
  ['o', 'h', 'l', 'c'].forEach(k => bind(`bar.${k}`, fmt(c[k]))); bind('bar.v', Intl.NumberFormat('en', { notation: 'compact', maximumFractionDigits: 2 }).format(c.v));
}
function heartbeat() {
  const f = S.feed, now = Date.now(), c = S.candles.at(-1);
  bind('clock.utc', new Date(now).toISOString().slice(11, 19));
  f.ticks = f.ticks.filter(t => now - t < 5000);
  bind('feed.latency', f.last ? `${f.lat.toFixed(0)} ms` : '-'); bind('feed.skew', `${f.skew.toFixed(0)} ms`); bind('feed.tps', (f.ticks.length / 5).toFixed(1));
  if (c) bind('bar.countdown', hms(c.t + TF[cfg.tf] - (now + f.skew)));
  if (f.ws && f.ws.readyState === 1) setFeed(f.last && now - f.last > 8000 ? 'stale' : 'live');
}

/* Renderer canvas */
const cv = { grid: $('#cv-grid'), cs: $('#cv-candles'), fc: $('#cv-forecast'), hud: $('#cv-hud') };
const cx = Object.fromEntries(Object.entries(cv).map(([k, c]) => [k, c.getContext('2d')]));
const box = { w: 0, h: 0 }, PAD = { l: 8, r: 72, t: 48, b: 26 };
const css = (n, d) => getComputedStyle(document.documentElement).getPropertyValue(n).trim() || d;
let C = {};
function resize() {
  const r = $('#chart').getBoundingClientRect(), d = Math.min(devicePixelRatio || 1, 2); box.w = r.width; box.h = r.height;
  for (const k in cv) { cv[k].width = r.width * d; cv[k].height = r.height * d; cx[k].setTransform(d, 0, 0, d, 0, 0); }
  S.dirty = true;
}
function geom() {
  const n = S.candles.length, v = clamp(Math.round(S.view.bars), 20, Math.min(400, n)), off = clamp(Math.round(S.view.off), 0, Math.max(0, n - v));
  S.view.off = off;
  const last = n - 1 - off, start = last - v + 1, showF = cfg.layers.forecast && !!S.an && off === 0, fut = showF ? S.an.H : 3;
  const pw = box.w - PAD.l - PAD.r, ph = box.h - PAD.t - PAD.b, bw = pw / (v + fut);
  let hi = -Infinity, lo = Infinity;
  for (let i = start; i <= last; i++) { const c = S.candles[i]; if (c.h > hi) hi = c.h; if (c.l < lo) lo = c.l; }
  if (showF) { hi = Math.max(hi, S.an.fc.Q(0.975, S.an.H)); lo = Math.min(lo, S.an.fc.Q(0.025, S.an.H)); }
  const pad = (hi - lo) * 0.06 || hi * 0.001; hi += pad; lo -= pad;
  return { n, v, off, last, start, showF, pw, ph, bw, hi, lo, x: i => PAD.l + (i - start + 0.5) * bw, y: p => PAD.t + ((hi - p) / (hi - lo)) * ph };
}
const idxOf = t => { let a = 0, b = S.candles.length; while (a < b) { const m = (a + b) >> 1; if (S.candles[m].t < t) a = m + 1; else b = m; } return a; };
const clip = (c, g) => { c.save(); c.beginPath(); c.rect(PAD.l, PAD.t - 8, g.pw + 2, box.h - PAD.b - PAD.t + 8); c.clip(); };
function orb(c, x, y, r, rgb) {
  if (![x, y, r].every(Number.isFinite)) return;
  const g = c.createRadialGradient(x - r * 0.3, y - r * 0.3, 0, x, y, r * 1.8);
  g.addColorStop(0, '#fff'); g.addColorStop(0.35, `rgba(${rgb},.95)`); g.addColorStop(1, `rgba(${rgb},0)`);
  c.fillStyle = g; c.beginPath(); c.arc(x, y, r * 1.8, 0, 7); c.fill();
}
function drawGrid(g) {
  const c = cx.grid; c.clearRect(0, 0, box.w, box.h);
  const raw = (g.hi - g.lo) / 6, mag = 10 ** Math.floor(Math.log10(raw)), step = [1, 2, 5, 10].map(m => m * mag).find(s => s >= raw);
  c.font = '11px Geist, system-ui, sans-serif'; c.fillStyle = C.ink3; c.strokeStyle = 'rgba(13,20,32,.07)'; c.lineWidth = 1; c.textAlign = 'left'; c.textBaseline = 'middle';
  for (let p = Math.ceil(g.lo / step) * step; p < g.hi; p += step) { const y = Math.round(g.y(p)) + 0.5; c.beginPath(); c.moveTo(PAD.l, y); c.lineTo(box.w - PAD.r, y); c.stroke(); c.fillText(fmt(p, step < 1 ? 4 : step < 10 ? 2 : 0), box.w - PAD.r + 8, y); }
  c.textAlign = 'center'; c.textBaseline = 'top';
  const every = Math.max(1, Math.ceil(90 / g.bw));
  for (let i = g.start; i <= g.last; i++) if ((g.last - i) % every === 0) { const d = new Date(S.candles[i].t).toISOString(); c.fillText(TF[cfg.tf] >= 864e5 ? d.slice(5, 10) : d.slice(11, 16), g.x(i), box.h - PAD.b + 8); }
}
function drawCandles(g) {
  const c = cx.cs, L = cfg.layers, A = S.an; c.clearRect(0, 0, box.w, box.h); clip(c, g);
  c.font = '11px Geist, system-ui, sans-serif'; c.textAlign = 'left'; c.textBaseline = 'bottom';
  if (L.sessions && TF[cfg.tf] <= 36e5) {
    for (let i = g.start; i <= g.last; i++) {
      const d = new Date(S.candles[i].t), nm = { 0: 'Asia', 7: 'London', 13: 'New York' }[d.getUTCHours()];
      if (nm && d.getUTCMinutes() === 0) { c.strokeStyle = 'rgba(13,20,32,.12)'; c.setLineDash([2, 4]); c.beginPath(); c.moveTo(g.x(i), PAD.t); c.lineTo(g.x(i), box.h - PAD.b); c.stroke(); c.setLineDash([]); c.fillStyle = C.ink3; c.fillText(nm, g.x(i) + 4, box.h - PAD.b - 4); }
    }
  }
  if (L.htf && A) A.t.htfc.forEach((h, j) => {
    const x1 = g.x(idxOf(h.t)) - g.bw / 2, x2 = g.x(idxOf(h.end)) + g.bw / 2;
    c.fillStyle = 'rgba(122,108,240,.05)'; c.strokeStyle = 'rgba(122,108,240,.4)'; c.beginPath(); c.roundRect(x1, g.y(h.h), x2 - x1, g.y(h.l) - g.y(h.h), 8); c.fill(); c.stroke();
    c.fillStyle = C.iris; c.textBaseline = 'top'; c.fillText(`C${j + 1}`, x1 + 6, g.y(h.h) + 5);
  });
  if (L.fvg && A) A.fv.forEach(f => { c.fillStyle = f.dir > 0 ? 'rgba(15,159,122,.11)' : 'rgba(229,72,77,.11)'; const x1 = g.x(f.i) - g.bw / 2; c.fillRect(x1, g.y(f.hi), PAD.l + g.pw - x1, g.y(f.lo) - g.y(f.hi)); });
  const w = Math.max(1, g.bw * 0.68);
  for (let i = Math.max(0, g.start); i <= g.last; i++) {
    const k = S.candles[i], x = g.x(i), yo = g.y(k.o), yc = g.y(k.c);
    c.fillStyle = c.strokeStyle = k.c >= k.o ? C.up : C.down; c.lineWidth = 1;
    c.beginPath(); c.moveTo(x, g.y(k.h)); c.lineTo(x, g.y(k.l)); c.stroke();
    c.beginPath(); c.roundRect(x - w / 2, Math.min(yo, yc), w, Math.max(1, Math.abs(yo - yc)), Math.min(2, w / 3)); c.fill();
  }
  if (L.liquidity && A) {
    c.textBaseline = 'bottom';
    [[A.g.erl, 'ERL', C.iris], [A.g.irl, 'IRL', C.amber], [A.t.tspot, 'T-spot', C.cyan]].forEach(([p, nm, col]) => {
      if (!p || p > g.hi || p < g.lo) return; const y = Math.round(g.y(p)) + 0.5;
      c.strokeStyle = col; c.fillStyle = col; c.setLineDash([5, 5]); c.lineWidth = 1; c.beginPath(); c.moveTo(PAD.l, y); c.lineTo(PAD.l + g.pw, y); c.stroke(); c.setLineDash([]); c.fillText(nm, PAD.l + 6, y - 3);
    });
  }
  c.restore();
}
/* Skenario: Filtered Historical Simulation. Residu terstandar historis dipakai berurutan (tanpa pengacakan), diskalakan oleh jalur varians GARCH */
function scenarios(M, H, N) {
  const t = M.n - 1, mu = muPath(M, t, H), vp = varPath(M, t, H), P0 = Math.exp(M.ly[t]);
  const z = M.r.map((r, i) => (i ? r / Math.sqrt(M.hv[i - 1]) : 0)).slice(1), zm = mean(z), zs = std(z) || 1, hi = Math.max(1, z.length - H), out = [];
  for (let j = 0; j < N; j++) {
    const s0 = Math.floor((j * hi) / N), p = [P0]; let x = 0, pm = 0, pv = 0;
    for (let k = 0; k < H; k++) { x += mu[k] - pm + Math.sqrt(Math.max(vp[k] - pv, 0)) * ((z[s0 + k] - zm) / zs); pm = mu[k]; pv = vp[k]; p.push(P0 * Math.exp(x)); }
    out.push(p);
  }
  return out;
}
const ease = x => 1 - (1 - x) ** 3;
function revealP() { return clamp((performance.now() - S.reveal) / 2600, 0, 1); }
function drawForecast(g) {
  const c = cx.fc; c.clearRect(0, 0, box.w, box.h); if (!g.showF) return;
  const A = S.an, f = A.fc, H = A.H, t = performance.now() / 1000, P = revealP(), x0 = g.x(g.last), y0 = g.y(f.P0), K = Math.max(1, Math.round(H * ease(P)));
  const X = k => g.x(g.last + k), pt = (v, k) => [X(k), g.y(v)], breath = 0.85 + 0.15 * Math.sin(t * 1.6);
  const line = pts => { c.beginPath(); pts.forEach((q, j) => (j ? c.lineTo(q[0], q[1]) : c.moveTo(q[0], q[1]))); };
  const edge = p => { const r = [[x0, y0]]; for (let k = 1; k <= K; k++) r.push(pt(f.Q(p, k), k)); return r; };
  const band = (a, b, rgb, al) => { line(edge(a).concat(edge(b).reverse())); c.closePath(); c.fillStyle = `rgba(${rgb},${al * breath})`; c.fill(); };
  const part = (p, kf) => { const k0 = Math.min(H, Math.floor(kf)), pts = p.slice(0, k0 + 1).map((v, k) => pt(v, k)); if (k0 < H) pts.push(pt(p[k0] + (p[k0 + 1] - p[k0]) * (kf - k0), kf)); return pts; };
  clip(c, g); c.lineJoin = 'round';
  if (cfg.b95) band(0.025, 0.975, '40,184,216', 0.09); if (cfg.b80) band(0.1, 0.9, '40,184,216', 0.13); if (cfg.b50) band(0.25, 0.75, '122,108,240', 0.15);
  c.lineWidth = 1;
  if (A.paths) {
    const N = A.paths.length;
    A.paths.forEach((p, j) => {
      const pj = clamp((P - (j / N) * 0.45) / 0.55, 0, 1); if (pj <= 0) return;
      const up = p[H] >= f.P0, pts = part(p, H * ease(pj)), e = pts[pts.length - 1];
      c.strokeStyle = up ? 'rgba(15,159,122,.34)' : 'rgba(229,72,77,.30)'; line(pts); c.stroke();
      if (pj < 1) orb(c, e[0], e[1], 2.6, up ? '15,159,122' : '229,72,77');
    });
    if (P >= 1) for (let j = 0; j < N; j += Math.max(1, Math.floor(N / 16))) {
      const p = A.paths[j], kf = (((t / 2.8) + j * 0.0713) % 1) * H, rgb = p[H] >= f.P0 ? '15,159,122' : '229,72,77', head = part(p, kf), tail = part(p, Math.max(0, kf - H * 0.22)), hp = head[head.length - 1];
      c.strokeStyle = `rgba(${rgb},.6)`; c.lineWidth = 2; line(head.slice(Math.max(0, tail.length - 1))); c.stroke(); orb(c, hp[0], hp[1], 3.4, rgb);
    }
  } else for (let i = 0; i < cfg.lines; i++) { const p = (i + 0.5) / cfg.lines; if (Math.abs(p - 0.5) < 0.5 / cfg.lines) continue; c.strokeStyle = `rgba(40,184,216,${0.14 + 0.5 * (1 - Math.abs(2 * p - 1))})`; line(edge(p)); c.stroke(); }
  const m = edge(0.5), grd = c.createLinearGradient(x0, 0, X(H) + 1, 0); grd.addColorStop(0, C.amber); grd.addColorStop(1, '#ff7e5f');
  c.shadowColor = 'rgba(247,169,53,.65)'; c.shadowBlur = 12; c.strokeStyle = grd; c.lineWidth = 2.6; line(m); c.stroke(); c.shadowBlur = 0;
  c.strokeStyle = 'rgba(255,255,255,.85)'; c.lineWidth = 1.3; c.setLineDash([5, 11]); c.lineDashOffset = -t * 36; line(m); c.stroke(); c.setLineDash([]); c.lineDashOffset = 0;
  const en = m[m.length - 1]; orb(c, en[0], en[1], 10 + 1.5 * Math.sin(t * 3), '247,169,53'); c.restore();
  if (P >= 0.98) {
    c.font = '11px Geist, system-ui, sans-serif'; c.textAlign = 'center'; c.textBaseline = 'middle';
    [[0.9, C.up], [0.5, C.amber], [0.1, C.down]].forEach(([p, col]) => { const v = f.Q(p, H), y = g.y(v); c.fillStyle = col; c.beginPath(); c.roundRect(box.w - PAD.r + 4, y - 10, PAD.r - 8, 20, 8); c.fill(); c.fillStyle = '#fff'; c.fillText(fmt(v, v >= 1000 ? 0 : undefined), box.w - PAD.r / 2, y); });
  }
}
function drawBeam(c, g) {
  if (!g.showF) return;
  const t = performance.now() / 1000, x = g.x(g.last), y = g.y(S.an.fc.P0), r = 14 + 2 * Math.sin(t * 2.2), h = box.h - PAD.t - PAD.b;
  const bg = c.createLinearGradient(0, PAD.t, 0, box.h - PAD.b); bg.addColorStop(0, 'rgba(40,184,216,0)'); bg.addColorStop(0.5, 'rgba(40,184,216,.22)'); bg.addColorStop(1, 'rgba(40,184,216,0)');
  c.fillStyle = bg; c.fillRect(x - 4, PAD.t, 8, h);
  const yb = PAD.t + ((Math.sin(t * 0.9) + 1) / 2) * h, sg = c.createLinearGradient(0, yb - 40, 0, yb + 40); sg.addColorStop(0, 'rgba(255,255,255,0)'); sg.addColorStop(0.5, 'rgba(120,220,240,.6)'); sg.addColorStop(1, 'rgba(255,255,255,0)');
  c.fillStyle = sg; c.fillRect(x - 2, yb - 40, 4, 80);
  for (let i = 0; i < 2; i++) { const ph = (t / 2.4 + i * 0.5) % 1; c.strokeStyle = `rgba(40,184,216,${(1 - ph) * 0.5})`; c.lineWidth = 1.2; c.beginPath(); c.arc(x, y, 10 + ph * 44, 0, 7); c.stroke(); }
  c.strokeStyle = 'rgba(40,184,216,.65)'; c.lineWidth = 1.5; c.beginPath(); c.arc(x, y, r, 0, 7); c.stroke();
  orb(c, x, y, 7, '40,184,216');
  c.font = '11px Geist, system-ui, sans-serif'; c.textAlign = 'center'; c.textBaseline = 'middle';
  c.fillStyle = 'rgba(255,255,255,.92)'; c.beginPath(); c.roundRect(x - 34, PAD.t - 2, 68, 20, 10); c.fill(); c.fillStyle = '#0d1420'; c.fillText('Sekarang', x, PAD.t + 8);
}
function drawHud(g) {
  const c = cx.hud, tip = $('#chart-tip'), L = S.candles[g.n - 1]; c.clearRect(0, 0, box.w, box.h); drawBeam(c, g);
  c.font = '11.5px Geist, system-ui, sans-serif'; c.textBaseline = 'middle';
  const tag = (txt, y, col) => { c.fillStyle = col; c.beginPath(); c.roundRect(box.w - PAD.r + 4, y - 10, PAD.r - 8, 20, 8); c.fill(); c.fillStyle = '#fff'; c.textAlign = 'center'; c.fillText(txt, box.w - PAD.r / 2 + 0, y); };
  if (g.off === 0 && L.c < g.hi && L.c > g.lo) { const y = Math.round(g.y(L.c)) + 0.5, col = L.c >= L.o ? C.up : C.down; c.strokeStyle = col; c.setLineDash([3, 4]); c.beginPath(); c.moveTo(PAD.l, y); c.lineTo(box.w - PAD.r, y); c.stroke(); c.setLineDash([]); tag(fmt(L.c), y, col); }
  const m = S.mouse;
  if (!m || m.x < PAD.l || m.x > PAD.l + g.pw || m.y < PAD.t - 30 || m.y > box.h - PAD.b) { tip.hidden = true; return; }
  c.strokeStyle = 'rgba(13,20,32,.35)'; c.setLineDash([4, 4]); c.beginPath(); c.moveTo(m.x, 0); c.lineTo(m.x, box.h - PAD.b); c.moveTo(PAD.l, m.y); c.lineTo(box.w - PAD.r, m.y); c.stroke(); c.setLineDash([]);
  tag(fmt(g.hi - ((m.y - PAD.t) / g.ph) * (g.hi - g.lo)), m.y, '#0d1420');
  const i = g.start + Math.floor((m.x - PAD.l) / g.bw), k = i - g.last; let html = '';
  if (i >= g.start && i <= g.last) { const q = S.candles[i], ch = q.c / q.o - 1; html = `<b>${new Date(q.t).toISOString().slice(0, 16).replace('T', ' ')}</b><br>O ${fmt(q.o)}<br>H ${fmt(q.h)}<br>L ${fmt(q.l)}<br>C ${fmt(q.c)} <span data-dir="${dirOf(ch)}">${pct(ch)}</span>`; }
  else if (k > 0 && g.showF && k <= S.an.H) {
    const f = S.an.fc, A = S.an;
    html = `<b>Prediksi +${k} candle</b><br>P10 ${fmt(f.Q(0.1, k))}<br>Median ${fmt(f.Q(0.5, k))}<br>P90 ${fmt(f.Q(0.9, k))}<br>Peluang naik ${(f.up(k) * 100).toFixed(0)}%`;
    if (A.paths) {
      const pr = g.hi - ((m.y - PAD.t) / g.ph) * (g.hi - g.lo); let best = A.paths[0]; A.paths.forEach(p => { if (Math.abs(p[k] - pr) < Math.abs(best[k] - pr)) best = p; });
      c.strokeStyle = best[A.H] >= f.P0 ? C.up : C.down; c.lineWidth = 2; c.shadowColor = c.strokeStyle; c.shadowBlur = 8; c.beginPath(); best.forEach((v, j) => (j ? c.lineTo(g.x(g.last + j), g.y(v)) : c.moveTo(g.x(g.last), g.y(v)))); c.stroke(); c.shadowBlur = 0;
      html += `<br>Jalur ini berakhir di ${fmt(best[A.H])}`;
    }
  }
  if (!html) { tip.hidden = true; return; }
  tip.innerHTML = html; tip.hidden = false; tip.style.left = `${m.x > box.w - 220 ? m.x - 170 : m.x + 14}px`; tip.style.top = `${clamp(m.y + 14, 8, box.h - 130)}px`;
}
const STAGES = ['Tahap 1/4: ambil candle', 'Tahap 2/4: fit Kalman dan GARCH', 'Tahap 3/4: sebar 100 skenario', 'Tahap 4/4: baca spread'];
let frameErr = 0;
function frame() {
  try {
    if (S.candles.length > 20) {
      const g = geom(), p = revealP(), anim = g.showF && p < 1;
      if (S.qDirty) { S.qDirty = false; updateQuote(); }
      if (S.dirty) { drawGrid(g); drawCandles(g); S.dirty = false; }
      drawForecast(g);
      if (anim) bind('model.state', STAGES[Math.min(3, Math.floor(p * 4))]);
      else if (S.stage && S.an) { S.stage = false; bind('model.state', S.an.fc.state); }
      drawHud(g);
    }
  } catch (e) { if (frameErr++ < 3) { console.error(e); log(`Render gagal: ${e.message}`, 'error'); } S.view.off = 0; S.dirty = true; }
  requestAnimationFrame(frame);
}
let pickCat = 'crypto', pickQ = '';
function renderPicker() {
  const q = pickQ.trim().toLowerCase(), items = CATALOG.filter(i => (q ? `${i.s} ${i.n}`.toLowerCase().includes(q) : i.c === pickCat));
  $('#sym-items').innerHTML = items.length ? items.map(i => `<li role="option" tabindex="0" data-s="${i.s}" data-v="${i.v}" aria-selected="${i.s === cfg.symbol && i.v === cfg.venue}">${i.n}<span>${i.s}</span></li>`).join('') : '<li>Instrumen tidak ditemukan</li>';
  $('#sym-cats').innerHTML = CATS.map(([k, n]) => `<button type="button" data-cat="${k}" aria-pressed="${!q && k === pickCat}">${n}</button>`).join('');
}
async function discover() {
  try {
    const info = await jget('/exchangeInfo', 'fut');
    info.symbols.filter(x => x.contractType === 'PERPETUAL' && x.status === 'TRADING' && x.quoteAsset === 'USDT' && !CATALOG.some(i => i.s === x.symbol && i.v === 'fut')).forEach(x => {
      const nm = NAMES[x.symbol], tf = x.underlyingType && !['COIN', 'INDEX'].includes(x.underlyingType), c = nm ? nm[1] : FX_RE.test(x.symbol) ? 'fx' : tf ? 'idx' : null;
      if (c) CATALOG.push(row(x.symbol, nm ? nm[0] : `${x.baseAsset} (futures)`, c, 'fut'));
    });
    renderPicker(); log(`Daftar instrumen diperbarui, ${CATALOG.length} pasangan`);
  } catch (e) { log(`Daftar futures tidak bisa dimuat: ${e.message}`, 'warn'); }
}

/* Antarmuka */
const zoom = f => { S.view.bars = clamp(S.view.bars * f, 20, 400); S.dirty = true; };
const apply = () => { queue(true); S.dirty = true; };
function fillForm() {
  $(`input[name="source"][value="${cfg.source}"]`).checked = true; $('#cfg-endpoint').value = cfg.endpoint; $('#cfg-endpoint').disabled = cfg.source !== 'external';
  $('#cfg-drift').value = cfg.drift; $('#cfg-dist').value = cfg.dist; $('#cfg-smt').value = cfg.smt;
  ['50', '80', '95'].forEach(b => { $(`#cfg-band${b}`).checked = cfg[`b${b}`]; });
  [['wq', 'q'], ['wt', 't'], ['wg', 'g']].forEach(([k, s]) => { $(`#cfg-${k}`).value = cfg[k]; bind(`cfg.${k}`, cfg[k]); void s; });
}
function readForm() {
  const old = pairSym(); cfg.source = $('input[name="source"]:checked').value; cfg.endpoint = $('#cfg-endpoint').value.trim();
  cfg.drift = $('#cfg-drift').value; cfg.dist = $('#cfg-dist').value; cfg.smt = $('#cfg-smt').value;
  ['50', '80', '95'].forEach(b => { cfg[`b${b}`] = $(`#cfg-band${b}`).checked; });
  ['wq', 'wt', 'wg'].forEach(k => { cfg[k] = +$(`#cfg-${k}`).value; });
  save(); S.btKey = ''; S.ext = null; if (pairSym() !== old) load(); else apply();
}
function initUi() {
  const menu = $('#symbol-list'), btn = $('#symbol-btn'), toggle = v => { menu.hidden = !v; btn.setAttribute('aria-expanded', String(v)); };
  btn.onclick = e => { e.stopPropagation(); toggle(menu.hidden); };
  const list = $('#symbol-list');
  list.addEventListener('click', e => {
    e.stopPropagation(); const b = e.target.closest('[data-cat]'), li = e.target.closest('li[data-s]');
    if (b) { pickCat = b.dataset.cat; pickQ = ''; $('#sym-q').value = ''; renderPicker(); }
    else if (li) { cfg.symbol = li.dataset.s; cfg.venue = li.dataset.v; save(); toggle(false); renderPicker(); load(); }
  });
  list.addEventListener('keydown', e => { if (e.key === 'Enter' && e.target.matches('li[data-s]')) e.target.click(); });
  $('#sym-q').oninput = e => { pickQ = e.target.value; renderPicker(); };
  document.addEventListener('click', () => toggle(false)); document.addEventListener('keydown', e => { if (e.key === 'Escape') toggle(false); });
  $$('#tf [data-tf]').forEach(b => { b.setAttribute('aria-checked', String(b.dataset.tf === cfg.tf)); b.onclick = () => { cfg.tf = b.dataset.tf; $$('#tf [data-tf]').forEach(o => o.setAttribute('aria-checked', String(o === b))); save(); load(); }; });
  $$('[data-layer]').forEach(b => { b.setAttribute('aria-pressed', String(!!cfg.layers[b.dataset.layer])); b.onclick = () => { const k = b.dataset.layer; cfg.layers[k] = !cfg.layers[k]; b.setAttribute('aria-pressed', String(cfg.layers[k])); save(); S.dirty = true; }; });
  $$('[data-zoom]').forEach(b => { b.onclick = () => { const z = b.dataset.zoom; if (z === 'in') zoom(0.8); else if (z === 'out') zoom(1.25); else { S.view.bars = z === 'fit' ? 160 : 90; if (z === 'reset') S.view.off = 0; S.dirty = true; } }; });
  const tabs = $$('[role="tab"]'); tabs.forEach((t, i) => {
    t.onclick = () => tabs.forEach(o => { const on = o === t; o.setAttribute('aria-selected', String(on)); o.tabIndex = on ? 0 : -1; $(`#${o.getAttribute('aria-controls')}`).hidden = !on; });
    t.onkeydown = e => { const j = e.key === 'ArrowRight' ? i + 1 : e.key === 'ArrowLeft' ? i - 1 : -1; if (j >= 0) { const n = tabs[(j + tabs.length) % tabs.length]; n.focus(); n.click(); } };
  });
  const hz = $('#cfg-horizon'), cn = $('#cfg-context'), ln = $('#cfg-lines');
  hz.value = cfg.horizon; cn.value = cfg.context; ln.value = cfg.lines; bind('cfg.horizon', cfg.horizon); bind('cfg.context', cfg.context);
  hz.oninput = () => { cfg.horizon = +hz.value; bind('cfg.horizon', cfg.horizon); save(); queue(); refreshExternal(); };
  cn.onchange = () => { cfg.context = +cn.value; bind('cfg.context', cfg.context); save(); S.btKey = ''; queue(true); };
  ln.onchange = () => { cfg.lines = +ln.value; save(); S.dirty = true; };
  const dlg = $('#settings'); $('#btn-settings').onclick = () => { fillForm(); dlg.showModal(); };
  dlg.addEventListener('close', () => { if (dlg.returnValue === 'save') readForm(); dlg.returnValue = ''; });
  $$('input[name="source"]').forEach(r => { r.onchange = () => { $('#cfg-endpoint').disabled = r.value !== 'external'; }; });
  [['wq', 'wq'], ['wt', 'wt'], ['wg', 'wg']].forEach(([id, k]) => { $(`#cfg-${id}`).oninput = e => bind(`cfg.${k}`, e.target.value); });
  $('#btn-reset').onclick = () => { Object.assign(cfg, { ...DEF, symbol: cfg.symbol, tf: cfg.tf, horizon: cfg.horizon, context: cfg.context, lines: cfg.lines, layers: cfg.layers }); fillForm(); };
  $('#btn-fullscreen').onclick = () => { if (document.fullscreenElement) document.exitFullscreen(); else document.documentElement.requestFullscreen?.(); };
  $('#btn-retry').onclick = load;
  const ch = $('#chart'); let drag = null;
  const pos = e => { const r = ch.getBoundingClientRect(); return { x: e.clientX - r.left, y: e.clientY - r.top }; };
  ch.addEventListener('pointerdown', e => { drag = { x: e.clientX, off: S.view.off }; ch.setPointerCapture(e.pointerId); });
  ch.addEventListener('pointermove', e => { S.mouse = pos(e); S.hud = true; if (drag) { S.view.off = drag.off + (e.clientX - drag.x) / (geom().bw || 1); S.dirty = true; } });
  ch.addEventListener('pointerup', () => { drag = null; }); ch.addEventListener('pointerleave', () => { S.mouse = null; S.hud = true; });
  ch.addEventListener('wheel', e => { e.preventDefault(); zoom(e.deltaY > 0 ? 1.1 : 0.9); }, { passive: false });
  ch.addEventListener('keydown', e => { const k = e.key; if (k === 'ArrowLeft') S.view.off += 5; else if (k === 'ArrowRight') S.view.off -= 5; else if (k === '+' || k === '=') zoom(0.8); else if (k === '-') zoom(1.25); else return; S.dirty = true; e.preventDefault(); });
  new ResizeObserver(resize).observe(ch);
}

colors();
function colors() { C = { up: css('--up', '#0f9f7a'), down: css('--down', '#e5484d'), ink3: css('--ink-3', '#8390a4'), cyan: css('--cyan', '#28b8d8'), amber: css('--amber', '#f7a935'), iris: css('--iris', '#7a6cf0') }; }
window.lucide?.createIcons();
renderPicker(); initUi(); resize(); load(); discover(); setInterval(heartbeat, 1000); requestAnimationFrame(frame);
})();
