/* Lumen feed: lapisan server-side berita dan kalender dengan fallback bertingkat dan cache.
   /api/feed?k=news  -> berita 24 jam (GDELT, CNBC, Reuters via Google News, Investing, BBC, Al Jazeera, FXStreet, Yahoo, CoinDesk)
   /api/feed?k=cal   -> kalender ekonomi (Fair Economy dengan dua host, cache, lalu data terakhir yang masih berlaku) */
const UA = 'Mozilla/5.0 (compatible; LumenFeed/1.0)';
const MEM = (globalThis.__lumenFeed = globalThis.__lumenFeed || { news: null, cal: null });
const TIER1 = [
  ['cnbc-top', 'https://www.cnbc.com/id/100003114/device/rss/rss.html'], ['cnbc-finance', 'https://www.cnbc.com/id/10000664/device/rss/rss.html'],
  ['cnbc-economy', 'https://www.cnbc.com/id/20910258/device/rss/rss.html'], ['cnbc-world', 'https://www.cnbc.com/id/100727362/device/rss/rss.html'],
  ['reuters', 'https://news.google.com/rss/search?q=site:reuters.com+when:1d&hl=en-US&gl=US&ceid=US:en'],
  ['reuters-markets', 'https://news.google.com/rss/search?q=site:reuters.com+(markets+OR+gold+OR+oil+OR+dollar+OR+fed+OR+bitcoin)+when:1d&hl=en-US&gl=US&ceid=US:en'],
  ['investing', 'https://www.investing.com/rss/news.rss'], ['investing-forex', 'https://www.investing.com/rss/news_1.rss'],
];
const TIER2 = [
  ['bbc-world', 'https://feeds.bbci.co.uk/news/world/rss.xml'], ['bbc-business', 'https://feeds.bbci.co.uk/news/business/rss.xml'], ['aljazeera', 'https://www.aljazeera.com/xml/rss/all.xml'],
  ['fxstreet', 'https://www.fxstreet.com/rss/news'], ['yahoo-finance', 'https://finance.yahoo.com/news/rssindex'], ['coindesk', 'https://www.coindesk.com/arc/outboundfeeds/rss/'], ['investing-commodities', 'https://www.investing.com/rss/news_11.rss'],
];
const GDELT_Q = '(war OR conflict OR sanctions OR tariff OR "central bank" OR inflation OR oil OR OPEC OR ceasefire OR missile OR recession OR "interest rate" OR gold OR bitcoin OR economy) sourcelang:english';
async function get(url, ms = 5500, extra = {}) {
  const ac = new AbortController(), to = setTimeout(() => ac.abort(), ms);
  try { const r = await fetch(url, { signal: ac.signal, headers: { 'user-agent': UA, accept: 'application/rss+xml, application/xml, application/json, text/xml, */*', ...extra }, redirect: 'follow' }); if (!r.ok) throw new Error(`HTTP ${r.status}`); return await r.text(); } finally { clearTimeout(to); }
}
const dec = s => String(s || '').replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1').replace(/<[^>]+>/g, ' ').replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#0?39;|&apos;/g, "'").replace(/&nbsp;/g, ' ').replace(/&#(\d+);/g, (_, n) => String.fromCharCode(+n)).replace(/\s+/g, ' ').trim();
const tag = (b, n) => { const r = new RegExp(`<${n}\\b[^>]*>([\\s\\S]*?)</${n}>`, 'i').exec(b); return r ? r[1] : ''; };
function parseRss(xml, id) {
  const out = [], re = /<(item|entry)\b[\s\S]*?<\/\1>/gi; let m;
  while ((m = re.exec(xml))) {
    const b = m[0], title = dec(tag(b, 'title')); let url = dec(tag(b, 'link')); if (!url) { const h = /<link[^>]+href=["']([^"']+)/i.exec(b); url = h ? h[1] : ''; }
    const t = Date.parse(dec(tag(b, 'pubDate') || tag(b, 'published') || tag(b, 'updated') || tag(b, 'dc:date'))), src = dec(tag(b, 'source')) || id;
    if (title && /^https?:\/\//.test(url) && Number.isFinite(t)) out.push({ title: id.startsWith('reuters') ? title.replace(/\s+-\s+Reuters$/i, '') : title, url, dom: id.startsWith('reuters') ? 'reuters.com' : new URL(url).hostname.replace(/^www\./, ''), t, src });
  }
  return out;
}
async function viaRss([id, url]) { const t0 = Date.now(), items = parseRss(await get(url), id); return { id, items, ms: Date.now() - t0 }; }
async function viaGdelt() {
  const t0 = Date.now(), j = JSON.parse(await get(`https://api.gdeltproject.org/api/v2/doc/doc?query=${encodeURIComponent(GDELT_Q)}&mode=artlist&maxrecords=150&format=json&sort=datedesc&timespan=24h`, 8000)), items = [];
  for (const a of j.articles || []) { const m = /^(\d{4})(\d\d)(\d\d)T(\d\d)(\d\d)(\d\d)Z$/.exec(a.seendate || ''); if (m && a.title && /^https?:\/\//.test(a.url || '')) items.push({ title: a.title, url: a.url, dom: a.domain || '', t: Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +m[6]), src: 'gdelt' }); }
  return { id: 'gdelt', items, ms: Date.now() - t0 };
}
async function runAll(jobs, tried) {
  const res = await Promise.allSettled(jobs.map(j => j())); const items = [];
  res.forEach((r, i) => { const id = jobs[i].id; if (r.status === 'fulfilled') { tried.push({ id: r.value.id, ok: true, n: r.value.items.length, ms: r.value.ms }); items.push(...r.value.items); } else tried.push({ id, ok: false, err: r.reason && r.reason.name === 'AbortError' ? 'timeout' : String((r.reason && r.reason.message) || r.reason) }); });
  return items;
}
const job = (id, fn) => { const f = () => fn(); f.id = id; return f; };
async function newsFeed() {
  const now = Date.now(); if (MEM.news && now - MEM.news.t < 60000) return { ...MEM.news.data, cache: 'memory' };
  const tried = [], fresh = a => a.filter(x => now - x.t <= 864e5 && x.t <= now + 3e5);
  let all = await runAll([job('gdelt', viaGdelt), ...TIER1.map(s => job(s[0], () => viaRss(s)))], tried);
  if (fresh(all).length < 30) all = all.concat(await runAll(TIER2.map(s => job(s[0], () => viaRss(s))), tried));
  const seen = new Set(), items = fresh(all).filter(a => { const k = a.title.toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim().slice(0, 90); if (seen.has(k)) return false; seen.add(k); return true; }).sort((a, b) => b.t - a.t).slice(0, 220);
  if (!items.length) { if (MEM.news && now - MEM.news.t < 6 * 36e5) return { ...MEM.news.data, cache: 'stale', tried }; return { ok: false, items: [], tried, t: now, error: 'semua sumber gagal' }; }
  const data = { ok: true, t: now, items, tried }; MEM.news = { t: now, data }; return { ...data, cache: 'live' };
}
async function calFeed() {
  const now = Date.now(); if (MEM.cal && now - MEM.cal.t < 15 * 60000) return { ...MEM.cal.data, cache: 'memory' };
  const tried = [], events = [], hosts = ['https://nfs.faireconomy.media', 'https://cdn-nfs.faireconomy.media'];
  for (const wk of ['thisweek', 'nextweek']) {
    for (const h of hosts) { try { const j = JSON.parse(await get(`${h}/ff_calendar_${wk}.json`, 6000)); if (Array.isArray(j)) { events.push(...j); tried.push({ id: `${wk}@${h.includes('cdn') ? 'cdn' : 'nfs'}`, ok: true, n: j.length }); break; } } catch (e) { tried.push({ id: `${wk}@${h.includes('cdn') ? 'cdn' : 'nfs'}`, ok: false, err: e.name === 'AbortError' ? 'timeout' : e.message }); } }
  }
  if (!events.length) { if (MEM.cal && now - MEM.cal.t < 7 * 864e5) return { ...MEM.cal.data, cache: 'stale', tried }; return { ok: false, events: [], tried, t: now, error: 'semua sumber kalender gagal' }; }
  const data = { ok: true, t: now, events, tried }; MEM.cal = { t: now, data }; return { ...data, cache: 'live' };
}
module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  let k = 'news'; try { k = new URL(req.url, 'http://x').searchParams.get('k') || 'news'; } catch { /* default */ }
  try { const out = k === 'cal' ? await calFeed() : await newsFeed(); res.setHeader('Cache-Control', `public, s-maxage=${k === 'cal' ? 900 : 90}, stale-while-revalidate=1800`); res.status(200).json(out); }
  catch (e) { res.setHeader('Cache-Control', 'no-store'); res.status(200).json({ ok: false, items: [], events: [], error: String((e && e.message) || e) }); }
};
