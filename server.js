// SC Lowcountry Permit Tracker — Express Server
const express = require('express');
const path = require('path');
const cron = require('node-cron');
const config = require('./config');
const db = require('./db/init');
const scraper = require('./scraper/index');
const drywallScanner = require('./scraper/drywall-scanner');
const builderLookup = require('./scraper/builderLookup');
const directoryScraper = require('./scraper/directory-scraper');

const cookieParser = require('cookie-parser');
const crypto = require('crypto');

const builderCache = require('./scraper/builder-cache');
const buyerList = require('./scraper/buyer-list');
const https = require('https');
const fs = require('fs');

// ─── Auto-sync builder cache to GitHub repo ─────────────────────────────────
const GITHUB_TOKEN = process.env.GITHUB_TOKEN || '';
const GITHUB_REPO = 'PowersellClaude/Pierpont';
const CACHE_FILE_PATH = 'data/builder-cache.json';
let lastCacheSyncCount = 0;

async function syncCacheToGitHub() {
  if (!GITHUB_TOKEN) { console.log('[GitSync] No GITHUB_TOKEN set — skipping cache sync'); return; }
  const cacheStats = builderCache.stats();
  // Only sync if new builders were added since last sync
  if (cacheStats.total <= lastCacheSyncCount) return;

  try {
    const cacheData = JSON.stringify(builderCache.loadCache(), null, 2);

    // Get current file SHA (required for update)
    const getRes = await fetch(`https://api.github.com/repos/${GITHUB_REPO}/contents/${CACHE_FILE_PATH}`, {
      headers: { 'Authorization': `token ${GITHUB_TOKEN}`, 'Accept': 'application/vnd.github.v3+json' },
    });
    const getJson = await getRes.json();
    const sha = getJson.sha || null;

    // Update file in repo
    const body = {
      message: `Auto-update builder cache (${cacheStats.total} builders, ${cacheStats.withPhone} phones, ${cacheStats.withEmail} emails)`,
      content: Buffer.from(cacheData).toString('base64'),
      ...(sha ? { sha } : {}),
    };

    const putRes = await fetch(`https://api.github.com/repos/${GITHUB_REPO}/contents/${CACHE_FILE_PATH}`, {
      method: 'PUT',
      headers: { 'Authorization': `token ${GITHUB_TOKEN}`, 'Content-Type': 'application/json', 'Accept': 'application/vnd.github.v3+json' },
      body: JSON.stringify(body),
    });

    if (putRes.ok) {
      lastCacheSyncCount = cacheStats.total;
      console.log(`[GitSync] Builder cache synced to GitHub: ${cacheStats.total} builders (${cacheStats.withPhone} phones, ${cacheStats.withEmail} emails)`);
    } else {
      const err = await putRes.text();
      console.error(`[GitSync] Failed to sync: ${putRes.status} ${err}`);
    }
  } catch (err) {
    console.error(`[GitSync] Sync error: ${err.message}`);
  }
}
const dailyEmail = require('./scraper/daily-email');
const foiaParser = require('./scraper/foia-parser');
const multer = require('multer');
const upload = multer({ dest: require('os').tmpdir(), limits: { fileSize: 20 * 1024 * 1024 } });

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(cookieParser());

// ─── Auth ───────────────────────────────────────────────────────────────────
const APP_PASSWORD = 'Bulleit';
const SESSION_SECRET = crypto.randomBytes(32).toString('hex');
const sessions = new Set();

function createSession() {
  const token = crypto.randomBytes(32).toString('hex');
  sessions.add(token);
  return token;
}

function isAuthenticated(req) {
  return req.cookies?.session && sessions.has(req.cookies.session);
}

const LOGIN_HTML = `<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Pierpont Money Printer — Login</title>
<link href="https://fonts.googleapis.com/css2?family=Fira+Code:wght@400;600;700&family=Fira+Sans:wght@300;400;500;600&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Fira Sans',system-ui,sans-serif;background:linear-gradient(135deg,#0F172A 0%,#1E293B 30%,#0F172A 60%,#1a1a3e 100%);color:#E2E8F0;min-height:100vh;display:flex;align-items:center;justify-content:center}
.card{background:rgba(255,255,255,0.06);backdrop-filter:blur(20px);border:1px solid rgba(255,255,255,0.1);border-radius:20px;padding:40px;width:380px;max-width:90vw;box-shadow:0 8px 32px rgba(0,0,0,0.4);text-align:center}
.logo{height:64px;margin-bottom:16px}
h1{font-family:'Fira Code',monospace;font-size:1.3rem;font-weight:700;background:linear-gradient(135deg,#3B82C4,#2B6CB0,#6B7B8D);-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:4px}
.sub{font-size:.75rem;color:#94A3B8;margin-bottom:28px;letter-spacing:.04em}
input[type=password]{width:100%;background:rgba(255,255,255,0.06);border:1px solid rgba(255,255,255,0.1);border-radius:10px;padding:12px 16px;font-size:.95rem;color:#E2E8F0;font-family:'Fira Sans',sans-serif;outline:none;transition:border .2s,box-shadow .2s;margin-bottom:16px}
input[type=password]:focus{border-color:#2B6CB0;box-shadow:0 0 0 3px rgba(43,108,176,0.3)}
input[type=password]::placeholder{color:#94A3B8;opacity:.6}
button{width:100%;background:linear-gradient(135deg,#3B82C4,#2B6CB0);color:white;font-weight:600;border:none;border-radius:12px;padding:12px;cursor:pointer;font-size:.95rem;transition:all .25s;box-shadow:0 4px 15px rgba(43,108,176,0.3)}
button:hover{transform:translateY(-1px);box-shadow:0 6px 20px rgba(43,108,176,0.5)}
</style>
</head><body>
<div class="card">
<img src="/logo.png" alt="Pierpont" class="logo">
<h1>Pierpont Money Printer</h1>
<p class="sub">SC Lowcountry Construction Lead Intelligence</p>
<!--ERROR-->
<form method="POST" action="/login">
<input type="password" name="password" placeholder="Enter password" autofocus required>
<button type="submit">Sign In</button>
</form>
</div>
</body></html>`;

// Login page
app.get('/login', (req, res) => {
  if (isAuthenticated(req)) return res.redirect('/');
  res.send(LOGIN_HTML);
});

app.post('/login', (req, res) => {
  if (req.body?.password === APP_PASSWORD) {
    const token = createSession();
    res.cookie('session', token, { httpOnly: true, maxAge: 30 * 24 * 60 * 60 * 1000 }); // 30 days
    return res.redirect('/');
  }
  res.send(LOGIN_HTML.replace('<!--ERROR-->', '<div style="color:#FCA5A5;font-size:.85rem;margin-bottom:12px;text-align:center">Incorrect password</div>'));
});

app.get('/logout', (req, res) => {
  if (req.cookies?.session) sessions.delete(req.cookies.session);
  res.clearCookie('session');
  res.redirect('/login');
});

// Auth middleware — protect everything except /login and static login assets
app.use((req, res, next) => {
  if (req.path === '/login' || req.path === '/logo.png') return next();
  if (!isAuthenticated(req)) return res.redirect('/login');
  next();
});

app.use(express.static(path.join(__dirname, 'public')));

// ─── Permits API ─────────────────────────────────────────────────────────────
app.get('/api/permits', async (req, res) => {
  try { res.json(await db.queryPermits(req.query)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// Today's new permits — must be before :id param route
app.get('/api/permits/today', async (req, res) => {
  try { res.json(await db.getTodaysNewPermits()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/permits/:id', async (req, res) => {
  try {
    const permit = await db.getPermitById(req.params.id);
    if (!permit) return res.status(404).json({ error: 'Not found' });
    res.json(permit);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── Delete permits ──────────────────────────────────────────────────────────
app.delete('/api/permits/:id', async (req, res) => {
  try {
    await db.deletePermit(Number(req.params.id));
    res.json({ message: 'Permit deleted' });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/permits/delete-batch', async (req, res) => {
  try {
    const ids = req.body?.ids;
    if (!ids || !Array.isArray(ids) || ids.length === 0) return res.status(400).json({ error: 'No IDs provided' });
    await db.deletePermits(ids.map(Number));
    res.json({ message: `Deleted ${ids.length} permits` });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── Email sent toggle ──────────────────────────────────────────────────────
app.patch('/api/permits/:id/email-sent', async (req, res) => {
  try {
    const sent = req.body?.sent !== undefined ? req.body.sent : true;
    await db.toggleEmailSent(Number(req.params.id), sent);
    res.json({ message: 'Updated', email_sent: sent ? 1 : 0 });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── Manual contact save (saves to DB + builder cache) ──────────────────────
app.patch('/api/permits/:id/contact', async (req, res) => {
  try {
    const permit = await db.getPermitById(Number(req.params.id));
    if (!permit) return res.status(404).json({ error: 'Permit not found' });

    const { phone, email, website } = req.body;
    const updates = {};
    if (phone !== undefined) updates.phone = phone || null;
    if (email !== undefined) updates.email = email || null;
    if (website !== undefined) updates.website = website || null;

    if (Object.keys(updates).length === 0) return res.status(400).json({ error: 'No fields to update' });

    // Save to permit record
    await db.updateBuilderContact(permit.id, updates);

    // Save to builder cache so ALL future permits from this builder auto-populate
    const companyKey = permit.builder_company || permit.builder_name;
    if (companyKey) {
      const builderCache = require('./scraper/builder-cache');
      const existing = builderCache.get(companyKey) || {};
      builderCache.set(companyKey, {
        website: updates.website !== undefined ? updates.website : (existing.website || null),
        phone: updates.phone !== undefined ? updates.phone : (existing.phone || null),
        email: updates.email !== undefined ? updates.email : (existing.email || null),
        allPhones: existing.allPhones || [],
        allEmails: existing.allEmails || [],
      });
      console.log(`[ManualSave] "${companyKey}" => phone: ${updates.phone || 'unchanged'}, email: ${updates.email || 'unchanged'} — saved to cache`);
    }

    res.json({ message: 'Contact info saved', updates });
    // Sync updated cache to GitHub
    syncCacheToGitHub().catch(e => console.error('[GitSync] Error:', e.message));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── Scraper API ─────────────────────────────────────────────────────────────
let scrapeInProgress = false;
let builderLookupInProgress = false;
let builderLookupStatus = null;

async function runBuilderLookupAfterScrape() {
  if (builderLookupInProgress) return;
  builderLookupInProgress = true;
  builderLookupStatus = { status: 'starting', total: 0, processed: 0 };
  try {
    await builderLookup.bulkLookupBuilders(db, (status) => { builderLookupStatus = status; });
  } catch (err) {
    console.error('Builder lookup error:', err);
    builderLookupStatus = { status: 'error', error: err.message };
  } finally {
    builderLookupInProgress = false;
    // Auto-sync cache to GitHub so it persists across deploys
    syncCacheToGitHub().catch(e => console.error('[GitSync] Error:', e.message));
  }
}

app.post('/api/scrape', async (req, res) => {
  if (scrapeInProgress) return res.status(409).json({ error: 'Scrape already in progress', status: scraper.getStatus() });
  scrapeInProgress = true;
  res.json({ message: 'Scrape started', status: 'running' });
  try {
    await scraper.runAllScrapers(req.body || {});
    // Auto-run builder lookup after scrape completes
    runBuilderLookupAfterScrape();
  }
  catch (err) { console.error('Scrape error:', err); }
  finally { scrapeInProgress = false; }
});

// Test scrape — only process N permits (default 5)
app.post('/api/scrape/test', async (req, res) => {
  if (scrapeInProgress) return res.status(409).json({ error: 'Scrape already in progress' });
  scrapeInProgress = true;
  const limit = parseInt(req.body?.limit) || 5;
  res.json({ message: `Test scrape started (limit: ${limit})`, status: 'running' });
  try {
    await scraper.runAllScrapers({ testLimit: limit });
    runBuilderLookupAfterScrape();
  }
  catch (err) { console.error('Test scrape error:', err); }
  finally { scrapeInProgress = false; }
});

app.get('/api/scrape/status', (req, res) => {
  res.json({ running: scrapeInProgress, ...(scraper.getStatus() || { status: 'idle' }) });
});

app.get('/api/scrapers', (req, res) => { res.json(scraper.getScraperInfo()); });

// ─── Municipalities & Filter Options ─────────────────────────────────────────
app.get('/api/municipalities', (req, res) => {
  const munis = config.municipalities;
  const driveTimes = config.driveTimesFrom29464;
  const result = Object.values(munis).map(m => ({
    name: m.name,
    slug: m.slug,
    active: m.active,
    portalType: m.portalType,
    driveTimeMinutes: m.driveTimeMinutes,
  }));
  res.json(result);
});

app.get('/api/filters/options', async (req, res) => {
  try {
    const distinct = await db.getDistinctValues();
    const driveTimes = config.driveTimesFrom29464;
    res.json({ ...distinct, driveTimes });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── FOIA Requests API ──────────────────────────────────────────────────────
const FOIA_BODY = `To Whom It May Concern,

Pursuant to the South Carolina Freedom of Information Act, I am a taxpaying citizen requesting the following records for research purposes only:

A list of all strapping inspections (also known as strap/banding inspections) that received a passing status within the last 90 days, including permit number, property address, contractor/builder name, contractor/builder phone number, contractor/builder email address, underlying build permit valuation, inspection date, and status.

Thank you for your time.`;

app.get('/api/foia/municipalities', (req, res) => {
  const munis = config.municipalities;
  const result = Object.values(munis)
    .filter(m => m.foia)
    .map(m => {
      const entry = { name: m.name, slug: m.slug, driveTimeMinutes: m.driveTimeMinutes, foiaType: m.foia.type };
      if (m.foia.type === 'email') {
        const subject = encodeURIComponent('FOIA REQUEST — Strapping Inspections');
        const body = encodeURIComponent(FOIA_BODY);
        entry.email = m.foia.email;
        entry.mailtoUrl = `mailto:${m.foia.email}?subject=${subject}&body=${body}`;
      } else if (m.foia.type === 'portal') {
        entry.portalUrl = m.foia.portalUrl;
        entry.foiaBody = FOIA_BODY;
      }
      return entry;
    });
  res.json(result);
});

// ─── FOIA Import API ────────────────────────────────────────────────────────
app.post('/api/foia/import', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
    const municipality = req.body.municipality;
    if (!municipality) return res.status(400).json({ error: 'Municipality is required' });

    const result = await foiaParser.parseFile(req.file.path, municipality);

    // Clean up temp file
    try { require('fs').unlinkSync(req.file.path); } catch (e) {}

    if (result.error && result.permits.length === 0) {
      return res.status(400).json(result);
    }

    // Upsert each permit into the database
    let inserted = 0, updated = 0, skipped = 0;
    for (const permit of result.permits) {
      const r = await db.upsertPermit(permit);
      if (r.action === 'inserted') inserted++;
      else if (r.action === 'updated') updated++;
      else skipped++;
    }

    res.json({
      message: `Imported ${inserted + updated} permits from FOIA response`,
      inserted,
      updated,
      skipped: skipped + result.skippedRows,
      totalRows: result.totalRows,
      mappedColumns: result.mappedColumns,
    });
  } catch (err) {
    console.error('FOIA import error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ─── Stats API ───────────────────────────────────────────────────────────────
app.get('/api/stats', async (req, res) => {
  try { res.json(await db.getStats(req.query)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── CSV Export ──────────────────────────────────────────────────────────────
app.get('/api/export/csv', async (req, res) => {
  try {
    const permits = await db.getAllPermitsForExport(req.query);
    const headers = ['Permit Number','Address','Municipality','Builder Name','Builder Company','Builder Phone','Builder Email','Builder Website','Personal Phone','Personal Email','Applicant Name','Applicant Phone','Applicant Email','Owner Name','Project Value','Permit Type','Inspection Type','Inspection Date','Inspection Status','Permit Issue Date','Opportunity Score','Source URL','Scraped At'];
    const esc = (v) => { if (v == null) return ''; const s = String(v); return (s.includes(',') || s.includes('"') || s.includes('\n')) ? `"${s.replace(/"/g, '""')}"` : s; };
    const rows = permits.map(p => [p.permit_number,p.address,p.municipality,p.builder_name,p.builder_company,p.builder_phone,p.builder_email,p.builder_website,p.personal_phone,p.personal_email,p.applicant_name,p.applicant_phone,p.applicant_email,p.owner_name,p.project_value,p.permit_type,p.inspection_type,p.inspection_date,p.inspection_status,p.permit_issue_date,p.opportunity_score,p.source_url,p.scraped_at].map(esc).join(','));
    const csv = [headers.join(','), ...rows].join('\n');
    const today = new Date().toISOString().split('T')[0];
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename="permit-tracker-${today}.csv"`);
    res.send(csv);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── Drywall Opportunity Scanner API ─────────────────────────────────────────
let drywallScanInProgress = false;
app.post('/api/scan/drywall', async (req, res) => {
  if (drywallScanInProgress) return res.status(409).json({ error: 'Scan already in progress' });
  drywallScanInProgress = true;
  res.json({ message: 'Drywall scan started' });
  try { await drywallScanner.runScan(); }
  catch (err) { console.error('Drywall scan error:', err); }
  finally { drywallScanInProgress = false; }
});

app.get('/api/scan/drywall/status', (req, res) => {
  res.json({ running: drywallScanInProgress, ...(drywallScanner.getScanStatus() || { status: 'idle' }) });
});

app.get('/api/opportunities', async (req, res) => {
  try { res.json(await db.getOpportunities(req.query)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── Builder Cache Export/Import (persist across deploys via git) ────────────
app.get('/api/builder-cache/export', (req, res) => {
  try {
    const data = builderCache.loadCache();
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Content-Disposition', 'attachment; filename="builder-cache.json"');
    res.send(JSON.stringify(data, null, 2));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/builder-cache/import', async (req, res) => {
  try {
    const data = req.body;
    if (!data || typeof data !== 'object') return res.status(400).json({ error: 'Invalid JSON' });
    let imported = 0;
    for (const [key, value] of Object.entries(data)) {
      if (key.startsWith('__')) { builderCache.loadCache()[key] = value; continue; }
      if (!builderCache.has(key) || (!builderCache.get(key)?.phone && value.phone)) {
        builderCache.set(key, value);
        imported++;
      }
    }
    builderCache.saveCache();
    res.json({ message: `Imported ${imported} builder entries`, total: builderCache.stats().total });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── Clear All Data ──────────────────────────────────────────────────────────
app.delete('/api/clear', async (req, res) => {
  try {
    await db.clearAllData();
    res.json({ message: 'All data cleared' });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── Builder Lookup API ──────────────────────────────────────────────────────
app.post('/api/builder-lookup/run', async (req, res) => {
  if (builderLookupInProgress) return res.status(409).json({ error: 'Builder lookup already in progress', status: builderLookupStatus });
  res.json({ message: 'Builder lookup started' });
  runBuilderLookupAfterScrape();
});

app.get('/api/builder-lookup/status', (req, res) => {
  res.json({ running: builderLookupInProgress, ...(builderLookupStatus || { status: 'idle' }) });
});

app.get('/api/builder-cache/stats', (req, res) => {
  res.json(builderCache.stats());
});

app.get('/api/buyer-list/stats', (req, res) => {
  res.json(buyerList.stats());
});

// ─── Directory Scraper API ──────────────────────────────────────────────────
let directoryScrapeInProgress = false;
app.post('/api/directory/scrape', async (req, res) => {
  if (directoryScrapeInProgress) return res.status(409).json({ error: 'Directory scrape already in progress', status: directoryScraper.getScanStatus() });
  const force = req.body?.force === true;
  directoryScrapeInProgress = true;
  res.json({ message: force ? 'Directory scrape started (forced)' : 'Directory scrape started' });
  try {
    await directoryScraper.scrapeDirectory(null, { force });
  } catch (err) {
    console.error('Directory scrape error:', err);
  } finally {
    directoryScrapeInProgress = false;
  }
});

app.get('/api/directory/status', (req, res) => {
  res.json({ running: directoryScrapeInProgress, ...(directoryScraper.getScanStatus() || { status: 'idle' }) });
});

app.post('/api/permits/:id/lookup-builder', async (req, res) => {
  try {
    const permit = await db.getPermitById(req.params.id);
    if (!permit) return res.status(404).json({ error: 'Permit not found' });

    const companyName = permit.builder_company || permit.builder_name;
    if (!companyName) return res.status(400).json({ error: 'No builder name or company on this permit' });

    const result = await builderLookup.lookupBuilder(companyName);

    // Update DB with found info (only overwrite if we found something and field was empty)
    const updates = {};
    if (result.phone && !permit.builder_phone) updates.phone = result.phone;
    if (result.email && !permit.builder_email) updates.email = result.email;
    if (result.website) updates.website = result.website;

    if (Object.keys(updates).length > 0) {
      await db.updateBuilderContact(permit.id, updates);
    }

    res.json({
      success: true,
      website: result.website,
      phone: result.phone,
      email: result.email,
      allPhones: result.allPhones || [],
      allEmails: result.allEmails || [],
      updated: updates,
    });
  } catch (err) {
    console.error('Builder lookup error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ─── Auto-Schedule (7am, 1pm, 6pm EST) ──────────────────────────────────────
let scheduleEnabled = true;
const scheduleTimes = ['7:00', '19:00']; // EST — 7am and 7pm
const scheduleHistory = []; // last 20 scheduled runs

async function runScheduledScrape() {
  const now = new Date().toLocaleString('en-US', { timeZone: 'America/New_York' });
  console.log(`\n⏰ Scheduled scrape triggered at ${now} EST (today only)`);
  if (scrapeInProgress) {
    console.log('⏭️  Skipping — scrape already in progress');
    scheduleHistory.unshift({ time: now, status: 'skipped', reason: 'scrape already in progress' });
    return;
  }
  scrapeInProgress = true;
  scheduleHistory.unshift({ time: now, status: 'running' });
  if (scheduleHistory.length > 20) scheduleHistory.pop();
  try {
    // Scheduled runs only scrape today's permits (not the full 30 days)
    const today = new Date().toISOString().split('T')[0];
    const result = await scraper.runAllScrapers({ dateFrom: today, dateTo: today, days: 1 });
    scheduleHistory[0].status = 'completed';
    scheduleHistory[0].permits = result.permitsFound;
    scheduleHistory[0].newPermits = result.permitsNew;
    runBuilderLookupAfterScrape();
  } catch (err) {
    console.error('Scheduled scrape error:', err);
    scheduleHistory[0].status = 'error';
    scheduleHistory[0].error = err.message;
  } finally {
    scrapeInProgress = false;
  }
}

// Schedule: 7:00 AM and 7:00 PM EST (America/New_York) — today's permits only
const cronJobs = [
  cron.schedule('0 7 * * *', () => { if (scheduleEnabled) runScheduledScrape(); }, { timezone: 'America/New_York' }),
  cron.schedule('0 19 * * *', () => { if (scheduleEnabled) runScheduledScrape(); }, { timezone: 'America/New_York' }),
  // Daily leads email — 7:30 AM EST Mon-Fri (after scrape finishes)
  cron.schedule('30 7 * * 1-5', async () => {
    try { await dailyEmail.sendDailyEmail(); } catch (err) { console.error('[Email] Cron error:', err.message); }
  }, { timezone: 'America/New_York' }),
];

app.get('/api/schedule', (req, res) => {
  res.json({ enabled: scheduleEnabled, times: scheduleTimes, timezone: 'EST', history: scheduleHistory });
});

app.post('/api/schedule/toggle', (req, res) => {
  scheduleEnabled = !scheduleEnabled;
  console.log(`⏰ Auto-schedule ${scheduleEnabled ? 'enabled' : 'disabled'}`);
  res.json({ enabled: scheduleEnabled });
});

// ─── Daily email ─────────────────────────────────────────────────────────────
// POST /api/email/send         — send new leads only
// POST /api/email/send?full=1  — send ALL leads >= $300k (first run)
app.post('/api/email/send', async (req, res) => {
  if (!isAuthenticated(req)) return res.status(401).json({ error: 'Not authenticated' });
  try {
    const fullRun = req.query.full === '1' || req.body.full === true;
    const result = await dailyEmail.sendDailyEmail({ fullRun });
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/email/preview', async (req, res) => {
  if (!isAuthenticated(req)) return res.status(401).json({ error: 'Not authenticated' });
  try {
    const full = req.query.full === '1';
    const leads = full
      ? await db.getAllHighValueLeads(dailyEmail.MIN_VALUE)
      : await db.getNewHighValueLeads(dailyEmail.MIN_VALUE);
    res.json({ count: leads.length, mode: full ? 'full' : 'new_only', leads: leads.slice(0, 20), configured: !!process.env.MAILJET_API_KEY });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Future placeholders ─────────────────────────────────────────────────────
app.post('/api/enrich/:permit_id', async (req, res) => { res.status(501).json({ error: 'Contact enrichment not yet implemented' }); });
app.post('/api/ghl/push/:permit_id', async (req, res) => { res.status(501).json({ error: 'GoHighLevel integration not yet implemented' }); });

// Serve frontend
app.get('*', (req, res) => { res.sendFile(path.join(__dirname, 'public', 'index.html')); });

// ─── Start ───────────────────────────────────────────────────────────────────
async function start() {
  await db.getDb();
  await db.backfillOpportunityScores();
  await db.backfillBuilderCache();
  await db.restoreContactsFromCache();
  app.listen(config.server.port, () => {
    console.log('');
    console.log('🏗️  Pierpont Money Printer');
    console.log(`📡 Server running at http://${config.server.host}:${config.server.port}`);
    console.log('💾 Database initialized');
    const blStats = buyerList.stats();
    console.log(`📋 Buyer lists loaded: ${blStats.totalEntries} entries (${blStats.withPhone} phones, ${blStats.withEmail} emails)`);
    console.log('🔍 Ready to scrape permits');
    console.log('⏰ Auto-scrape scheduled: 7:00 AM, 7:00 PM EST (today only)');
    console.log(`📧 Daily email: ${process.env.EMAIL_FROM ? 'configured' : 'NOT configured (set EMAIL_FROM, EMAIL_TO, EMAIL_APP_PASSWORD)'}`);
    if (process.env.EMAIL_FROM) console.log(`   From: ${process.env.EMAIL_FROM} → To: ${process.env.EMAIL_TO || process.env.EMAIL_FROM}`);
    console.log('');

    // Auto-scrape: only today's permits, never full 30-day on startup
    // Full scrapes should be triggered manually via "Run Scraper" button
    setTimeout(async () => {
      try {
        const stats = await db.getStats();
        const hasData = stats.total_permits > 0;

        if (hasData) {
          console.log(`✅ ${stats.total_permits} permits in DB — ready to go`);
        } else {
          console.log('📭 Empty database — click "Run Scraper" for initial load, or wait for 7am/7pm auto-scrape');
        }
      } catch (err) {
        console.error('Startup check error:', err);
      }
    }, 5000);
  });
}
start();

process.on('SIGINT', () => { db.close(); process.exit(0); });
process.on('SIGTERM', () => { db.close(); process.exit(0); });
