// SC Lowcountry Permit Tracker — Express Server
const express = require('express');
const path = require('path');
const cron = require('node-cron');
const config = require('./config');
const db = require('./db/init');
const scraper = require('./scraper/index');
const drywallScanner = require('./scraper/drywall-scanner');
const builderLookup = require('./scraper/builderLookup');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ─── Permits API ─────────────────────────────────────────────────────────────
app.get('/api/permits', async (req, res) => {
  try { res.json(await db.queryPermits(req.query)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/permits/:id', async (req, res) => {
  try {
    const permit = await db.getPermitById(req.params.id);
    if (!permit) return res.status(404).json({ error: 'Not found' });
    res.json(permit);
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

A list of all strapping inspections (also known as strap/banding inspections) that received a passing status within the last 90 days, including permit number, property address, contractor/builder name, inspection date, and status.

Thank you for your time.`;

app.get('/api/foia/municipalities', (req, res) => {
  const munis = config.municipalities;
  const result = Object.values(munis)
    .filter(m => m.foia)
    .map(m => {
      const entry = { name: m.name, slug: m.slug, driveTimeMinutes: m.driveTimeMinutes, foiaType: m.foia.type };
      if (m.foia.type === 'email') {
        const subject = encodeURIComponent('FOIA REQUEST');
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
const scheduleTimes = ['7:00', '13:00', '18:00']; // EST
const scheduleHistory = []; // last 20 scheduled runs

async function runScheduledScrape() {
  const now = new Date().toLocaleString('en-US', { timeZone: 'America/New_York' });
  console.log(`\n⏰ Scheduled scrape triggered at ${now} EST`);
  if (scrapeInProgress) {
    console.log('⏭️  Skipping — scrape already in progress');
    scheduleHistory.unshift({ time: now, status: 'skipped', reason: 'scrape already in progress' });
    return;
  }
  scrapeInProgress = true;
  scheduleHistory.unshift({ time: now, status: 'running' });
  if (scheduleHistory.length > 20) scheduleHistory.pop();
  try {
    const result = await scraper.runAllScrapers();
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

// Schedule: 7:00 AM, 1:00 PM, 6:00 PM EST (America/New_York)
const cronJobs = [
  cron.schedule('0 7 * * *', () => { if (scheduleEnabled) runScheduledScrape(); }, { timezone: 'America/New_York' }),
  cron.schedule('0 13 * * *', () => { if (scheduleEnabled) runScheduledScrape(); }, { timezone: 'America/New_York' }),
  cron.schedule('0 18 * * *', () => { if (scheduleEnabled) runScheduledScrape(); }, { timezone: 'America/New_York' }),
];

app.get('/api/schedule', (req, res) => {
  res.json({ enabled: scheduleEnabled, times: scheduleTimes, timezone: 'EST', history: scheduleHistory });
});

app.post('/api/schedule/toggle', (req, res) => {
  scheduleEnabled = !scheduleEnabled;
  console.log(`⏰ Auto-schedule ${scheduleEnabled ? 'enabled' : 'disabled'}`);
  res.json({ enabled: scheduleEnabled });
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
  app.listen(config.server.port, () => {
    console.log('');
    console.log('🏗️  Pierpont Money Printer');
    console.log(`📡 Server running at http://${config.server.host}:${config.server.port}`);
    console.log('💾 Database initialized');
    console.log('🔍 Ready to scrape permits');
    console.log('⏰ Auto-scrape scheduled: 7:00 AM, 1:00 PM, 6:00 PM EST');
    console.log('');
  });
}
start();

process.on('SIGINT', () => { db.close(); process.exit(0); });
process.on('SIGTERM', () => { db.close(); process.exit(0); });
