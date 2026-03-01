// Database initialization and CRUD operations — sql.js (pure JS SQLite)
const initSqlJs = require('sql.js');
const fs = require('fs');
const path = require('path');
const config = require('../config');
const { calculateOpportunityScore } = require('../scraper/utils');

let db;
let dbReady;

async function getDb() {
  if (db) return db;
  if (dbReady) return dbReady;
  dbReady = (async () => {
    const SQL = await initSqlJs();
    const dbPath = config.database.path;
    const dbDir = path.dirname(dbPath);
    if (!fs.existsSync(dbDir)) fs.mkdirSync(dbDir, { recursive: true });
    if (fs.existsSync(dbPath)) {
      db = new SQL.Database(fs.readFileSync(dbPath));
    } else {
      db = new SQL.Database();
    }
    initSchema();
    return db;
  })();
  return dbReady;
}

function saveToFile() {
  if (!db) return;
  fs.writeFileSync(config.database.path, Buffer.from(db.export()));
}

function initSchema() {
  db.run(`
    CREATE TABLE IF NOT EXISTS permits (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      permit_number TEXT UNIQUE,
      address TEXT NOT NULL,
      municipality TEXT NOT NULL,
      builder_name TEXT,
      builder_company TEXT,
      builder_phone TEXT,
      builder_email TEXT,
      applicant_name TEXT,
      applicant_phone TEXT,
      applicant_email TEXT,
      owner_name TEXT,
      project_value REAL,
      permit_type TEXT,
      inspection_type TEXT,
      inspection_date TEXT,
      inspection_status TEXT,
      permit_issue_date TEXT,
      source_url TEXT,
      scraped_at TEXT DEFAULT (datetime('now')),
      raw_data TEXT,
      is_drywall_opportunity INTEGER DEFAULT 0,
      opportunity_confidence TEXT,
      opportunity_signals TEXT,
      estimated_drywall_date TEXT
    )
  `);
  db.run(`CREATE INDEX IF NOT EXISTS idx_municipality ON permits(municipality)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_inspection_date ON permits(inspection_date)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_project_value ON permits(project_value)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_builder_name ON permits(builder_name)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_permit_number ON permits(permit_number)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_drywall ON permits(is_drywall_opportunity)`);

  // Migration: add opportunity_score column if it doesn't exist
  try {
    db.run(`ALTER TABLE permits ADD COLUMN opportunity_score INTEGER`);
  } catch (e) {
    // Column already exists — ignore
  }

  // Migration: add builder_website column if it doesn't exist
  try {
    db.run(`ALTER TABLE permits ADD COLUMN builder_website TEXT`);
  } catch (e) {
    // Column already exists — ignore
  }

  // Migration: add personal contact columns for skip-trace API (future)
  try {
    db.run(`ALTER TABLE permits ADD COLUMN personal_phone TEXT`);
  } catch (e) {}
  try {
    db.run(`ALTER TABLE permits ADD COLUMN personal_email TEXT`);
  } catch (e) {}
  db.run(`CREATE INDEX IF NOT EXISTS idx_opportunity_score ON permits(opportunity_score)`);

  db.run(`
    CREATE TABLE IF NOT EXISTS scrape_runs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      started_at TEXT DEFAULT (datetime('now')),
      completed_at TEXT,
      status TEXT DEFAULT 'running',
      municipalities_attempted INTEGER DEFAULT 0,
      municipalities_succeeded INTEGER DEFAULT 0,
      permits_found INTEGER DEFAULT 0,
      permits_new INTEGER DEFAULT 0,
      permits_updated INTEGER DEFAULT 0,
      errors TEXT,
      log TEXT
    )
  `);
  saveToFile();
}

// ─── Helpers ─────────────────────────────────────────────────────────────────
const clean = (v) => (v === undefined || v === null || v === '') ? null : String(v);
const cleanNum = (v) => {
  if (v === undefined || v === null || v === '') return null;
  const n = Number(String(v).replace(/[^0-9.-]/g, ''));
  return isNaN(n) ? null : n;
};

function queryAll(sql, params = []) {
  const stmt = db.prepare(sql);
  if (params.length > 0) stmt.bind(params);
  const rows = [];
  while (stmt.step()) rows.push(stmt.getAsObject());
  stmt.free();
  return rows;
}

function queryOne(sql, params = []) {
  const rows = queryAll(sql, params);
  return rows.length > 0 ? rows[0] : null;
}

function execute(sql, params = []) {
  db.run(sql, params);
  saveToFile();
}

// ─── Upsert ──────────────────────────────────────────────────────────────────
async function upsertPermit(permit) {
  await getDb();
  const p = {
    permit_number: clean(permit.permit_number),
    address: clean(permit.address) || '',
    municipality: clean(permit.municipality) || '',
    builder_name: clean(permit.builder_name),
    builder_company: clean(permit.builder_company),
    builder_phone: clean(permit.builder_phone),
    builder_email: clean(permit.builder_email),
    applicant_name: clean(permit.applicant_name),
    applicant_phone: clean(permit.applicant_phone),
    applicant_email: clean(permit.applicant_email),
    owner_name: clean(permit.owner_name),
    project_value: cleanNum(permit.project_value),
    permit_type: clean(permit.permit_type),
    inspection_type: clean(permit.inspection_type),
    inspection_date: clean(permit.inspection_date),
    inspection_status: clean(permit.inspection_status),
    permit_issue_date: clean(permit.permit_issue_date),
    source_url: clean(permit.source_url),
    raw_data: permit.raw_data ? (typeof permit.raw_data === 'string' ? permit.raw_data : JSON.stringify(permit.raw_data)) : null,
  };

  if (!p.permit_number) {
    // Generate a synthetic key from address + type + date
    p.permit_number = `AUTO-${(p.address || '').substring(0,30)}-${(p.inspection_type || '').substring(0,10)}-${p.inspection_date || Date.now()}`.replace(/\s+/g, '-');
  }

  // Reject records that are clearly form labels, not real permit data
  const JUNK_LABELS = /^(address[12]?|city|state|zip|name|phone|email|password|fax|county|submit|cancel|update):?$/i;
  if (JUNK_LABELS.test((p.address || '').trim())) {
    return { action: 'skipped', reason: 'address is a form label' };
  }

  // Calculate opportunity score
  const { score } = calculateOpportunityScore({
    project_value: p.project_value,
    inspection_date: p.inspection_date,
    municipality: p.municipality,
  });
  p.opportunity_score = score;

  const existing = queryOne('SELECT id FROM permits WHERE permit_number = ?', [p.permit_number]);

  if (existing) {
    execute(`UPDATE permits SET address=?,municipality=?,builder_name=?,builder_company=?,builder_phone=?,builder_email=?,applicant_name=?,applicant_phone=?,applicant_email=?,owner_name=?,project_value=?,permit_type=?,inspection_type=?,inspection_date=?,inspection_status=?,permit_issue_date=?,source_url=?,scraped_at=datetime('now'),raw_data=?,opportunity_score=? WHERE permit_number=?`,
      [p.address, p.municipality, p.builder_name, p.builder_company, p.builder_phone, p.builder_email, p.applicant_name, p.applicant_phone, p.applicant_email, p.owner_name, p.project_value, p.permit_type, p.inspection_type, p.inspection_date, p.inspection_status, p.permit_issue_date, p.source_url, p.raw_data, p.opportunity_score, p.permit_number]);
    return { action: 'updated', id: existing.id };
  } else {
    execute(`INSERT INTO permits (permit_number,address,municipality,builder_name,builder_company,builder_phone,builder_email,applicant_name,applicant_phone,applicant_email,owner_name,project_value,permit_type,inspection_type,inspection_date,inspection_status,permit_issue_date,source_url,raw_data,opportunity_score) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
      [p.permit_number, p.address, p.municipality, p.builder_name, p.builder_company, p.builder_phone, p.builder_email, p.applicant_name, p.applicant_phone, p.applicant_email, p.owner_name, p.project_value, p.permit_type, p.inspection_type, p.inspection_date, p.inspection_status, p.permit_issue_date, p.source_url, p.raw_data, p.opportunity_score]);
    const row = queryOne('SELECT last_insert_rowid() as id');
    return { action: 'inserted', id: row ? row.id : null };
  }
}

// ─── Queries ─────────────────────────────────────────────────────────────────
async function queryPermits(params = {}) {
  await getDb();
  const conditions = [];
  const values = [];

  if (params.municipality) { conditions.push('municipality = ?'); values.push(params.municipality); }
  if (params.min_value) { conditions.push('project_value >= ?'); values.push(Number(params.min_value)); }
  if (params.max_value) { conditions.push('project_value <= ?'); values.push(Number(params.max_value)); }
  if (params.date_from) { conditions.push('inspection_date >= ?'); values.push(params.date_from); }
  if (params.date_to) { conditions.push('inspection_date <= ?'); values.push(params.date_to); }
  if (params.inspection_status) { conditions.push('inspection_status = ?'); values.push(params.inspection_status); }
  if (params.search) {
    conditions.push(`(address LIKE ? OR municipality LIKE ? OR builder_name LIKE ? OR builder_company LIKE ? OR applicant_name LIKE ? OR owner_name LIKE ? OR permit_number LIKE ? OR builder_phone LIKE ? OR builder_email LIKE ? OR applicant_phone LIKE ? OR applicant_email LIKE ?)`);
    const s = `%${params.search}%`;
    for (let i = 0; i < 11; i++) values.push(s);
  }

  // Drive-time filter: restrict to municipalities within N minutes of 29464
  if (params.max_drive_time) {
    const driveTimes = require('../config').driveTimesFrom29464;
    const maxMin = Number(params.max_drive_time);
    const withinRange = Object.entries(driveTimes)
      .filter(([, min]) => min <= maxMin)
      .map(([name]) => name);
    if (withinRange.length > 0) {
      conditions.push(`municipality IN (${withinRange.map(() => '?').join(',')})`);
      values.push(...withinRange);
    } else {
      conditions.push('1=0'); // no municipalities in range
    }
  }

  const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
  const allowed = ['municipality','address','builder_name','builder_company','applicant_name','project_value','inspection_date','inspection_status','permit_number','scraped_at','permit_issue_date','opportunity_score'];
  const sortBy = allowed.includes(params.sort_by) ? params.sort_by : 'inspection_date';
  const sortOrder = params.sort_order === 'asc' ? 'ASC' : 'DESC';
  const page = Math.max(1, parseInt(params.page) || 1);
  const perPage = Math.min(200, Math.max(1, parseInt(params.per_page) || 50));
  const offset = (page - 1) * perPage;

  const countRow = queryOne(`SELECT COUNT(*) as total FROM permits ${where}`, values);
  const total = countRow ? countRow.total : 0;
  const rows = queryAll(`SELECT * FROM permits ${where} ORDER BY ${sortBy} ${sortOrder} LIMIT ? OFFSET ?`, [...values, perPage, offset]);

  return { data: rows, pagination: { page, per_page: perPage, total, total_pages: Math.ceil(total / perPage) } };
}

async function getPermitById(id) { await getDb(); return queryOne('SELECT * FROM permits WHERE id = ?', [id]); }

async function getStats(params = {}) {
  await getDb();
  const cond = []; const vals = [];
  if (params.date_from) { cond.push('inspection_date >= ?'); vals.push(params.date_from); }
  if (params.date_to) { cond.push('inspection_date <= ?'); vals.push(params.date_to); }
  if (params.min_value) { cond.push('project_value >= ?'); vals.push(Number(params.min_value)); }
  const where = cond.length ? `WHERE ${cond.join(' AND ')}` : '';

  const totals = queryOne(`SELECT COUNT(*) as total_permits, COALESCE(AVG(project_value),0) as avg_value, COALESCE(MIN(project_value),0) as min_value, COALESCE(MAX(project_value),0) as max_value, MIN(inspection_date) as earliest_date, MAX(inspection_date) as latest_date FROM permits ${where}`, vals);
  const byMuni = queryAll(`SELECT municipality, COUNT(*) as count FROM permits ${where} GROUP BY municipality ORDER BY count DESC`, vals);
  const lastRun = queryOne('SELECT * FROM scrape_runs ORDER BY id DESC LIMIT 1');

  return { ...(totals || {}), by_municipality: byMuni, last_run: lastRun || null };
}

async function getAllPermitsForExport(params = {}) {
  await getDb();
  const cond = []; const vals = [];
  if (params.municipality) { cond.push('municipality = ?'); vals.push(params.municipality); }
  if (params.min_value) { cond.push('project_value >= ?'); vals.push(Number(params.min_value)); }
  if (params.max_value) { cond.push('project_value <= ?'); vals.push(Number(params.max_value)); }
  if (params.date_from) { cond.push('inspection_date >= ?'); vals.push(params.date_from); }
  if (params.date_to) { cond.push('inspection_date <= ?'); vals.push(params.date_to); }
  if (params.inspection_status) { cond.push('inspection_status = ?'); vals.push(params.inspection_status); }
  if (params.search) { cond.push(`(address LIKE ? OR builder_name LIKE ? OR permit_number LIKE ?)`); const s=`%${params.search}%`; vals.push(s,s,s); }
  if (params.max_drive_time) {
    const driveTimes = require('../config').driveTimesFrom29464;
    const maxMin = Number(params.max_drive_time);
    const withinRange = Object.entries(driveTimes).filter(([, min]) => min <= maxMin).map(([name]) => name);
    if (withinRange.length > 0) {
      cond.push(`municipality IN (${withinRange.map(() => '?').join(',')})`);
      vals.push(...withinRange);
    } else { cond.push('1=0'); }
  }
  const where = cond.length ? `WHERE ${cond.join(' AND ')}` : '';
  return queryAll(`SELECT * FROM permits ${where} ORDER BY inspection_date DESC`, vals);
}

// Get distinct values for filter dropdown population
async function getDistinctValues() {
  await getDb();
  const municipalities = queryAll('SELECT DISTINCT municipality FROM permits WHERE municipality IS NOT NULL AND municipality != "" ORDER BY municipality');
  const statuses = queryAll('SELECT DISTINCT inspection_status FROM permits WHERE inspection_status IS NOT NULL AND inspection_status != "" ORDER BY inspection_status');
  return {
    municipalities: municipalities.map(r => r.municipality),
    statuses: statuses.map(r => r.inspection_status),
  };
}

// ─── Builder contact lookup ─────────────────────────────────────────────────
async function updateBuilderContact(id, data) {
  await getDb();
  const fields = [];
  const vals = [];
  if (data.phone) { fields.push('builder_phone = ?'); vals.push(data.phone); }
  if (data.email) { fields.push('builder_email = ?'); vals.push(data.email); }
  if (data.website) { fields.push('builder_website = ?'); vals.push(data.website); }
  if (data.personal_phone) { fields.push('personal_phone = ?'); vals.push(data.personal_phone); }
  if (data.personal_email) { fields.push('personal_email = ?'); vals.push(data.personal_email); }
  if (fields.length === 0) return;
  vals.push(id);
  execute(`UPDATE permits SET ${fields.join(', ')} WHERE id = ?`, vals);
}

async function getPermitsNeedingLookup() {
  await getDb();
  return queryAll(`SELECT id, builder_name, builder_company, builder_phone, builder_email, builder_website
    FROM permits
    WHERE (builder_company IS NOT NULL AND builder_company != '')
      AND (builder_website IS NULL OR builder_website = '')
    ORDER BY opportunity_score DESC`);
}

// ─── Drywall opportunities ──────────────────────────────────────────────────
async function updateDrywallOpportunity(permitNumber, data) {
  await getDb();
  execute(`UPDATE permits SET is_drywall_opportunity=?, opportunity_confidence=?, opportunity_signals=?, estimated_drywall_date=? WHERE permit_number=?`,
    [data.is_opportunity ? 1 : 0, clean(data.confidence), clean(data.signals), clean(data.estimated_date), permitNumber]);
}

async function getOpportunities(params = {}) {
  await getDb();
  const cond = ['is_drywall_opportunity = 1']; const vals = [];
  if (params.confidence) { cond.push('opportunity_confidence = ?'); vals.push(params.confidence); }
  if (params.municipality) { cond.push('municipality = ?'); vals.push(params.municipality); }
  const where = `WHERE ${cond.join(' AND ')}`;
  return queryAll(`SELECT * FROM permits ${where} ORDER BY CASE opportunity_confidence WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 WHEN 'LOW' THEN 3 END, inspection_date DESC`, vals);
}

// ─── Backfill opportunity scores ─────────────────────────────────────────────
async function backfillOpportunityScores() {
  await getDb();
  const rows = queryAll('SELECT id, project_value, inspection_date, municipality FROM permits');
  let updated = 0;
  for (const row of rows) {
    const { score } = calculateOpportunityScore({
      project_value: row.project_value,
      inspection_date: row.inspection_date,
      municipality: row.municipality,
    });
    db.run('UPDATE permits SET opportunity_score = ? WHERE id = ?', [score, row.id]);
    updated++;
  }
  saveToFile();
  console.log(`[DB] Backfilled opportunity scores for ${updated} permits`);
}

// ─── Scrape runs ─────────────────────────────────────────────────────────────
async function createScrapeRun() { await getDb(); execute('INSERT INTO scrape_runs (status) VALUES (?)', ['running']); const r=queryOne('SELECT last_insert_rowid() as id'); return r.id; }
async function updateScrapeRun(id, data) { await getDb(); const f=[]; const v=[]; for (const [k,val] of Object.entries(data)) { f.push(`${k}=?`); v.push(val===undefined?null:val); } v.push(id); if(f.length) execute(`UPDATE scrape_runs SET ${f.join(',')} WHERE id=?`, v); }
async function getLatestScrapeRun() { await getDb(); return queryOne('SELECT * FROM scrape_runs ORDER BY id DESC LIMIT 1'); }
function close() { if(db){saveToFile();db.close();db=null;dbReady=null;} }

async function clearAllData() {
  await getDb();
  execute('DELETE FROM permits');
  execute('DELETE FROM scrape_runs');
  // Also clear the seen_permits.json file so the next scrape fetches everything
  const seenFile = path.join(__dirname, '..', 'seen_permits.json');
  try { if (fs.existsSync(seenFile)) fs.unlinkSync(seenFile); } catch (e) {}
}

module.exports = { getDb, upsertPermit, queryPermits, getPermitById, getStats, getAllPermitsForExport, getDistinctValues, updateBuilderContact, getPermitsNeedingLookup, updateDrywallOpportunity, getOpportunities, backfillOpportunityScores, createScrapeRun, updateScrapeRun, getLatestScrapeRun, clearAllData, close };
