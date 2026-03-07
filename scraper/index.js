// Main Scraper Orchestrator for SC Lowcountry Permit Tracker
// Coordinates all municipality scrapers and stores results in the database
const config = require('../config');
const db = require('../db/init');
const utils = require('./utils');
const { lookupBuilder, launchBrowser } = require('./builderLookup');
const directoryScraper = require('./directory-scraper');

// Import all portal scrapers
const citizenserve = require('./portals/citizenserve');
const energovGeneric = require('./portals/energov-generic');

const scrapers = [
  // Core tier: 0-30 min
  require('./portals/charleston'),
  require('./portals/mount-pleasant'),
  require('./portals/sullivans-island'),
  citizenserve.isleOfPalms,
  require('./portals/north-charleston'),
  require('./portals/charleston-county'),
  // Core tier: 30-60 min
  citizenserve.kiawah,
  citizenserve.seabrook,
  citizenserve.summerville,
  require('./portals/opengov'),
  energovGeneric.berkeleyCounty,
  require('./portals/evolve-public'),
  // Extended tier: 60-90 min
  energovGeneric.hiltonHead,
  energovGeneric.bluffton,
  citizenserve.hardeeville,
];

// Track scraper status for the API
let currentRun = null;

function getStatus() {
  return currentRun;
}

async function runAllScrapers(options = {}) {
  const dateRange = utils.getDateRange(options.days || config.scraper.defaultDateRangeDays);
  const minValue = options.minValue || config.scraper.minProjectValue;

  const scrapeOptions = {
    dateFrom: options.dateFrom || dateRange.from,
    dateTo: options.dateTo || dateRange.to,
    minValue,
    testLimit: options.testLimit || null,
  };

  utils.log('═══════════════════════════════════════════════════════════════');
  utils.log('🏗️  SC Lowcountry Permit Tracker — Starting Scrape Run');
  utils.log(`📅 Date range: ${scrapeOptions.dateFrom} to ${scrapeOptions.dateTo}`);
  utils.log(`💰 Minimum value: $${minValue.toLocaleString()}`);
  utils.log(`🔍 Searching for: New construction with approved framing inspections`);
  utils.log('═══════════════════════════════════════════════════════════════');

  // Create a scrape run record
  const runId = await db.createScrapeRun();
  currentRun = {
    id: runId,
    status: 'running',
    started_at: new Date().toISOString(),
    municipalities_attempted: 0,
    municipalities_succeeded: 0,
    permits_found: 0,
    permits_new: 0,
    permits_updated: 0,
    errors: [],
    current_municipality: null,
    log: [],
  };

  const allPermits = [];
  let municipalitiesAttempted = 0;
  let municipalitiesSucceeded = 0;
  let totalNew = 0;
  let totalUpdated = 0;
  const errors = [];

  for (const scraper of scrapers) {
    if (!scraper.active) {
      utils.log(`⏭️  [${scraper.name}] Skipped — inactive`);
      continue;
    }

    municipalitiesAttempted++;
    currentRun.municipalities_attempted = municipalitiesAttempted;
    currentRun.current_municipality = scraper.name;

    utils.log(`\n${'─'.repeat(60)}`);
    utils.log(`📍 Scraping: ${scraper.name}`);
    utils.log(`   Portal: ${scraper.portalUrl}`);
    utils.log(`   Type: ${scraper.portalType}`);
    utils.log(`${'─'.repeat(60)}`);

    try {
      const permits = await utils.withRetry(
        () => scraper.scrape(scrapeOptions),
        { maxRetries: 2, label: scraper.name }
      );

      if (permits && permits.length > 0) {
        utils.log(`✅ [${scraper.name}] Found ${permits.length} permits — saving to database...`);

        for (const permit of permits) {
          try {
            const result = await db.upsertPermit(permit);
            if (result.action === 'inserted') totalNew++;
            else totalUpdated++;
          } catch (err) {
            utils.log(`⚠️  [${scraper.name}] Error saving permit ${permit.permit_number}: ${err?.message || err || 'unknown error'}`);
          }
        }

        allPermits.push(...permits);
      } else {
        utils.log(`ℹ️  [${scraper.name}] No matching permits found`);
      }

      municipalitiesSucceeded++;
      currentRun.municipalities_succeeded = municipalitiesSucceeded;

    } catch (error) {
      const errMsg = `[${scraper.name}] ${error.message}`;
      utils.log(`❌ ${errMsg}`);
      errors.push(errMsg);
      currentRun.errors = errors;
      // Continue with other scrapers — don't let one failure kill the run
    }

    currentRun.permits_found = allPermits.length;
    currentRun.permits_new = totalNew;
    currentRun.permits_updated = totalUpdated;

    // Rate limit between municipalities
    await utils.delay(config.scraper.requestDelayMs);
  }

  // ── Pre-populate builder cache from CHBA directory ──
  try {
    utils.log('\n📒 Crawling Charleston HBA directory for builder contacts...');
    currentRun.current_municipality = 'CHBA Directory';
    await directoryScraper.scrapeDirectory();
  } catch (err) {
    utils.log(`⚠️  Directory scrape error: ${err.message}`);
  }

  // ── Auto-crawl builder websites (uses ONE shared browser to avoid OOM) ──
  const excluded = config.excludedBuilders || [];
  const isExcluded = (name) => {
    if (!name) return false;
    const lower = name.toLowerCase();
    return excluded.some(pattern => lower.includes(pattern.toLowerCase()));
  };

  const companyMap = new Map();
  for (const p of allPermits) {
    const company = (p.builder_company || '').trim();
    if (!company) continue;
    // Skip excluded builders — they were already filtered during upsert
    if (isExcluded(company) || isExcluded(p.builder_name)) continue;
    if (!companyMap.has(company)) companyMap.set(company, []);
    companyMap.get(company).push(p);
  }

  const uniqueCompanies = [...companyMap.keys()];
  if (uniqueCompanies.length > 0) {
    utils.log(`\n🔍 Auto-crawling ${uniqueCompanies.length} builder websites...`);
    currentRun.current_municipality = 'Builder Lookup';

    let lookupBrowser;
    let lookupFound = 0;
    try {
      lookupBrowser = await launchBrowser();
      utils.log('[BuilderLookup] Shared browser launched for auto-crawl');

      for (const company of uniqueCompanies) {
        try {
          const result = await lookupBuilder(company, lookupBrowser);
          if (result.website) {
            lookupFound++;
            const permits = companyMap.get(company);
            for (const permit of permits) {
              const updates = {};
              if (result.phone) updates.phone = result.phone;
              if (result.email) updates.email = result.email;
              if (result.website) updates.website = result.website;
              if (Object.keys(updates).length > 0) {
                // Look up the DB record by permit_number (raw scraper objects don't have DB id)
                const dbRow = await db.getPermitByNumber(permit.permit_number);
                if (dbRow) {
                  await db.updateBuilderContact(dbRow.id, updates);
                } else {
                  utils.log(`⚠️  Could not find DB record for permit ${permit.permit_number}`);
                }
              }
            }
          }
          await utils.delay(2000);
        } catch (err) {
          utils.log(`⚠️  Builder lookup failed for "${company}": ${err.message}`);
        }
      }
    } catch (err) {
      utils.log(`⚠️  Builder lookup browser error: ${err.message}`);
    } finally {
      if (lookupBrowser) try { await lookupBrowser.close(); } catch {}
    }
    utils.log(`✅ Builder lookup complete: ${lookupFound}/${uniqueCompanies.length} websites found`);
  }

  // Update the scrape run record
  const completedAt = new Date().toISOString();
  await db.updateScrapeRun(runId, {
    completed_at: completedAt,
    status: errors.length > 0 ? 'completed_with_errors' : 'completed',
    municipalities_attempted: municipalitiesAttempted,
    municipalities_succeeded: municipalitiesSucceeded,
    permits_found: allPermits.length,
    permits_new: totalNew,
    permits_updated: totalUpdated,
    errors: errors.length > 0 ? JSON.stringify(errors) : null,
  });

  currentRun = {
    ...currentRun,
    status: errors.length > 0 ? 'completed_with_errors' : 'completed',
    completed_at: completedAt,
    current_municipality: null,
  };

  // Summary
  utils.log('\n═══════════════════════════════════════════════════════════════');
  utils.log('📊 Scrape Run Summary');
  utils.log('═══════════════════════════════════════════════════════════════');
  utils.log(`   Municipalities attempted: ${municipalitiesAttempted}`);
  utils.log(`   Municipalities succeeded: ${municipalitiesSucceeded}`);
  utils.log(`   Total permits found:      ${allPermits.length}`);
  utils.log(`   New permits:              ${totalNew}`);
  utils.log(`   Updated permits:          ${totalUpdated}`);
  if (errors.length > 0) {
    utils.log(`   Errors:                   ${errors.length}`);
    errors.forEach(e => utils.log(`     ⚠️  ${e}`));
  }
  utils.log('═══════════════════════════════════════════════════════════════\n');

  return {
    runId,
    municipalitiesAttempted,
    municipalitiesSucceeded,
    permitsFound: allPermits.length,
    permitsNew: totalNew,
    permitsUpdated: totalUpdated,
    errors,
  };
}

// Get info about all scrapers (active vs stub status)
function getScraperInfo() {
  return scrapers.map(s => ({
    name: s.name,
    slug: s.slug,
    portalUrl: s.portalUrl,
    portalType: s.portalType,
    active: s.active,
    notes: config.municipalities[
      Object.keys(config.municipalities).find(
        k => config.municipalities[k].slug === s.slug
      )
    ]?.notes || null,
  }));
}

// Allow running standalone: node scraper/index.js
if (require.main === module) {
  (async () => {
    try {
      utils.log('🏗️  SC Lowcountry Permit Tracker — Standalone Scraper');
      await db.getDb(); // Initialize database
      const result = await runAllScrapers();
      utils.log(`Done. ${result.permitsFound} permits found (${result.permitsNew} new, ${result.permitsUpdated} updated)`);
      process.exit(0);
    } catch (err) {
      utils.log(`Fatal error: ${err.message}`);
      console.error(err);
      process.exit(1);
    }
  })();
}

module.exports = {
  runAllScrapers,
  getStatus,
  getScraperInfo,
};
