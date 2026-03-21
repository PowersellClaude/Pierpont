// Town of Mount Pleasant -- Oracle OPAL Permit System Scraper
// ViewPoint Cloud was discontinued July 2024; Mount Pleasant moved to Oracle OPAL.
// Strategy: Try Oracle OPAL REST API patterns first, fall back to Puppeteer for SPA.

const puppeteer = require('puppeteer');
const axios = require('axios');
const config = require('../../config');
const utils = require('../utils');

const municipalityConfig = config.municipalities.mountPleasant;

// Oracle OPAL common API patterns
const OPAL_BASE = 'https://mtpleasantsc.gov';
const OPAL_API_PATHS = [
  '/api/permits',
  '/api/v1/permits',
  '/opal/api/permits',
  '/opal/api/v1/records',
  '/portal/api/permits',
  '/citizen-portal/api/permits',
];

module.exports = {
  name: municipalityConfig.name,
  slug: municipalityConfig.slug,
  portalUrl: municipalityConfig.portalUrl,
  portalType: municipalityConfig.portalType,
  active: municipalityConfig.active,

  async scrape(options = {}) {
    const { dateFrom, dateTo, minValue = config.scraper.minProjectValue } = options;
    const dateRange = dateFrom && dateTo
      ? { from: dateFrom, to: dateTo }
      : utils.getDateRange();

    utils.log(`[Mt Pleasant] Starting Oracle OPAL scrape -- ${dateRange.from} to ${dateRange.to}`);

    const permits = [];

    // Try API-first approach
    try {
      const apiPermits = await this.tryApiScrape(dateRange, minValue);
      if (apiPermits.length > 0) {
        permits.push(...apiPermits);
        utils.log(`[Mt Pleasant] API scrape complete -- ${permits.length} permits found`);
        return permits;
      }
    } catch (err) {
      utils.log(`[Mt Pleasant] API approach failed: ${err.message} -- falling back to Puppeteer`);
    }

    // Fallback: Puppeteer scraping for the SPA
    let browser;
    try {
      browser = await puppeteer.launch(config.scraper.puppeteer);
      const page = await browser.newPage();
      await page.setUserAgent(utils.getRandomUserAgent());
      page.setDefaultTimeout(config.scraper.pageTimeoutMs);

      utils.log('[Mt Pleasant] Navigating to permit portal...');

      // Intercept API calls to discover endpoints
      const discoveredApis = [];
      page.on('response', async (response) => {
        const url = response.url();
        if (url.includes('api') || url.includes('permit') || url.includes('record') || url.includes('opal')) {
          try {
            const contentType = response.headers()['content-type'] || '';
            if (contentType.includes('json')) {
              discoveredApis.push({ url, status: response.status() });
            }
          } catch (e) { /* ignore */ }
        }
      });

      await utils.withRetry(async () => {
        await page.goto(municipalityConfig.portalUrl, {
          waitUntil: 'networkidle2',
          timeout: config.scraper.pageTimeoutMs,
        });
      }, { label: 'Mt Pleasant portal load' });

      await utils.delay(4000); // Wait for SPA to render

      // Look for permit search functionality
      const searchSelectors = [
        'a[href*="permit"]',
        'a[href*="building"]',
        'a[href*="search"]',
        'button:has-text("Search")',
        'a:has-text("Permits")',
        'a:has-text("Building")',
        'input[type="search"]',
        'input[placeholder*="search" i]',
        'input[placeholder*="permit" i]',
      ];

      for (const selector of searchSelectors) {
        try {
          const el = await page.$(selector);
          if (el) {
            const text = await el.evaluate(e => e.textContent || e.placeholder || '');
            utils.log(`[Mt Pleasant] Found element: "${text.trim().substring(0, 50)}" (${selector})`);
            if (text.toLowerCase().includes('permit') || text.toLowerCase().includes('building') || text.toLowerCase().includes('search')) {
              await el.click();
              await utils.delay(3000);
              break;
            }
          }
        } catch (e) { /* try next */ }
      }

      // Log discovered APIs for future optimization
      if (discoveredApis.length > 0) {
        utils.log(`[Mt Pleasant] Discovered ${discoveredApis.length} API endpoints during navigation`);
        discoveredApis.slice(0, 5).forEach(api => utils.log(`  -> ${api.url} (${api.status})`));
      }

      // Parse any visible results
      const resultRows = await page.$$eval(
        'table tr, [class*="result"], [class*="record"], [class*="list-item"], [class*="card"], [class*="permit"]',
        elements => elements.map(el => ({
          text: el.textContent.trim().substring(0, 500),
          link: el.querySelector('a')?.href || null,
        }))
      );

      utils.log(`[Mt Pleasant] Found ${resultRows.length} potential result elements`);

      for (const row of resultRows) {
        if (!row.text || row.text.length < 10) continue;

        const valueMatch = row.text.match(/\$[\d,]+\.?\d*/);
        const value = valueMatch ? utils.parseDollarValue(valueMatch[0]) : null;
        if (value !== null && value < minValue) continue;

        const permitMatch = row.text.match(/\b(BLD|BP|PMT|RES|COM|BLDG)[-\s]?\d{2,}[-\s]?\d+\b/i);

        if (permitMatch || value) {
          const permit = utils.createPermitRecord({
            permit_number: permitMatch ? permitMatch[0] : null,
            address: row.text.substring(0, 100),
            municipality: municipalityConfig.name,
            project_value: value,
            source_url: row.link || municipalityConfig.portalUrl,
            raw_data: { source: 'oracle-opal-puppeteer' },
          });
          permits.push(permit);
        }
      }

      if (permits.length === 0) {
        utils.log('[Mt Pleasant] No permits found via Puppeteer -- portal structure may need manual review');
        await utils.saveScreenshot(page, 'mount-pleasant-results');
      }

    } catch (error) {
      utils.log(`[Mt Pleasant] Scraper error: ${error.message}`);
      if (browser) {
        try {
          const pages = await browser.pages();
          if (pages.length > 0) await utils.saveScreenshot(pages[0], 'mount-pleasant-error');
        } catch (e) { /* ignore */ }
      }
      throw error;
    } finally {
      if (browser) await browser.close();
    }

    utils.log(`[Mt Pleasant] Scrape complete -- ${permits.length} permits found`);
    return permits;
  },

  // Try Oracle OPAL REST API directly
  async tryApiScrape(dateRange, minValue) {
    const permits = [];
    const headers = {
      'User-Agent': utils.getRandomUserAgent(),
      'Accept': 'application/json',
    };

    for (const apiPath of OPAL_API_PATHS) {
      try {
        const response = await axios.get(`${OPAL_BASE}${apiPath}`, {
          params: {
            type: 'building',
            startDate: dateRange.from,
            endDate: dateRange.to,
            status: 'issued',
            pageSize: 100,
          },
          headers,
          timeout: 15000,
        });

        if (response.data && Array.isArray(response.data)) {
          utils.log(`[Mt Pleasant] API hit at ${apiPath}: ${response.data.length} records`);

          for (const record of response.data) {
            const value = utils.parseDollarValue(record.value || record.projectValue || record.estimatedCost);
            if (value !== null && value < minValue) continue;

            const permit = utils.createPermitRecord({
              permit_number: record.recordNumber || record.permitNumber || record.id,
              address: record.address || record.location || record.siteAddress,
              municipality: municipalityConfig.name,
              builder_name: record.contractor || record.builderName,
              builder_company: record.contractorCompany || record.builderCompany,
              builder_phone: record.contractorPhone || record.builderPhone || record.phone || null,
              builder_email: record.contractorEmail || record.builderEmail || record.email || null,
              applicant_name: record.applicant || record.applicantName,
              owner_name: record.owner || record.ownerName,
              project_value: value,
              permit_type: record.type || record.recordType || record.permitType,
              inspection_type: record.inspectionType,
              inspection_date: record.inspectionDate || record.lastInspectionDate,
              inspection_status: record.inspectionStatus || record.status,
              permit_issue_date: record.issueDate || record.issuedDate,
              source_url: `${OPAL_BASE}/permits/${record.id || record.recordNumber}`,
              raw_data: { source: 'oracle-opal-api', ...record },
            });
            permits.push(permit);
          }
          break; // Found working endpoint
        }

        // Also try if response wraps data in a results field
        if (response.data && response.data.results && Array.isArray(response.data.results)) {
          utils.log(`[Mt Pleasant] API hit at ${apiPath}: ${response.data.results.length} records (wrapped)`);
          for (const record of response.data.results) {
            const value = utils.parseDollarValue(record.value || record.projectValue || record.estimatedCost);
            if (value !== null && value < minValue) continue;

            const permit = utils.createPermitRecord({
              permit_number: record.recordNumber || record.permitNumber || record.id,
              address: record.address || record.location,
              municipality: municipalityConfig.name,
              project_value: value,
              permit_type: record.type || record.recordType,
              inspection_status: record.status,
              source_url: `${OPAL_BASE}/permits/${record.id || record.recordNumber}`,
              raw_data: { source: 'oracle-opal-api', ...record },
            });
            permits.push(permit);
          }
          break;
        }
      } catch (err) {
        continue;
      }
    }

    return permits;
  },
};
