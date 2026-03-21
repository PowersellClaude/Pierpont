// North Charleston -- Custom ArcGIS Customer Portal Scraper
// Portal: https://maps.northcharleston.org/CustomerPortal/
// Strategy: Try ArcGIS REST endpoints, fall back to Puppeteer for the portal SPA.

const puppeteer = require('puppeteer');
const axios = require('axios');
const config = require('../../config');
const utils = require('../utils');

const municipalityConfig = config.municipalities.northCharleston;
const PORTAL_BASE = 'https://maps.northcharleston.org';
const PORTAL_URL = `${PORTAL_BASE}/CustomerPortal/`;

// ArcGIS REST API patterns for permit data
const ARCGIS_API_PATHS = [
  '/arcgis/rest/services/Permits/FeatureServer/0/query',
  '/arcgis/rest/services/BuildingPermits/FeatureServer/0/query',
  '/arcgis/rest/services/Planning/FeatureServer/0/query',
  '/server/rest/services/Permits/FeatureServer/0/query',
  '/server/rest/services/BuildingPermits/FeatureServer/0/query',
];

module.exports = {
  name: municipalityConfig.name,
  slug: municipalityConfig.slug,
  portalUrl: PORTAL_URL,
  portalType: municipalityConfig.portalType,
  active: municipalityConfig.active,

  async scrape(options = {}) {
    const { dateFrom, dateTo, minValue = config.scraper.minProjectValue } = options;
    const dateRange = dateFrom && dateTo
      ? { from: dateFrom, to: dateTo }
      : utils.getDateRange();

    utils.log(`[N. Charleston] Starting ArcGIS scrape -- ${dateRange.from} to ${dateRange.to}`);

    const permits = [];
    const headers = {
      'User-Agent': utils.getRandomUserAgent(),
    };

    // Try ArcGIS REST API endpoints
    for (const apiPath of ARCGIS_API_PATHS) {
      try {
        const fromDate = new Date(dateRange.from).getTime();
        const toDate = new Date(dateRange.to).getTime();

        const resp = await axios.get(`${PORTAL_BASE}${apiPath}`, {
          params: {
            where: `IssueDate >= timestamp '${dateRange.from} 00:00:00' AND IssueDate <= timestamp '${dateRange.to} 23:59:59'`,
            outFields: '*',
            returnGeometry: false,
            f: 'json',
            resultRecordCount: 200,
            orderByFields: 'IssueDate DESC',
          },
          headers,
          timeout: 20000,
        });

        const features = resp.data?.features;
        if (features && features.length > 0) {
          utils.log(`[N. Charleston] ArcGIS hit at ${apiPath}: ${features.length} features`);

          for (const feature of features) {
            const attrs = feature.attributes || feature.properties || feature;
            const value = utils.parseDollarValue(
              attrs.ProjectValue || attrs.EstimatedValue || attrs.Value || attrs.PermitValue || attrs.Cost
            );
            if (value !== null && value < minValue) continue;

            // ArcGIS dates are often epoch milliseconds
            const parseArcDate = (val) => {
              if (!val) return null;
              if (typeof val === 'number') return utils.formatDate(new Date(val));
              return utils.formatDate(val);
            };

            const permit = utils.createPermitRecord({
              permit_number: attrs.PermitNumber || attrs.PermitNo || attrs.CaseNumber || attrs.OBJECTID,
              address: attrs.Address || attrs.FullAddress || attrs.SiteAddress || attrs.Location || '',
              municipality: municipalityConfig.name,
              builder_name: attrs.ContractorName || attrs.Contractor || attrs.Applicant,
              builder_company: attrs.ContractorCompany || attrs.CompanyName,
              builder_phone: attrs.ContractorPhone || attrs.Phone || attrs.ContactPhone || null,
              builder_email: attrs.ContractorEmail || attrs.Email || attrs.ContactEmail || null,
              owner_name: attrs.OwnerName || attrs.Owner || attrs.PropertyOwner,
              project_value: value,
              permit_type: attrs.PermitType || attrs.Type || attrs.WorkType,
              inspection_type: attrs.InspectionType || attrs.SubType,
              inspection_date: parseArcDate(attrs.InspectionDate || attrs.LastInspection),
              inspection_status: attrs.Status || attrs.PermitStatus || attrs.InspectionStatus,
              permit_issue_date: parseArcDate(attrs.IssueDate || attrs.IssuedDate),
              source_url: PORTAL_URL,
              raw_data: { source: 'arcgis-rest', ...attrs },
            });
            permits.push(permit);
          }
          break;
        }
      } catch (e) { continue; }
    }

    // Fallback: Puppeteer for the Customer Portal SPA
    if (permits.length === 0) {
      let browser;
      try {
        browser = await puppeteer.launch(config.scraper.puppeteer);
        const page = await browser.newPage();
        await page.setUserAgent(utils.getRandomUserAgent());
        page.setDefaultTimeout(config.scraper.pageTimeoutMs);

        // Discover APIs during navigation
        const discoveredApis = [];
        page.on('response', async (response) => {
          const url = response.url();
          const ct = response.headers()['content-type'] || '';
          if (ct.includes('json') && (url.includes('FeatureServer') || url.includes('api') || url.includes('permit'))) {
            discoveredApis.push(url);
          }
        });

        utils.log('[N. Charleston] Loading Customer Portal...');
        await page.goto(PORTAL_URL, { waitUntil: 'networkidle2', timeout: 45000 });
        await utils.delay(4000);

        // Try to navigate to permit search
        const searchSelectors = [
          'a[href*="permit"]', 'a:has-text("Permit")', 'a:has-text("Building")',
          'button:has-text("Search")', 'input[type="search"]',
        ];

        for (const sel of searchSelectors) {
          try {
            const el = await page.$(sel);
            if (el) {
              await el.click();
              await utils.delay(3000);
              break;
            }
          } catch (e) { /* next */ }
        }

        // Parse visible results
        const rows = await page.$$eval(
          'table tr, [class*="result"], [class*="record"], [class*="permit"], [class*="card"]',
          elements => elements.map(el => ({
            text: el.textContent.trim().substring(0, 500),
            link: el.querySelector('a')?.href || null,
          }))
        );

        for (const row of rows) {
          if (!row.text || row.text.length < 10) continue;
          const valueMatch = row.text.match(/\$[\d,]+\.?\d*/);
          const value = valueMatch ? utils.parseDollarValue(valueMatch[0]) : null;
          const permitMatch = row.text.match(/\b(BLD|BP|PMT|NC|BLDG)[-\s]?\d{2,}[-\s]?\d+\b/i);

          if (permitMatch || value) {
            permits.push(utils.createPermitRecord({
              permit_number: permitMatch ? permitMatch[0] : null,
              address: row.text.substring(0, 100),
              municipality: municipalityConfig.name,
              project_value: value,
              source_url: row.link || PORTAL_URL,
              raw_data: { source: 'arcgis-puppeteer' },
            }));
          }
        }

        if (discoveredApis.length > 0) {
          utils.log(`[N. Charleston] Discovered ${discoveredApis.length} APIs for future use`);
          discoveredApis.slice(0, 5).forEach(u => utils.log(`  -> ${u}`));
        }

        if (permits.length === 0) {
          await utils.saveScreenshot(page, 'north-charleston-results');
        }
      } catch (error) {
        utils.log(`[N. Charleston] Puppeteer error: ${error.message}`);
        throw error;
      } finally {
        if (browser) await browser.close();
      }
    }

    utils.log(`[N. Charleston] Scrape complete -- ${permits.length} permits found`);
    return permits;
  },
};
