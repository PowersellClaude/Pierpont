// City of Charleston — EnerGov Self-Service Portal Scraper
// Portal: https://egcss.charleston-sc.gov/energov_prod/selfservice#/search
//
// STRAPPING-ONLY scraper — direct API approach with tenant headers:
//   1. Navigate to EnerGov to establish session cookies
//   2. POST directly to /api/energov/search/search with tenant headers + proper body
//   3. For each result, call contacts API + inspection detail API for contact info

const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');
const config = require('../../config');
const utils = require('../utils');

const SEEN_FILE = path.join(__dirname, '..', '..', 'seen_permits.json');

const BASE_URL = 'https://egcss.charleston-sc.gov/EnerGov_Prod/selfservice';
const SEARCH_API = `${BASE_URL}/api/energov/search/search`;
const CONTACTS_API = `${BASE_URL}/api/energov/entity/contacts/search/search`;
const PERMIT_DETAIL_API = `${BASE_URL}/api/energov/permits/permitdetail`;
const PORTAL_URL = `${BASE_URL}#/search`;
const DETAIL_URL = `${BASE_URL}#/inspectionDetail/inspection`;
const PERMIT_DETAIL_URL = `${BASE_URL}#/permitDetail/permit`;

// EnerGov tenant headers (required for all API calls)
const TENANT_HEADERS = {
  'Content-Type': 'application/json;charset=UTF-8',
  'Accept': 'application/json, text/plain, */*',
  'tenantid': '1',
  'tenantname': 'CharlestonSC',
  'tyler-tenant-culture': 'en-US',
  'tyler-tenanturl': 'CharlestonSC',
};

// EnerGov GUIDs from the portal's Advanced search dropdowns
const STRAPPING_TYPES = [
  { name: 'Residential Building Strapping', id: '2a1af85e-dcdc-4a9e-b17a-e95e3d92d918' },
];

const PASSED_STATUS_ID = 'c0351663-6112-4181-a943-cf58f67c6c9d'; // "Passed - Permit"

const emptyCriteria = (pn, ps) => ({
  PageNumber: pn || 0, PageSize: ps || 0, SortBy: null, SortAscending: false,
});

// Build the full POST body for inspection search (last 30 days)
function buildSearchBody(typeId, statusId, pageNumber = 1, pageSize = 100) {
  const now = new Date();
  const from = new Date(now);
  from.setDate(from.getDate() - 30);
  const scheduleDateFrom = from.toISOString();
  const scheduleDateTo = now.toISOString();

  return {
    Keyword: '',
    ExactMatch: false,
    SearchModule: 4,
    FilterModule: 4,
    SearchMainAddress: false,
    PlanCriteria: { ...emptyCriteria() },
    PermitCriteria: { ...emptyCriteria() },
    InspectionCriteria: {
      Keyword: null, ExactMatch: false, Complete: null,
      InspectionNumber: null, InspectionTypeId: typeId, InspectionStatusId: statusId,
      RequestDateFrom: null, RequestDateTo: null,
      ScheduleDateFrom: scheduleDateFrom, ScheduleDateTo: scheduleDateTo,
      Address: null, SearchMainAddress: false, ContactId: null,
      TypeId: [], WorkClassIds: [], ParcelNumber: null,
      DisplayCodeInspections: false, ExcludeCases: [], ExcludeFilterModules: [],
      HiddenInspectionTypeIDs: null,
      PageNumber: pageNumber, PageSize: pageSize,
      SortBy: 'ScheduledDate', SortAscending: false,
    },
    CodeCaseCriteria: { ...emptyCriteria() },
    RequestCriteria: { ...emptyCriteria() },
    BusinessLicenseCriteria: emptyCriteria(),
    ProfessionalLicenseCriteria: emptyCriteria(),
    LicenseCriteria: emptyCriteria(),
    ProjectCriteria: emptyCriteria(),
    ExcludeCases: null, HiddenInspectionTypeIDs: null,
    PageNumber: 0, PageSize: 0, SortBy: 'ScheduledDate', SortAscending: false,
  };
}

// Build a POST body for searching permits by address (uses Keyword search like the portal UI)
function buildPermitSearchBody(address) {
  // Mirrors the portal URL: ?m=1&fm=1&ps=10&pn=1&em=true&st=<address>
  return {
    Keyword: address,
    ExactMatch: true,
    SearchModule: 1,
    FilterModule: 1,
    SearchMainAddress: false,
    PlanCriteria: { PageNumber: 1, PageSize: 100, SortBy: null, SortAscending: false },
    PermitCriteria: {
      PermitNumber: null, PermitTypeId: null, PermitWorkclassId: null, PermitStatusId: null,
      ProjectName: null, IssueDateFrom: null, IssueDateTo: null,
      Address: null, Description: null,
      ExpireDateFrom: null, ExpireDateTo: null, FinalDateFrom: null, FinalDateTo: null,
      ApplyDateFrom: null, ApplyDateTo: null,
      SearchMainAddress: false, ContactId: null, TypeId: null, WorkClassIds: null,
      ParcelNumber: null, ExcludeCases: null, EnableDescriptionSearch: false,
      PageNumber: 1, PageSize: 100, SortBy: 'IssueDate', SortAscending: false,
    },
    InspectionCriteria: { PageNumber: 1, PageSize: 100, SortBy: null, SortAscending: false },
    CodeCaseCriteria: { PageNumber: 1, PageSize: 100, SortBy: null, SortAscending: false },
    RequestCriteria: { PageNumber: 1, PageSize: 100, SortBy: null, SortAscending: false },
    BusinessLicenseCriteria: { PageNumber: 1, PageSize: 100, SortBy: null, SortAscending: false },
    ProfessionalLicenseCriteria: { PageNumber: 1, PageSize: 100, SortBy: null, SortAscending: false },
    LicenseCriteria: { PageNumber: 1, PageSize: 100, SortBy: null, SortAscending: false },
    ProjectCriteria: { PageNumber: 1, PageSize: 100, SortBy: null, SortAscending: false },
    ExcludeCases: null, HiddenInspectionTypeIDs: null,
    PageNumber: 1, PageSize: 100, SortBy: 'IssueDate', SortAscending: false,
  };
}

function loadSeenPermits() {
  try {
    if (fs.existsSync(SEEN_FILE)) {
      return new Set(JSON.parse(fs.readFileSync(SEEN_FILE, 'utf8')));
    }
  } catch (err) {
    utils.log(`[Charleston] Warning: could not read ${SEEN_FILE}: ${err.message}`);
  }
  return new Set();
}

function saveSeenPermits(seenSet) {
  try {
    fs.writeFileSync(SEEN_FILE, JSON.stringify([...seenSet], null, 2));
  } catch (err) {
    utils.log(`[Charleston] Warning: could not write ${SEEN_FILE}: ${err.message}`);
  }
}

function permitKey(permit) {
  if (permit.permit_number) return permit.permit_number;
  return `${permit.address || ''}|${permit.permit_type || ''}|${permit.inspection_date || ''}`;
}

module.exports = {
  name: config.municipalities.charleston.name,
  slug: config.municipalities.charleston.slug,
  portalUrl: PORTAL_URL,
  portalType: 'energov',
  active: true,

  async scrape(options = {}) {
    utils.log('[Charleston] Starting strapping-only scrape...');

    const seenPermits = loadSeenPermits();
    utils.log(`[Charleston] Loaded ${seenPermits.size} previously seen permits`);

    let browser;
    const allPermits = new Map();
    let caughtUp = false;

    try {
      browser = await puppeteer.launch(config.scraper.puppeteer);
      const page = await browser.newPage();
      await page.setUserAgent(utils.getRandomUserAgent());
      page.setDefaultTimeout(config.scraper.pageTimeoutMs);

      // Navigate to establish session cookies
      utils.log('[Charleston] Loading portal to establish session...');
      await page.goto(PORTAL_URL, { waitUntil: 'networkidle2', timeout: 45000 });
      await new Promise(r => setTimeout(r, 3000));

      // Search for each strapping type
      for (const strappingType of STRAPPING_TYPES) {
        if (caughtUp) break;

        try {
          utils.log(`[Charleston] Searching: ${strappingType.name} + Passed - Permit`);
          const permits = await this.apiSearch(page, strappingType.id, PASSED_STATUS_ID, seenPermits);

          let newCount = 0;
          for (const p of permits) {
            const key = permitKey(p);
            if (seenPermits.has(key)) {
              utils.log(`[Charleston] Hit known permit ${key} — caught up, stopping`);
              caughtUp = true;
              break;
            }
            if (key && !allPermits.has(key)) {
              allPermits.set(key, p);
              newCount++;
            }
          }

          utils.log(`[Charleston] ${strappingType.name}: ${permits.length} results, ${newCount} new (${allPermits.size} total)`);
        } catch (err) {
          utils.log(`[Charleston] ${strappingType.name} failed: ${err.message}`);
        }

        await new Promise(r => setTimeout(r, 1500));
      }

      // Enrich with contact data via direct API calls (much faster than page loads)
      if (allPermits.size > 0) {
        const sortedEntries = Array.from(allPermits.entries())
          .sort((a, b) => (b[1].inspection_date || '').localeCompare(a[1].inspection_date || ''));

        const DETAIL_LIMIT = options.testLimit || 100;
        const toEnrich = sortedEntries.slice(0, DETAIL_LIMIT);
        utils.log(`[Charleston] Enriching ${toEnrich.length} most recent permits with contact data (of ${allPermits.size} total)...`);

        let enrichedCount = 0;
        for (const [key, permit] of toEnrich) {
          try {
            const enriched = await this.enrichWithContactData(page, permit);
            if (enriched) {
              allPermits.set(key, enriched);
              enrichedCount++;
            }
          } catch (err) {
            utils.log(`[Charleston] Enrich error for ${key}: ${err.message}`);
          }
          await new Promise(r => setTimeout(r, 500));
        }
        utils.log(`[Charleston] Enriched ${enrichedCount}/${toEnrich.length} with contact data`);
      }

      // Mark all new permits as seen
      for (const [key] of allPermits) {
        seenPermits.add(key);
      }
      saveSeenPermits(seenPermits);
      utils.log(`[Charleston] Saved ${seenPermits.size} total seen permits`);

      await page.close();
      utils.log(`[Charleston] Scrape complete — ${allPermits.size} new permits${caughtUp ? ' (caught up with previous run)' : ''}`);

    } catch (error) {
      utils.log(`[Charleston] Fatal error: ${error.message}`);
      throw error;
    } finally {
      if (browser) await browser.close();
    }

    return Array.from(allPermits.values());
  },

  // Direct API call with tenant headers — bypasses Angular's PageSize=10 default
  // Pages through results until fewer than pageSize returned OR a seen permit is hit
  async apiSearch(page, typeId, statusId, seenPermits) {
    const permits = [];
    let pageNumber = 1;
    const pageSize = 100;
    let hitSeen = false;

    do {
      const body = buildSearchBody(typeId, statusId, pageNumber, pageSize);

      const result = await page.evaluate(async (url, body, headers) => {
        try {
          const resp = await fetch(url, {
            method: 'POST',
            headers: headers,
            body: JSON.stringify(body),
          });
          return await resp.json();
        } catch (e) {
          return { error: e.message };
        }
      }, SEARCH_API, body, TENANT_HEADERS);

      if (result.error) {
        utils.log(`[Charleston] API error: ${result.error}`);
        break;
      }

      if (!result.Success) {
        utils.log(`[Charleston] API failed: ${result.ErrorMessage || 'Unknown error'}`);
        break;
      }

      if (!result.Result || !result.Result.EntityResults || result.Result.EntityResults.length === 0) {
        if (pageNumber === 1) utils.log(`[Charleston] API returned no results`);
        break;
      }

      const entities = result.Result.EntityResults;
      const totalResults = result.Result.TotalResults || 0;

      utils.log(`[Charleston] Page ${pageNumber}: ${entities.length} entities${totalResults ? ` (${totalResults} total)` : ''}`);

      for (const entity of entities) {
        const permit = this.mapEntity(entity);
        if (!permit) continue;

        // Stop pagination early if we hit a previously seen permit
        if (seenPermits && seenPermits.has(permitKey(permit))) {
          utils.log(`[Charleston] Hit seen permit ${permitKey(permit)} on page ${pageNumber} — stopping pagination`);
          hitSeen = true;
          break;
        }

        permits.push(permit);
      }

      if (hitSeen) break;

      // Stop if we got fewer than a full page (no more results)
      if (entities.length < pageSize) break;
      pageNumber++;

      await new Promise(r => setTimeout(r, 1000));
    } while (pageNumber <= 10);

    utils.log(`[Charleston] Total: ${permits.length} permits across ${pageNumber} page(s)${hitSeen ? ' (stopped at seen permit)' : ''}`);
    return permits;
  },

  // Search the permit portal by address to find the residential building permit
  // Returns enrichment data (value, builder, contacts) or null
  async searchBuildingPermitByAddress(page, address, inspectionDate) {
    utils.log(`[Charleston] Permit keyword search: "${address}"`);
    const body = buildPermitSearchBody(address);

    const result = await page.evaluate(async (url, body, headers) => {
      try {
        const resp = await fetch(url, { method: 'POST', headers, body: JSON.stringify(body) });
        return await resp.json();
      } catch (e) { return { error: e.message }; }
    }, SEARCH_API, body, TENANT_HEADERS);

    if (result.error) {
      utils.log(`[Charleston] Permit search API error: ${result.error}`);
      return null;
    }
    if (!result.Success) {
      utils.log(`[Charleston] Permit search API failed: ${result.ErrorMessage || 'unknown'}`);
      return null;
    }
    if (!result.Result?.EntityResults?.length) {
      utils.log(`[Charleston] Permit search returned 0 results for "${address}"`);
      return null;
    }

    utils.log(`[Charleston] Permit search returned ${result.Result.EntityResults.length} results for "${address}"`);

    const entities = result.Result.EntityResults;

    // Match actual permits (not inspections) with "Building" in the type
    // Permit CaseType = "Permit - Building", workclass = "Residential New", etc.
    // Inspection CaseType = "Residential Building Strapping" (no "Permit" prefix)
    const buildingPermits = entities.filter(e => {
      const caseType = (e.CaseType || '').toLowerCase();
      const caseNum = (e.CaseNumber || '').toUpperCase();
      // Must be an actual permit (CaseType starts with "permit") not an inspection
      if (!caseType.includes('permit')) return false;
      // Must be a building permit
      if (!caseType.includes('building')) return false;
      // Skip if case number looks like an inspection
      if (caseNum.startsWith('INS-') || caseNum.startsWith('INSP-')) return false;
      return true;
    });

    if (buildingPermits.length === 0) {
      const allTypes = entities.map(e => `${e.CaseType}/${e.CaseNumber}`).slice(0, 8);
      utils.log(`[Charleston] No building PERMITS at ${address} (${entities.length} results, types: ${allTypes.join(', ')})`);
      return null;
    }

    utils.log(`[Charleston] Found ${buildingPermits.length} building permit(s) at ${address}: ${buildingPermits.map(p => `${p.CaseType}/${p.CaseWorkclass} [${p.CaseNumber}] val=${p.ProjectValue||p.EstimatedValue||'?'}`).join(', ')}`);

    // Pick the building permit with the closest date to our inspection
    let best;
    if (inspectionDate) {
      const inspTime = new Date(inspectionDate).getTime();
      best = buildingPermits.reduce((a, b) => {
        const dateA = parseDate(a.IssueDate || a.ApplyDate || a.OpenedDate);
        const dateB = parseDate(b.IssueDate || b.ApplyDate || b.OpenedDate);
        const diffA = dateA ? Math.abs(new Date(dateA).getTime() - inspTime) : Infinity;
        const diffB = dateB ? Math.abs(new Date(dateB).getTime() - inspTime) : Infinity;
        return diffB < diffA ? b : a;
      });
    } else {
      best = buildingPermits.reduce((a, b) => {
        const valA = parseFloat(a.ProjectValue || a.EstimatedValue || 0);
        const valB = parseFloat(b.ProjectValue || b.EstimatedValue || 0);
        return valB > valA ? b : a;
      });
    }

    const caseId = best.CaseId;
    if (!caseId) return null;

    // Get full permit detail via POST permitdetail (has the Value field)
    const permitResult = await page.evaluate(async (url, body, headers) => {
      try {
        const resp = await fetch(url, { method: 'POST', headers, body: JSON.stringify(body) });
        return await resp.json();
      } catch (e) { return { error: e.message }; }
    }, PERMIT_DETAIL_API, { EntityId: caseId, ModuleId: 1 }, TENANT_HEADERS);

    const p = (permitResult.Success && permitResult.Result) ? permitResult.Result : {};

    const rawValue = p.Value || best.ProjectValue || best.EstimatedValue || null;
    utils.log(`[Charleston] Permit ${best.CaseNumber}: Value=${p.Value}, SquareFeet=${p.SquareFeet}`);

    const permitData = {
      project_value: utils.parseDollarValue(rawValue),
      permit_type: p.WorkClassName || p.PermitType || best.CaseWorkclass || best.CaseType || null,
      permit_description: p.Description || null,
      permit_number: p.PermitNumber || best.CaseNumber || null,
      permit_issue_date: p.IssueDate || p.ApplyDate || null,
    };

    // Get contacts from the building permit (ModuleId: 1 = permits)
    const contactsResult = await page.evaluate(async (url, body, headers) => {
      try {
        const resp = await fetch(url, { method: 'POST', headers, body: JSON.stringify(body) });
        return await resp.json();
      } catch (e) { return { error: e.message }; }
    }, CONTACTS_API, {
      EntityId: caseId,
      ModuleId: 1,
      PageNumber: 1,
      PageSize: 100,
      SortBy: 'Type',
      SortAscending: true,
    }, TENANT_HEADERS);

    const contacts = Array.isArray(contactsResult?.Result) ? contactsResult.Result : [];
    const allContacts = [];
    let applicant = null;
    let applicantCompany = null;
    let contractors = [];
    let owner = null;

    for (const contact of contacts) {
      const type = (contact.ContactTypeName || '').toLowerCase();
      const fullName = [contact.FirstName, contact.LastName].filter(Boolean).join(' ');
      const company = contact.GlobalEntityName || null;

      allContacts.push({ type: contact.ContactTypeName, name: fullName || null, company });

      if (type.includes('applicant')) {
        if (!applicant) applicant = fullName || null;
        if (!applicantCompany && company) applicantCompany = company;
      } else if (type.includes('contractor') || type.includes('builder')) {
        contractors.push({ name: fullName || null, company });
      } else if (type.includes('owner')) {
        if (!owner) owner = fullName || null;
      }
    }

    permitData.builder_name = applicant || (contractors[0]?.name) || null;
    permitData.builder_company = applicantCompany || (contractors[0]?.company) || null;
    permitData.owner_name = owner || null;
    permitData._allContacts = allContacts;
    permitData._contractors = contractors;
    permitData._permitUrl = `${PERMIT_DETAIL_URL}/${caseId}`;

    return permitData;
  },

  // Enrich a permit with contact data via address-based building permit search
  async enrichWithContactData(page, permit) {
    const address = permit.address;
    if (!address) return null;

    const enriched = { ...permit };
    const detailData = {};
    let dataSource = 'inspection_contacts';

    // Preferred path: search for building permit by address
    try {
      const permitData = await this.searchBuildingPermitByAddress(page, address, permit.inspection_date);
      if (permitData) {
        dataSource = 'building_permit_search';
        for (const key of ['project_value', 'builder_name', 'builder_company', 'owner_name', 'permit_type', 'permit_issue_date']) {
          if (permitData[key] != null && permitData[key] !== '') enriched[key] = permitData[key];
        }
        detailData._permitData = permitData;
        detailData._allContacts = permitData._allContacts;
        detailData._contractors = permitData._contractors;
        detailData._linkedPermit = permitData.permit_number;
        detailData._linkedPermitUrl = permitData._permitUrl;

        const val = enriched.project_value;
        utils.log(`[Charleston] Found building permit at ${address} — ${permitData.permit_number} val=$${val ? Number(val).toLocaleString() : '?'}, builder=${enriched.builder_name || '?'} @ ${enriched.builder_company || '?'}`);
      }
    } catch (err) {
      utils.log(`[Charleston] Building permit search failed for ${address}: ${err.message}`);
    }

    // Fallback: use inspection contacts if address search found nothing
    if (dataSource === 'inspection_contacts') {
      const rawData = typeof permit.raw_data === 'string' ? JSON.parse(permit.raw_data) : permit.raw_data;
      const caseId = rawData?.CaseId || rawData?.Id || rawData?.InspectionId;
      if (!caseId) return null;

      const contactsResult = await page.evaluate(async (url, body, headers) => {
        try {
          const resp = await fetch(url, { method: 'POST', headers, body: JSON.stringify(body) });
          return await resp.json();
        } catch (e) { return { error: e.message }; }
      }, CONTACTS_API, {
        EntityId: caseId,
        ModuleId: 7,
        PageNumber: 1,
        PageSize: 100,
        SortBy: 'Type',
        SortAscending: true,
      }, TENANT_HEADERS);

      const contacts = Array.isArray(contactsResult?.Result) ? contactsResult.Result : [];
      const allContacts = [];
      let applicant = null;
      let applicantCompany = null;
      let contractors = [];
      let owner = null;

      for (const contact of contacts) {
        const type = (contact.ContactTypeName || '').toLowerCase();
        const fullName = [contact.FirstName, contact.LastName].filter(Boolean).join(' ');
        const company = contact.GlobalEntityName || null;

        allContacts.push({ type: contact.ContactTypeName, name: fullName || null, company });

        if (type.includes('applicant')) {
          if (!applicant) applicant = fullName || null;
          if (!applicantCompany && company) applicantCompany = company;
        } else if (type.includes('contractor') || type.includes('builder')) {
          contractors.push({ name: fullName || null, company });
        } else if (type.includes('owner')) {
          if (!owner) owner = fullName || null;
        }
      }

      detailData.builder_name = applicant || (contractors[0]?.name) || null;
      detailData.builder_company = applicantCompany || (contractors[0]?.company) || null;
      detailData.owner_name = owner || null;
      detailData._allContacts = allContacts;
      detailData._contractors = contractors;

      for (const key of ['builder_name', 'builder_company', 'owner_name', 'project_value']) {
        if (detailData[key]) enriched[key] = detailData[key];
      }

      const hasContact = detailData.builder_name || detailData.builder_company || detailData.owner_name;
      if (hasContact) {
        utils.log(`[Charleston] ${permit.permit_number}: builder=${detailData.builder_name || '?'} @ ${detailData.builder_company || '?'}, owner=${detailData.owner_name || '?'}`);
      }
    }

    // Store enrichment data in raw_data
    const existingRaw = typeof enriched.raw_data === 'string' ? JSON.parse(enriched.raw_data) : (enriched.raw_data || {});
    existingRaw._detailData = detailData;
    existingRaw._linkedPermit = detailData._linkedPermit || null;
    existingRaw._linkedPermitUrl = detailData._linkedPermitUrl || null;
    existingRaw._allContacts = detailData._allContacts || [];
    existingRaw._contractors = detailData._contractors || [];
    existingRaw._dataSource = dataSource;
    enriched.raw_data = existingRaw;

    return enriched;
  },

  mapEntity(entity) {
    if (!entity || typeof entity !== 'object') return null;

    let address = '';
    if (entity.Address && typeof entity.Address === 'object') {
      address = entity.Address.FullAddress || entity.Address.AddressLine1 || '';
    } else if (entity.AddressDisplay) {
      address = entity.AddressDisplay;
    } else if (typeof entity.MainAddress === 'string') {
      address = entity.MainAddress;
    }

    const caseNumber = entity.CaseNumber || entity.InspectionNumber || entity.PermitNumber || null;
    if (!caseNumber && !address) return null;

    const parseDate = (val) => {
      if (!val) return null;
      const msMatch = String(val).match(/\/Date\((\d+)\)\//);
      if (msMatch) return utils.formatDate(new Date(parseInt(msMatch[1])));
      return utils.formatDate(val);
    };

    return utils.createPermitRecord({
      permit_number: caseNumber,
      address: address,
      municipality: config.municipalities.charleston.name,
      builder_name: entity.ContractorName ||
        (entity.HolderFirstName ? [entity.HolderFirstName, entity.HolderLastName].filter(Boolean).join(' ') : null),
      builder_company: entity.CompanyName || entity.HolderCompanyName || null,
      builder_phone: entity.ContractorPhone || null,
      builder_email: entity.ContractorEmail || null,
      applicant_name: entity.ApplicantName || null,
      owner_name: entity.OwnerName || null,
      project_value: entity.ProjectValue || entity.EstimatedValue || null,
      permit_type: entity.CaseType || entity.CaseWorkclass || null,
      inspection_type: entity.CaseType || null,
      inspection_date: parseDate(entity.ScheduleDate || entity.RequestDate || entity.IssueDate),
      inspection_status: entity.CaseStatus || entity.Status || null,
      permit_issue_date: parseDate(entity.IssueDate || entity.ApplyDate),
      source_url: entity.CaseId ? `${DETAIL_URL}/${entity.CaseId}` : PORTAL_URL,
      raw_data: entity,
    });
  },
};
