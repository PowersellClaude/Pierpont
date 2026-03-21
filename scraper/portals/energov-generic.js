// EnerGov Generic Factory Scraper
// Reusable EnerGov self-service portal scraper parameterized by tenant config.
// Covers: Berkeley County, Hilton Head, Bluffton
// Modeled after the proven Charleston EnerGov approach (API-first with tenant headers).

const puppeteer = require('puppeteer');
const config = require('../../config');
const utils = require('../utils');

// EnerGov tenant configurations per municipality
const TENANTS = {
  berkeleyCounty: {
    municipalityKey: 'berkeleyCounty',
    baseUrl: 'https://build.berkeleycountysc.gov',
    selfServicePath: '/EnerGov_Prod/selfservice',
    tenantHeaders: {
      'Content-Type': 'application/json;charset=UTF-8',
      'Accept': 'application/json, text/plain, */*',
      'tenantid': '1',
      'tenantname': 'BerkeleyCountySC',
      'tyler-tenant-culture': 'en-US',
      'tyler-tenanturl': 'BerkeleyCountySC',
    },
  },
  hiltonHead: {
    municipalityKey: 'hiltonHead',
    baseUrl: 'https://service.hiltonheadislandsc.gov',
    selfServicePath: '/CitizenAccess',
    tenantHeaders: {
      'Content-Type': 'application/json;charset=UTF-8',
      'Accept': 'application/json, text/plain, */*',
      'tenantid': '1',
      'tenantname': 'HiltonHeadIslandSC',
      'tyler-tenant-culture': 'en-US',
      'tyler-tenanturl': 'HiltonHeadIslandSC',
    },
  },
  bluffton: {
    municipalityKey: 'bluffton',
    baseUrl: 'https://townofblufftonsc-energovweb.tylerhost.net',
    selfServicePath: '/apps/selfservice',
    tenantHeaders: {
      'Content-Type': 'application/json;charset=UTF-8',
      'Accept': 'application/json, text/plain, */*',
      'tenantid': '1',
      'tenantname': 'TownOfBlufftonSC',
      'tyler-tenant-culture': 'en-US',
      'tyler-tenanturl': 'TownOfBlufftonSC',
    },
  },
};

// Build the EnerGov search body (same structure as Charleston)
function buildSearchBody(pageNumber = 1, pageSize = 100) {
  const emptyCriteria = () => ({ PageNumber: 0, PageSize: 0, SortBy: null, SortAscending: false });

  return {
    Keyword: '',
    ExactMatch: false,
    SearchModule: 1, // Permits module
    FilterModule: 1,
    SearchMainAddress: false,
    PlanCriteria: emptyCriteria(),
    PermitCriteria: {
      PermitNumber: null, PermitTypeId: null, PermitWorkclassId: null, PermitStatusId: null,
      ProjectName: null, IssueDateFrom: null, IssueDateTo: null,
      Address: null, Description: null,
      ExpireDateFrom: null, ExpireDateTo: null, FinalDateFrom: null, FinalDateTo: null,
      ApplyDateFrom: null, ApplyDateTo: null,
      SearchMainAddress: false, ContactId: null, TypeId: null, WorkClassIds: null,
      ParcelNumber: null, ExcludeCases: null, EnableDescriptionSearch: false,
      PageNumber: pageNumber,
      PageSize: pageSize,
      SortBy: 'IssueDate',
      SortAscending: false,
    },
    InspectionCriteria: emptyCriteria(),
    CodeCaseCriteria: emptyCriteria(),
    RequestCriteria: emptyCriteria(),
    BusinessLicenseCriteria: emptyCriteria(),
    ProfessionalLicenseCriteria: emptyCriteria(),
    LicenseCriteria: emptyCriteria(),
    ProjectCriteria: emptyCriteria(),
    ExcludeCases: null,
    HiddenInspectionTypeIDs: null,
    PageNumber: 0,
    PageSize: 0,
    SortBy: 'IssueDate',
    SortAscending: false,
  };
}

function createEnerGovScraper(tenantKey) {
  const tenant = TENANTS[tenantKey];
  if (!tenant) throw new Error(`Unknown EnerGov tenant: ${tenantKey}`);

  const muniConfig = config.municipalities[tenant.municipalityKey];
  if (!muniConfig) throw new Error(`Municipality "${tenant.municipalityKey}" not found in config`);

  const portalUrl = `${tenant.baseUrl}${tenant.selfServicePath}#/search`;
  const searchApi = `${tenant.baseUrl}${tenant.selfServicePath}/api/energov/search/search`;

  return {
    name: muniConfig.name,
    slug: muniConfig.slug,
    portalUrl,
    portalType: 'energov',
    active: muniConfig.active,

    async scrape(options = {}) {
      utils.log(`[${muniConfig.name}] Starting EnerGov scrape...`);

      let browser;
      const allPermits = new Map();

      try {
        browser = await puppeteer.launch(config.scraper.puppeteer);
        const page = await browser.newPage();
        await page.setUserAgent(utils.getRandomUserAgent());
        page.setDefaultTimeout(config.scraper.pageTimeoutMs);

        // Navigate to establish session cookies
        utils.log(`[${muniConfig.name}] Loading portal to establish session...`);
        await page.goto(portalUrl, { waitUntil: 'networkidle2', timeout: 45000 });
        await new Promise(r => setTimeout(r, 3000));

        // Search for permits via API
        let pageNumber = 1;
        const pageSize = 100;

        do {
          const body = buildSearchBody(pageNumber, pageSize);

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
          }, searchApi, body, tenant.tenantHeaders);

          if (result.error) {
            utils.log(`[${muniConfig.name}] API error: ${result.error}`);
            break;
          }

          if (!result.Success) {
            utils.log(`[${muniConfig.name}] API returned unsuccessful: ${result.ErrorMessage || 'Unknown'}`);
            break;
          }

          const entities = result.Result?.EntityResults || [];
          if (entities.length === 0) {
            if (pageNumber === 1) utils.log(`[${muniConfig.name}] No results from API`);
            break;
          }

          utils.log(`[${muniConfig.name}] Page ${pageNumber}: ${entities.length} entities`);

          for (const entity of entities) {
            const permit = mapEnerGovEntity(entity, muniConfig.name, portalUrl);
            if (permit && permit.permit_number && !allPermits.has(permit.permit_number)) {
              allPermits.set(permit.permit_number, permit);
            }
          }

          if (entities.length < pageSize) break;
          pageNumber++;
          await new Promise(r => setTimeout(r, 1000));
        } while (pageNumber <= 5); // cap at 500 results

        await page.close();
        utils.log(`[${muniConfig.name}] Scrape complete -- ${allPermits.size} permits`);

      } catch (error) {
        utils.log(`[${muniConfig.name}] Error: ${error.message}`);
        throw error;
      } finally {
        if (browser) await browser.close();
      }

      return Array.from(allPermits.values());
    },
  };
}

// Map an EnerGov entity to a permit record
function mapEnerGovEntity(entity, municipalityName, portalUrl) {
  if (!entity || typeof entity !== 'object') return null;

  let address = '';
  if (entity.Address && typeof entity.Address === 'object') {
    address = entity.Address.FullAddress || entity.Address.AddressLine1 || '';
  } else if (entity.AddressDisplay) {
    address = entity.AddressDisplay;
  } else if (typeof entity.MainAddress === 'string') {
    address = entity.MainAddress;
  }

  const caseNumber = entity.CaseNumber || entity.PermitNumber || entity.InspectionNumber || null;
  if (!caseNumber && !address) return null;

  const parseDate = (val) => {
    if (!val) return null;
    const msMatch = String(val).match(/\/Date\((\d+)\)\//);
    if (msMatch) return utils.formatDate(new Date(parseInt(msMatch[1])));
    return utils.formatDate(val);
  };

  return utils.createPermitRecord({
    permit_number: caseNumber,
    address,
    municipality: municipalityName,
    builder_name: entity.ContractorName ||
      (entity.HolderFirstName ? [entity.HolderFirstName, entity.HolderLastName].filter(Boolean).join(' ') : null),
    builder_company: entity.CompanyName || entity.HolderCompanyName || null,
    builder_phone: entity.ContractorPhone || entity.PhoneNumber || entity.Phone || null,
    builder_email: entity.ContractorEmail || entity.EmailAddress || entity.Email || null,
    applicant_name: entity.ApplicantName || null,
    owner_name: entity.OwnerName || null,
    project_value: entity.ProjectValue || entity.EstimatedValue || null,
    permit_type: entity.CaseType || entity.CaseWorkclass || null,
    inspection_type: entity.CaseType || null,
    inspection_date: parseDate(entity.ScheduleDate || entity.RequestDate || entity.IssueDate),
    inspection_status: entity.CaseStatus || entity.Status || null,
    permit_issue_date: parseDate(entity.IssueDate || entity.ApplyDate),
    source_url: entity.CaseId ? `${portalUrl.replace('#/search', '')}#/permit/${entity.CaseId}` : portalUrl,
    raw_data: { source: 'energov-generic', ...entity },
  });
}

module.exports = {
  createEnerGovScraper,
  berkeleyCounty: createEnerGovScraper('berkeleyCounty'),
  hiltonHead: createEnerGovScraper('hiltonHead'),
  bluffton: createEnerGovScraper('bluffton'),
};
