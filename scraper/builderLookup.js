// Builder Website Lookup — searches for builder company websites and scrapes contact info
const axios = require('axios');
const cheerio = require('cheerio');
const puppeteer = require('puppeteer');

const PHONE_RE = /(?:\+1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/g;
const EMAIL_RE = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;

// Directories/aggregators to skip when picking company website
const SKIP_DOMAINS = [
  'yelp.com', 'bbb.org', 'facebook.com', 'instagram.com', 'twitter.com',
  'linkedin.com', 'yellowpages.com', 'angi.com', 'angieslist.com',
  'homeadvisor.com', 'thumbtack.com', 'houzz.com', 'buildzoom.com',
  'manta.com', 'mapquest.com', 'google.com', 'bing.com', 'youtube.com',
  'pinterest.com', 'nextdoor.com', 'porch.com', 'chamberofcommerce.com',
  'dnb.com', 'buzzfile.com', 'bloomberg.com', 'zoominfo.com',
  'tiktok.com', 'reddit.com', 'wikipedia.org', 'amazon.com',
  'duckduckgo.com', 'apple.com', 'x.com', 'bizapedia.com',
  'opencorporates.com', 'sec.gov', 'companieslist.co',
  'newhomesource.com', 'newhomeguide.com', 'zillow.com',
  'realtor.com', 'redfin.com', 'trulia.com',
];

/**
 * Search DuckDuckGo via Puppeteer for a builder company website.
 * Uses a real browser to avoid bot detection.
 */
async function findCompanyWebsite(companyName) {
  if (!companyName) return null;

  const query = `${companyName} South Carolina`;
  const searchUrl = `https://duckduckgo.com/?q=${encodeURIComponent(query)}`;

  let browser;
  try {
    browser = await puppeteer.launch({
      headless: 'new',
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
    });
    const page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
    await page.goto(searchUrl, { waitUntil: 'networkidle2', timeout: 20000 });

    // Wait for results to render
    await new Promise(r => setTimeout(r, 3000));

    // Extract all result links
    const links = await page.evaluate(() => {
      const results = [];
      document.querySelectorAll('article a[href^="http"], a[data-testid="result-title-a"]').forEach(a => {
        results.push(a.href);
      });
      // Fallback: any external links
      if (results.length === 0) {
        document.querySelectorAll('a[href^="http"]').forEach(a => {
          if (!a.href.includes('duckduckgo.com')) results.push(a.href);
        });
      }
      return [...new Set(results)];
    });

    await browser.close();
    browser = null;

    // Filter out aggregators and pick the first real company site
    for (const link of links) {
      try {
        const hostname = new URL(link).hostname.toLowerCase();
        const isSkipped = SKIP_DOMAINS.some(d => hostname === d || hostname.endsWith('.' + d));
        if (isSkipped) continue;
        if (hostname.includes('duckduckgo.')) continue;
        return link;
      } catch { continue; }
    }

    return null;
  } catch (err) {
    console.error(`[BuilderLookup] Search failed for "${companyName}":`, err.message);
    return null;
  } finally {
    if (browser) try { await browser.close(); } catch {}
  }
}

/**
 * Scrape a website for contact info (phone numbers and emails).
 * Checks the homepage and common contact page paths.
 */
async function scrapeContactInfo(websiteUrl) {
  if (!websiteUrl) return { phones: [], emails: [] };

  const allPhones = new Set();
  const allEmails = new Set();
  const baseUrl = new URL(websiteUrl).origin;

  const pagesToTry = [
    websiteUrl,
    `${baseUrl}/contact`,
    `${baseUrl}/contact-us`,
    `${baseUrl}/about`,
    `${baseUrl}/about-us`,
  ];

  for (const pageUrl of pagesToTry) {
    try {
      const { data } = await axios.get(pageUrl, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
          'Accept': 'text/html',
        },
        timeout: 10000,
        maxRedirects: 3,
        validateStatus: s => s < 400,
      });

      const $ = cheerio.load(data);

      // Remove scripts/styles to avoid false positives
      $('script, style, noscript').remove();
      const text = $('body').text();

      // Extract phones
      const phones = text.match(PHONE_RE) || [];
      phones.forEach(p => {
        const cleaned = p.replace(/[^\d]/g, '');
        // Must be 10 or 11 digits (US phone)
        if (cleaned.length === 10 || (cleaned.length === 11 && cleaned.startsWith('1'))) {
          allPhones.add(p.trim());
        }
      });

      // Also check href="tel:..." links
      $('a[href^="tel:"]').each((_, el) => {
        const tel = $(el).attr('href').replace('tel:', '').trim();
        const cleaned = tel.replace(/[^\d]/g, '');
        if (cleaned.length >= 10) allPhones.add(tel);
      });

      // Extract emails
      const emails = text.match(EMAIL_RE) || [];
      emails.forEach(e => {
        const lower = e.toLowerCase();
        // Skip obvious non-contact emails
        if (lower.includes('example.com') || lower.includes('sentry.io') ||
            lower.includes('wixpress') || lower.includes('.png') ||
            lower.includes('.jpg') || lower.includes('.svg')) return;
        allEmails.add(lower);
      });

      // Also check mailto: links
      $('a[href^="mailto:"]').each((_, el) => {
        const mail = $(el).attr('href').replace('mailto:', '').split('?')[0].trim().toLowerCase();
        if (mail && mail.includes('@')) allEmails.add(mail);
      });

      // If we found contact info, no need to check more pages
      if (allPhones.size > 0 && allEmails.size > 0) break;
    } catch {
      // Page doesn't exist or errored — try next
      continue;
    }
  }

  return {
    phones: [...allPhones],
    emails: [...allEmails],
  };
}

/**
 * Full builder lookup: search for company website, then scrape contact info.
 */
async function lookupBuilder(companyName) {
  console.log(`[BuilderLookup] Looking up "${companyName}"...`);

  const website = await findCompanyWebsite(companyName);
  if (!website) {
    console.log(`[BuilderLookup] No website found for "${companyName}"`);
    return { website: null, phone: null, email: null };
  }

  console.log(`[BuilderLookup] Found website: ${website}`);
  const { phones, emails } = await scrapeContactInfo(website);
  console.log(`[BuilderLookup] Found ${phones.length} phone(s), ${emails.length} email(s)`);

  return {
    website,
    phone: phones[0] || null,
    email: emails[0] || null,
    allPhones: phones,
    allEmails: emails,
  };
}

/**
 * Bulk lookup: find websites and scrape contact info for all permits missing it.
 * Deduplicates by company name so the same builder is only searched once.
 */
async function bulkLookupBuilders(db, statusCallback) {
  const permits = await db.getPermitsNeedingLookup();
  if (permits.length === 0) {
    console.log('[BuilderLookup] No permits need lookup');
    if (statusCallback) statusCallback({ status: 'completed', total: 0, processed: 0 });
    return { total: 0, found: 0, errors: 0 };
  }

  // Deduplicate by company name — look up each company only once
  const companyMap = new Map(); // companyName -> [permitIds]
  for (const p of permits) {
    const company = (p.builder_company || '').trim();
    if (!company) continue;
    if (!companyMap.has(company)) companyMap.set(company, []);
    companyMap.get(company).push(p);
  }

  const uniqueCompanies = [...companyMap.keys()];
  console.log(`[BuilderLookup] Bulk lookup: ${uniqueCompanies.length} unique companies across ${permits.length} permits`);

  let processed = 0;
  let found = 0;
  let errors = 0;

  for (const company of uniqueCompanies) {
    processed++;
    if (statusCallback) {
      statusCallback({ status: 'running', total: uniqueCompanies.length, processed, current: company });
    }

    try {
      const result = await lookupBuilder(company);
      const companyPermits = companyMap.get(company);

      for (const permit of companyPermits) {
        const updates = {};
        if (result.phone && !permit.builder_phone) updates.phone = result.phone;
        if (result.email && !permit.builder_email) updates.email = result.email;
        if (result.website) updates.website = result.website;

        if (Object.keys(updates).length > 0) {
          await db.updateBuilderContact(permit.id, updates);
        }
      }

      if (result.website) found++;
    } catch (err) {
      console.error(`[BuilderLookup] Error looking up "${company}":`, err.message);
      errors++;
    }

    // Rate limit between lookups to avoid getting blocked
    await new Promise(r => setTimeout(r, 2000));
  }

  console.log(`[BuilderLookup] Bulk lookup complete: ${found}/${uniqueCompanies.length} companies found, ${errors} errors`);
  if (statusCallback) statusCallback({ status: 'completed', total: uniqueCompanies.length, processed, found, errors });

  return { total: uniqueCompanies.length, found, errors };
}

/**
 * Placeholder for future skip-trace API integration.
 * Will accept a person name and company, return personal contact info.
 */
async function skipTraceLookup(name, company) {
  // TODO: Integrate skip-trace API here when API key is provided
  return null;
}

module.exports = { lookupBuilder, bulkLookupBuilders, findCompanyWebsite, scrapeContactInfo, skipTraceLookup };
