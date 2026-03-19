// Builder Website Lookup — searches for builder company websites and scrapes contact info
const axios = require('axios');
const cheerio = require('cheerio');
const puppeteer = require('puppeteer');
const config = require('../config');
const utils = require('./utils');
const builderCache = require('./builder-cache');
const buyerList = require('./buyer-list');
const llrLookup = require('./llr-lookup');

const PHONE_RE = /(?:\+1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/g;
const EMAIL_RE = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;

// Directories/aggregators to skip when picking company website
const SKIP_DOMAINS = [
  // Social media
  'facebook.com', 'instagram.com', 'twitter.com', 'x.com',
  'linkedin.com', 'youtube.com', 'pinterest.com', 'tiktok.com',
  'reddit.com', 'nextdoor.com',
  // Search engines / tech
  'google.com', 'bing.com', 'duckduckgo.com', 'apple.com', 'amazon.com',
  // Review / directory aggregators
  'yelp.com', 'bbb.org', 'yellowpages.com', 'angi.com', 'angieslist.com',
  'homeadvisor.com', 'thumbtack.com', 'houzz.com', 'buildzoom.com',
  'manta.com', 'mapquest.com', 'porch.com', 'chamberofcommerce.com',
  // Business data aggregators
  'dnb.com', 'buzzfile.com', 'bloomberg.com', 'zoominfo.com',
  'bizapedia.com', 'opencorporates.com', 'companieslist.co',
  'govtribe.com', 'allbiz.com', 'infobel.com', 'cylex.us.com',
  'dandb.com', 'corporationwiki.com', 'buzzfile.com', 'owler.com',
  'crunchbase.com', 'glassdoor.com', 'indeed.com', 'bizmappr.com',
  'findglocal.com', 'spoke.com', 'ripoffreport.com', 'trustpilot.com',
  'sitejabber.com', 'birdeye.com', 'g2.com',
  // Real estate aggregators
  'newhomesource.com', 'newhomeguide.com', 'zillow.com',
  'realtor.com', 'redfin.com', 'trulia.com', 'homes.com',
  'homesnap.com', 'movoto.com', 'apartments.com',
  // Government / legal
  'sec.gov', 'wikipedia.org',
];

// Junk email patterns to skip
const JUNK_EMAIL_PATTERNS = [
  'example.com', 'sentry.io', 'wixpress', 'wix.com', 'squarespace',
  'wordpress.com', 'w3.org', 'schema.org', 'googleapis.com', 'gstatic.com',
  'gravatar.com', 'cloudflare', '.png', '.jpg', '.svg', '.gif', '.webp',
  'noreply', 'no-reply', 'mailer-daemon', 'postmaster@', 'user@domain',
  'test@', 'admin@', 'webmaster@', 'hostmaster@', 'abuse@',
];

// Get Puppeteer launch options from config (respects PUPPETEER_EXECUTABLE_PATH on Railway)
function getPuppeteerLaunchOpts() {
  return {
    headless: 'new',
    ...(process.env.PUPPETEER_EXECUTABLE_PATH
      ? { executablePath: process.env.PUPPETEER_EXECUTABLE_PATH }
      : {}),
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
      '--disable-accelerated-2d-canvas',
      '--single-process',
    ],
  };
}

/**
 * Launch a shared browser instance for builder lookups.
 * Reuses one Chromium process to avoid OOM on Railway.
 */
async function launchBrowser() {
  const browser = await puppeteer.launch(getPuppeteerLaunchOpts());
  return browser;
}

function isValidPhone(p) {
  const cleaned = p.replace(/[^\d]/g, '');
  if (cleaned.length !== 10 && !(cleaned.length === 11 && cleaned.startsWith('1'))) return false;
  const d10 = cleaned.slice(-10);
  if (/^(\d)\1{9}$/.test(d10)) return false;
  if (d10.startsWith('000') || d10.startsWith('111') || d10.startsWith('555')) return false;
  return true;
}

function isValidEmail(e) {
  const lower = e.toLowerCase();
  if (JUNK_EMAIL_PATTERNS.some(p => lower.includes(p))) return false;
  if (lower.length > 50) return false;
  if (!/^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$/.test(lower)) return false;
  const local = lower.split('@')[0];
  const digitsInLocal = local.replace(/[^\d]/g, '');
  if (digitsInLocal.length >= 7) return false;
  return true;
}

/**
 * Extract phones and emails from parsed HTML.
 * Priority: tel:/mailto: links > footer/header/contact sections > visible body text.
 */
function extractContactFromHtml(html, $) {
  const telPhones = new Set();
  const mailtoEmails = new Set();
  const sectionPhones = new Set();
  const sectionEmails = new Set();

  if ($) {
    $('a[href^="tel:"]').each((_, el) => {
      const tel = $(el).attr('href').replace(/^tel:\s*/, '').replace(/\s/g, '');
      if (isValidPhone(tel)) telPhones.add(tel);
    });

    $('a[href^="mailto:"]').each((_, el) => {
      const mail = $(el).attr('href').replace(/^mailto:\s*/, '').split('?')[0].trim().toLowerCase();
      if (isValidEmail(mail)) mailtoEmails.add(mail);
    });

    $('script, style, noscript, svg, code, pre').remove();
    const sections = ['footer', 'header', 'nav',
      '[class*="contact"]', '[class*="footer"]', '[class*="header"]',
      '[id*="contact"]', '[id*="footer"]', '[class*="top-bar"]',
      '[class*="topbar"]', '[class*="info"]', '[class*="phone"]',
      '[class*="email"]', '[class*="widget"]', '[class*="sidebar"]'];
    for (const sel of sections) {
      try {
        $(sel).each((_, el) => {
          const text = $(el).text();
          (text.match(PHONE_RE) || []).forEach(p => { if (isValidPhone(p)) sectionPhones.add(p.trim()); });
          (text.match(EMAIL_RE) || []).forEach(e => { if (isValidEmail(e)) sectionEmails.add(e); });
        });
      } catch {}
    }

    if (telPhones.size === 0 && sectionPhones.size === 0) {
      const bodyText = $('body').text();
      (bodyText.match(PHONE_RE) || []).forEach(p => { if (isValidPhone(p)) sectionPhones.add(p.trim()); });
    }
    if (mailtoEmails.size === 0 && sectionEmails.size === 0) {
      const bodyText = $('body').text();
      (bodyText.match(EMAIL_RE) || []).forEach(e => { if (isValidEmail(e)) sectionEmails.add(e); });
    }
  }

  (html.match(/href=["']tel:([^"']+)["']/gi) || []).forEach(m => {
    const tel = m.replace(/href=["']tel:\s*/i, '').replace(/["']$/, '');
    if (isValidPhone(tel)) telPhones.add(tel);
  });
  (html.match(/href=["']mailto:([^"'?]+)/gi) || []).forEach(m => {
    const mail = m.replace(/href=["']mailto:\s*/i, '').trim().toLowerCase();
    if (isValidEmail(mail)) mailtoEmails.add(mail);
  });

  const phones = [...telPhones, ...sectionPhones];
  const emails = [...mailtoEmails, ...sectionEmails];
  return { phones, emails };
}

/**
 * Search Google for a builder company website.
 * Tries Google first (better results), falls back to DuckDuckGo if Google blocks.
 * Accepts an existing browser page to reuse (avoids launching new browsers).
 */
async function findCompanyWebsite(companyName, page) {
  if (!companyName) return null;

  const ownBrowser = !page;
  let browser;
  try {
    if (!page) {
      browser = await launchBrowser();
      page = await browser.newPage();
      await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
    }

    // Clean up company name for better search results
    const cleanName = companyName
      .replace(/,?\s*(LLC|Inc\.?|Corp\.?|Co\.?|L\.?L\.?C\.?)$/i, '')
      .trim();

    const query = `${cleanName} contractor Charleston SC`;

    // Try Google first — much better results than DuckDuckGo
    let links = [];
    try {
      const googleUrl = `https://www.google.com/search?q=${encodeURIComponent(query)}&num=10`;
      await page.goto(googleUrl, { waitUntil: 'networkidle2', timeout: 15000 });
      await new Promise(r => setTimeout(r, 2000));

      links = await page.evaluate(() => {
        const results = [];
        // Google search result links
        document.querySelectorAll('a[href^="http"]').forEach(a => {
          const href = a.href;
          // Skip Google's own links
          if (href.includes('google.com') || href.includes('google.co') ||
              href.includes('googleapis.com') || href.includes('gstatic.com') ||
              href.includes('accounts.google') || href.includes('support.google')) return;
          // Skip Google redirect URLs — extract the actual URL
          if (href.includes('/url?') || href.includes('google.com/url')) {
            try {
              const u = new URL(href);
              const actual = u.searchParams.get('q') || u.searchParams.get('url');
              if (actual && actual.startsWith('http')) results.push(actual);
            } catch {}
          } else {
            results.push(href);
          }
        });
        return [...new Set(results)];
      });
    } catch {
      // Google blocked us — fall back to DuckDuckGo
    }

    // Fallback to DuckDuckGo if Google returned nothing
    if (links.length === 0) {
      try {
        const ddgUrl = `https://duckduckgo.com/?q=${encodeURIComponent(query)}`;
        await page.goto(ddgUrl, { waitUntil: 'networkidle2', timeout: 15000 });
        await new Promise(r => setTimeout(r, 3000));

        links = await page.evaluate(() => {
          const results = [];
          document.querySelectorAll('article a[href^="http"], a[data-testid="result-title-a"]').forEach(a => {
            results.push(a.href);
          });
          if (results.length === 0) {
            document.querySelectorAll('a[href^="http"]').forEach(a => {
              if (!a.href.includes('duckduckgo.com')) results.push(a.href);
            });
          }
          return [...new Set(results)];
        });
      } catch {}
    }

    for (const link of links) {
      try {
        const hostname = new URL(link).hostname.toLowerCase();
        const isSkipped = SKIP_DOMAINS.some(d => hostname === d || hostname.endsWith('.' + d));
        if (isSkipped) continue;
        if (hostname.includes('duckduckgo.') || hostname.includes('google.')) continue;
        return link;
      } catch { continue; }
    }

    return null;
  } catch (err) {
    utils.log(`[BuilderLookup] Search failed for "${companyName}": ${err.message}`);
    return null;
  } finally {
    if (ownBrowser && browser) try { await browser.close(); } catch {}
  }
}

/**
 * Scrape a website for contact info.
 * Strategy: Load homepage first and discover the real contact page from nav links.
 * Only tries 3-4 pages max instead of 13. Falls back to axios for speed.
 */
async function scrapeContactInfo(websiteUrl, page) {
  if (!websiteUrl) return { phones: [], emails: [] };

  const allPhones = new Set();
  const allEmails = new Set();
  let baseUrl;
  try { baseUrl = new URL(websiteUrl).origin; } catch { return { phones: [], emails: [] }; }

  const visited = new Set();

  // Helper: scrape a single page via axios (fast, no JS but works for most sites)
  async function scrapePage(url) {
    if (visited.has(url)) return;
    visited.add(url);
    try {
      const { data } = await axios.get(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
          'Accept': 'text/html,application/xhtml+xml',
        },
        timeout: 8000,
        maxRedirects: 5,
        validateStatus: s => s < 400,
      });
      const $ = cheerio.load(data);
      const { phones, emails } = extractContactFromHtml(data, $);
      phones.forEach(p => allPhones.add(p));
      emails.forEach(e => allEmails.add(e));

      // Return discovered contact page links
      const contactLinks = [];
      $('a[href]').each((_, el) => {
        const href = $(el).attr('href') || '';
        const text = ($(el).text() || '').toLowerCase();
        if (text.includes('contact') || text.includes('get in touch') ||
            text.includes('reach us') || href.toLowerCase().includes('/contact')) {
          try {
            const full = href.startsWith('http') ? href : new URL(href, baseUrl).href;
            if (full.startsWith(baseUrl) && !visited.has(full)) contactLinks.push(full);
          } catch {}
        }
      });
      return contactLinks;
    } catch { return []; }
  }

  // Helper: scrape via Puppeteer (for JS-rendered sites)
  async function scrapePagePuppeteer(url) {
    if (visited.has(url)) return;
    visited.add(url);
    try {
      const resp = await page.goto(url, { waitUntil: 'networkidle2', timeout: 10000 });
      if (!resp || resp.status() >= 400) return;
      await new Promise(r => setTimeout(r, 1500));
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      await new Promise(r => setTimeout(r, 500));

      const html = await page.content();
      const $ = cheerio.load(html);
      const { phones, emails } = extractContactFromHtml(html, $);
      phones.forEach(p => allPhones.add(p));
      emails.forEach(e => allEmails.add(e));
    } catch {}
  }

  // ── Phase 1: Fast axios scrape of homepage ──
  const contactLinks = await scrapePage(websiteUrl) || [];
  if (allPhones.size > 0 && allEmails.size > 0) {
    return { phones: [...allPhones], emails: [...allEmails] };
  }

  // ── Phase 2: Follow discovered contact links (max 2) ──
  for (const link of contactLinks.slice(0, 2)) {
    await scrapePage(link);
    if (allPhones.size > 0 && allEmails.size > 0) {
      return { phones: [...allPhones], emails: [...allEmails] };
    }
  }

  // ── Phase 3: Try common contact paths if nothing discovered ──
  if (contactLinks.length === 0) {
    for (const path of ['/contact', '/contact-us', '/about']) {
      const url = `${baseUrl}${path}`;
      await scrapePage(url);
      if (allPhones.size > 0 || allEmails.size > 0) break;
    }
  }

  // ── Phase 4: If still nothing, try Puppeteer on homepage (JS-rendered) ──
  if (allPhones.size === 0 && allEmails.size === 0 && page) {
    visited.clear(); // Allow re-visiting with Puppeteer
    await scrapePagePuppeteer(websiteUrl);
    if (allPhones.size === 0 && allEmails.size === 0) {
      await scrapePagePuppeteer(`${baseUrl}/contact`);
    }
  }

  return { phones: [...allPhones], emails: [...allEmails] };
}

/**
 * Full builder lookup: search for company website, then scrape contact info.
 * Uses a SINGLE shared browser instance for both steps to avoid OOM.
 */
async function lookupBuilder(companyName, sharedBrowser, { skipCache = false } = {}) {
  // Check cache first — instant return if we already know this builder
  if (!skipCache) {
    const cached = builderCache.get(companyName);
    if (cached) {
      utils.log(`[BuilderLookup] Cache hit for "${companyName}" => ${cached.website || 'no website'}, ${cached.phone || 'no phone'}, ${cached.email || 'no email'}`);
      return cached;
    }
  }

  // Step 1: Check buyer lists FIRST — instant local lookup
  const buyerMatch = buyerList.lookup(companyName);
  if (buyerMatch && (buyerMatch.phone || buyerMatch.email)) {
    utils.log(`[BuilderLookup] Buyer list match for "${companyName}" => ${buyerMatch.phone || 'no phone'}, ${buyerMatch.email || 'no email'} (from ${buyerMatch.entityName})`);
    const result = {
      website: null,
      phone: buyerMatch.phone || null,
      email: buyerMatch.email || null,
      allPhones: buyerMatch.phone ? [buyerMatch.phone] : [],
      allEmails: buyerMatch.email ? [buyerMatch.email] : [],
      source: 'buyer-list',
    };
    builderCache.set(companyName, result);
    return result;
  }

  // Step 2: No buyer list match — proceed with web scraping (unchanged from before)
  utils.log(`[BuilderLookup] No buyer list match, web scraping "${companyName}"...`);

  const ownBrowser = !sharedBrowser;
  let browser = sharedBrowser;
  let page;

  try {
    if (!browser) {
      browser = await launchBrowser();
    }
    page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
    await page.setExtraHTTPHeaders({ 'Accept-Language': 'en-US,en;q=0.9' });

    const website = await findCompanyWebsite(companyName, page);
    if (!website) {
      utils.log(`[BuilderLookup] No website found for "${companyName}"`);
      // Don't cache empty results — allows retry on next run
      return { website: null, phone: null, email: null, allPhones: [], allEmails: [] };
    }

    utils.log(`[BuilderLookup] Found website: ${website}`);

    const { phones, emails } = await scrapeContactInfo(website, page);
    utils.log(`[BuilderLookup] "${companyName}" => ${website} | ${phones.length} phone(s), ${emails.length} email(s)`);

    const result = {
      website,
      phone: phones[0] || null,
      email: emails[0] || null,
      allPhones: phones,
      allEmails: emails,
    };

    builderCache.set(companyName, result);

    return result;
  } catch (err) {
    utils.log(`[BuilderLookup] Error looking up "${companyName}": ${err.message}`);
    // Don't cache errors — we may want to retry later
    return { website: null, phone: null, email: null, allPhones: [], allEmails: [] };
  } finally {
    if (page) try { await page.close(); } catch {}
    if (ownBrowser && browser) try { await browser.close(); } catch {}
  }
}

/**
 * Bulk lookup: find websites and scrape contact info for all permits missing it.
 * Uses ONE shared browser for all lookups to avoid OOM on Railway.
 */
async function bulkLookupBuilders(db, statusCallback) {
  const permits = await db.getPermitsNeedingLookup();
  if (permits.length === 0) {
    utils.log('[BuilderLookup] No permits need lookup');
    if (statusCallback) statusCallback({ status: 'completed', total: 0, processed: 0, found: 0, errors: 0 });
    return { total: 0, found: 0, errors: 0 };
  }

  // Deduplicate by company name (fall back to builder_name if no company)
  const companyMap = new Map();
  for (const p of permits) {
    const company = (p.builder_company || p.builder_name || '').trim();
    if (!company) continue;
    if (!companyMap.has(company)) companyMap.set(company, []);
    companyMap.get(company).push(p);
  }

  const uniqueCompanies = [...companyMap.keys()];
  utils.log(`[BuilderLookup] Bulk lookup: ${uniqueCompanies.length} unique companies across ${permits.length} permits`);

  let browser;
  let processed = 0;
  let found = 0;
  let errors = 0;

  try {
    browser = await launchBrowser();
    utils.log('[BuilderLookup] Browser launched for bulk lookup');

    // Health check SC LLR site once for the whole batch
    const llrPage = await browser.newPage();
    await llrPage.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36');
    const llrAvailable = await llrLookup.isAvailable(llrPage);
    utils.log(`[BuilderLookup] SC LLR site: ${llrAvailable ? 'available' : 'DOWN — will skip'}`);

    for (const company of uniqueCompanies) {
      processed++;
      if (statusCallback) {
        statusCallback({ status: 'running', total: uniqueCompanies.length, processed, found, errors, current: company });
      }

      try {
        const companyPermits = companyMap.get(company);
        let result = null;

        // ── Step 1: Check cache — only trust if it has phone or email ──
        const cached = builderCache.get(company);
        if (cached && (cached.phone || cached.email)) {
          utils.log(`[BuilderLookup] Cache hit for "${company}" => ${cached.phone || 'no phone'}, ${cached.email || 'no email'}`);
          result = cached;
        }

        // ── Step 2: SC LLR contractor license lookup ──
        if (!result && llrAvailable) {
          try {
            const llrResult = await llrLookup.lookupContractor(company, llrPage);
            if (llrResult && (llrResult.phone || llrResult.email)) {
              result = llrResult;
              utils.log(`[BuilderLookup] LLR hit for "${company}" => ${llrResult.phone || 'no phone'}, ${llrResult.email || 'no email'}`);
              builderCache.set(company, {
                website: null,
                phone: llrResult.phone,
                email: llrResult.email,
                allPhones: llrResult.allPhones || [],
                allEmails: llrResult.allEmails || [],
              });
            }
            await utils.delay(1500);
          } catch (err) {
            utils.log(`[BuilderLookup] LLR error for "${company}": ${err.message}`);
          }
        }

        // ── Step 3: Web search (buyer list + Google/DDG + website scrape) ──
        if (!result) {
          // If permits already have a website but no phone/email, re-scrape that website
          const existingWebsite = companyPermits.find(p => p.builder_website)?.builder_website;

          if (existingWebsite && companyPermits.every(p => !p.builder_phone && !p.builder_email)) {
            utils.log(`[BuilderLookup] Re-scraping "${company}" at ${existingWebsite} for contact info...`);
            const page = await browser.newPage();
            await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
            await page.setExtraHTTPHeaders({ 'Accept-Language': 'en-US,en;q=0.9' });
            const { phones, emails } = await scrapeContactInfo(existingWebsite, page);
            try { await page.close(); } catch {}
            result = { website: existingWebsite, phone: phones[0] || null, email: emails[0] || null, allPhones: phones, allEmails: emails };
            utils.log(`[BuilderLookup] Re-scrape "${company}": ${phones.length} phone(s), ${emails.length} email(s)`);
            if (result.phone || result.email) builderCache.set(company, result);
          } else {
            result = await lookupBuilder(company, browser, { skipCache: true });
          }
        }

        if (result) {
          for (const permit of companyPermits) {
            const updates = {};
            if (result.phone && !permit.builder_phone) updates.phone = result.phone;
            if (result.email && !permit.builder_email) updates.email = result.email;
            if (result.website) updates.website = result.website;

            if (Object.keys(updates).length > 0) {
              await db.updateBuilderContact(permit.id, updates);
            }
          }
          if (result.phone || result.email || result.website) found++;
        }
      } catch (err) {
        utils.log(`[BuilderLookup] Error looking up "${company}": ${err.message}`);
        errors++;
      }

      await new Promise(r => setTimeout(r, 2000));
    }

    try { await llrPage.close(); } catch {}
  } catch (err) {
    utils.log(`[BuilderLookup] Fatal browser error: ${err.message}`);
  } finally {
    if (browser) try { await browser.close(); } catch {}
  }

  utils.log(`[BuilderLookup] Bulk lookup complete: ${found}/${uniqueCompanies.length} companies found, ${errors} errors`);
  if (statusCallback) statusCallback({ status: 'completed', total: uniqueCompanies.length, processed, found, errors });

  return { total: uniqueCompanies.length, found, errors };
}

/**
 * Placeholder for future skip-trace API integration.
 */
async function skipTraceLookup(name, company) {
  return null;
}

module.exports = { lookupBuilder, bulkLookupBuilders, findCompanyWebsite, scrapeContactInfo, skipTraceLookup, launchBrowser };
