// Builder Website Lookup — searches for builder company websites and scrapes contact info
const axios = require('axios');
const cheerio = require('cheerio');
const puppeteer = require('puppeteer');
const config = require('../config');
const utils = require('./utils');

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
  'realtor.com', 'redfin.com', 'trulia.com', 'homes.com',
  'homesnap.com', 'movoto.com', 'apartments.com',
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
 * Search DuckDuckGo for a builder company website.
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

    const query = `${companyName} South Carolina`;
    const searchUrl = `https://duckduckgo.com/?q=${encodeURIComponent(query)}`;
    await page.goto(searchUrl, { waitUntil: 'networkidle2', timeout: 20000 });
    await new Promise(r => setTimeout(r, 3000));

    const links = await page.evaluate(() => {
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
    utils.log(`[BuilderLookup] Search failed for "${companyName}": ${err.message}`);
    return null;
  } finally {
    if (ownBrowser && browser) try { await browser.close(); } catch {}
  }
}

/**
 * Scrape a website for contact info.
 * Accepts an existing browser page to reuse (avoids launching new browsers).
 * Falls back to axios for extra pages if Puppeteer misses info.
 */
async function scrapeContactInfo(websiteUrl, page) {
  if (!websiteUrl) return { phones: [], emails: [] };

  const allPhones = new Set();
  const allEmails = new Set();
  const baseUrl = new URL(websiteUrl).origin;

  const pagesToTry = [
    websiteUrl,
    `${baseUrl}/contact`,
    `${baseUrl}/contact-us`,
    `${baseUrl}/contact.html`,
    `${baseUrl}/about`,
    `${baseUrl}/about-us`,
    `${baseUrl}/about.html`,
    `${baseUrl}/get-in-touch`,
    `${baseUrl}/connect`,
    `${baseUrl}/team`,
    `${baseUrl}/our-team`,
    `${baseUrl}/locations`,
    `${baseUrl}/footer`,
  ];

  // ── Phase 1: Puppeteer (handles JS-rendered sites) ──
  const ownBrowser = !page;
  let browser;
  try {
    if (!page) {
      browser = await launchBrowser();
      page = await browser.newPage();
      await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
      await page.setExtraHTTPHeaders({ 'Accept-Language': 'en-US,en;q=0.9' });
    }

    for (const pageUrl of pagesToTry) {
      try {
        const resp = await page.goto(pageUrl, { waitUntil: 'networkidle2', timeout: 12000 });
        if (!resp || resp.status() >= 400) continue;

        await new Promise(r => setTimeout(r, 2500));

        await page.evaluate(() => { window.scrollTo(0, document.body.scrollHeight); });
        await new Promise(r => setTimeout(r, 1000));

        const html = await page.content();
        const $ = cheerio.load(html);
        const { phones, emails } = extractContactFromHtml(html, $);
        phones.forEach(p => allPhones.add(p));
        emails.forEach(e => allEmails.add(e));

        utils.log(`[BuilderLookup]   ${pageUrl.replace(baseUrl, '') || '/'}: ${phones.length}ph, ${emails.length}em`);

        // Discover contact page links from nav/footer
        if (pageUrl === websiteUrl) {
          const contactLinks = await page.evaluate(() => {
            const links = [];
            document.querySelectorAll('a[href]').forEach(a => {
              const href = a.href.toLowerCase();
              const text = a.textContent.toLowerCase();
              if (text.includes('contact') || text.includes('get in touch') ||
                  text.includes('reach us') || text.includes('connect') ||
                  href.includes('/contact') || href.includes('/get-in-touch')) {
                links.push(a.href);
              }
            });
            return [...new Set(links)].slice(0, 3);
          });
          for (const link of contactLinks) {
            if (!pagesToTry.includes(link) && link.startsWith(baseUrl)) {
              pagesToTry.push(link);
            }
          }
        }

        // Stop early if we already have both phone and email
        if (allPhones.size > 0 && allEmails.size > 0) break;
      } catch { continue; }
    }
  } catch (err) {
    utils.log(`[BuilderLookup] Puppeteer scrape failed: ${err.message}`);
  } finally {
    if (ownBrowser && browser) try { await browser.close(); } catch {}
  }

  // ── Phase 2: Axios fallback ──
  if (allPhones.size === 0 || allEmails.size === 0) {
    utils.log(`[BuilderLookup] Puppeteer found ${allPhones.size} phone(s), ${allEmails.size} email(s) — trying axios fallback...`);
    for (const pageUrl of pagesToTry.slice(0, 6)) {
      try {
        const { data } = await axios.get(pageUrl, {
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml',
          },
          timeout: 10000,
          maxRedirects: 5,
          validateStatus: s => s < 400,
        });
        const $ = cheerio.load(data);
        const { phones, emails } = extractContactFromHtml(data, $);
        phones.forEach(p => allPhones.add(p));
        emails.forEach(e => allEmails.add(e));
        if (allPhones.size > 0 && allEmails.size > 0) break;
      } catch { continue; }
    }
  }

  return { phones: [...allPhones], emails: [...allEmails] };
}

/**
 * Full builder lookup: search for company website, then scrape contact info.
 * Uses a SINGLE shared browser instance for both steps to avoid OOM.
 */
async function lookupBuilder(companyName, sharedBrowser) {
  utils.log(`[BuilderLookup] Looking up "${companyName}"...`);

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

    // Step 1: Find website via DuckDuckGo
    const website = await findCompanyWebsite(companyName, page);
    if (!website) {
      utils.log(`[BuilderLookup] No website found for "${companyName}"`);
      return { website: null, phone: null, email: null, allPhones: [], allEmails: [] };
    }

    utils.log(`[BuilderLookup] Found website: ${website}`);

    // Step 2: Scrape contact info from the website (reuse same page)
    const { phones, emails } = await scrapeContactInfo(website, page);
    utils.log(`[BuilderLookup] "${companyName}" => ${website} | ${phones.length} phone(s), ${emails.length} email(s)`);

    return {
      website,
      phone: phones[0] || null,
      email: emails[0] || null,
      allPhones: phones,
      allEmails: emails,
    };
  } catch (err) {
    utils.log(`[BuilderLookup] Error looking up "${companyName}": ${err.message}`);
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

  // Deduplicate by company name
  const companyMap = new Map();
  for (const p of permits) {
    const company = (p.builder_company || '').trim();
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

    for (const company of uniqueCompanies) {
      processed++;
      if (statusCallback) {
        statusCallback({ status: 'running', total: uniqueCompanies.length, processed, found, errors, current: company });
      }

      try {
        // If permits already have a website but no phone/email, re-scrape that website
        const companyPermits = companyMap.get(company);
        const existingWebsite = companyPermits.find(p => p.builder_website)?.builder_website;
        let result;

        if (existingWebsite && companyPermits.every(p => !p.builder_phone && !p.builder_email)) {
          // Already have website but missing contact info — just re-scrape the site
          utils.log(`[BuilderLookup] Re-scraping "${company}" at ${existingWebsite} for contact info...`);
          const page = await browser.newPage();
          await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
          await page.setExtraHTTPHeaders({ 'Accept-Language': 'en-US,en;q=0.9' });
          const { phones, emails } = await scrapeContactInfo(existingWebsite, page);
          try { await page.close(); } catch {}
          result = { website: existingWebsite, phone: phones[0] || null, email: emails[0] || null, allPhones: phones, allEmails: emails };
          utils.log(`[BuilderLookup] Re-scrape "${company}": ${phones.length} phone(s), ${emails.length} email(s)`);
        } else {
          result = await lookupBuilder(company, browser);
        }

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
        utils.log(`[BuilderLookup] Error looking up "${company}": ${err.message}`);
        errors++;
      }

      await new Promise(r => setTimeout(r, 2000));
    }
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
