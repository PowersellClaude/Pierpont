// Builder Website Lookup — searches for builder company websites and scrapes contact info
// Multi-step enrichment pipeline: cache → buyer list → Google snippets → Google Maps →
// aggregator profiles (BBB, Houzz, etc.) → builder's own website → Facebook → SC SOS
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

// Domains to skip when choosing the builder's PRIMARY website
// (aggregators are NOT their own site, but we still scrape them for contact info)
const SKIP_DOMAINS_FOR_WEBSITE = [
  // Social media
  'facebook.com', 'instagram.com', 'twitter.com', 'x.com',
  'linkedin.com', 'youtube.com', 'pinterest.com', 'tiktok.com',
  'reddit.com', 'nextdoor.com',
  // Search engines / tech
  'google.com', 'bing.com', 'duckduckgo.com', 'apple.com', 'amazon.com',
  // Review / directory aggregators (skip for "website" but scrape for contact)
  'yelp.com', 'bbb.org', 'yellowpages.com', 'angi.com', 'angieslist.com',
  'homeadvisor.com', 'thumbtack.com', 'houzz.com', 'buildzoom.com',
  'manta.com', 'mapquest.com', 'porch.com', 'chamberofcommerce.com',
  // Business data aggregators
  'dnb.com', 'buzzfile.com', 'bloomberg.com', 'zoominfo.com',
  'bizapedia.com', 'opencorporates.com', 'companieslist.co',
  'govtribe.com', 'allbiz.com', 'infobel.com', 'cylex.us.com',
  'dandb.com', 'corporationwiki.com', 'owler.com',
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

// Keep the old name for backward compat (same list)
const SKIP_DOMAINS = SKIP_DOMAINS_FOR_WEBSITE;

// Aggregator domains worth scraping for direct contact info
const CONTACT_SCRAPE_DOMAINS = [
  'bbb.org', 'houzz.com', 'angi.com', 'angieslist.com',
  'porch.com', 'buildzoom.com', 'manta.com', 'thumbtack.com',
  'yelp.com', 'yellowpages.com', 'homeadvisor.com', 'chamberofcommerce.com',
];

// Junk email patterns to skip
const JUNK_EMAIL_PATTERNS = [
  'example.com', 'sentry.io', 'wixpress', 'wix.com', 'squarespace',
  'wordpress.com', 'w3.org', 'schema.org', 'googleapis.com', 'gstatic.com',
  'gravatar.com', 'cloudflare', '.png', '.jpg', '.svg', '.gif', '.webp',
  'noreply', 'no-reply', 'mailer-daemon', 'postmaster@', 'user@domain',
  'test@', 'admin@', 'webmaster@', 'hostmaster@', 'abuse@',
  'facebook.com', 'yelp.com', 'bbb.org', 'houzz.com', 'angi.com',
  'thumbtack.com', 'porch.com', 'homeadvisor.com',
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

// ─── Clean company name (strip LLC/Inc/Corp suffixes) ────────────────────────
function cleanCompanyName(name) {
  if (!name) return '';
  return name
    .replace(/,?\s*(LLC|Inc\.?|Corp\.?|Co\.?|L\.?L\.?C\.?|Incorporated|Corporation|Company|Group|Services|Enterprises?)$/i, '')
    .replace(/\s+/g, ' ')
    .trim();
}

// ─── Verify a page is about the right company ───────────────────────────────
function verifyCompanyMatch(companyName, pageText) {
  if (!companyName || !pageText) return false;
  const clean = cleanCompanyName(companyName).toLowerCase();
  const text = pageText.toLowerCase();

  // Direct substring match of the full cleaned name
  if (text.includes(clean)) return true;

  // Word-level matching
  const stopWords = new Set(['construction', 'builders', 'homes', 'home', 'building',
    'group', 'llc', 'inc', 'the', 'and', 'of', 'sc', 'charleston', 'contractor',
    'general', 'custom', 'residential', 'commercial', 'services', 'enterprises', 'company']);
  const words = clean.split(/\s+/).filter(w => w.length > 2 && !stopWords.has(w));

  if (words.length === 0) return text.includes(clean);
  if (words.length === 1) return text.includes(words[0]);

  const matched = words.filter(w => text.includes(w));
  return matched.length >= Math.min(2, words.length);
}

// ─── Generate multiple search query variations ──────────────────────────────
function generateSearchQueries(companyName, builderName) {
  const clean = cleanCompanyName(companyName);
  const queries = [
    `"${clean}" contractor Charleston SC`,
    `"${clean}" SC contractor`,
    `"${clean}" builder South Carolina`,
    `${clean} general contractor Charleston`,
  ];

  if (builderName && builderName.toLowerCase() !== companyName.toLowerCase()) {
    const cleanBuilder = cleanCompanyName(builderName);
    queries.push(`"${cleanBuilder}" builder Charleston SC`);
    queries.push(`"${cleanBuilder}" contractor SC`);
  }

  return [...new Set(queries)];
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

// ─── NEW: Extract phone/email from Google search result snippets ─────────────
async function extractContactsFromSnippets(page) {
  try {
    return await page.evaluate(() => {
      const phones = [];
      const emails = [];
      const phoneRe = /(?:\+1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/g;
      const emailRe = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;

      // Snippet text from search results
      const snippetSelectors = [
        '.VwiC3b', 'span.st', '[data-sncf]', '.IsZvec', '.lEBKkf',
        '.yDYNvb', '.GzssTd', '.ITZIwc',
      ];
      for (const sel of snippetSelectors) {
        document.querySelectorAll(sel).forEach(el => {
          const text = el.textContent || '';
          (text.match(phoneRe) || []).forEach(p => phones.push(p.trim()));
          (text.match(emailRe) || []).forEach(e => emails.push(e.toLowerCase()));
        });
      }

      // Knowledge Panel / sidebar (Google Business Profile)
      const kpSelectors = [
        '[data-attrid*="phone"]', '.LrzXr', '.Z1hOCe',
        '[data-attrid*="kc"]', '.IzNS7c', '.zloOqf',
        '[class*="phone"]', '[class*="Phone"]',
      ];
      for (const sel of kpSelectors) {
        document.querySelectorAll(sel).forEach(el => {
          const text = el.textContent || '';
          (text.match(phoneRe) || []).forEach(p => phones.push(p.trim()));
          (text.match(emailRe) || []).forEach(e => emails.push(e.toLowerCase()));
        });
      }

      // Also check the entire visible text for phone in the right column
      const sidePanel = document.querySelector('.kp-wholepage') || document.querySelector('.liYKde');
      if (sidePanel) {
        const text = sidePanel.textContent || '';
        (text.match(phoneRe) || []).forEach(p => phones.push(p.trim()));
      }

      return { phones: [...new Set(phones)], emails: [...new Set(emails)] };
    });
  } catch {
    return { phones: [], emails: [] };
  }
}

// ─── NEW: Search Google Maps for business phone ──────────────────────────────
async function searchGoogleMaps(companyName, page) {
  try {
    const clean = cleanCompanyName(companyName);
    const mapsUrl = `https://www.google.com/maps/search/${encodeURIComponent(clean + ' Charleston SC')}`;

    await page.goto(mapsUrl, { waitUntil: 'networkidle2', timeout: 15000 });
    await new Promise(r => setTimeout(r, 3000));

    const result = await page.evaluate(() => {
      const phoneRe = /\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/g;
      let phone = null;
      let website = null;

      // Look for phone in the business listing
      const allText = document.body.innerText || '';

      // Google Maps shows phone with icon - look for aria labels and data attributes
      document.querySelectorAll('[data-tooltip="Copy phone number"], [aria-label*="Phone"]').forEach(el => {
        const text = (el.textContent || el.getAttribute('aria-label') || '').trim();
        const matches = text.match(phoneRe);
        if (matches) phone = matches[0];
      });

      // Also look for phone pattern near "Phone" text
      if (!phone) {
        const phoneMatch = allText.match(/(?:Phone|phone|Call)[:\s]*(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})/);
        if (phoneMatch) phone = phoneMatch[1];
      }

      // Fallback: find any phone in the sidebar content
      if (!phone) {
        // The action buttons area typically has the phone
        document.querySelectorAll('button[data-item-id*="phone"], a[data-item-id*="phone"]').forEach(el => {
          const text = el.textContent || el.getAttribute('aria-label') || '';
          const matches = text.match(phoneRe);
          if (matches) phone = matches[0];
        });
      }

      // Look for website link
      document.querySelectorAll('a[data-item-id="authority"], a[data-tooltip="Open website"]').forEach(el => {
        website = el.href || null;
      });

      return { phone, website };
    });

    if (result.phone) {
      utils.log(`[GoogleMaps] Found phone for "${companyName}": ${result.phone}`);
    }

    return { phone: result.phone, email: null, website: result.website };
  } catch (err) {
    utils.log(`[GoogleMaps] Error for "${companyName}": ${err.message}`);
    return { phone: null, email: null, website: null };
  }
}

// ─── NEW: Scrape aggregator profile for contact info ─────────────────────────
async function scrapeAggregatorForContact(url, companyName) {
  try {
    const { data } = await axios.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml',
      },
      timeout: 10000,
      maxRedirects: 5,
      validateStatus: s => s < 400,
    });

    const $ = cheerio.load(data);
    const bodyText = $('body').text();

    // Verify this page is about the right company
    if (companyName && !verifyCompanyMatch(companyName, bodyText)) {
      utils.log(`[Aggregator] Page at ${url} doesn't match "${companyName}" — skipping`);
      return { phones: [], emails: [] };
    }

    // Standard extraction
    const { phones, emails } = extractContactFromHtml(data, $);

    // BBB-specific selectors
    const hostname = new URL(url).hostname.toLowerCase();
    if (hostname.includes('bbb.org')) {
      $('[itemprop="telephone"], .dtm-phone, .business-phone').each((_, el) => {
        const text = $(el).text().trim();
        (text.match(PHONE_RE) || []).forEach(p => { if (isValidPhone(p)) phones.push(p); });
      });
    }

    // Houzz-specific
    if (hostname.includes('houzz.com')) {
      $('[itemprop="telephone"], .pro-contact-phone').each((_, el) => {
        const text = $(el).text().trim();
        (text.match(PHONE_RE) || []).forEach(p => { if (isValidPhone(p)) phones.push(p); });
      });
    }

    // YellowPages-specific
    if (hostname.includes('yellowpages.com')) {
      $('.phone, [itemprop="telephone"]').each((_, el) => {
        const text = $(el).text().trim();
        (text.match(PHONE_RE) || []).forEach(p => { if (isValidPhone(p)) phones.push(p); });
      });
    }

    // Generic structured data
    $('[itemprop="telephone"], [itemprop="email"]').each((_, el) => {
      const text = $(el).text().trim();
      const itemprop = $(el).attr('itemprop');
      if (itemprop === 'telephone') {
        (text.match(PHONE_RE) || []).forEach(p => { if (isValidPhone(p)) phones.push(p); });
      } else if (itemprop === 'email') {
        (text.match(EMAIL_RE) || []).forEach(e => { if (isValidEmail(e)) emails.push(e); });
      }
    });

    // JSON-LD structured data
    $('script[type="application/ld+json"]').each((_, el) => {
      try {
        const json = JSON.parse($(el).html());
        const items = Array.isArray(json) ? json : [json];
        for (const item of items) {
          if (item.telephone) {
            (String(item.telephone).match(PHONE_RE) || []).forEach(p => { if (isValidPhone(p)) phones.push(p); });
          }
          if (item.email) {
            const e = String(item.email).toLowerCase();
            if (isValidEmail(e)) emails.push(e);
          }
        }
      } catch {}
    });

    const uniquePhones = [...new Set(phones)].filter(isValidPhone);
    const uniqueEmails = [...new Set(emails)].filter(isValidEmail);

    if (uniquePhones.length || uniqueEmails.length) {
      utils.log(`[Aggregator] ${url}: ${uniquePhones.length} phone(s), ${uniqueEmails.length} email(s)`);
    }

    return { phones: uniquePhones, emails: uniqueEmails };
  } catch (err) {
    utils.log(`[Aggregator] Error scraping ${url}: ${err.message}`);
    return { phones: [], emails: [] };
  }
}

// ─── NEW: Search Facebook business page for contact info ─────────────────────
async function searchFacebookBusiness(companyName, page) {
  try {
    const clean = cleanCompanyName(companyName);
    const query = `"${clean}" Charleston SC site:facebook.com`;
    const googleUrl = `https://www.google.com/search?q=${encodeURIComponent(query)}&num=5`;

    await page.goto(googleUrl, { waitUntil: 'networkidle2', timeout: 12000 });
    await new Promise(r => setTimeout(r, 2000));

    // Find a Facebook link from the search results
    const fbLink = await page.evaluate(() => {
      const links = [];
      document.querySelectorAll('a[href*="facebook.com"]').forEach(a => {
        let href = a.href;
        if (href.includes('google.com/url')) {
          try {
            const u = new URL(href);
            href = u.searchParams.get('q') || u.searchParams.get('url') || href;
          } catch {}
        }
        if (href.includes('facebook.com') && !href.includes('login') && !href.includes('signup')) {
          links.push(href);
        }
      });
      return links[0] || null;
    });

    if (!fbLink) {
      return { phone: null, email: null };
    }

    utils.log(`[Facebook] Found page for "${companyName}": ${fbLink}`);

    // Navigate to the Facebook page
    await page.goto(fbLink, { waitUntil: 'networkidle2', timeout: 12000 });
    await new Promise(r => setTimeout(r, 2000));

    // Extract contact info from the visible page content
    const result = await page.evaluate(() => {
      const phoneRe = /\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/g;
      const emailRe = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
      const text = document.body.innerText || '';

      const phones = [...new Set(text.match(phoneRe) || [])];
      const emails = [...new Set((text.match(emailRe) || []).filter(e =>
        !e.includes('facebook.com') && !e.includes('fb.com')
      ))];

      return { phones, emails };
    });

    const validPhones = result.phones.filter(isValidPhone);
    const validEmails = result.emails.filter(isValidEmail);

    if (validPhones.length || validEmails.length) {
      utils.log(`[Facebook] "${companyName}": ${validPhones.length} phone(s), ${validEmails.length} email(s)`);
    }

    return {
      phone: validPhones[0] || null,
      email: validEmails[0] || null,
    };
  } catch (err) {
    utils.log(`[Facebook] Error for "${companyName}": ${err.message}`);
    return { phone: null, email: null };
  }
}

// ─── NEW: SC Secretary of State business entity search ───────────────────────
async function searchSCSecretaryOfState(companyName, page) {
  try {
    const clean = cleanCompanyName(companyName);
    if (clean.length < 3) return null;

    const searchUrl = 'https://businessfilings.sc.gov/BusinessFiling/Entity/Search';
    await page.goto(searchUrl, { waitUntil: 'networkidle2', timeout: 15000 });
    await new Promise(r => setTimeout(r, 1500));

    // Find the search input and submit
    const found = await page.evaluate((name) => {
      const inputs = document.querySelectorAll('input[type="text"]');
      for (const input of inputs) {
        const id = (input.id || input.name || '').toLowerCase();
        if (id.includes('search') || id.includes('name') || id.includes('entity')) {
          input.value = name;
          input.dispatchEvent(new Event('input', { bubbles: true }));
          return input.id || input.name;
        }
      }
      // Fallback: first text input
      if (inputs.length > 0) {
        inputs[0].value = name;
        inputs[0].dispatchEvent(new Event('input', { bubbles: true }));
        return inputs[0].id || inputs[0].name || 'first-input';
      }
      return null;
    }, clean);

    if (!found) return null;

    // Click search button
    const clicked = await page.evaluate(() => {
      const btns = document.querySelectorAll('button, input[type="submit"]');
      for (const btn of btns) {
        const text = (btn.textContent || btn.value || '').toLowerCase();
        if (text.includes('search') || text.includes('find') || text.includes('submit')) {
          btn.click();
          return true;
        }
      }
      return false;
    });

    if (!clicked) return null;

    await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 15000 }).catch(() => {});
    await new Promise(r => setTimeout(r, 2000));

    // Look for results and click the first matching one
    const detailLink = await page.evaluate((companyLower) => {
      const links = document.querySelectorAll('a');
      for (const link of links) {
        const text = (link.textContent || '').toLowerCase();
        if (text.includes(companyLower.substring(0, 10).toLowerCase()) && link.href.includes('Entity')) {
          return link.href;
        }
      }
      // Try table rows
      const rows = document.querySelectorAll('tr');
      for (const row of rows) {
        const text = (row.textContent || '').toLowerCase();
        if (text.includes(companyLower.substring(0, 10).toLowerCase())) {
          const link = row.querySelector('a');
          if (link) return link.href;
        }
      }
      return null;
    }, clean);

    if (detailLink) {
      await page.goto(detailLink, { waitUntil: 'networkidle2', timeout: 15000 });
      await new Promise(r => setTimeout(r, 1500));
    }

    // Extract contact info from the page
    const result = await page.evaluate(() => {
      const phoneRe = /\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/g;
      const emailRe = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
      const text = document.body.innerText || '';

      const phones = [...new Set(text.match(phoneRe) || [])].filter(p =>
        !p.includes('803') || true // SC area codes: 803, 843, 854, 864
      );
      const emails = [...new Set(text.match(emailRe) || [])].filter(e =>
        !e.includes('sc.gov') && !e.includes('sos.sc.gov')
      );

      // Look for registered agent info
      let registeredAgent = null;
      const agentMatch = text.match(/Registered Agent[:\s]*([^\n]+)/i);
      if (agentMatch) registeredAgent = agentMatch[1].trim();

      return { phones, emails, registeredAgent };
    });

    const validPhones = result.phones.filter(isValidPhone);
    const validEmails = result.emails.filter(isValidEmail);

    if (validPhones.length || validEmails.length) {
      utils.log(`[SC-SOS] "${companyName}": ${validPhones.length} phone(s), ${validEmails.length} email(s)`);
    }

    return {
      phone: validPhones[0] || null,
      email: validEmails[0] || null,
      registeredAgent: result.registeredAgent,
      source: 'sc-sos',
    };
  } catch (err) {
    utils.log(`[SC-SOS] Error for "${companyName}": ${err.message}`);
    return null;
  }
}

/**
 * Enhanced Google search — tries multiple queries, extracts snippet contacts,
 * and collects aggregator profile links for later scraping.
 * Returns rich result instead of just a URL.
 */
async function findCompanyWebsite(queries, page) {
  // Accept old-style single string for backward compat
  if (typeof queries === 'string') {
    const clean = cleanCompanyName(queries);
    queries = [`${clean} contractor Charleston SC`];
  }

  const result = {
    website: null,
    snippetPhones: [],
    snippetEmails: [],
    aggregatorLinks: [],
    allLinks: [],
  };

  const ownBrowser = !page;
  let browser;
  try {
    if (!page) {
      browser = await launchBrowser();
      page = await browser.newPage();
      await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
    }

    for (const query of queries) {
      // Try Google
      let links = [];
      try {
        const googleUrl = `https://www.google.com/search?q=${encodeURIComponent(query)}&num=10`;
        await page.goto(googleUrl, { waitUntil: 'networkidle2', timeout: 15000 });
        await new Promise(r => setTimeout(r, 2000));

        // Extract contacts from snippets BEFORE clicking anything
        const snippets = await extractContactsFromSnippets(page);
        snippets.phones.filter(isValidPhone).forEach(p => result.snippetPhones.push(p));
        snippets.emails.filter(isValidEmail).forEach(e => result.snippetEmails.push(e));

        links = await page.evaluate(() => {
          const results = [];
          document.querySelectorAll('a[href^="http"]').forEach(a => {
            const href = a.href;
            if (href.includes('google.com') || href.includes('google.co') ||
                href.includes('googleapis.com') || href.includes('gstatic.com') ||
                href.includes('accounts.google') || href.includes('support.google')) return;
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
      } catch {}

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

      // Process all links from this query
      for (const link of links) {
        try {
          const hostname = new URL(link).hostname.toLowerCase();
          if (hostname.includes('duckduckgo.') || hostname.includes('google.')) continue;

          // Check if it's an aggregator — save for contact scraping later
          const isAggregator = CONTACT_SCRAPE_DOMAINS.some(d => hostname === d || hostname.endsWith('.' + d));
          if (isAggregator && !result.aggregatorLinks.includes(link)) {
            result.aggregatorLinks.push(link);
            continue;
          }

          // Check if it's a Facebook page — save separately
          if (hostname.includes('facebook.com')) {
            if (!result.aggregatorLinks.includes(link)) result.aggregatorLinks.push(link);
            continue;
          }

          // Skip other social/junk domains for the primary website
          const isSkipped = SKIP_DOMAINS_FOR_WEBSITE.some(d => hostname === d || hostname.endsWith('.' + d));
          if (isSkipped) continue;

          // First non-skipped link is the builder's own website
          if (!result.website) {
            result.website = link;
          }

          result.allLinks.push(link);
        } catch { continue; }
      }

      // If we found a website, don't need to try more queries
      if (result.website) break;

      // If we got snippet contacts, we can still try more queries for the website
      await new Promise(r => setTimeout(r, 1000));
    }

    // Deduplicate
    result.snippetPhones = [...new Set(result.snippetPhones)];
    result.snippetEmails = [...new Set(result.snippetEmails)];

    return result;
  } catch (err) {
    utils.log(`[Search] Failed: ${err.message}`);
    return result;
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
 * Full builder lookup — multi-step enrichment pipeline.
 * Steps (all additive, stops early when phone + email found):
 *   0. Cache check
 *   1. Buyer list
 *   2. Google Search (multi-query) + snippet extraction
 *   3. Google Maps / Business Profile
 *   4. Aggregator profiles (BBB, Houzz, Angi, YellowPages, etc.)
 *   5. Builder's own website scrape
 *   6. Facebook business page
 *   7. SC Secretary of State
 *   8. Retry with builder's personal name (if company search failed)
 */
async function lookupBuilder(companyName, sharedBrowser, { skipCache = false, builderName = null } = {}) {
  // ── Step 0: Cache check ──
  if (!skipCache) {
    const cached = builderCache.get(companyName);
    if (cached && (cached.phone || cached.email)) {
      utils.log(`[BuilderLookup] Cache hit for "${companyName}" => ${cached.phone || 'no phone'}, ${cached.email || 'no email'}`);
      return cached;
    }
  }

  // ── Step 1: Buyer list (instant local lookup) ──
  const buyerMatch = buyerList.lookup(companyName);
  if (buyerMatch && (buyerMatch.phone || buyerMatch.email)) {
    utils.log(`[BuilderLookup] Buyer list match for "${companyName}" => ${buyerMatch.phone || 'no phone'}, ${buyerMatch.email || 'no email'}`);
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

  // ── Begin multi-step web enrichment ──
  utils.log(`[BuilderLookup] Starting enrichment pipeline for "${companyName}"${builderName ? ` (builder: ${builderName})` : ''}...`);

  const found = {
    website: null,
    phone: null,
    email: null,
    allPhones: new Set(),
    allEmails: new Set(),
    sources: [],
  };

  function addContact(phones, emails, source) {
    for (const p of (phones || [])) {
      if (isValidPhone(p)) {
        found.allPhones.add(p);
        if (!found.phone) { found.phone = p; found.sources.push({ field: 'phone', source }); }
      }
    }
    for (const e of (emails || [])) {
      if (isValidEmail(e)) {
        found.allEmails.add(e);
        if (!found.email) { found.email = e; found.sources.push({ field: 'email', source }); }
      }
    }
  }

  function hasFullContact() { return found.phone && found.email; }

  const ownBrowser = !sharedBrowser;
  let browser = sharedBrowser;
  let page;

  try {
    if (!browser) browser = await launchBrowser();
    page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
    await page.setExtraHTTPHeaders({ 'Accept-Language': 'en-US,en;q=0.9' });

    // ── Step 2: Google Search (multi-query) + snippet extraction ──
    const queries = generateSearchQueries(companyName, builderName);
    const searchResult = await findCompanyWebsite(queries, page);

    // Harvest snippet contacts (phone/email Google already shows)
    addContact(searchResult.snippetPhones, searchResult.snippetEmails, 'google-snippet');
    if (searchResult.website) found.website = searchResult.website;

    if (searchResult.snippetPhones.length || searchResult.snippetEmails.length) {
      utils.log(`[BuilderLookup] Google snippets for "${companyName}": ${searchResult.snippetPhones.length} phone(s), ${searchResult.snippetEmails.length} email(s)`);
    }

    // ── Step 3: Google Maps / Business Profile ──
    if (!hasFullContact()) {
      const mapsResult = await searchGoogleMaps(companyName, page);
      if (mapsResult.phone) addContact([mapsResult.phone], [], 'google-maps');
      if (mapsResult.email) addContact([], [mapsResult.email], 'google-maps');
      if (!found.website && mapsResult.website) found.website = mapsResult.website;
    }

    // ── Step 4: Scrape aggregator profiles (BBB, Houzz, Angi, etc.) ──
    if (!hasFullContact() && searchResult.aggregatorLinks.length > 0) {
      utils.log(`[BuilderLookup] Checking ${searchResult.aggregatorLinks.length} aggregator profile(s) for "${companyName}"...`);
      for (const aggUrl of searchResult.aggregatorLinks.slice(0, 4)) {
        try {
          const aggResult = await scrapeAggregatorForContact(aggUrl, companyName);
          addContact(aggResult.phones, aggResult.emails, `aggregator:${new URL(aggUrl).hostname}`);
          if (hasFullContact()) break;
        } catch {}
        await utils.delay(800);
      }
    }

    // ── Step 5: Scrape builder's own website ──
    if (!hasFullContact() && found.website) {
      utils.log(`[BuilderLookup] Scraping website for "${companyName}": ${found.website}`);
      const siteResult = await scrapeContactInfo(found.website, page);
      addContact(siteResult.phones, siteResult.emails, 'website');
    }

    // ── Step 6: Facebook business page ──
    if (!hasFullContact()) {
      const fbResult = await searchFacebookBusiness(companyName, page);
      if (fbResult.phone) addContact([fbResult.phone], [], 'facebook');
      if (fbResult.email) addContact([], [fbResult.email], 'facebook');
    }

    // ── Step 7: SC Secretary of State ──
    if (!hasFullContact()) {
      const sosResult = await searchSCSecretaryOfState(companyName, page);
      if (sosResult) {
        if (sosResult.phone) addContact([sosResult.phone], [], 'sc-sos');
        if (sosResult.email) addContact([], [sosResult.email], 'sc-sos');
      }
    }

    // ── Step 8: Retry with builder's personal name ──
    if (!found.phone && !found.email && !found.website && builderName && builderName.toLowerCase() !== companyName.toLowerCase()) {
      utils.log(`[BuilderLookup] Company search failed, trying builder name: "${builderName}"`);
      const nameQueries = [
        `"${builderName}" builder Charleston SC`,
        `"${builderName}" contractor SC`,
        `"${builderName}" construction Charleston`,
      ];
      const nameSearch = await findCompanyWebsite(nameQueries, page);
      addContact(nameSearch.snippetPhones, nameSearch.snippetEmails, 'name-search-snippet');

      if (nameSearch.website) {
        found.website = nameSearch.website;
        const siteResult = await scrapeContactInfo(nameSearch.website, page);
        addContact(siteResult.phones, siteResult.emails, 'name-search-website');
      }

      // Try aggregators from name search too
      if (!hasFullContact() && nameSearch.aggregatorLinks.length > 0) {
        for (const aggUrl of nameSearch.aggregatorLinks.slice(0, 2)) {
          try {
            const aggResult = await scrapeAggregatorForContact(aggUrl, builderName);
            addContact(aggResult.phones, aggResult.emails, 'name-search-aggregator');
            if (hasFullContact()) break;
          } catch {}
        }
      }
    }

    // ── Build final result ──
    const finalResult = {
      website: found.website,
      phone: found.phone || null,
      email: found.email || null,
      allPhones: [...found.allPhones],
      allEmails: [...found.allEmails],
      sources: found.sources,
    };

    // Log summary
    const sourceStr = found.sources.map(s => `${s.field}:${s.source}`).join(', ');
    utils.log(`[BuilderLookup] "${companyName}" RESULT: phone=${finalResult.phone || 'NONE'}, email=${finalResult.email || 'NONE'}, website=${finalResult.website || 'NONE'}${sourceStr ? ` [${sourceStr}]` : ''}`);

    // Cache if we found anything useful (don't cache empty results)
    if (finalResult.phone || finalResult.email || finalResult.website) {
      builderCache.set(companyName, finalResult);
    }

    return finalResult;
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

        // ── Step 3: Full enrichment pipeline (multi-query, Maps, aggregators, FB, SOS) ──
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
            // Pass builderName so the pipeline can try personal name as fallback
            const builderName = companyPermits.find(p => p.builder_name)?.builder_name || null;
            result = await lookupBuilder(company, browser, { skipCache: true, builderName });
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

module.exports = {
  lookupBuilder, bulkLookupBuilders, findCompanyWebsite, scrapeContactInfo,
  skipTraceLookup, launchBrowser,
  // New enrichment functions
  extractContactsFromSnippets, searchGoogleMaps, scrapeAggregatorForContact,
  searchFacebookBusiness, searchSCSecretaryOfState, verifyCompanyMatch,
  generateSearchQueries, cleanCompanyName,
};
