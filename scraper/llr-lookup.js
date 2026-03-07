// SC LLR Contractor License Lookup — verify.llronline.com
// Searches by company name, extracts phone/email from license records
// ASP.NET WebForms page — requires Puppeteer for viewstate/postback handling
const utils = require('./utils');

const LLR_URL = 'https://verify.llronline.com/LicLookup/Contractors/Contractor.aspx?div=69';
const PHONE_RE = /\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/g;
const EMAIL_RE = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;

/**
 * Look up a contractor on the SC LLR license database.
 * Uses an existing Puppeteer page (shared browser) to avoid spawning extra processes.
 * Returns { phone, email, licenseName, licenseNumber, address } or null.
 */
async function lookupContractor(companyName, page) {
  if (!companyName || !page) return null;

  // Simplify company name for search — remove LLC, Inc, etc.
  const searchName = companyName
    .replace(/,?\s*(LLC|Inc\.?|Corp\.?|Co\.?|L\.?L\.?C\.?|Incorporated|Corporation|Company|Group|Services|dba\s+.*)$/i, '')
    .trim();

  if (searchName.length < 3) return null;

  try {
    // Navigate to the search page
    const resp = await page.goto(LLR_URL, { waitUntil: 'networkidle2', timeout: 15000 });
    if (!resp || resp.status() >= 400) {
      utils.log(`[LLR] Page returned status ${resp ? resp.status() : 'null'}`);
      return null;
    }

    // Find the business/company name input field
    // ASP.NET WebForms: field IDs contain generated prefixes like ctl00$ContentPlaceHolder1$
    const nameField = await page.evaluate(() => {
      const inputs = document.querySelectorAll('input[type="text"]');
      for (const input of inputs) {
        const id = (input.id || '').toLowerCase();
        const name = (input.name || '').toLowerCase();
        // Look for business name, company name, or licensee name field
        if (id.includes('busname') || id.includes('business') || id.includes('company') ||
            id.includes('licensee') || id.includes('name') || name.includes('busname') ||
            name.includes('business') || name.includes('company')) {
          return input.id || input.name;
        }
      }
      // Fallback: first text input that isn't a license number field
      for (const input of inputs) {
        const id = (input.id || '').toLowerCase();
        if (!id.includes('license') && !id.includes('number') && !id.includes('zip')) {
          return input.id || input.name;
        }
      }
      return null;
    });

    if (!nameField) {
      utils.log('[LLR] Could not find name search field');
      return null;
    }

    // Clear field and type company name
    await page.evaluate((fieldId) => {
      const el = document.getElementById(fieldId) || document.querySelector(`[name="${fieldId}"]`);
      if (el) el.value = '';
    }, nameField);
    await page.type(`#${nameField}, [name="${nameField}"]`, searchName);

    // Find and click the search button
    const clicked = await page.evaluate(() => {
      const btns = document.querySelectorAll('input[type="submit"], input[type="button"], button');
      for (const btn of btns) {
        const val = (btn.value || btn.textContent || '').toLowerCase();
        if (val.includes('search') || val.includes('find') || val.includes('lookup') || val.includes('submit')) {
          btn.click();
          return true;
        }
      }
      return false;
    });

    if (!clicked) {
      utils.log('[LLR] Could not find search button');
      return null;
    }

    // Wait for results
    await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 15000 }).catch(() => {});
    await new Promise(r => setTimeout(r, 1500));

    // Parse results — look for a results table or grid
    const result = await page.evaluate((originalName) => {
      const bodyText = document.body.innerText || '';

      // Check for "no results" message
      if (bodyText.toLowerCase().includes('no records found') ||
          bodyText.toLowerCase().includes('no results') ||
          bodyText.toLowerCase().includes('0 records')) {
        return { noResults: true };
      }

      // Look for result links (detail pages) or result table rows
      const results = [];

      // Try table rows first
      const tables = document.querySelectorAll('table');
      for (const table of tables) {
        const rows = table.querySelectorAll('tr');
        for (let i = 1; i < rows.length; i++) {
          const cells = Array.from(rows[i].querySelectorAll('td'));
          if (cells.length >= 2) {
            const rowText = cells.map(c => c.textContent.trim()).join(' | ');
            const link = rows[i].querySelector('a');
            results.push({
              text: rowText,
              href: link ? link.href : null,
              cells: cells.map(c => c.textContent.trim()),
            });
          }
        }
      }

      // Also try gridview/datagrid (common in ASP.NET)
      const gridRows = document.querySelectorAll('[class*="grid"] tr, [class*="Grid"] tr, [id*="grid"] tr, [id*="Grid"] tr');
      for (let i = 1; i < gridRows.length; i++) {
        const cells = Array.from(gridRows[i].querySelectorAll('td'));
        if (cells.length >= 2) {
          const link = gridRows[i].querySelector('a');
          results.push({
            text: cells.map(c => c.textContent.trim()).join(' | '),
            href: link ? link.href : null,
            cells: cells.map(c => c.textContent.trim()),
          });
        }
      }

      // Extract phones and emails from the full page text
      const phoneRe = /\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/g;
      const emailRe = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
      const phones = [...new Set((bodyText.match(phoneRe) || []))];
      const emails = [...new Set((bodyText.match(emailRe) || []))].filter(e =>
        !e.includes('llr.sc.gov') && !e.includes('sc.gov'));

      return { results: results.slice(0, 10), phones, emails, bodyPreview: bodyText.substring(0, 1000) };
    }, companyName);

    if (result.noResults) {
      utils.log(`[LLR] No records found for "${searchName}"`);
      return null;
    }

    // If we got results with a detail link, click the first one for full info
    if (result.results && result.results.length > 0 && result.results[0].href) {
      try {
        await page.goto(result.results[0].href, { waitUntil: 'networkidle2', timeout: 15000 });
        await new Promise(r => setTimeout(r, 1000));

        const detail = await page.evaluate(() => {
          const text = document.body.innerText || '';
          const phoneRe = /\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/g;
          const emailRe = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
          const phones = [...new Set((text.match(phoneRe) || []))];
          const emails = [...new Set((text.match(emailRe) || []))].filter(e =>
            !e.includes('llr.sc.gov') && !e.includes('sc.gov'));

          // Try to find specific labeled fields
          let licenseName = null;
          let licenseNumber = null;
          let address = null;

          // Common ASP.NET label patterns
          const labels = document.querySelectorAll('span, label, td');
          let nextIsValue = null;
          for (const el of labels) {
            const t = el.textContent.trim();
            if (t.match(/^(business\s*name|company|licensee)/i)) {
              nextIsValue = 'name';
            } else if (t.match(/^(license\s*(number|#|no))/i)) {
              nextIsValue = 'license';
            } else if (t.match(/^(address|location)/i)) {
              nextIsValue = 'address';
            } else if (nextIsValue && t.length > 2 && t.length < 200) {
              if (nextIsValue === 'name') licenseName = t;
              else if (nextIsValue === 'license') licenseNumber = t;
              else if (nextIsValue === 'address') address = t;
              nextIsValue = null;
            }
          }

          return { phones, emails, licenseName, licenseNumber, address };
        });

        if (detail.phones.length > 0 || detail.emails.length > 0) {
          utils.log(`[LLR] "${searchName}": ${detail.phones.length} phone(s), ${detail.emails.length} email(s)`);
          return {
            phone: detail.phones[0] || null,
            email: detail.emails[0] || null,
            allPhones: detail.phones,
            allEmails: detail.emails,
            licenseName: detail.licenseName,
            licenseNumber: detail.licenseNumber,
            address: detail.address,
            source: 'sc-llr',
          };
        }
      } catch (err) {
        utils.log(`[LLR] Detail page error: ${err.message}`);
      }
    }

    // Fall back to phones/emails from the search results page
    if (result.phones.length > 0 || result.emails.length > 0) {
      utils.log(`[LLR] "${searchName}" (from results page): ${result.phones.length} phone(s), ${result.emails.length} email(s)`);
      return {
        phone: result.phones[0] || null,
        email: result.emails[0] || null,
        allPhones: result.phones,
        allEmails: result.emails,
        source: 'sc-llr',
      };
    }

    utils.log(`[LLR] "${searchName}": found records but no contact info`);
    return null;
  } catch (err) {
    // Graceful failure — site may be down (it's government, it goes down a lot)
    if (err.message.includes('timeout') || err.message.includes('ERR_CONNECTION')) {
      utils.log(`[LLR] Site unreachable (timeout) — skipping`);
    } else {
      utils.log(`[LLR] Error looking up "${searchName}": ${err.message}`);
    }
    return null;
  }
}

/**
 * Check if the LLR site is reachable (quick health check).
 * Returns true if the site responds, false if it's down.
 */
async function isAvailable(page) {
  try {
    const resp = await page.goto(LLR_URL, { waitUntil: 'domcontentloaded', timeout: 8000 });
    return resp && resp.status() < 400;
  } catch {
    return false;
  }
}

module.exports = { lookupContractor, isAvailable, LLR_URL };
