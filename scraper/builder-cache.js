// Builder Contact Cache — persistent JSON lookup so we never scrape the same builder twice
const fs = require('fs');
const path = require('path');

const CACHE_PATH = path.join(__dirname, '..', 'db', 'builder-cache.json');

let cache = null;

function loadCache() {
  if (cache) return cache;
  try {
    if (fs.existsSync(CACHE_PATH)) {
      cache = JSON.parse(fs.readFileSync(CACHE_PATH, 'utf-8'));
    } else {
      cache = {};
    }
  } catch (err) {
    console.error('[BuilderCache] Failed to load cache, starting fresh:', err.message);
    cache = {};
  }
  return cache;
}

function saveCache() {
  if (!cache) return;
  try {
    const dir = path.dirname(CACHE_PATH);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(CACHE_PATH, JSON.stringify(cache, null, 2));
  } catch (err) {
    console.error('[BuilderCache] Failed to save cache:', err.message);
  }
}

// Normalize company name for cache key: lowercase, trim, collapse whitespace, strip suffixes
function normalizeKey(name) {
  if (!name) return null;
  return name.toLowerCase().trim()
    .replace(/,?\s*(llc|inc\.?|corp\.?|co\.?|l\.?l\.?c\.?|incorporated|corporation|company|group|enterprises?)\.?$/i, '')
    .replace(/\s+/g, ' ')
    .replace(/[.,]+$/, '')
    .trim();
}

// Also try alternate keys for fuzzy matching
function getWithFuzzy(companyName) {
  loadCache();
  const key = normalizeKey(companyName);
  if (!key) return null;
  // Exact match first
  if (cache[key]) return cache[key];
  // Try without common suffixes that normalizeKey might not catch
  const shorter = key.replace(/\s*(construction|builders?|homes?|building|contracting|enterprises?|services?)$/i, '').trim();
  if (shorter && shorter !== key && cache[shorter]) return cache[shorter];
  // Try matching the other direction — check if any cache key starts with our name
  for (const [k, v] of Object.entries(cache)) {
    if (k.startsWith('__')) continue;
    if (k.startsWith(shorter) || shorter.startsWith(k)) {
      if (v.phone || v.email) return v;
    }
  }
  return null;
}

// Look up a builder in the cache. Returns { website, phone, email, allPhones, allEmails } or null.
function get(companyName) {
  loadCache();
  const key = normalizeKey(companyName);
  if (!key) return null;
  return cache[key] || null;
}

// Save a builder's contact info to the cache.
function set(companyName, data) {
  loadCache();
  const key = normalizeKey(companyName);
  if (!key) return;
  cache[key] = {
    website: data.website || null,
    phone: data.phone || null,
    email: data.email || null,
    allPhones: data.allPhones || [],
    allEmails: data.allEmails || [],
    lookedUpAt: new Date().toISOString(),
  };
  saveCache();
}

// Check if a builder exists in the cache (even if lookup found nothing — avoids re-lookups)
function has(companyName) {
  loadCache();
  const key = normalizeKey(companyName);
  if (!key) return false;
  return key in cache;
}

// Get cache stats
function stats() {
  loadCache();
  const entries = Object.entries(cache)
    .filter(([k]) => !k.startsWith('__'))
    .map(([, v]) => v);
  return {
    total: entries.length,
    withWebsite: entries.filter(e => e.website).length,
    withPhone: entries.filter(e => e.phone).length,
    withEmail: entries.filter(e => e.email).length,
    directoryLastCrawled: cache.__directory_last_crawled || null,
  };
}

// Track when the directory was last crawled
function getDirectoryLastCrawled() {
  loadCache();
  return cache.__directory_last_crawled || null;
}

function setDirectoryLastCrawled() {
  loadCache();
  cache.__directory_last_crawled = new Date().toISOString();
  saveCache();
}

module.exports = { get, getWithFuzzy, set, has, stats, loadCache, saveCache, normalizeKey, getDirectoryLastCrawled, setDirectoryLastCrawled, CACHE_PATH };
