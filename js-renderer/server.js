const express = require('express');
const { chromium } = require('playwright');
const ipaddr = require('ipaddr.js');
const dns = require('dns');
const { URL } = require('url');

const app = express();
const port = 8888;

// --- Config ---
const MAX_CONCURRENT_PAGES = 5;
const REQUEST_TIMEOUT = 30000;
// ----------------

let browser;
let activeRequests = 0;
const queue = [];

// Initialize browser
(async () => {
    try {
        browser = await chromium.launch({
            headless: true,
            args: ['--no-sandbox', '--disable-setuid-sandbox']
        });
        console.log('Browser launched globally.');
    } catch (e) {
        console.error('Failed to launch browser:', e);
        process.exit(1);
    }
})();

// Process Queue
async function processQueue() {
    if (activeRequests >= MAX_CONCURRENT_PAGES || queue.length === 0) {
        return;
    }

    const { req, res } = queue.shift();
    activeRequests++;

    try {
        await handleRender(req, res);
    } catch (e) {
        console.error('Error handling request:', e);
        if (!res.headersSent) {
            res.status(500).send('Internal Server Error');
        }
    } finally {
        activeRequests--;
        processQueue();
    }
}

// Check for private IPs (SSRF Protection)
function isPrivateIP(ip) {
    try {
        const addr = ipaddr.parse(ip);
        if (addr.range() !== 'unicast') { // 'private', 'loopback', 'linkLocal', 'reserved' etc.
            // ipaddr.js classifies as:
            // unicast: global unicast
            // private: 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
            // loopback: 127.0.0.0/8, ::1
            // uniqueLocal: fc00::/7 (IPv6 private)
            // ...
            // We want to allow ONLY unicast (public internet) basically.
            // But 'unicast' covers public IPs.
            // Let's check specifically for what we want to block.
            
            const range = addr.range();
            return ['private', 'loopback', 'linkLocal', 'uniqueLocal', 'carrierGradeNat'].includes(range);
        }
        return false;
    } catch (e) {
        return true; // If invalid IP, treat as unsafe
    }
}

async function validateURL(urlString) {
    try {
        const url = new URL(urlString);
        const hostname = url.hostname;

        // Resolve DNS
        const lookup = await new Promise((resolve, reject) => {
            dns.lookup(hostname, { all: true }, (err, addresses) => {
                if (err) reject(err);
                else resolve(addresses);
            });
        });

        for (const addr of lookup) {
            if (isPrivateIP(addr.address)) {
                return false;
            }
        }
        return true;
    } catch (e) {
        console.error(`DNS lookup failed for ${urlString}:`, e);
        return false;
    }
}

async function handleRender(req, res) {
    const url = req.query.url;
    if (!url) {
        return res.status(400).send('URL is required');
    }

    console.log(`Processing URL: ${url}`);

    // SSRF Check
    const isSafe = await validateURL(url);
    if (!isSafe) {
        console.warn(`Blocked unsafe URL: ${url}`);
        return res.status(403).send('Forbidden: Internal or Invalid URL');
    }

    let page;
    try {
        const context = await browser.newContext();
        page = await context.newPage();

        // Optimized waiting
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: REQUEST_TIMEOUT });
        // Optional: wait a bit for hydration if needed, or rely on domcontentloaded
        // User complained about 'networkidle' hanging. 'domcontentloaded' is faster.
        // We can add a small race/timeout for network idle if we really want, but let's stick to faster.
        
        const content = await page.content();
        res.send(content);

    } catch (error) {
        console.error(`Error rendering ${url}:`, error);
        res.status(500).send('Error rendering page');
    } finally {
        if (page) {
            await page.close().catch(() => {});
        }
    }
}

app.get('/render', (req, res) => {
    queue.push({ req, res });
    processQueue();
});

app.listen(port, () => {
    console.log(`JS Renderer listening at http://localhost:${port}`);
});
