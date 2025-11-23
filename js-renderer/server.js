const express = require('express');
const { chromium } = require('playwright');

const app = express();
const port = 8888;

app.get('/render', async (req, res) => {
    const url = req.query.url;
    if (!url) {
        return res.status(400).send('URL is required');
    }

    console.log(`Rendering URL: ${url}`);

    let browser;
    try {
        browser = await chromium.launch({
            headless: true,
            args: ['--no-sandbox', '--disable-setuid-sandbox']
        });
        const page = await browser.newPage();
        await page.goto(url, { waitUntil: 'networkidle', timeout: 30000 });
        
        // Get the full HTML content
        const content = await page.content();
        
        res.send(content);
    } catch (error) {
        console.error(`Error rendering ${url}:`, error);
        res.status(500).send('Error rendering page');
    } finally {
        if (browser) {
            await browser.close();
        }
    }
});

app.listen(port, () => {
    console.log(`JS Renderer listening at http://localhost:${port}`);
});
