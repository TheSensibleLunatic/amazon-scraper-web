from flask import Flask, render_template_string, request, send_file, jsonify
import asyncio
import threading
import uuid
import os

# Import Scrapers
from scrapers.amazon import AmazonScraper
from scrapers.flipkart import FlipkartScraper
from scrapers.blinkit import BlinkitScraper
from scrapers.zepto import ZeptoScraper
from scrapers.jiomart import JiomartScraper
from scrapers.swiggy import SwiggyScraper
from scrapers.bigbasket import BigBasketScraper

app = Flask(__name__)

# Global dictionary to store job status
JOBS = {}

HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>Universal E-Commerce Scraper</title>
    <style>
        body { font-family: 'Segoe UI', sans-serif; display: flex; flex-direction: column; align-items: center; min-height: 100vh; margin: 0; background: #f0f2f5; padding: 20px; }
        .container { display: flex; gap: 20px; flex-wrap: wrap; justify-content: center; }
        .card { background: white; padding: 30px; border-radius: 12px; box-shadow: 0 8px 24px rgba(0,0,0,0.1); width: 400px; text-align: center; }
        h2 { color: #1a1a1a; margin-bottom: 5px; }
        p { color: #555; font-size: 14px; margin-bottom: 20px; }
        input, select, textarea { width: 100%; padding: 12px; margin: 15px 0; border: 1px solid #ddd; border-radius: 8px; box-sizing: border-box; font-size: 14px; }
        button { background: #007bff; color: white; border: none; padding: 12px; border-radius: 8px; cursor: pointer; width: 100%; font-weight: bold; transition: 0.2s; font-size: 16px; }
        button:hover { background: #0056b3; }
        button:disabled { background: #e7e7e7; cursor: not-allowed; color: #888; }
        
        .progress-box { display: none; margin-top: 15px; text-align: left; background: #f9f9f9; padding: 10px; border-radius: 8px; }
        .status-text { font-size: 13px; color: #333; margin-bottom: 6px; font-weight: 500; }
        .bar-container { width: 100%; background: #ddd; height: 8px; border-radius: 4px; overflow: hidden; }
        .bar { height: 100%; width: 0%; background: #007bff; transition: width 0.3s; }
    </style>
    <script>
        async function startJob(event, type) {
            event.preventDefault();
            const form = event.target;
            const btn = form.querySelector('button');
            const progressBox = form.querySelector('.progress-box');
            
            btn.disabled = true;
            btn.innerText = "Starting...";
            
            const formData = new FormData(form);
            
            const endpoint = type === 'search' ? '/start_scrape' : (type === 'bulk' ? '/start_bulk_scrape' : '/start_review_scrape');

            const response = await fetch(endpoint, { method: 'POST', body: formData });
            const data = await response.json();
            const jobId = data.job_id;

            progressBox.style.display = 'block';
            pollStatus(jobId, form);
        }

        function pollStatus(jobId, form) {
            const statusText = form.querySelector('.status-text');
            const progressBar = form.querySelector('.bar');
            const btn = form.querySelector('button');
            
            const interval = setInterval(async () => {
                const res = await fetch(`/status/${jobId}`);
                const data = await res.json();
                
                statusText.innerText = data.status;
                
                if (data.progress && data.total) {
                    const pct = (data.progress / data.total) * 100;
                    progressBar.style.width = pct + "%";
                } else if (data.done) {
                    progressBar.style.width = "100%";
                } else {
                    progressBar.style.width = "10%"; 
                }
                
                if (data.done) {
                    clearInterval(interval);
                    if (data.filename) {
                        statusText.innerText = "Done! Downloading...";
                        window.location.href = `/download/${data.filename}`;
                    } else {
                        statusText.innerText = "Error: " + data.status;
                    }
                    btn.disabled = false;
                    btn.innerText = "Start Again";
                } 
            }, 1000);
        }
    </script>
</head>
<body>
    <div style="text-align:center; margin-bottom: 30px;">
        <h1 style="color:#1a1a1a;">Universal E-Commerce Scraper</h1>
        <p>Scrape Amazon, Flipkart, Blinkit, Zepto, Jiomart, Swiggy, and Big Basket</p>
    </div>

    <div class="container">
        <!-- Card 1: Product Search -->
        <div class="card">
            <h2>Product Search</h2>
            <p>Scrape Search Results & Details</p>
            <form onsubmit="startJob(event, 'search')">
                <select name="platform" required>
                    <option value="amazon">Amazon</option>
                    <option value="flipkart">Flipkart</option>
                    <!-- <option value="blinkit">Blinkit</option> -->
                    <!-- <option value="zepto">Zepto</option> -->
                    <!-- <option value="jiomart">Jiomart</option> -->
                    <!-- <option value="swiggy">Swiggy Instamart</option> -->
                    <!-- <option value="bigbasket">Big Basket</option> -->
                </select>
                <input type="text" name="url" placeholder="Paste Search Link OR Keyword" required>
                <button type="submit">Get Products CSV</button>
                
                <div class="progress-box">
                    <div class="status-text">Ready</div>
                    <div class="bar-container"><div class="bar"></div></div>
                </div>
            </form>
        </div>

        <!-- Card 2: Bulk Scraper -->
        <div class="card">
            <h2>Bulk Product Scraper</h2>
            <p>Get Details for Multiple Product URLs</p>
            <form onsubmit="startJob(event, 'bulk')">
                <select name="platform" required>
                    <option value="amazon">Amazon</option>
                    <option value="flipkart">Flipkart</option>
                    <!-- <option value="blinkit">Blinkit</option> -->
                    <!-- <option value="zepto">Zepto</option> -->
                    <!-- <option value="jiomart">Jiomart</option> -->
                    <!-- <option value="swiggy">Swiggy Instamart</option> -->
                    <!-- <option value="bigbasket">Big Basket</option> -->
                </select>
                <textarea name="urls" rows="4" placeholder="Paste Product URLs (one per line)" required></textarea>
                <button type="submit">Get Products XLSX</button>
                
                <div class="progress-box">
                    <div class="status-text">Ready</div>
                    <div class="bar-container"><div class="bar"></div></div>
                </div>
            </form>
        </div>

        <!-- Card 3: Reviews -->
        <div class="card">
            <h2>Review Scraper</h2>
            <p>Scrape Reviews</p>
            <form onsubmit="startJob(event, 'reviews')">
                <select name="platform" required>
                    <option value="amazon">Amazon</option>
                    <option value="flipkart">Flipkart (Limited)</option>
                </select>
                <input type="text" name="url" placeholder="Paste Product Page Link" required>
                <button type="submit">Get Reviews CSV</button>
                
                <div class="progress-box">
                    <div class="status-text">Ready</div>
                    <div class="bar-container"><div class="bar"></div></div>
                </div>
            </form>
        </div>
    </div>
</body>
</html>
'''

def get_scraper(platform, job_id):
    if platform == 'amazon': return AmazonScraper(job_id, JOBS)
    if platform == 'flipkart': return FlipkartScraper(job_id, JOBS)
    if platform == 'blinkit': return BlinkitScraper(job_id, JOBS)
    if platform == 'zepto': return ZeptoScraper(job_id, JOBS)
    if platform == 'jiomart': return JiomartScraper(job_id, JOBS)
    if platform == 'swiggy': return SwiggyScraper(job_id, JOBS)
    if platform == 'bigbasket': return BigBasketScraper(job_id, JOBS)
    return None

def run_async_job(func, *args):
    asyncio.run(func(*args))

@app.route('/')
def index(): return render_template_string(HTML_TEMPLATE)

@app.route('/start_scrape', methods=['POST'])
def start_scrape():
    platform = request.form.get('platform')
    url = request.form.get('url')
    job_id = str(uuid.uuid4())
    JOBS[job_id] = {"status": "Queued", "done": False}
    
    scraper = get_scraper(platform, job_id)
    if scraper:
        threading.Thread(target=run_async_job, args=(scraper.run_search, url)).start()
        return jsonify({"job_id": job_id})
    return jsonify({"error": "Invalid Platform"}), 400

@app.route('/start_bulk_scrape', methods=['POST'])
def start_bulk_scrape():
    platform = request.form.get('platform')
    url_text = request.form.get('urls')
    job_id = str(uuid.uuid4())
    JOBS[job_id] = {"status": "Queued", "done": False}
    
    scraper = get_scraper(platform, job_id)
    if scraper:
        threading.Thread(target=run_async_job, args=(scraper.run_bulk, url_text)).start()
        return jsonify({"job_id": job_id})
    return jsonify({"error": "Invalid Platform"}), 400

@app.route('/start_review_scrape', methods=['POST'])
def start_review_scrape():
    platform = request.form.get('platform')
    url = request.form.get('url')
    job_id = str(uuid.uuid4())
    JOBS[job_id] = {"status": "Queued", "done": False}
    
    scraper = get_scraper(platform, job_id)
    if scraper:
        threading.Thread(target=run_async_job, args=(scraper.run_reviews, url)).start()
        return jsonify({"job_id": job_id})
    return jsonify({"error": "Invalid Platform"}), 400

@app.route('/status/<job_id>')
def status(job_id):
    return jsonify(JOBS.get(job_id, {"status": "Unknown", "done": True}))

@app.route('/download/<filename>')
def download(filename):
    return send_file(filename, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True, port=5000)