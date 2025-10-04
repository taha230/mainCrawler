

<div align="center" style="margin-bottom:30px;">
  <img width="500" alt="tor_plus_google_organic_results" src="https://github.com/user-attachments/assets/32d41ced-b31f-4233-bc4c-a8bc271df73a" />
</div>
<br><br> <!-- add more <br> if you need more space -->



# 🌐 Main Crawler – B2B Websites Extractor

This project is a **web crawler** designed to extract supplier information and websites from major **B2B trading platforms**.  
It stores results in **MongoDB**, using `unique_key` to ensure no duplicates, and provides a modular crawler setup so new platforms can be easily added.

---

## 🚀 Supported Platforms
Currently supports crawling suppliers from:
- 🛒 **Alibaba**
- 🏭 **ECVV**
- 🌍 **TradeBoss**
- 📦 **eWorldTrade**
- 📑 **GlobalSources**
- 🔎 **Go4WorldBusiness (go4w)**
- 📈 **Importer TradeKey**
- 🇮🇳 **IndiaMart**
- 🏗 **Made-in-China**

---

## 🛠 Tech Stack
- **Python 3**
- **MongoDB** (for supplier storage, using `unique_key`)
- **Requests / HTTP clients**
- **BeautifulSoup / lxml** for HTML parsing
- Optional: **Scrapy** framework for modular crawlers

---

## 📦 Installation
Clone the repository and install dependencies:

```bash
git clone https://github.com/your-username/supplier-crawler.git
cd supplier-crawler
pip install -r requirements.txt
Set up MongoDB (local or cloud):

🛤 Roadmap
 Add automatic scheduling with Celery or Cron.

 Implement proxy rotation to bypass request limits.

 Add captcha handling where needed.

 Integrate ScrapingBee/Oxylabs for Cloudflare-protected sites.

 Build web dashboard to monitor crawls.

🤝 Contributing
Pull requests are welcome! Please open an issue if you’d like to request support for another B2B platform.

📬 Contact
Developer: Taha Hamedani
Email: taha.hamedani8@gmail.com
