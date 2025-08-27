import os
import re
import asyncio
import json
import logging
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse, parse_qs

import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from fastapi import FastAPI, BackgroundTasks, HTTPException
import uvicorn

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("foreclosure-scraper")

# Configuration
BASE_URL = "https://salesweb.civilview.com/"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_INFO = os.getenv('GOOGLE_CREDENTIALS')
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')
LOCAL_CSV_PATH = "foreclosure_sales.csv"
LAST_SCRAPED_FILE = "last_scraped.json"

TARGET_COUNTIES = [
    {"county_id": "52", "county_name": "Cape May County, NJ"},
    {"county_id": "1", "county_name": "Camden County, NJ"},
    {"county_id": "3", "county_name": "Burlington County, NJ"},
    {"county_id": "6", "county_name": "Cumberland County, NJ"},
    {"county_id": "19", "county_name": "Gloucester County, NJ"},
    {"county_id": "20", "county_name": "Salem County, NJ"},
    {"county_id": "15", "county_name": "Union County, NJ"}
]

app = FastAPI(title="Foreclosure Scraper API")

class ForeclosureScraper:
    def __init__(self):
        self.credentials = None
        self.service = None
        self.scheduler = AsyncIOScheduler()
        self.setup_google_credentials()
        self.last_scraped_ids = self.load_last_scraped_ids()
        
    def load_last_scraped_ids(self):
        try:
            if os.path.exists(LAST_SCRAPED_FILE):
                with open(LAST_SCRAPED_FILE, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Error loading last scraped IDs: {e}")
        return {}
    
    def save_last_scraped_ids(self):
        try:
            with open(LAST_SCRAPED_FILE, 'w') as f:
                json.dump(self.last_scraped_ids, f)
        except Exception as e:
            logger.error(f"Error saving last scraped IDs: {e}")

    def setup_google_credentials(self):
        try:
            if not SERVICE_ACCOUNT_INFO:
                raise ValueError("GOOGLE_CREDENTIALS environment variable is missing")
                
            service_account_info = json.loads(SERVICE_ACCOUNT_INFO)
            self.credentials = service_account.Credentials.from_service_account_info(
                service_account_info, scopes=SCOPES)
            self.service = build('sheets', 'v4', credentials=self.credentials)
            logger.info("Google Sheets API client initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing Google Sheets client: {e}")
            self.service = None

    def norm_text(self, s: str) -> str:
        if not s:
            return ""
        return re.sub(r"\s+", " ", s).strip()

    def extract_property_id_from_href(self, href: str) -> str:
        try:
            q = parse_qs(urlparse(href).query)
            return q.get("PropertyId", [""])[0]
        except Exception:
            return ""

    async def goto_with_retry(self, page, url: str, max_retries=3):
        for attempt in range(max_retries):
            try:
                response = await page.goto(url, wait_until="networkidle", timeout=60000)
                if response and response.status == 200:
                    return response
                logger.warning(f"Attempt {attempt+1}/{max_retries} failed for {url}")
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                logger.warning(f"Attempt {attempt+1}/{max_retries} failed: {str(e)}")
                await asyncio.sleep(2 ** attempt)
        return None

    async def dismiss_banners(self, page):
        selectors = [
            "button:has-text('Accept')", "button:has-text('I Agree')",
            "button:has-text('Close')", "button.cookie-accept",
            "button[aria-label='Close']", ".modal-footer button:has-text('OK')",
        ]
        for sel in selectors:
            try:
                loc = page.locator(sel)
                if await loc.count():
                    await loc.first.click(timeout=1500)
                    await page.wait_for_timeout(200)
            except Exception:
                pass

    async def scrape_county_sales(self, page, county, max_retries=3):
        url = f"{BASE_URL}Sales/SalesSearch?countyId={county['county_id']}"
        logger.info(f"Scraping {county['county_name']} -> {url}")

        for attempt in range(max_retries):
            try:
                await self.goto_with_retry(page, url)
                await self.dismiss_banners(page)

                try:
                    await page.wait_for_selector("table.table.table-striped tbody tr, .no-sales, #noData", timeout=30000)
                except PlaywrightTimeoutError:
                    logger.warning(f"No sales found for {county['county_name']}")
                    return []

                rows = page.locator("table.table.table-striped tbody tr")
                n = await rows.count()
                results = []
                new_properties_found = False

                for i in range(n):
                    row = rows.nth(i)
                    details_a = row.locator("td.hidden-print a")
                    details_href = await details_a.get_attribute("href") or ""
                    details_url = details_href if details_href.startswith("http") else urljoin(BASE_URL, details_href)
                    property_id = self.extract_property_id_from_href(details_href)

                    # Check if we've already scraped this property
                    county_key = county['county_name']
                    if county_key in self.last_scraped_ids and property_id in self.last_scraped_ids[county_key]:
                        logger.info(f"Skipping already scraped property: {property_id}")
                        continue
                    
                    new_properties_found = True

                    # Scrape from table first
                    try:
                        sales_date = self.norm_text(await row.locator("td").nth(2).inner_text())
                    except Exception:
                        sales_date = ""
                    try:
                        defendant = self.norm_text(await row.locator("td").nth(4).inner_text())
                    except Exception:
                        defendant = ""
                    
                    # Address extraction
                    try:
                        tds = row.locator("td")
                        td_count = await tds.count()
                        logger.debug(f"{county['county_name']} - Row {i+1} has {td_count} columns")
                        
                        if td_count >= 6:
                            prop_address = self.norm_text(await tds.nth(5).inner_text())
                            logger.debug(f"Extracted address from table: '{prop_address}'")
                        else:
                            prop_address = ""
                            logger.debug(f"Not enough columns ({td_count}) to extract address from table")
                    except Exception as e:
                        prop_address = ""
                        logger.debug(f"Error extracting address from table: {e}")
                    
                    approx_judgment = ""

                    # Get details from details page
                    if details_url:
                        try:
                            logger.debug(f"Navigating to details page: {details_url}")
                            await self.goto_with_retry(page, details_url)
                            await self.dismiss_banners(page)
                            
                            try:
                                await page.wait_for_selector(".sale-details-list", timeout=30000)
                                items = page.locator(".sale-details-list .sale-detail-item")
                            except PlaywrightTimeoutError:
                                logger.warning(f".sale-details-list not found for {county['county_name']} (PropertyId: {property_id})")
                                items = page.locator(".sale-detail-item")  # fallback

                            for j in range(await items.count()):
                                label = self.norm_text(await items.nth(j).locator(".sale-detail-label").inner_text())
                                val = self.norm_text(await items.nth(j).locator(".sale-detail-value").inner_text())

                                # Address fallback or cleaning
                                if ("Address" in label or "Property Address" in label):
                                    try:
                                        val_html = await items.nth(j).locator(".sale-detail-value").inner_html()
                                        val_html = re.sub(r"<br\s*/?>", " ", val_html)
                                        val_clean = re.sub(r"<.*?>", "", val_html).strip()
                                        details_address = self.norm_text(val_clean)
                                        logger.debug(f"Found address in details page: '{details_address}'")
                                        if not prop_address or len(details_address) > len(prop_address):
                                            prop_address = details_address
                                            logger.debug(f"Using details page address: '{prop_address}'")
                                    except Exception as e:
                                        logger.debug(f"Error processing address from details: {e}")
                                        if not prop_address:
                                            prop_address = self.norm_text(val)
                                # Handle both Approx. Judgment and Approx. Upset
                                elif ("Approx. Judgment" in label or "Approx. Upset" in label or "Approximate Judgment:" in label):
                                    approx_judgment = val
                                elif "Defendant" in label and not defendant:
                                    defendant = val
                                elif "Sale Date" in label and not sales_date:
                                    sales_date = val

                        except Exception as e:
                            logger.error(f"Error scraping details for {county['county_name']}: {str(e)}")
                        finally:
                            await self.goto_with_retry(page, url)
                            await self.dismiss_banners(page)
                            await page.wait_for_selector("table.table.table-striped tbody tr", timeout=30000)

                    logger.debug(f"Final data for row {i+1}: ID='{property_id}', Address='{prop_address}', Defendant='{defendant}', Date='{sales_date}'")
                    
                    results.append({
                        "Property ID": property_id,
                        "Address": prop_address,
                        "Defendant": defendant,
                        "Sales Date": sales_date,
                        "Approx Judgment": approx_judgment,
                        "County": county['county_name'],
                        "Scrape Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })

                # If no new properties found, return empty results
                if not new_properties_found:
                    logger.info(f"No new properties found for {county['county_name']}")
                    return []

                # Check for missing essential fields in any record
                missing = [
                    r for r in results
                    if not all([r["Property ID"], r["Address"], r["Defendant"], r["Sales Date"]])
                ]
                if missing:
                    logger.warning(f"Missing fields detected for {county['county_name']} (Attempt {attempt+1}/{max_retries})")
                    for idx, r in enumerate(missing):
                        missing_fields = [k for k, v in r.items() if k in ["Property ID", "Address", "Defendant", "Sales Date"] and not v]
                        logger.warning(f"  Row {idx+1}: Missing {missing_fields}")
                    await asyncio.sleep(2 ** attempt)
                    continue  # Retry
                
                # Update last scraped IDs for this county
                if county_key not in self.last_scraped_ids:
                    self.last_scraped_ids[county_key] = []
                self.last_scraped_ids[county_key].extend([r["Property ID"] for r in results])
                self.save_last_scraped_ids()
                
                return results
            except Exception as e:
                logger.error(f"Error scraping {county['county_name']} (Attempt {attempt+1}/{max_retries}): {str(e)}")
                await asyncio.sleep(2 ** attempt)
                continue
        logger.error(f"Could not get complete data for {county['county_name']} after {max_retries} attempts.")
        return []

    def save_to_csv(self, data):
        try:
            df = pd.DataFrame(data)
            df.to_csv(LOCAL_CSV_PATH, index=False, encoding='utf-8')
            logger.info(f"Data saved to local CSV: {LOCAL_CSV_PATH}")
            return True
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")
            return False

    async def update_google_sheet_tab(self, df, sheet_name):
        """Update one Google Sheet tab (per county or All Data)"""
        try:
            if not self.service:
                logger.error("Google Sheets service not initialized")
                return False

            sheet = self.service.spreadsheets()

            # Ensure sheet exists
            try:
                # Check if sheet exists
                spreadsheet = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
                sheet_exists = any(s['properties']['title'] == sheet_name for s in spreadsheet['sheets'])
                
                if not sheet_exists:
                    add_sheet_request = {"addSheet": {"properties": {"title": sheet_name}}}
                    self.service.spreadsheets().batchUpdate(
                        spreadsheetId=SPREADSHEET_ID,
                        body={"requests": [add_sheet_request]}
                    ).execute()
                    logger.info(f"Created new sheet: {sheet_name}")
            except HttpError as e:
                logger.error(f"Error checking/creating sheet: {e}")
                return False

            # Prepare values with date header
            today = datetime.now().strftime("%Y-%m-%d")
            date_header = [[f"Data scraped on: {today}"]]
            column_headers = [list(df.columns)]
            values = date_header + [[]] + column_headers  # Add empty row for spacing
            
            for _, row in df.iterrows():
                row_values = []
                for col in df.columns:
                    val = row[col]
                    if isinstance(val, (pd.Timestamp, datetime)):
                        row_values.append(val.strftime("%Y-%m-%d"))
                    else:
                        row_values.append("" if pd.isna(val) else str(val))
                values.append(row_values)

            # Clear & update
            sheet.values().clear(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{sheet_name}'!A:Z"
            ).execute()
            
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{sheet_name}'!A1",
                valueInputOption="USER_ENTERED",
                body={"values": values}
            ).execute()

            # Format the date header
            try:
                format_request = {
                    "requests": [
                        {
                            "repeatCell": {
                                "range": {
                                    "sheetId": self.get_sheet_id(sheet_name),
                                    "startRowIndex": 0,
                                    "endRowIndex": 1
                                },
                                "cell": {
                                    "userEnteredFormat": {
                                        "backgroundColor": {"red": 0.8, "green": 0.8, "blue": 0.8},
                                        "textFormat": {"bold": True, "fontSize": 12}
                                    }
                                },
                                "fields": "userEnteredFormat(backgroundColor,textFormat)"
                            }
                        }
                    ]
                }
                sheet.batchUpdate(
                    spreadsheetId=SPREADSHEET_ID,
                    body=format_request
                ).execute()
            except Exception as e:
                logger.warning(f"Could not format header: {e}")

            logger.info(f"Updated Google Sheet tab: {sheet_name} ({len(df)} rows)")
            return True
        except Exception as e:
            logger.error(f"Google Sheets update error on {sheet_name}: {e}")
            return False

    def get_sheet_id(self, sheet_name):
        """Get the sheet ID for a given sheet name"""
        try:
            spreadsheet = self.service.spreadsheets().get(
                spreadsheetId=SPREADSHEET_ID
            ).execute()
            
            for sheet in spreadsheet['sheets']:
                if sheet['properties']['title'] == sheet_name:
                    return sheet['properties']['sheetId']
        except Exception as e:
            logger.error(f"Error getting sheet ID for {sheet_name}: {e}")
        return None

    async def scrape_all_counties(self):
        all_data = []
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            for county in TARGET_COUNTIES:
                try:
                    county_data = await self.scrape_county_sales(page, county)
                    all_data.extend(county_data)
                    logger.info(f"Completed {county['county_name']}: {len(county_data)} records")

                    # Update Google Sheet tab immediately
                    if county_data and self.service:
                        df_county = pd.DataFrame(county_data)
                        await self.update_google_sheet_tab(df_county, county["county_name"][:30])

                    await asyncio.sleep(2)
                except Exception as e:
                    logger.error(f"Failed to scrape {county['county_name']}: {str(e)}")
                    continue
            
            await browser.close()
        
        return all_data

    async def scheduled_scrape(self):
        logger.info(f"Starting scrape at {datetime.now()}")
        data = await self.scrape_all_counties()
        
        if data:
            self.save_to_csv(data)

            # Update "All Data" at the end
            if self.service:
                df_all = pd.DataFrame(data)
                await self.update_google_sheet_tab(df_all, "All Data")
        else:
            logger.info("No data scraped")
            
        logger.info(f"Finished scrape at {datetime.now()}")
        return data

# Global scraper instance
scraper = ForeclosureScraper()

@app.get("/")
async def root():
    return {"message": "Foreclosure Scraper API", "status": "active"}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/scrape")
async def run_scrape(background_tasks: BackgroundTasks):
    """Trigger a scrape manually"""
    background_tasks.add_task(scraper.scheduled_scrape)
    return {"message": "Scrape started in background", "start_time": datetime.now().isoformat()}

@app.get("/status")
async def get_status():
    """Get scraping status"""
    return {
        "last_scraped_ids": scraper.last_scraped_ids,
        "target_counties": TARGET_COUNTIES,
        "spreadsheet_id": SPREADSHEET_ID
    }

def schedule_daily_scrape():
    """Schedule the daily scrape at 9:30 AM EST"""
    trigger = CronTrigger(hour=9, minute=30, timezone="America/New_York")
    scraper.scheduler.add_job(
        scraper.scheduled_scrape,
        trigger=trigger,
        id="daily_scrape",
        name="Daily foreclosure data scrape at 9:30 AM EST",
        replace_existing=True
    )
    scraper.scheduler.start()
    logger.info("Scheduled daily scrape at 9:30 AM EST")

@app.on_event("startup")
async def startup_event():
    schedule_daily_scrape()

if __name__ == "__main__":
    # Start the FastAPI server
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")