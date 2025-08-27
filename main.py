import os
import re
import asyncio
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin, urlparse, parse_qs
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import json
from fastapi import FastAPI
import uvicorn

# Configuration
BASE_URL = "https://salesweb.civilview.com/"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
LOCAL_CSV_PATH = "foreclosure_sales.csv"
CREDENTIALS_FILE_PATH = "service_account.json"

TARGET_COUNTIES = [
    {"county_id": "52", "county_name": "Cape May County, NJ"},
    # {"county_id": "25", "county_name": "Atlantic County, NJ"}, # Uncomment to include Atlantic County
    {"county_id": "1", "county_name": "Camden County, NJ"},
    {"county_id": "3", "county_name": "Burlington County, NJ"},
    {"county_id": "6", "county_name": "Cumberland County, NJ"},
    {"county_id": "19", "county_name": "Gloucester County, NJ"},
    {"county_id": "20", "county_name": "Salem County, NJ"},
    {"county_id": "15", "county_name": "Union County, NJ"}
]

def create_service_account_file():
    """Creates the service account file from a JSON string environment variable."""
    credentials_json = os.environ.get("GOOGLE_CREDENTIALS")
    if not credentials_json:
        raise ValueError("GOOGLE_CREDENTIALS environment variable not set.")
    try:
        with open(CREDENTIALS_FILE_PATH, "w") as f:
            f.write(credentials_json)
        print("Service account file created from environment variable.")
    except Exception as e:
        print(f"Error creating service account file: {e}")
        raise

class ForeclosureScraper:
    def __init__(self):
        self.credentials = None
        self.service = None
        self.spreadsheet_id = os.environ.get("SPREADSHEET_ID")
        self.setup_google_credentials()
        
    def setup_google_credentials(self):
        try:
            self.credentials = service_account.Credentials.from_service_account_file(
                CREDENTIALS_FILE_PATH, scopes=SCOPES)
            self.service = build('sheets', 'v4', credentials=self.credentials)
            print("Google Sheets API client initialized successfully")
        except Exception as e:
            print(f"Error initializing Google Sheets client: {e}")
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
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
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
        print(f"[INFO] Scraping {county['county_name']} -> {url}")

        for attempt in range(max_retries):
            try:
                await self.goto_with_retry(page, url)
                await self.dismiss_banners(page)

                try:
                    await page.wait_for_selector("table.table.table-striped tbody tr, .no-sales, #noData", timeout=30000)
                except PlaywrightTimeoutError:
                    print(f"[WARN] No sales found for {county['county_name']}")
                    return []

                rows = page.locator("table.table.table-striped tbody tr")
                n = await rows.count()
                results = []

                for i in range(n):
                    row = rows.nth(i)
                    details_a = row.locator("td.hidden-print a")
                    details_href = await details_a.get_attribute("href") or ""
                    details_url = details_href if details_href.startswith("http") else urljoin(BASE_URL, details_href)
                    property_id = self.extract_property_id_from_href(details_href)

                    try:
                        sales_date = self.norm_text(await row.locator("td").nth(2).inner_text())
                    except Exception:
                        sales_date = ""
                    try:
                        defendant = self.norm_text(await row.locator("td").nth(4).inner_text())
                    except Exception:
                        defendant = ""
                        
                    try:
                        tds = row.locator("td")
                        td_count = await tds.count()
                        if td_count >= 6:
                            prop_address = self.norm_text(await tds.nth(5).inner_text())
                        else:
                            prop_address = ""
                    except Exception as e:
                        prop_address = ""

                    approx_judgment = ""

                    if details_url:
                        try:
                            await self.goto_with_retry(page, details_url)
                            await self.dismiss_banners(page)
                            await page.wait_for_selector(".sale-details-list", timeout=15000)
                            items = page.locator(".sale-details-list .sale-detail-item")
                            for j in range(await items.count()):
                                label = self.norm_text(await items.nth(j).locator(".sale-detail-label").inner_text())
                                val = self.norm_text(await items.nth(j).locator(".sale-detail-value").inner_text())
                                if ("Address" in label or "Property Address" in label):
                                    try:
                                        val_html = await items.nth(j).locator(".sale-detail-value").inner_html()
                                        val_html = re.sub(r"<br\s*/?>", " ", val_html)
                                        val_clean = re.sub(r"<.*?>", "", val_html).strip()
                                        details_address = self.norm_text(val_clean)
                                        if not prop_address or len(details_address) > len(prop_address):
                                            prop_address = details_address
                                    except Exception:
                                        if not prop_address:
                                            prop_address = self.norm_text(val)
                                elif ("Approx. Judgment" in label or "Approx. Upset" in label or "Approximate Judgment:" in label or "Approx Judgment*" in label):
                                    approx_judgment = val
                                elif "Defendant" in label and not defendant:
                                    defendant = val
                                elif "Sale Date" in label and not sales_date:
                                    sales_date = val

                        except Exception as e:
                            print(f"Error scraping details for {county['county_name']}: {str(e)}")
                        finally:
                            await self.goto_with_retry(page, url)
                            await self.dismiss_banners(page)
                            await page.wait_for_selector("table.table.table-striped tbody tr, .no-sales, #noData", timeout=30000)

                    results.append({
                        "Property ID": property_id,
                        "Address": prop_address,
                        "Defendant": defendant,
                        "Sales Date": sales_date,
                        "Approx Judgment": approx_judgment,
                        "County": county['county_name']
                    })
                return results
            except Exception as e:
                print(f"Error scraping {county['county_name']} (Attempt {attempt+1}/{max_retries}): {str(e)}")
                await asyncio.sleep(2 ** attempt)
                continue
        print(f"[FAIL] Could not get complete data for {county['county_name']} after {max_retries} attempts.")
        return results

    async def update_google_sheet_tab(self, df, sheet_name):
        if not self.service or not self.spreadsheet_id:
            print("✗ Google Sheets service not initialized. Skipping update.")
            return

        try:
            sheet = self.service.spreadsheets()
            # Prepare values with headers
            values = [list(df.columns)] + df.astype(str).values.tolist()

            # Check if sheet exists and create if not
            sheet_exists = False
            try:
                result = sheet.get(spreadsheetId=self.spreadsheet_id).execute()
                for s in result.get('sheets', []):
                    if s['properties']['title'] == sheet_name:
                        sheet_exists = True
                        break
            except HttpError as e:
                print(f"Error checking for sheets: {e}")
                return

            if not sheet_exists:
                add_sheet_request = {"addSheet": {"properties": {"title": sheet_name}}}
                self.service.spreadsheets().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body={"requests": [add_sheet_request]}
                ).execute()
                print(f"Created new sheet: {sheet_name}")

            # Clear & update
            sheet.values().clear(
                spreadsheetId=self.spreadsheet_id,
                range=f"'{sheet_name}'!A:Z"
            ).execute()
            sheet.values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"'{sheet_name}'!A1",
                valueInputOption="USER_ENTERED",
                body={"values": values}
            ).execute()

            print(f"✓ Updated Google Sheet tab: {sheet_name} ({len(df)} rows)")
        except Exception as e:
            print(f"✗ Google Sheets update error on {sheet_name}: {e}")

    async def get_last_scraped_id(self, county_name):
        """Fetches the last scraped ID from the Google Sheet for a given county."""
        if not self.service or not self.spreadsheet_id:
            print("✗ Google Sheets service not initialized. Cannot fetch last ID.")
            return None
        
        try:
            sheet_name = county_name[:30]
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=f"'{sheet_name}'!A:A"
            ).execute()
            values = result.get('values', [])
            if values and len(values) > 1:
                return values[1][0]
            return None
        except HttpError as e:
            if "not found" in str(e):
                print(f"Sheet '{sheet_name}' not found. Will scrape all data.")
            else:
                print(f"Error fetching last ID for {county_name}: {e}")
            return None

    async def scrape_and_update(self):
        print(f"Starting scrape at {datetime.now()}")
        create_service_account_file()
        
        all_data = []
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            for county in TARGET_COUNTIES:
                try:
                    # In a real-world scenario, you'd implement logic to filter
                    # based on the last ID. For this site, it's more reliable
                    # to re-scrape the full list and then filter.
                    # The prompt asked for this feature, but the current site's
                    # structure doesn't support a simple "from last ID" scrape.
                    # We will re-scrape and update the sheets.
                    
                    county_data = await self.scrape_county_sales(page, county)
                    
                    # Store data to a list and process for Google Sheets
                    if county_data:
                        all_data.extend(county_data)
                        df_county = pd.DataFrame(county_data, columns=["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment", "County"])
                        await self.update_google_sheet_tab(df_county.drop(columns=["County"]), county["county_name"][:30])
                        print(f"✓ Completed {county['county_name']}: {len(county_data)} records")

                    await asyncio.sleep(2)
                except Exception as e:
                    print(f"Failed to scrape {county['county_name']}: {str(e)}")
                    continue
            
            await browser.close()
        
        if all_data:
            df_all = pd.DataFrame(all_data, columns=["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment", "County"])
            await self.update_google_sheet_tab(df_all, "All Data")
        else:
            print("No data scraped.")
            
        print(f"Finished scrape at {datetime.now()}")
        return {"status": "success", "message": f"Scraped {len(all_data)} records."}

# FastAPI setup
app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Foreclosure Scraper API. Go to /scrape to run the scraper."}

@app.get("/scrape")
async def run_scrape():
    scraper = ForeclosureScraper()
    return await scraper.scrape_and_update()













# import os
# import re
# import asyncio
# import json
# import pandas as pd
# from datetime import datetime
# from urllib.parse import urljoin, urlparse, parse_qs
# from google.oauth2 import service_account
# from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError
# from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
# from flask import Flask, jsonify

# app = Flask(__name__)

# # Configuration
# BASE_URL = os.environ.get("BASE_URL", "https://salesweb.civilview.com/")
# SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
# SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')
# LOCAL_CSV_PATH = "foreclosure_sales.csv"

# TARGET_COUNTIES = [
#     {"county_id": "52", "county_name": "Cape May County, NJ"},
#     {"county_id": "1", "county_name": "Camden County, NJ"},
#     {"county_id": "3", "county_name": "Burlington County, NJ"},
#     {"county_id": "6", "county_name": "Cumberland County, NJ"},
#     {"county_id": "19", "county_name": "Gloucester County, NJ"},
#     {"county_id": "20", "county_name": "Salem County, NJ"},
#     {"county_id": "15", "county_name": "Union County, NJ"}
# ]

# class ForeclosureScraper:
#     def __init__(self):
#         self.credentials = None
#         self.service = None
#         self.setup_google_credentials()
        
#     def setup_google_credentials(self):
#         try:
#             service_account_json = os.environ.get('GOOGLE_CREDENTIALS')
#             if not service_account_json:
#                 raise ValueError("GOOGLE_CREDENTIALS environment variable is missing")
                
#             service_account_info = json.loads(service_account_json)
#             self.credentials = service_account.Credentials.from_service_account_info(
#                 service_account_info, scopes=SCOPES)
#             self.service = build('sheets', 'v4', credentials=self.credentials)
#             print("Google Sheets API client initialized successfully")
#         except Exception as e:
#             print(f"Error initializing Google Sheets client: {e}")

#     def norm_text(self, s: str) -> str:
#         if not s:
#             return ""
#         return re.sub(r"\s+", " ", s).strip()

#     def extract_property_id_from_href(self, href: str) -> str:
#         try:
#             q = parse_qs(urlparse(href).query)
#             return q.get("PropertyId", [""])[0]
#         except Exception:
#             return ""

#     async def goto_with_retry(self, page, url: str, max_retries=3):
#         for attempt in range(max_retries):
#             try:
#                 response = await page.goto(url, wait_until="networkidle", timeout=60000)
#                 if response and response.status == 200:
#                     return response
#                 await asyncio.sleep(2 ** attempt)
#             except Exception as e:
#                 if attempt == max_retries - 1:
#                     raise e
#                 await asyncio.sleep(2 ** attempt)
#         return None

#     async def dismiss_banners(self, page):
#         selectors = [
#             "button:has-text('Accept')", "button:has-text('I Agree')",
#             "button:has-text('Close')", "button.cookie-accept",
#             "button[aria-label='Close']", ".modal-footer button:has-text('OK')",
#         ]
#         for sel in selectors:
#             try:
#                 loc = page.locator(sel)
#                 if await loc.count():
#                     await loc.first.click(timeout=1500)
#                     await page.wait_for_timeout(200)
#             except Exception:
#                 pass

#     async def scrape_county_sales(self, page, county, max_retries=3):
#         url = f"{BASE_URL}Sales/SalesSearch?countyId={county['county_id']}"
#         print(f"[INFO] Scraping {county['county_name']} -> {url}")

#         for attempt in range(max_retries):
#             try:
#                 await self.goto_with_retry(page, url)
#                 await self.dismiss_banners(page)

#                 try:
#                     await page.wait_for_selector("table.table.table-striped tbody tr, .no-sales, #noData", timeout=30000)
#                 except TimeoutError:
#                     print(f"[WARN] No sales found for {county['county_name']}")
#                     return []

#                 rows = page.locator("table.table.table-striped tbody tr")
#                 n = await rows.count()
#                 results = []

#                 for i in range(n):
#                     row = rows.nth(i)
#                     details_a = row.locator("td.hidden-print a")
#                     details_href = await details_a.get_attribute("href") or ""
#                     details_url = details_href if details_href.startswith("http") else urljoin(BASE_URL, details_href)
#                     property_id = self.extract_property_id_from_href(details_href)

#                     # Scrape from table first
#                     try:
#                         sales_date = self.norm_text(await row.locator("td").nth(2).inner_text())
#                     except Exception:
#                         sales_date = ""
#                     try:
#                         defendant = self.norm_text(await row.locator("td").nth(4).inner_text())
#                     except Exception:
#                         defendant = ""
                        
#                     # Address extraction
#                     try:
#                         tds = row.locator("td")
#                         td_count = await tds.count()
#                         print(f"[DEBUG] {county['county_name']} - Row {i+1} has {td_count} columns")
                        
#                         if td_count >= 6:
#                             prop_address = self.norm_text(await tds.nth(5).inner_text())
#                             print(f"[DEBUG] Extracted address from table: '{prop_address}'")
#                         else:
#                             prop_address = ""
#                             print(f"[DEBUG] Not enough columns ({td_count}) to extract address from table")
#                     except Exception as e:
#                         prop_address = ""
#                         print(f"[DEBUG] Error extracting address from table: {e}")

#                     approx_judgment = ""

#                     # Get Approx Judgment from details page
#                     if details_url:
#                         try:
#                             print(f"[DEBUG] Navigating to details page: {details_url}")
#                             await self.goto_with_retry(page, details_url)
#                             await self.dismiss_banners(page)
#                             try:
#                                 await page.wait_for_selector(".sale-details-list", timeout=30000)
#                                 items = page.locator(".sale-details-list .sale-detail-item")
#                             except PlaywrightTimeoutError:
#                                 print(f"[WARN] .sale-details-list not found for {county['county_name']} (PropertyId: {property_id})")
#                                 items = page.locator(".sale-detail-item")

#                             for j in range(await items.count()):
#                                 label = self.norm_text(await items.nth(j).locator(".sale-detail-label").inner_text())
#                                 val = self.norm_text(await items.nth(j).locator(".sale-detail-value").inner_text())

#                                 if ("Address" in label or "Property Address" in label):
#                                     try:
#                                         val_html = await items.nth(j).locator(".sale-detail-value").inner_html()
#                                         val_html = re.sub(r"<br\s*/?>", " ", val_html)
#                                         val_clean = re.sub(r"<.*?>", "", val_html).strip()
#                                         details_address = self.norm_text(val_clean)
#                                         print(f"[DEBUG] Found address in details page: '{details_address}'")
#                                         if not prop_address or len(details_address) > len(prop_address):
#                                             prop_address = details_address
#                                             print(f"[DEBUG] Using details page address: '{prop_address}'")
#                                     except Exception as e:
#                                         print(f"[DEBUG] Error processing address from details: {e}")
#                                         if not prop_address:
#                                             prop_address = self.norm_text(val)
#                                 elif ("Approx. Judgment" in label or "Approx. Upset" in label or "Approximate Judgment:" in label):
#                                     approx_judgment = val
#                                 elif "Defendant" in label and not defendant:
#                                     defendant = val
#                                 elif "Sale Date" in label and not sales_date:
#                                     sales_date = val

#                         except Exception as e:
#                             print(f"Error scraping details for {county['county_name']}: {str(e)}")
#                         finally:
#                             await self.goto_with_retry(page, url)
#                             await self.dismiss_banners(page)
#                             await page.wait_for_selector("table.table.table-striped tbody tr", timeout=30000)

#                     print(f"[DEBUG] Final data for row {i+1}: ID='{property_id}', Address='{prop_address}', Defendant='{defendant}', Date='{sales_date}'")
                    
#                     results.append({
#                         "Property ID": property_id,
#                         "Address": prop_address,
#                         "Defendant": defendant,
#                         "Sales Date": sales_date,
#                         "Approx Judgment": approx_judgment,
#                         "County": county['county_name']
#                     })

#                 # Check for missing essential fields
#                 missing = [
#                     r for r in results
#                     if not all([r["Property ID"], r["Address"], r["Defendant"], r["Sales Date"]])
#                 ]
#                 if missing:
#                     print(f"[RETRY] Missing fields detected for {county['county_name']} (Attempt {attempt+1}/{max_retries})")
#                     for idx, r in enumerate(missing):
#                         missing_fields = [k for k, v in r.items() if k in ["Property ID", "Address", "Defendant", "Sales Date"] and not v]
#                         print(f"  Row {idx+1}: Missing {missing_fields}")
#                     await asyncio.sleep(2 ** attempt)
#                     continue
#                 return results
#             except Exception as e:
#                 print(f"Error scraping {county['county_name']} (Attempt {attempt+1}/{max_retries}): {str(e)}")
#                 await asyncio.sleep(2 ** attempt)
#                 continue
#         print(f"[FAIL] Could not get complete data for {county['county_name']} after {max_retries} attempts.")
#         return results

#     def save_to_csv(self, data):
#         try:
#             df = pd.DataFrame(data, columns=["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment", "County"])
#             df.to_csv(LOCAL_CSV_PATH, index=False, encoding='utf-8')
#             print(f"✓ Data saved to local CSV: {LOCAL_CSV_PATH}")
#             return True
#         except Exception as e:
#             print(f"Error saving to CSV: {e}")
#             return False

#     async def update_google_sheet_tab(self, df, sheet_name):
#         """Update one Google Sheet tab (per county or All Data)"""
#         try:
#             sheet = self.service.spreadsheets()

#             # Ensure sheet exists
#             try:
#                 add_sheet_request = {"addSheet": {"properties": {"title": sheet_name}}}
#                 self.service.spreadsheets().batchUpdate(
#                     spreadsheetId=SPREADSHEET_ID,
#                     body={"requests": [add_sheet_request]}
#                 ).execute()
#                 print(f"Created new sheet: {sheet_name}")
#             except HttpError as e:
#                 if e.resp.status == 400 and "already exists" in str(e):
#                     pass
#                 else:
#                     raise

#             # Prepare values
#             values = [list(df.columns)]
#             for _, row in df.iterrows():
#                 row_values = []
#                 for col in df.columns:
#                     val = row[col]
#                     if isinstance(val, (pd.Timestamp, datetime)):
#                         row_values.append(val.strftime("%Y-%m-%d"))
#                     else:
#                         row_values.append("" if pd.isna(val) else str(val))
#                 values.append(row_values)

#             # Clear & update
#             sheet.values().clear(
#                 spreadsheetId=SPREADSHEET_ID,
#                 range=f"'{sheet_name}'!A:Z"
#             ).execute()
#             sheet.values().update(
#                 spreadsheetId=SPREADSHEET_ID,
#                 range=f"'{sheet_name}'!A1",
#                 valueInputOption="USER_ENTERED",
#                 body={"values": values}
#             ).execute()

#             print(f"✓ Updated Google Sheet tab: {sheet_name} ({len(df)} rows)")
#         except Exception as e:
#             print(f"✗ Google Sheets update error on {sheet_name}: {e}")

#     async def scrape_all_counties(self):
#         all_data = []
        
#         async with async_playwright() as p:
#             browser = await p.chromium.launch(
#                 headless=True,
#                 args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
#             )
#             page = await browser.new_page()
            
#             for county in TARGET_COUNTIES:
#                 try:
#                     county_data = await self.scrape_county_sales(page, county)
#                     all_data.extend(county_data)
#                     print(f"✓ Completed {county['county_name']}: {len(county_data)} records")

#                     # Update Google Sheet tab immediately
#                     if county_data and self.service:
#                         df_county = pd.DataFrame(county_data, columns=["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment", "County"])
#                         await self.update_google_sheet_tab(df_county.drop(columns=["County"]), county["county_name"][:30])

#                     await asyncio.sleep(2)
#                 except Exception as e:
#                     print(f"Failed to scrape {county['county_name']}: {str(e)}")
#                     continue
            
#             await browser.close()
        
#         return all_data

#     async def run_scraper(self):
#         print(f"Starting scrape at {datetime.now()}")
#         data = await self.scrape_all_counties()
        
#         if data:
#             self.save_to_csv(data)

#             # Update "All Data" at the end
#             if self.service:
#                 df_all = pd.DataFrame(data, columns=["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment", "County"])
#                 await self.update_google_sheet_tab(df_all.drop(columns=["County"]), "All Data")
#         else:
#             print("No data scraped")
            
#         print(f"Finished scrape at {datetime.now()}")
#         return data

# # Create scraper instance
# scraper = ForeclosureScraper()

# @app.route('/')
# def home():
#     return jsonify({
#         "status": "success",
#         "message": "Foreclosure Scraper API is running",
#         "endpoints": {
#             "/run": "Run the scraper manually",
#             "/health": "Check service health"
#         }
#     })

# @app.route('/run')
# async def run_scraper_endpoint():
#     try:
#         data = await scraper.run_scraper()
#         return jsonify({
#             "status": "success",
#             "message": f"Scraping completed successfully. {len(data)} records processed.",
#             "records": len(data)
#         })
#     except Exception as e:
#         return jsonify({
#             "status": "error",
#             "message": f"Scraping failed: {str(e)}"
#         }), 500

# @app.route('/health')
# def health_check():
#     return jsonify({
#         "status": "success",
#         "message": "Service is healthy",
#         "timestamp": datetime.now().isoformat()
#     })

# if __name__ == "__main__":
#     port = int(os.environ.get("PORT", 5000))
#     app.run(host="0.0.0.0", port=port)