from playwright.sync_api import sync_playwright
import re
import json
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from logger import log
from config import CONFIG

SESSION_FILE = "auth.json"
SESSION_MAX_AGE_HOURS = int(os.getenv("SESSION_MAX_AGE_HOURS", "6"))
PROGRESS_SAVE_EVERY = int(os.getenv("PROGRESS_SAVE_EVERY", "15"))
PROGRESS_BACKUP_DIR = "data/backups"
PROGRESS_LAST_DATE_FILE = "data/last_date.json"
PROGRESS_LATEST_SCRAPE = "data/latest_scrape.json"
PLAYWRIGHT_TIMEOUT_MS = int(os.getenv("PLAYWRIGHT_TIMEOUT_MS", "120000"))
DETAIL_SCRAPE_THREADS = int(os.getenv("DETAIL_SCRAPE_THREADS", "15"))


def is_session_expired():
    """Check if session file exists and is older than SESSION_MAX_AGE_HOURS"""
    if not os.path.exists(SESSION_FILE):
        return True  # No session, needs new one
    
    try:
        file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(SESSION_FILE))
        max_age = timedelta(hours=SESSION_MAX_AGE_HOURS)
        
        if file_age > max_age:
            log(f"⏰ Session is {file_age.total_seconds()/3600:.1f} hours old (max: {SESSION_MAX_AGE_HOURS}h)")
            return True
        else:
            log(f"✅ Session is fresh ({file_age.total_seconds()/3600:.1f} hours old)")
            return False
    except Exception as e:
        log(f"⚠️ Error checking session age: {e}")
        return True


def delete_expired_session():
    """Delete the session file if it's expired"""
    if is_session_expired() and os.path.exists(SESSION_FILE):
        try:
            os.remove(SESSION_FILE)
            log(f"🗑️  Deleted expired session: {SESSION_FILE}")
        except Exception as e:
            log(f"❌ Error deleting session: {e}")


def load_or_create_session(context, page):
    """
    Load existing session if fresh, or delete and create new one if expired
    """
    if is_session_expired():
        delete_expired_session()
        log("🔐 Creating new session...")
        login_and_save_session(context, page)
    else:
        log("⚡ Using saved session")



def normalize_phone(phone):
    return re.sub(r"\D", "", phone or "")


def parse_appointment_date(value):
    if not value:
        return None

    for fmt in ["%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%d-%b-%Y", "%m-%d-%Y"]:
        try:
            return datetime.strptime(value.strip(), fmt)
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(value.strip())
    except ValueError:
        return None


def filter_summary_rows(rows):
    unique = {}

    for row in rows:
        name = (row.get("name") or "").strip().lower()
        phone = normalize_phone(row.get("phone"))
        if not name and not phone:
            continue

        key = (name, phone)
        candidate_date = parse_appointment_date(row.get("appointment"))
        existing = unique.get(key)

        if existing is None:
            unique[key] = row
            continue

        existing_date = parse_appointment_date(existing.get("appointment"))
        if candidate_date and existing_date:
            if candidate_date > existing_date:
                unique[key] = row
        elif candidate_date and not existing_date:
            unique[key] = row

    return list(unique.values())


def load_field_config():
    with open("field_config.json") as f:
        return json.load(f)


def build_report_url(start_date, end_date):
    return f"https://prpt.todaysales.us/reports/salesrep/activities/recent?repid=s507821&startdate={start_date}&enddate={end_date}"


def scrape_detail(page, activity_id):
    url = f"https://prpt.todaysales.us/reports/salesrep/customerhistory?activityid={activity_id}"
    page.goto(url)
    page.wait_for_timeout(2000)

    return page.evaluate(r"""
        () => {
            function clean(v){ return (v||'').trim(); }

            // Email from definition list
            function getEmail(){
                const rows = Array.from(document.querySelectorAll('dl.row'));
                for(const row of rows){
                    const label = row.querySelector('dt')?.innerText?.toLowerCase() || '';
                    if(label.includes('email')){
                        return clean(row.querySelector('dd')?.innerText);
                    }
                }
                return '';
            }

            function getCellValue(td){
                if(!td) return '';
                return clean(td.dataset.expValue || td.innerText);
            }

            function getPrimaryQuoteRow(){
                const headers = Array.from(document.querySelectorAll('thead th'))
                    .map(th => th.innerText.replace(/\s+/g,' ').trim().toLowerCase());

                const quoteIdx = headers.findIndex(h => 
                    h.includes('quote option') || h.includes('quote')
                );
                const primaryIdx = headers.findIndex(h => 
                    h.includes('is primary') || h.includes('primary')
                );
                const contractIdx = headers.findIndex(h => 
                    h.includes('contract total')
                );

                const rows = Array.from(document.querySelectorAll('tbody > tr'));

                for(const tr of rows){
                    if(tr.classList.contains('collapse')) continue;

                    const tds = tr.querySelectorAll('td');
                    const isPrimaryValue = primaryIdx >= 0 ? getCellValue(tds[primaryIdx]).toLowerCase() : '';
                    const button = tr.querySelector('button[data-toggle="collapse"]');
                    const collapseTarget = button ? (button.dataset.target || button.getAttribute('data-target')) : '';

                    if(isPrimaryValue.includes('true')){
                        return {
                            quote_option: getCellValue(tds[quoteIdx]),
                            is_primary: getCellValue(tds[primaryIdx]),
                            contract_total: getCellValue(tds[contractIdx]),
                            collapse_target: collapseTarget
                        };
                    }
                }

                return {
                    quote_option: '',
                    is_primary: '',
                    contract_total: '',
                    collapse_target: ''
                };
            }

            function getQuoteDetailFields(targetSelector){
                const emptyResult = {
                    area: '',
                    product_line: '',
                    series: '',
                    style: ''
                };

                if(!targetSelector) return emptyResult;

                const collapseRow = document.querySelector(targetSelector);
                if(!collapseRow) return emptyResult;

                // Only use the direct rows of the summary table inside the collapse row.
                // Ignore nested/detail rows from expanded sections.
                const summaryRows = Array.from(
                    collapseRow.querySelectorAll(':scope > td > table > tbody > tr')
                );

                const areas = [];
                const productLines = [];
                const seriesList = [];
                const styles = [];

                for(const tr of summaryRows){
                    const cells = Array.from(tr.querySelectorAll(':scope > td[data-exp-col]'));
                    if(cells.length < 4) continue;

                    const areaButton = cells[0].querySelector('button');
                    const area = clean(areaButton ? areaButton.innerText : getCellValue(cells[0]));
                    const productLine = clean(cells[1].dataset.expValue || cells[1].innerText);
                    const series = clean(cells[2].dataset.expValue || cells[2].innerText);
                    const style = clean(cells[3].dataset.expValue || cells[3].innerText);

                    if(area) areas.push(area);
                    if(productLine) productLines.push(productLine);
                    if(series) seriesList.push(series);
                    if(style) styles.push(style);
                }

                const unique = arr => [...new Set(arr.filter(Boolean))];

                return {
                    area: unique(areas).join(' | '),
                    product_line: unique(productLines).join(' | '),
                    series: unique(seriesList).join(' | '),
                    style: unique(styles).join(' | ')
                };
            }

            const primaryQuote = getPrimaryQuoteRow();
            const quoteDetails = getQuoteDetailFields(primaryQuote.collapse_target);

            return {
                email: getEmail(),
                quote_option: primaryQuote.quote_option,
                is_primary: primaryQuote.is_primary,
                contract_total: primaryQuote.contract_total,
                area: quoteDetails.area,
                product_line: quoteDetails.product_line,
                series: quoteDetails.series,
                style: quoteDetails.style
            };
        }
    """)


def login_and_save_session(context, page):
    log("🔐 Logging in...")

    page.goto("https://prpt.todaysales.us/login")
    page.wait_for_timeout(2000)

    page.locator("text=Login With Active Directory").click()
    page.wait_for_timeout(3000)

    page.locator('input[type="email"]').fill(CONFIG["username"])
    page.locator('input[type="submit"]').click()

    page.wait_for_timeout(2000)

    page.locator('input[type="password"]').fill(CONFIG["password"])
    page.locator('input[type="submit"]').click()

    log("📲 Complete OTP manually if prompted...")
    page.wait_for_timeout(45000)

    if page.locator("#idSIButton9").count() > 0:
        page.locator("#idSIButton9").click()

    context.storage_state(path=SESSION_FILE)
    log("✅ Session saved!")


def extract_rows(page):
    return page.evaluate(r"""
        () => {
            const headers = Array.from(document.querySelectorAll('thead th'))
              .map(th => th.innerText.replace(/\s+/g, ' ').trim().toLowerCase());

            const fallback = {
                activity_id: 0,
                self_gen: 2,
                opportunity_id: 3,
                contract_number: 4,
                appointment: 5,
                status: 6,
                name: 8,
                phone: 9,
                alternate_phone: 10,
                address: 11
            };

            const labels = {
                activity_id: 'activity id',
                self_gen: 'self gen',
                opportunity_id: 'opportunity id',
                contract_number: 'contract number',
                appointment: 'appointment date',
                status: 'status',
                name: 'customer name',
                phone: 'primary phone',
                alternate_phone: 'alternate phone',
                address: 'address'
            };

            const getIndex = key => {
                const idx = headers.findIndex(h => h.includes(labels[key]));
                return idx >= 0 ? idx : fallback[key];
            };

            return Array.from(document.querySelectorAll('tbody tr')).map(tr => {
                const tds = Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim());

                const selfGenCell = tr.querySelectorAll('td')[getIndex('self_gen')];
                const isSelfGen = selfGenCell
                    ? selfGenCell.querySelector('.bi-check-circle-fill') !== null
                    : false;

                // Extract address from the address column cell
                const addressIdx = getIndex('address');
                const addressCell = tr.querySelectorAll('td')[addressIdx];
                let addressText = '';
                if (addressCell) {
                    // Try to get from span inside the cell first
                    const addressSpan = addressCell.querySelector('span.text-truncate');
                    addressText = addressSpan ? addressSpan.innerText.trim() : addressCell.innerText.trim();
                }

                return {
                    activity_id: tds[getIndex('activity_id')] || '',
                    self_gen: isSelfGen,
                    opportunity_id: tds[getIndex('opportunity_id')] || '',
                    appointment: tds[getIndex('appointment')] || '',
                    status: tds[getIndex('status')] || '',
                    contract_number: tds[getIndex('contract_number')] || '',
                    name: tds[getIndex('name')] || '',
                    phone: tds[getIndex('phone')] || '',
                    alternate_phone: tds[getIndex('alternate_phone')] || '',
                    address: addressText
                };
            });
        }
    """)


def scrape_detail_parallel(activity_id):
    """Scrape detail for a single activity in a thread. Creates its own page/context."""
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)

            # Use saved session if available
            if os.path.exists(SESSION_FILE):
                context = browser.new_context(storage_state=SESSION_FILE)
            else:
                context = browser.new_context()

            page = context.new_page()

            # Set timeouts
            try:
                page.set_default_navigation_timeout(PLAYWRIGHT_TIMEOUT_MS)
                page.set_default_timeout(PLAYWRIGHT_TIMEOUT_MS)
            except Exception:
                pass

            # Call the scrape_detail function
            detail = scrape_detail(page, activity_id)
            browser.close()
            return detail
    except Exception as e:
        log(f"❌ Error scraping detail for {activity_id}: {e}")
        return {}


def scrape_all(start_date, end_date):
    field_config = load_field_config()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)

        # Check session age and create new one if expired
        if not is_session_expired() and os.path.exists(SESSION_FILE):
            context = browser.new_context(storage_state=SESSION_FILE)
            log("⚡ Using saved session")
        else:
            context = browser.new_context()
            log("🔐 Need to login (no session or expired)")

        page = context.new_page()

        # Increase navigation and action timeouts (configurable via env PLAYWRIGHT_TIMEOUT_MS)
        try:
            page.set_default_navigation_timeout(PLAYWRIGHT_TIMEOUT_MS)
            page.set_default_timeout(PLAYWRIGHT_TIMEOUT_MS)
            log(f"⏱️  Playwright timeouts set to {PLAYWRIGHT_TIMEOUT_MS} ms")
        except Exception:
            # Older Playwright versions may not support these; ignore failures
            pass

        # Login if session is missing or expired
        if is_session_expired():
            delete_expired_session()
            login_and_save_session(context, page)

        report_url = build_report_url(start_date, end_date)
        log(f"🌐 Loading activity list: {report_url}")

        page.goto(report_url)
        
        # Wait for table to load, but don't require tbody tr
        try:
            page.wait_for_selector("table", timeout=10000)
            log("✅ Table loaded")
        except:
            log("⚠️ Table not found, proceeding anyway")
        
        # Check if there are any tbody tr, with short timeout
        try:
            page.wait_for_selector("tbody tr", timeout=2000)
        except:
            log("⚠️ No data rows found on page")

        # Scroll + collect for virtualized tables
        all_rows = []
        seen_ids = set()
        empty_scrolls = 0

        while empty_scrolls < 5:  # Keep scrolling even if 0 new rows, up to 5 times
            rows = extract_rows(page)

            new_count = 0
            for r in rows:
                aid = r.get("activity_id")
                if aid and aid not in seen_ids:
                    seen_ids.add(aid)
                    all_rows.append(r)
                    new_count += 1
                    log(f"➕ Found row: {aid} — {r.get('name','')} — {r.get('phone','')}")

            log(f"Collected {len(all_rows)} rows so far...")

            if new_count == 0:
                empty_scrolls += 1
                log(f"⚠️ No new rows (attempt {empty_scrolls}/5), continuing to scroll...")
            else:
                empty_scrolls = 0

            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(1500)

        # unique_rows = filter_summary_rows(all_rows)
        unique_rows = all_rows
        log(f"🔎 Filtered to {len(unique_rows)} unique rows before detail scraping")

        results = []
        os.makedirs(PROGRESS_BACKUP_DIR, exist_ok=True)

        # Scrape details in batches of DETAIL_SCRAPE_THREADS
        log(f"🚀 Scraping details in batches of {DETAIL_SCRAPE_THREADS} threads...")
        total_rows = len(unique_rows)

        for batch_start in range(0, total_rows, DETAIL_SCRAPE_THREADS):
            batch_end = min(batch_start + DETAIL_SCRAPE_THREADS, total_rows)
            batch_rows = unique_rows[batch_start:batch_end]
            batch_num = (batch_start // DETAIL_SCRAPE_THREADS) + 1

            log(f"📦 Processing batch {batch_num} ({batch_start + 1}-{batch_end}/{total_rows})...")

            details_map = {}  # Map activity_id -> detail data for this batch

            # Process this batch in parallel
            with ThreadPoolExecutor(max_workers=DETAIL_SCRAPE_THREADS) as executor:
                futures = {}
                for row in batch_rows:
                    activity_id = row["activity_id"]
                    future = executor.submit(scrape_detail_parallel, activity_id)
                    futures[future] = (activity_id, row)

                # Wait for all futures in this batch to complete
                for future in futures:
                    activity_id, row = futures[future]
                    try:
                        detail = future.result()
                        details_map[activity_id] = detail
                        log(f"✔️ Scraped detail for: {activity_id}")
                    except Exception as e:
                        log(f"❌ Failed to scrape detail for {activity_id}: {e}")
                        details_map[activity_id] = {}

            # Combine details with rows for this batch
            batch_results = []
            for row in batch_rows:
                activity_id = row["activity_id"]
                detail = details_map.get(activity_id, {})

                combined = {
                    "activity_id": row["activity_id"],
                    "self_gen": row["self_gen"],
                    "opportunity_id": row["opportunity_id"],
                    "appointment": row["appointment"],
                    "status": row["status"],
                    "contract_number": row["contract_number"],
                    "name": row["name"],
                    "phone": normalize_phone(row["phone"]),
                    "alternate_phone": normalize_phone(row.get("alternate_phone", "")),
                    "address": row["address"],
                    **detail
                }

                filtered = {k: v for k, v in combined.items() if field_config.get(k, False)}
                batch_results.append(filtered)
                results.append(filtered)

            # Save this batch to a timestamped file
            try:
                batch_filename = datetime.now().strftime(f"%Y-%m-%d_%H-%M-%S_batch_{batch_num}.json")
                batch_path = os.path.join(PROGRESS_BACKUP_DIR, batch_filename)
                with open(batch_path, "w") as f:
                    json.dump(batch_results, f, indent=2)
                log(f"💾 Batch {batch_num} saved: {batch_path} ({len(batch_results)} records)")

                # Also update latest_scrape.json with all results so far
                with open(PROGRESS_LATEST_SCRAPE, "w") as f:
                    json.dump(results, f, indent=2)
                log(f"📋 Updated latest_scrape.json: {len(results)} total records")
            except Exception as e:
                log(f"⚠️ Failed to persist progress: {e}")

        browser.close()
        return results


def extract_installation_rows(page):
    """Extract installation data from the installations table"""
    return page.evaluate(r"""
        () => {
            const rows = Array.from(document.querySelectorAll('tbody tr'));
            console.log('Total rows found:', rows.length);
            
            const results = [];
            
            rows.forEach((tr, rowIdx) => {
                const tds = Array.from(tr.querySelectorAll('td'));
                const rowData = {};
                
                tds.forEach((td, cellIdx) => {
                    const value = td.getAttribute('data-exp-value') || td.innerText.trim();
                    rowData[cellIdx] = value;
                });
                
                // Log first few rows to see structure
                if (rowIdx < 3) {
                    console.log(`Row ${rowIdx} cells:`, rowData);
                }
                
                results.push(rowData);
            });
            
            // Return raw cell data for inspection
            return results.map(rowData => ({
                cells: Object.values(rowData),
                raw: rowData
            }));
        }
    """)


def scrape_installations(start_date=None, end_date=None):
    """
    Scrape installations from the upcoming installations URL
    Filters for "Completed" order status only
    Returns dict keyed by activity_id
    """
    installations = {}
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)

        # Check session age and create new one if expired
        if not is_session_expired() and os.path.exists(SESSION_FILE):
            context = browser.new_context(storage_state=SESSION_FILE)
            log("⚡ Using saved session for installations")
        else:
            context = browser.new_context()
            log("🔐 Session missing or expired, need to login for installations")
            browser.close()
            return installations

        page = context.new_page()

        try:
            page.set_default_navigation_timeout(PLAYWRIGHT_TIMEOUT_MS)
            page.set_default_timeout(PLAYWRIGHT_TIMEOUT_MS)
        except Exception:
            pass

        try:
            # Build URL with date range if provided
            url = "https://prpt.todaysales.us/reports/salesrep/installations/upcoming"
            if start_date and end_date:
                # Convert dates to URL format (M/D/YYYY)
                url += f"?repid=s507821&startdate={start_date}&enddate={end_date}"
            else:
                url += "?repid=s507821&startdate=1%2F4%2F2024&enddate=6%2F11%2F2026"
            
            log(f"📍 Navigating to installations URL...")
            page.goto(url, wait_until="networkidle")
            
            # Wait for table to load
            try:
                page.wait_for_selector("table", timeout=10000)
                log("✅ Installations table loaded")
            except:
                log("⚠️ Installations table not found, proceeding anyway")
            
            page.wait_for_timeout(2000)

            # Extract rows
            raw_rows = extract_installation_rows(page)
            log(f"📊 Extracted {len(raw_rows)} installation records")

            # Log first row structure for debugging
            if raw_rows:
                log(f"📋 First row cells: {raw_rows[0].get('cells', [])[:6]}")

            # Filter for "Completed" status and deduplicate by activity_id (keep latest date)
            for row in raw_rows:
                cells = row.get('cells', [])
                
                # Based on table structure: [Installation Date, Order Id, Order Status, Contract Number, Opportunity Id, Activity Id, ...]
                installation_date = cells[0] if len(cells) > 0 else ''
                order_id = cells[1] if len(cells) > 1 else ''
                order_status = cells[2] if len(cells) > 2 else ''
                contract_number = cells[3] if len(cells) > 3 else ''
                opportunity_id = cells[4] if len(cells) > 4 else ''
                activity_id = cells[5] if len(cells) > 5 else ''

                activity_id = (activity_id or '').strip()
                order_status = (order_status or '').strip()
                installation_date = (installation_date or '').strip()

                # Only include if "Completed" is in the order status
                if activity_id and "completed" in order_status.lower():
                    new_row = {
                        'activity_id': activity_id,
                        'order_status': order_status,
                        'installation_date': installation_date,
                        'order_id': order_id,
                        'contract_number': contract_number,
                        'opportunity_id': opportunity_id
                    }
                    
                    existing = installations.get(activity_id)
                    if existing:
                        # Keep the latest (max) installation date
                        existing_date = parse_appointment_date(existing.get('installation_date', ''))
                        new_date = parse_appointment_date(installation_date)
                        if new_date and existing_date:
                            if new_date > existing_date:
                                installations[activity_id] = new_row
                        elif new_date and not existing_date:
                            installations[activity_id] = new_row
                    else:
                        installations[activity_id] = new_row

            log(f"✅ Found {len(installations)} completed installations (deduped by activity_id)")
            return installations

        except Exception as e:
            error(f"❌ Error scraping installations: {e}")
            return installations
        finally:
            browser.close()
