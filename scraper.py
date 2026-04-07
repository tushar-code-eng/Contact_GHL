from playwright.sync_api import sync_playwright
import re
import json
from datetime import datetime
from logger import log
from config import CONFIG


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
    page.wait_for_timeout(1500)

    return page.evaluate("""
        () => {
            function clean(v){ return (v||'').trim(); }
            function getValue(label){
                const tds = Array.from(document.querySelectorAll('td'));
                for(let i=0;i<tds.length;i++){
                    if(clean(tds[i].innerText)===label){
                        return clean(tds[i+1]?.innerText);
                    }
                }
                return '';
            }
            return {
                email: getValue('Email Address'),
                contract_number: getValue('Contract Number'),
                opportunity_id: getValue('Opportunity Id'),
                service_region: getValue('Service Region'),
                address: getValue('Address')
            }
        }
    """)


def scrape_all(start_date, end_date):
    field_config = load_field_config()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        log("🔐 Logging in...")

        page.goto("https://prpt.todaysales.us/login")
        page.wait_for_timeout(2000)

        page.locator("text=Login With Active Directory").click()
        page.wait_for_timeout(3000)

        page.locator('input[type="email"]').fill(CONFIG["username"])
        page.locator('input[type="password"]').fill(CONFIG["password"])
        page.wait_for_timeout(2000)

        page.locator('input[type="submit"]').click()
        page.wait_for_timeout(3000)

        if page.locator("#idSIButton9").count() > 0:
            page.locator("#idSIButton9").click()

        report_url = build_report_url(start_date, end_date)
        log(f"🌐 Loading activity list: {report_url}")
        page.goto(report_url)
        page.wait_for_selector("tbody tr")

        rows = page.evaluate("""
            () => {
                const headers = Array.from(document.querySelectorAll('thead th')).map(th => th.innerText.trim().toLowerCase());
                const fallback = {
                    activity_id: 0,
                    self_gen: 1,
                    opportunity_id: 2,
                    contract_number: 3,
                    appointment: 4,
                    status: 5,
                    name: 7,
                    phone: 8,
                    alternate_phone: 9,
                    address: 10
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
                    const idx = headers.indexOf(labels[key]);
                    return idx >= 0 ? idx : fallback[key];
                };

                return Array.from(document.querySelectorAll('tbody tr')).map(tr => {
                    const tds = Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim());
                    return {
                        activity_id: tds[getIndex('activity_id')] || '',
                        self_gen: tds[getIndex('self_gen')] || '',
                        opportunity_id: tds[getIndex('opportunity_id')] || '',
                        appointment: tds[getIndex('appointment')] || '',
                        status: tds[getIndex('status')] || '',
                        contract_number: tds[getIndex('contract_number')] || '',
                        name: tds[getIndex('name')] || '',
                        phone: tds[getIndex('phone')] || '',
                        alternate_phone: tds[getIndex('alternate_phone')] || '',
                        address: tds[getIndex('address')] || ''
                    };
                });
            }
        """)

        unique_rows = filter_summary_rows(rows)
        log(f"🔎 Filtered to {len(unique_rows)} unique rows before detail scraping")

        results = []

        for row in unique_rows:
            detail = scrape_detail(page, row["activity_id"])

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
            results.append(filtered)

        browser.close()
        return results