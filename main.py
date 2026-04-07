import os
import json
import re
from datetime import datetime, timedelta

from scraper import scrape_all
from uploader import send_to_ghl
from dedupe import load_processed, save_processed, is_new
from logger import log

BACKUP_DIR = "data/backups"
LAST_DATE_FILE = "data/last_date.json"
ENABLE_GHL_PUSH = os.getenv("ENABLE_GHL_PUSH", "false").lower() in ("1", "true", "yes")
os.makedirs(BACKUP_DIR, exist_ok=True)


def save_backup(data):
    filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.json")
    path = os.path.join(BACKUP_DIR, filename)

    with open(path, "w") as f:
        json.dump(data, f, indent=2)

    log(f"💾 Backup saved: {path}")


def save_local_list(data):
    path = "data/latest_scrape.json"
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    log(f"💾 Saved latest scrape to: {path}")


def load_last_date():
    if not os.path.exists(LAST_DATE_FILE):
        return None
    with open(LAST_DATE_FILE, "r") as f:
        payload = json.load(f)
    return payload.get("last_end_date")


def save_last_date(date_str):
    with open(LAST_DATE_FILE, "w") as f:
        json.dump({"last_end_date": date_str}, f)
    log(f"🗓️  Stored last end date: {date_str}")


def format_query_date(dt):
    return f"{dt.month}/{dt.day}/{dt.year}"


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


def filter_unique_rows(rows):
    unique = {}

    for row in rows:
        name = (row.get("name") or "").strip().lower()
        phone = re.sub(r"\D", "", row.get("phone") or "")
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


def cleanup_old_files():
    now = datetime.now()

    for file in os.listdir(BACKUP_DIR):
        path = os.path.join(BACKUP_DIR, file)

        if os.path.isfile(path):
            created = datetime.fromtimestamp(os.path.getctime(path))
            if now - created > timedelta(days=7):
                os.remove(path)
                log(f"🧹 Deleted old file: {file}")


def build_date_range():
    saved_end_date = load_last_date()
    if os.getenv("FULL_LOAD", "false").lower() in ("1", "true", "yes") or not saved_end_date:
        start_date = "1/1/2024"
    else:
        start_date = saved_end_date

    end_date = format_query_date(datetime.now())
    return start_date, end_date


def main():
    log("🚀 Starting script...")

    processed_ids = load_processed()
    new_processed = set(processed_ids)
    start_date, end_date = build_date_range()

    log(f"📅 Scraping range: {start_date} → {end_date}")

    data = scrape_all(start_date, end_date)
    log(f"📊 Scraped {len(data)} rows")

    save_backup(data)
    save_local_list(data)

    unique_rows = filter_unique_rows(data)
    log(f"🔎 Reduced to {len(unique_rows)} unique rows by customer name + primary phone")

    if ENABLE_GHL_PUSH:
        contacts_to_send = []
        for row in unique_rows:
            activity_id = row.get("activity_id")
            if not is_new(activity_id, processed_ids):
                contacts_to_send.append(row)
                new_processed.add(activity_id)
            else:
                log(f"⏭️  Already processed: {row.get('name')} ({activity_id})")

        if contacts_to_send:
            send_to_ghl(contacts_to_send)
            log(f"✅ Sent {len(contacts_to_send)} contacts to GHL")
            save_processed(new_processed)
        else:
            log("ℹ️ No new contacts to send to GHL")
    else:
        log("⏸️ GHL push is disabled. No contacts were sent.")

    save_last_date(end_date)
    cleanup_old_files()

    log(f"✅ Done. Found {len(unique_rows)} unique rows")


if __name__ == "__main__":
    main()