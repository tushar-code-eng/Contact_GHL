import os
import json
import re
from datetime import datetime, timedelta

from scraper import scrape_all, scrape_installations
from uploader import send_to_ghl
from dedupe import load_processed, save_processed, is_new, load_processed_hashes, save_processed_hashes, has_record_changed, compute_record_hash
from logger import log
from config import CONFIG

BACKUP_DIR = "data/backups"
LAST_DATE_FILE = "data/last_date.json"
INSTALLATION_LAST_DATE_FILE = "data/installation_last_date.json"

# New workflow files
DEDUPED_SALES_FILE = "data/deduped_sales.json"
DEDUPED_INSTALLATIONS_FILE = "data/deduped_installations.json"
MERGED_FINAL_FILE = "data/merged_final.json"
ALL_RECORDS_FILE = "data/all_records.json"  # Accumulates all records from URL 1
SCHEDULED_RECORDS_FILE = "data/scheduled_records.json"  # Tracks scheduled records separately
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


def save_deduped_sales(data):
    """Save deduplicated sales data"""
    with open(DEDUPED_SALES_FILE, "w") as f:
        json.dump(data, f, indent=2)
    log(f"💾 Saved {len(data)} deduped sales records to: {DEDUPED_SALES_FILE}")


def save_deduped_installations(data):
    """Save deduplicated installations data as list"""
    installations_list = list(data.values()) if isinstance(data, dict) else data
    with open(DEDUPED_INSTALLATIONS_FILE, "w") as f:
        json.dump(installations_list, f, indent=2)
    log(f"💾 Saved {len(installations_list)} deduped installations to: {DEDUPED_INSTALLATIONS_FILE}")


def save_merged_final(data):
    """Save final merged data ready for GHL"""
    with open(MERGED_FINAL_FILE, "w") as f:
        json.dump(data, f, indent=2)
    log(f"💾 Saved {len(data)} merged records to: {MERGED_FINAL_FILE}")


def load_merged_final():
    """Load previously merged data from file"""
    if not os.path.exists(MERGED_FINAL_FILE):
        return None
    try:
        with open(MERGED_FINAL_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        log(f"❌ Error loading merged file: {e}")
        return None


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


def load_installation_last_date():
    """Load the max installation date from previous run"""
    if not os.path.exists(INSTALLATION_LAST_DATE_FILE):
        return None
    with open(INSTALLATION_LAST_DATE_FILE, "r") as f:
        payload = json.load(f)
    return payload.get("max_installation_date")


def save_installation_last_date(date_str):
    """Save the max installation date for next run"""
    with open(INSTALLATION_LAST_DATE_FILE, "w") as f:
        json.dump({"max_installation_date": date_str}, f)
    log(f"🗓️  Stored max installation date: {date_str}")


def merge_with_installations(sales_records, installations_dict):
    """
    Merge installation data with sales records.
    For each matching activity_id:
    - Change status to "completed"
    - Change tag to "recent_tag"
    - Replace appointment date with installation_date
    
    Returns updated sales_records and max_installation_date
    """
    merged_records = []
    max_install_date = None
    
    for record in sales_records:
        activity_id = record.get("activity_id", "").strip()
        
        if activity_id in installations_dict:
            installation = installations_dict[activity_id]
            
            # Update the record with installation data
            record["status"] = "completed"
            record["tag"] = "recent_tag"
            record["appointment"] = installation.get("installation_date", record.get("appointment", ""))
            
            # Track max installation date
            inst_date = parse_appointment_date(installation.get("installation_date"))
            if inst_date:
                if max_install_date is None:
                    max_install_date = inst_date
                else:
                    max_install_date = max(max_install_date, inst_date)
            
            log(f"✏️  Updated {record.get('name')} (activity_id: {activity_id}) with installation data")
        
        merged_records.append(record)
    
    # Convert max date to string format for storage
    max_date_str = None
    if max_install_date:
        max_date_str = format_query_date(max_install_date)
    
    return merged_records, max_date_str


def format_query_date(dt):
    return f"{dt.month}/{dt.day}/{dt.year}"


def parse_appointment_date(value):
    if not value:
        return None

    # Extract just the date part (before any time info like "8 AM - 10 AM")
    # Handle formats like "4/18/2026 8 AM - 10 AM" or "4/18/2026"
    value_str = value.strip()
    date_part = value_str.split(' ')[0]  # Get "4/18/2026" part

    for fmt in ["%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%d-%b-%Y", "%m-%d-%Y"]:
        try:
            return datetime.strptime(date_part, fmt)
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(date_part)
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


def filter_unique_by_email_phone(rows):
    """Filter unique records by email (primary) or phone (fallback), keeping latest appointment"""
    unique = {}

    for row in rows:
        email = (row.get("email") or "").strip().lower()
        phone = re.sub(r"\D", "", row.get("phone") or "")
        
        # Determine key: email if present, else phone
        if email:
            key = email
        elif phone:
            key = phone
        else:
            continue  # Skip if neither email nor phone

        candidate_date = parse_appointment_date(row.get("appointment"))
        
        if key not in unique:
            unique[key] = row
        else:
            existing_date = parse_appointment_date(unique[key].get("appointment"))
            if candidate_date and existing_date:
                if candidate_date > existing_date:
                    unique[key] = row
            elif candidate_date and not existing_date:
                unique[key] = row
            # If candidate has no date, keep existing

    return list(unique.values())


def add_tag_field(rows):
    """Add tag field based on status. Supports multiple tags."""
    for row in rows:
        # Skip if tag already exists (from installation merge)
        if row.get("tag"):
            base_tag = row["tag"]
        else:
            status = (row.get("status") or "").strip().lower()
            base_tag = f"{status}_tag" if status else "unknown_tag"

        # Always convert to list
        tags = [base_tag]

        # 🔥 YOUR NEW LOGIC
        if base_tag == "sold_tag":
            tags.append("google_contact")

        row["tags"] = tags  # use plural
        row.pop("tag", None)  # optional: remove old single tag

    return rows


def cleanup_old_files():
    now = datetime.now()
    
    # Cleanup old backup files
    for file in os.listdir(BACKUP_DIR):
        path = os.path.join(BACKUP_DIR, file)

        if os.path.isfile(path):
            created = datetime.fromtimestamp(os.path.getctime(path))
            if now - created > timedelta(days=7):
                os.remove(path)
                log(f"🧹 Deleted old backup: {file}")
    
    # Cleanup old log files
    log_dir = "logs"
    if os.path.exists(log_dir):
        for file in os.listdir(log_dir):
            path = os.path.join(log_dir, file)
            
            if os.path.isfile(path):
                created = datetime.fromtimestamp(os.path.getctime(path))
                if now - created > timedelta(days=7):
                    os.remove(path)
                    log(f"🧹 Deleted old log: {file}")


def build_date_range():
    saved_end_date = load_last_date()
    if os.getenv("FULL_LOAD", "false").lower() in ("1", "true", "yes") or not saved_end_date:
        start_date = "1/1/2024"
    else:
        start_date = saved_end_date

    end_date = format_query_date(datetime.now())
    return start_date, end_date


def load_latest_scrape():
    """Load data from latest_scrape.json if it exists"""
    if os.path.exists("data/latest_scrape.json"):
        with open("data/latest_scrape.json", "r") as f:
            return json.load(f)
    return None


def load_all_records():
    """Load all accumulated records from URL 1"""
    if not os.path.exists(ALL_RECORDS_FILE):
        return {}
    try:
        with open(ALL_RECORDS_FILE, "r") as f:
            records_list = json.load(f)
            # Convert list to dict keyed by (email, phone) for deduping
            unique = {}
            for record in records_list:
                email = (record.get("email") or "").strip().lower()
                phone = re.sub(r"\D", "", record.get("phone") or "")
                
                if email:
                    key = email
                elif phone:
                    key = phone
                else:
                    continue
                
                # Keep latest appointment date
                if key not in unique:
                    unique[key] = record
                else:
                    candidate_date = parse_appointment_date(record.get("appointment"))
                    existing_date = parse_appointment_date(unique[key].get("appointment"))
                    if candidate_date and existing_date:
                        if candidate_date > existing_date:
                            unique[key] = record
                    elif candidate_date and not existing_date:
                        unique[key] = record
            
            return unique
    except Exception as e:
        log(f"❌ Error loading all records: {e}")
        return {}


def save_all_records(records_dict):
    """Save all accumulated records from URL 1"""
    records_list = list(records_dict.values())
    with open(ALL_RECORDS_FILE, "w") as f:
        json.dump(records_list, f, indent=2)
    log(f"💾 Saved {len(records_list)} accumulated records to: {ALL_RECORDS_FILE}")


def load_scheduled_records():
    """Load scheduled records from file"""
    if not os.path.exists(SCHEDULED_RECORDS_FILE):
        return {}
    try:
        with open(SCHEDULED_RECORDS_FILE, "r") as f:
            records_list = json.load(f)
            # Convert to dict keyed by activity_id
            return {r['activity_id']: r for r in records_list}
    except Exception as e:
        log(f"❌ Error loading scheduled records: {e}")
        return {}


def save_scheduled_records(records_dict):
    """Save scheduled records to file"""
    records_list = list(records_dict.values())
    with open(SCHEDULED_RECORDS_FILE, "w") as f:
        json.dump(records_list, f, indent=2)
    log(f"💾 Saved {len(records_list)} scheduled records to: {SCHEDULED_RECORDS_FILE}")


def rescrape_scheduled_records():
    """Re-scrape each scheduled record individually to get current status"""
    from scraper import scrape_detail_parallel

    scheduled_records = load_scheduled_records()
    if not scheduled_records:
        log("ℹ️ No scheduled records to re-scrape")
        return

    log(f"🔄 Re-scraping {len(scheduled_records)} scheduled records...")

    updated_records = {}

    for activity_id, old_record in scheduled_records.items():
        try:
            log(f"🔍 Re-scraping scheduled record: {activity_id} ({old_record.get('name', '')})")

            # Scrape the detail page for this activity_id
            detail = scrape_detail_parallel(activity_id)

            if detail:
                # Merge the fresh detail data with the existing record
                updated_record = old_record.copy()
                updated_record.update(detail)

                # Re-scrape the summary data too (status, appointment, etc.)
                # For now, we'll keep the old summary data and just update details
                # The status will be updated when we do the normal scraping

                updated_records[activity_id] = updated_record
                log(f"✅ Updated scheduled record: {activity_id}")
            else:
                log(f"⚠️ Failed to scrape detail for: {activity_id}")
                # Keep the old record if scraping failed
                updated_records[activity_id] = old_record

        except Exception as e:
            log(f"❌ Error re-scraping {activity_id}: {e}")
            # Keep the old record on error
            updated_records[activity_id] = old_record

    # Overwrite the scheduled records file with updated data
    save_scheduled_records(updated_records)
    log(f"💾 Overwrote scheduled_records.json with {len(updated_records)} updated records")


def merge_new_with_accumulated(new_records, all_records_dict):
    """
    Merge new scraped records with accumulated records.
    Track which records are newly scraped.
    Returns: updated all_records_dict, newly_scraped_ids
    """
    newly_scraped_ids = set()
    
    for record in new_records:
        email = (record.get("email") or "").strip().lower()
        phone = re.sub(r"\D", "", record.get("phone") or "")
        
        if email:
            key = email
        elif phone:
            key = phone
        else:
            continue
        
        # Mark as newly scraped
        newly_scraped_ids.add(record.get("activity_id", ""))
        
        # If record doesn't exist or has newer appointment, update it
        if key not in all_records_dict:
            all_records_dict[key] = record
        else:
            candidate_date = parse_appointment_date(record.get("appointment"))
            existing_date = parse_appointment_date(all_records_dict[key].get("appointment"))
            if candidate_date and existing_date:
                if candidate_date > existing_date:
                    all_records_dict[key] = record
            elif candidate_date and not existing_date:
                all_records_dict[key] = record
    
    return all_records_dict, newly_scraped_ids


def apply_ghl_push_limits(contacts_to_send, max_records_config):
    """
    Apply per-tag limits to contacts before sending to GHL.
    
    If max_records_config is set:
    - ONLY send tags that are explicitly configured
    - Skip all other tags
    - Apply the specified limit for each configured tag
    
    If max_records_config is None/empty:
    - Send all records (no limits)
    
    Args:
        contacts_to_send: list of contact dicts
        max_records_config: dict like {"quoted_tag": 10, "completed_tag": 5} or None
    
    Returns:
        Limited list of contacts to send
    """
    if not max_records_config:
        return contacts_to_send  # No limits, return all
    
    # Group by tag
    by_tag = {}
    for contact in contacts_to_send:
        tags = contact.get("tags", ["unknown_tag"])

        for tag in tags:
            if tag not in by_tag:
                by_tag[tag] = []
            by_tag[tag].append(contact)
        if tag not in by_tag:
            by_tag[tag] = []
        by_tag[tag].append(contact)
    
    limited_contacts = []
    
    for tag, contacts in by_tag.items():
        max_for_tag = max_records_config.get(tag)
        
        if max_for_tag is None:
            # Tag NOT in config - skip entirely
            skipped = len(contacts)
            log(f"📤 {tag}: NOT in config, skipping all {skipped} records")
        else:
            # Tag in config - apply limit
            to_send = contacts[:max_for_tag]
            limited_contacts.extend(to_send)
            skipped = len(contacts) - len(to_send)
            log(f"📤 {tag}: Sending {len(to_send)}/{len(contacts)} records (limit: {max_for_tag}, deferred: {skipped})")
    
    return limited_contacts


def main():
    log("🚀 Starting script...")

    processed_ids = load_processed()
    new_processed = set(processed_ids)
    processed_hashes = load_processed_hashes()
    new_processed_hashes = dict(processed_hashes)

    # ============================================
    # STEP 0: RE-SCRAPE SCHEDULED RECORDS FIRST
    # ============================================
    log("\n📍 STEP 0: Re-scraping scheduled records...")
    rescrape_scheduled_records()

    # ============================================
    # STEP 1: SCRAPE & DEDUPE SALES DATA (URL 1)
    # ============================================
    log("\n📍 STEP 1: Scraping sales data from URL 1...")
    load_from_file = os.getenv("LOAD_FROM_FILE", "false").lower() in ("1", "true", "yes")
    
    newly_scraped_ids = set()
    
    if load_from_file and os.path.exists(DEDUPED_SALES_FILE):
        log("📂 Loading deduped sales from file...")
        with open(DEDUPED_SALES_FILE, "r") as f:
            sales_data = json.load(f)
        log(f"✅ Loaded {len(sales_data)} deduped sales from file")
    else:
        start_date, end_date = build_date_range()
        log(f"📅 Scraping range: {start_date} → {end_date}")
        raw_sales = scrape_all(start_date, end_date)
        log(f"📊 Scraped {len(raw_sales)} rows from URL 1")
        save_backup(raw_sales)
        save_last_date(end_date)
        
        # Deduplicate new scraped data
        new_deduped = filter_unique_by_email_phone(raw_sales)
        log(f"🔎 Deduped to {len(new_deduped)} unique sales records from this run")
        
        # Load accumulated records from all_records.json
        all_records_dict = load_all_records()
        log(f"📂 Loaded {len(all_records_dict)} accumulated records from previous runs")
        
        # Merge new scraped records with accumulated records
        all_records_dict, newly_scraped_ids = merge_new_with_accumulated(new_deduped, all_records_dict)
        log(f"🔗 Merged with accumulated records: {len(newly_scraped_ids)} newly scraped")
        
        # Save updated accumulated records
        save_all_records(all_records_dict)
        
        # Convert to list for further processing
        sales_data = list(all_records_dict.values())
        log(f"📊 Using {len(sales_data)} total records for comparison with URL 2")
    
    # Save deduped sales
    save_deduped_sales(sales_data)

    # ============================================
    # STEP 2: SCRAPE & DEDUPE INSTALLATIONS (URL 2)
    # ============================================
    log("\n📍 STEP 2: Scraping installations from URL 2...")
    
    if load_from_file and os.path.exists(DEDUPED_INSTALLATIONS_FILE):
        log("📂 Loading deduped installations from file...")
        with open(DEDUPED_INSTALLATIONS_FILE, "r") as f:
            installations_list = json.load(f)
        
        # Convert list to dict keyed by activity_id
        installations_dict = {}
        for inst in installations_list:
            act_id = inst.get('activity_id', '').strip()
            if act_id:
                installations_dict[act_id] = inst
        log(f"✅ Loaded {len(installations_dict)} deduped installations from file")
    else:
        raw_installations = scrape_installations()
        log(f"📊 Scraped {len(raw_installations)} installations from URL 2")
        
        # Installations already deduplicated by scraper (deduped by activity_id)
        installations_dict = raw_installations
        log(f"🔎 Already deduped by activity_id: {len(installations_dict)} installations")
    
    # Save deduped installations
    save_deduped_installations(installations_dict)

    # ============================================
    # STEP 3: COMPARE & MERGE DATA
    # ============================================
    log("\n📍 STEP 3: Comparing accumulated sales with installations by activity_id...")
    
    merged_data, max_install_date = merge_with_installations(sales_data, installations_dict)
    log(f"🔗 Merged {len(installations_dict)} installations with {len(sales_data)} sales records")
    
    if max_install_date:
        save_installation_last_date(max_install_date)
        log(f"📅 Saved max installation date: {max_install_date}")

    # ============================================
    # STEP 4: ADD TAGS & FINAL DATA
    # ============================================
    log("\n📍 STEP 4: Final filtering and tagging...")
    
    # Add tags for non-merged records
    final_data = add_tag_field(merged_data)
    log(f"🏷️  Added tags to {len(final_data)} records")
    
    # Save final merged data to file
    save_merged_final(final_data)

    # ============================================
    # STEP 4.5: ACCUMULATE SCHEDULED RECORDS
    # ============================================
    log("\n📍 STEP 4.5: Accumulating scheduled records...")
    scheduled_records_dict = {}
    
    for record in final_data:
        status = record.get('status', '').strip().lower()
        activity_id = record.get('activity_id', '')
        
        if status == 'scheduled' and activity_id:
            scheduled_records_dict[activity_id] = record
            log(f"📋 Added to scheduled: {record.get('name')} ({activity_id})")
    
    save_scheduled_records(scheduled_records_dict)
    log(f"💾 Saved {len(scheduled_records_dict)} scheduled records")

    # ============================================
    # STEP 5: SEND TO GHL
    # ============================================
    log("\n📍 STEP 5: Sending to GHL...")
    
    if ENABLE_GHL_PUSH:
        contacts_to_send = []
        updated_by_installation = set()
        
        # Track which activity_ids were updated by installations
        for record in final_data:
            activity_id = record.get("activity_id", "").strip()
            if activity_id in installations_dict:
                updated_by_installation.add(activity_id)
        
        # Collect records that have never been sent or whose content changed.
        # This also allows previously deferred contacts to be retried.
        for row in final_data:
            activity_id = row.get("activity_id", "").strip()
            status = row.get("status", "").strip().lower()
            
            # Skip scheduled records - they are not sent until status changes
            if status == "scheduled":
                log(f"⏸️  Skipping scheduled record: {row.get('name')} ({activity_id})")
                continue
            
            has_changed = has_record_changed(activity_id, row, processed_hashes)

            if has_changed:
                contacts_to_send.append(row)
                if activity_id not in processed_hashes:
                    reason = "unsent contact"
                elif activity_id in updated_by_installation:
                    reason = "updated with installation"
                else:
                    reason = "record changed"
                log(f"✅ Marked for send: {row.get('name')} ({reason})")
            else:
                log(f"⏭️  Skipped unchanged already sent: {row.get('name')} ({activity_id})")

        # Apply per-tag limits if configured
        if contacts_to_send:
            log(f"\n📊 {len(contacts_to_send)} contacts eligible to send")
            contacts_limited = apply_ghl_push_limits(contacts_to_send, CONFIG["max_records_to_push"])
            
            if contacts_limited:
                send_to_ghl(contacts_limited)
                
                # Update processed tracking and hashes for actually sent contacts
                for contact in contacts_limited:
                    activity_id = contact.get("activity_id", "").strip()
                    new_processed.add(activity_id)
                    new_processed_hashes[activity_id] = compute_record_hash(contact)
                
                log(f"✅ Sent {len(contacts_limited)}/{len(contacts_to_send)} eligible contacts to GHL")
                save_processed(new_processed)
                save_processed_hashes(new_processed_hashes)
                
                # Log deferred records
                deferred_count = len(contacts_to_send) - len(contacts_limited)
                if deferred_count > 0:
                    log(f"⏸️  {deferred_count} contacts deferred due to per-tag limits (will process next run)")
            else:
                log("ℹ️ No contacts to send after applying limits")
        else:
            log("ℹ️ No new or updated contacts to send to GHL")
    else:
        log("⏸️ GHL push is disabled. No contacts were sent.")

    cleanup_old_files()

    log(f"\n✅ COMPLETE. Processed {len(final_data)} final records")


if __name__ == "__main__":
    main()