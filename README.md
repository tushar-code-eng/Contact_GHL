# 📊 PRPT → GoHighLevel Automation Pipeline

This project automates the process of extracting contact data from the PRPT portal and sending it to GoHighLevel (GHL).

It is designed to run **locally**, requires **no cloud infrastructure**, and can be executed manually once per day.

---

## 🚀 Features

* 🔐 Automated login using Playwright
* 📥 Scrapes contact + detailed customer data
* 🧹 Deduplication (avoids duplicate uploads)
* ⚙️ Configurable fields (non-technical friendly)
* 💾 Local backup with timestamped files
* 🗑️ Auto-deletes backups older than 7 days
* 📤 Sends data to GoHighLevel via webhook/API
* 📝 Logging system for monitoring runs

---

## 🏗️ Project Structure

```
project/
│
├── main.py              # Entry point (run this)
├── scraper.py           # Scraping logic
├── uploader.py          # Sends data to GHL
├── dedupe.py            # Deduplication logic
├── config.py            # Loads environment variables
├── logger.py            # Logging system
├── field_config.json    # Field selection (editable)
│
├── data/
│   ├── backups/         # Saved data files
│   └── processed_ids.json
│
├── logs/
│   └── app.log          # Logs
│
└── .env                 # Credentials (DO NOT SHARE)
```

---

## ⚙️ Setup Instructions

### 1. Install Dependencies

```bash
pip install playwright requests python-dotenv
playwright install
```

---

### 2. Create `.env` File

Create a `.env` file in the root directory:

```
PRPT_USERNAME=your_login_email
PRPT_PASSWORD=your_login_password
GHL_WEBHOOK_URL=https://your-webhook-url
```

⚠️ Never share or upload this file.

---

### 3. Configure Fields (Non-Technical)

Edit `field_config.json`:

```json
{
  "name": true,
  "phone": true,
  "email": true,
  "status": true,
  "appointment": true,
  "contract_number": false,
  "opportunity_id": false,
  "service_region": false,
  "address": true
}
```

* `true` → field included
* `false` → field excluded

---

## ▶️ How to Run

Run the script manually:

```bash
python main.py
```

---

## 🔄 What Happens When You Run It

1. Logs into PRPT portal
2. Navigates to recent activities
3. Scrapes contact data
4. Fetches detailed data per contact
5. Filters fields based on config
6. Removes already processed entries
7. Sends new contacts to GoHighLevel
8. Saves backup locally
9. Deletes files older than 7 days
10. Logs all activity

---

## 💾 Backup System

* Files are saved in:

```
data/backups/
```

* Format:

```
YYYY-MM-DD_HH-MM-SS.json
```

Example:

```
2026-04-05_14-30-12.json
```

---

## 🧹 Deduplication

* Uses `activity_id` as unique identifier
* Stored in:

```
data/processed_ids.json
```

* Prevents duplicate uploads across runs

---

## 📝 Logs

Logs are saved in:

```
logs/app.log
```

Includes:

* Start/end of run
* Successful uploads
* Errors

---

## ⚠️ Important Notes

* First run may take longer due to login
* Website UI changes can break selectors
* Keep browser visible (`headless=False`) while debugging
* Ensure stable internet connection

---

## 🧑‍💼 For Non-Technical Users

You only need to:

1. Open the folder
2. Edit `field_config.json` (optional)
3. Double-click or run:

```bash
python main.py
```

That’s it ✅

---

## 🔒 Security

* Do NOT share `.env` file
* Do NOT expose webhook URL publicly
* Add `.env` to `.gitignore`

---

## 🚀 Future Improvements (Optional)

* Retry failed uploads
* CSV export for Excel users
* Simple UI (button-based execution)
* Scheduler for automatic daily runs
* Parallel scraping for speed

---

## 📞 Support

If something breaks:

* Check `logs/app.log`
* Run in visible mode (`headless=False`)
* Verify credentials in `.env`

---

## ✅ Summary

This project is a **lightweight data pipeline** that:

* Extracts data from a web portal
* Processes and filters it
* Sends it to GoHighLevel
* Maintains local backups and logs

All without requiring any paid infrastructure.

---
