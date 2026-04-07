import http.client
import json
from logger import log, error
from config import CONFIG

def map_contact_to_ghl(contact):
    """Map scraped contact data to GHL API format"""
    # Split name into first and last (simple split on first space)
    name_parts = contact.get("name", "").split(" ", 1)
    first_name = name_parts[0] if name_parts else ""
    last_name = name_parts[1] if len(name_parts) > 1 else ""

    payload = {
        "firstName": first_name,
        "lastName": last_name,
        "name": contact.get("name", ""),
        "email": contact.get("email", ""),
        "locationId": CONFIG["ghl_location_id"],
        "phone": contact.get("phone", ""),
        "address1": contact.get("address", ""),
        "source": "Contact Automation Script",
        "createNewIfDuplicateAllowed": False
    }

    # Add optional fields if they exist
    if contact.get("status"):
        # You might want to add status as a tag or custom field
        payload["tags"] = [contact["status"]]

    # Remove empty fields
    payload = {k: v for k, v in payload.items() if v not in [None, "", []]}

    return payload

def send_to_ghl(contact_data):
    """Send contact(s) to GHL via API"""
    try:
        conn = http.client.HTTPSConnection("services.leadconnectorhq.com")

        # Handle both single contact and list of contacts
        if isinstance(contact_data, list):
            # Send each contact individually (GHL API doesn't support batch)
            sent_count = 0
            for contact in contact_data:
                payload = map_contact_to_ghl(contact)
                success = send_single_contact(conn, payload)
                if success:
                    sent_count += 1
            log(f"✅ Sent {sent_count}/{len(contact_data)} contacts to GHL")
        else:
            # Single contact
            payload = map_contact_to_ghl(contact_data)
            if send_single_contact(conn, payload):
                log(f"✅ Sent: {contact_data.get('name')}")

        conn.close()

    except Exception as e:
        error(f"❌ Exception: {str(e)}")

def send_single_contact(conn, payload):
    """Send a single contact to GHL API"""
    try:
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Version': '2021-07-28',
            'Authorization': f'Bearer {CONFIG["ghl_api_token"]}'
        }

        conn.request("POST", "/contacts/upsert", json.dumps(payload), headers)
        res = conn.getresponse()
        data = res.read()

        if res.status == 200 or res.status == 201:
            return True
        else:
            error(f"❌ GHL API Error ({res.status}): {data.decode('utf-8')}")
            return False

    except Exception as e:
        error(f"❌ API Request Exception: {str(e)}")
        return False