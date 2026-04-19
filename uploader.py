import http.client
import json
from logger import log, error
from config import CONFIG

def map_contact_to_ghl(contact):
    """Map scraped contact data to GHL API format according to GHL schema"""
    # Split name into first and last (simple split on first space)
    name_parts = contact.get("name", "").split(" ", 1)
    first_name = name_parts[0] if name_parts else ""
    last_name = name_parts[1] if len(name_parts) > 1 else ""

    payload = {
        # Required
        "locationId": CONFIG["ghl_location_id"],
        
        # Core contact info
        "firstName": first_name,
        "lastName": last_name,
        "name": contact.get("name", ""),
        "email": contact.get("email", ""),
        "phone": contact.get("phone", ""),
        
        # Address fields
        "address1": contact.get("address", ""),
        
        # Additional optional fields we can populate
        "source": "Contact Automation Script",
        "createNewIfDuplicateAllowed": False
    }

    # Add tags if available
    if contact.get("tag"):
        payload["tags"] = [contact["tag"]]
    
    # Add status as additional tag if available
    if contact.get("status"):
        if "tags" not in payload:
            payload["tags"] = []
        status_tag = f"{contact['status'].lower()}_tag"
        if status_tag not in payload["tags"]:
            payload["tags"].append(status_tag)

    # Add additional details to GHL custom fields
    custom_fields = []
    for field_name in ["activity_id", "self_gen", "appointment", "quote_option", "is_primary", "contract_total", "area", "product_line", "series", "style"]:
        value = contact.get(field_name)
        if value is not None and value != "":
            key = "appointment_date" if field_name == "appointment" else field_name
            # Convert boolean values to strings for GHL
            if isinstance(value, bool):
                field_value = str(value).lower()
            elif isinstance(value, list):
                field_value = ", ".join(str(v) for v in value if v is not None)
            else:
                field_value = str(value)
            custom_fields.append({"key": key, "field_value": field_value})

    if contact.get("status") not in [None, ""]:
        custom_fields.append({"key": "status", "field_value": str(contact.get("status"))})

    if custom_fields:
        payload["customFields"] = custom_fields

    # Remove null, empty string, and empty list values
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