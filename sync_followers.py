import os
import json
import requests
from rocketapi import InstagramAPI
from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- ENV VARIABLES ---
AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
ROCKETAPI_TOKEN = os.environ.get("ROCKETAPI_TOKEN")
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON")

# --- CONFIG ---
AIRTABLE_BASE_ID = "appTxTTXPTBFwjelH"
AIRTABLE_TABLE_NAME = "Accounts"
AIRTABLE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"

# --- INIT SERVICES ---
headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
instagram_api = InstagramAPI(token=ROCKETAPI_TOKEN)

credentials = service_account.Credentials.from_service_account_info(
    json.loads(GOOGLE_CREDENTIALS_JSON),
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
sheets_service = build("sheets", "v4", credentials=credentials)

# --- FUNCTIONS ---
def get_active_accounts():
    print("Fetching active Airtable records...")
    params = {
        "filterByFormula": "OR({STATUS} = '✅ OK', {STATUS} = 'Ad Issue?')"
    }
    response = requests.get(AIRTABLE_URL, headers=headers, params=params)
    response.raise_for_status()
    return response.json().get("records", [])

def extract_sheet_id(sheet_url):
    try:
        return sheet_url.split("/d/")[1].split("/")[0]
    except (IndexError, AttributeError):
        return None

def get_instagram_user_id(username):
    try:
        user_info = instagram_api.get_web_profile_info(username)
        return user_info.get("id")
    except Exception as e:
        print(f"❌ Could not get IG user ID for {username}: {e}")
        return None

def get_followers(user_id):
    followers = []
    max_id = None
    try:
        while True:
            result = instagram_api.get_user_followers(user_id, count=50, max_id=max_id)
            users = result.get("users", [])
            followers.extend([u.get("username") for u in users])
            max_id = result.get("next_max_id")
            if not max_id:
                break
    except Exception as e:
        print(f"❌ Error fetching followers: {e}")
    return followers

def update_google_sheet(sheet_id, followers):
    try:
        range_name = "Sheet1!A:A"
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=range_name
        ).execute()
        existing = result.get("values", [])
        existing_usernames = {row[0] for row in existing if row}

        new_followers = [[f] for f in followers if f not in existing_usernames]

        if new_followers:
            sheets_service.spreadsheets().values().append(
                spreadsheetId=sheet_id,
                range="Sheet1!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": new_followers}
            ).execute()
            print(f"✅ Added {len(new_followers)} new followers to sheet {sheet_id}")
        else:
            print(f"✅ No new followers to add for sheet {sheet_id}")
    except Exception as e:
        print(f"❌ Failed to update Google Sheet {sheet_id}: {e}")

# --- MAIN ---
def main():
    records = get_active_accounts()
    print(f"🔍 Found {len(records)} active records.")

    for record in records:
        fields = record.get("fields", {})
        username = fields.get("Name")  # Adjust if username is in another field
        sheet_url = fields.get("Google Sheets")

        if not username or not sheet_url:
            print("⚠️ Missing username or sheet URL. Skipping record.")
            continue

        sheet_id = extract_sheet_id(sheet_url)
        if not sheet_id:
            print(f"⚠️ Invalid sheet URL for {username}")
            continue

        user_id = get_instagram_user_id(username)
        if not user_id:
            continue

        followers = get_followers(user_id)
        if not followers:
            print(f"⚠️ No followers found for {username}")
            continue

        update_google_sheet(sheet_id, followers)

if __name__ == "__main__":
    main()
