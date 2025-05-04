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
def get_all_accounts():
    print("📦 Fetching ALL Airtable records (no status filter)...")
    response = requests.get(AIRTABLE_URL, headers=headers)
    response.raise_for_status()
    return response.json().get("records", [])

def extract_sheet_id(sheet_url):
    try:
        return sheet_url.split("/d/")[1].split("/")[0]
    except (IndexError, AttributeError):
        return None

def get_instagram_user_id(username):
    try:
        raw = instagram_api.get_web_profile_info(username)
        print(f"🧪 DEBUG [{username}] RocketAPI raw response:")
        print(json.dumps(raw, indent=2)[:1500])  # limit to 1500 chars

        user_id = raw.get("response", {}).get("body", {}).get("data", {}).get("user", {}).get("id")
        if not user_id:
            raise ValueError("ID not found in RocketAPI response")
        return user_id

    except Exception as e:
        print(f"❌ IG user ID not found for @{username}: {e}")
        return None

def get_followers(user_id, username):
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
        print(f"❌ Error fetching followers for @{username}: {e}")
    print(f"📊 Pulled {len(followers)} followers from @{username}")
    return followers

def update_google_sheet(sheet_id, followers, username):
    try:
        range_name = f"{username}!A:A"
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
                range=f"{username}!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": new_followers}
            ).execute()
            print(f"✅ Synced {len(new_followers)} new followers from @{username} → Sheet tab: {username}")
        else:
            print(f"✅ No new followers to sync for @{username}")
    except Exception as e:
        print(f"❌ Failed to update Google Sheet tab {username} in {sheet_id}: {e}")

# --- MAIN ---
def main():
    records = get_all_accounts()
    print(f"\n🔍 Found {len(records)} total records in Airtable.")

    for record in records:
        fields = record.get("fields", {})
        username = fields.get("Username")
        sheet_url = fields.get("Google Sheets")

        if not username:
            print("⚠️ Skipping: no Username provided.")
            continue
        if not sheet_url:
            print(f"⚠️ Skipping @{username}: no Google Sheets URL.")
            continue

        sheet_id = extract_sheet_id(sheet_url)
        if not sheet_id:
            print(f"⚠️ Skipping @{username}: invalid sheet URL.")
            continue

        print(f"\n🔄 Processing @{username} (IG)...")

        user_id = get_instagram_user_id(username)
        if not user_id:
            print(f"⚠️ Skipping @{username}: Instagram user not found.")
            continue

        followers = get_followers(user_id, username)
        if not followers:
            print(f"⚠️ Skipping @{username}: no followers returned.")
            continue

        update_google_sheet(sheet_id, followers, username)

    print("\n✅ Follower sync completed.")

if __name__ == "__main__":
    main()
