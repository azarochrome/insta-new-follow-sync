import os
import json
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- ENV VARIABLES ---
AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
ROCKETAPI_TOKEN = os.environ.get("ROCKETAPI_TOKEN")
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON")
ROCKETAPI_URL = "https://v1.rocketapi.io/instagram/user/followers"

# --- CONFIG ---
AIRTABLE_BASE_ID = "appTxTTXPTBFwjelH"
AIRTABLE_TABLE_NAME = "Accounts"
AIRTABLE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"

# --- INIT SERVICES ---
headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}

credentials = service_account.Credentials.from_service_account_info(
    json.loads(GOOGLE_CREDENTIALS_JSON),
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
sheets_service = build("sheets", "v4", credentials=credentials)

# --- FUNCTIONS ---
def get_all_accounts():
    print("\U0001F4E6 Fetching ALL Airtable records (no status filter)...")
    response = requests.get(AIRTABLE_URL, headers=headers)
    response.raise_for_status()
    return response.json().get("records", [])

def extract_sheet_id(sheet_url):
    try:
        return sheet_url.split("/d/")[1].split("/")[0]
    except (IndexError, AttributeError):
        return None

def get_followers(username):
    followers = []
    next_max_id = None
    headers = {
        "Authorization": f"Token {ROCKETAPI_TOKEN}",
        "Content-Type": "application/json"
    }
    print(f"\n🔄 Processing @{username} (IG)...")

    while True:
        payload = {"username": username}
        if next_max_id:
            payload["next_max_id"] = next_max_id

        try:
            response = requests.post(ROCKETAPI_URL, headers=headers, json=payload)
            if response.status_code != 200:
                print(f"❌ Failed to fetch from RocketAPI ({response.status_code}): {response.text}")
                break
            data = response.json()
            print(f"🧪 DEBUG [{username}] RocketAPI raw response:\n***\n  {json.dumps(data.get('data', {}), indent=2)}\n***")

            users = data.get("data", {}).get("users", [])
            followers.extend([user.get("username") for user in users if user.get("username")])

            next_max_id = data.get("data", {}).get("next_max_id")
            if not next_max_id:
                break

        except Exception as e:
            print(f"❌ Error fetching followers for @{username}: {e}")
            break

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
            print(f"✅ No new followers to sync from @{username} → Sheet tab: {username}")

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

        followers = get_followers(username)
        if not followers:
            print(f"⚠️ Skipping @{username}: no followers returned.")
            continue

        update_google_sheet(sheet_id, followers, username)

    print("\n✅ Follower sync completed.")

if __name__ == "__main__":
    main()
