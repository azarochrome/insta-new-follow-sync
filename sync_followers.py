import os
import json
import requests
from datetime import datetime

# --- ENV VARIABLES ---
AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
ROCKETAPI_TOKEN = os.environ.get("ROCKETAPI_TOKEN")

# --- ENDPOINTS ---
AIRTABLE_BASE_ID = "appoLMfEjRaZGXMh4"
ACCOUNTS_TABLE = "Instagram Statistics"
STATS_TABLE = "Instagram FC"
POSTS_TABLE = "Instagram Posts"

ROCKETAPI_INFO_URL = "https://v1.rocketapi.io/instagram/user/get_info"
ROCKETAPI_POSTS_URL = "https://v1.rocketapi.io/instagram/user/posts"

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}

# --- FUNCTIONS ---
def get_all_accounts():
    print("📦 Fetching all Instagram accounts from Airtable...")
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ACCOUNTS_TABLE}"
    response = requests.get(url, headers=AIRTABLE_HEADERS)
    response.raise_for_status()
    return response.json().get("records", [])

def get_follower_count(username):
    print(f"🔍 Fetching follower count for @{username}...")
    headers = {
        "Authorization": f"Token {ROCKETAPI_TOKEN}",
        "Content-Type": "application/json"
    }
    response = requests.post(ROCKETAPI_INFO_URL, headers=headers, json={"username": username})
    response.raise_for_status()
    data = response.json().get("data", {})
    return data.get("follower_count", 0)

def update_airtable_account(record_id, follower_count):
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ACCOUNTS_TABLE}/{record_id}"
    payload = {
        "fields": {
            "Latest Followers": follower_count,
            "Last Checked": datetime.utcnow().isoformat()
        }
    }
    requests.patch(url, headers=AIRTABLE_HEADERS, json=payload)

def log_statistics_entry(username, follower_count):
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{STATS_TABLE}"
    payload = {
        "fields": {
            "Username": [username],
            "Follower Count": follower_count,
            "Timestamp": datetime.utcnow().isoformat()
        }
    }
    requests.post(url, headers=AIRTABLE_HEADERS, json=payload)

def sync_instagram_posts(username):
    print(f"🖼 Fetching posts for @{username}...")
    headers = {
        "Authorization": f"Token {ROCKETAPI_TOKEN}",
        "Content-Type": "application/json"
    }
    response = requests.post(ROCKETAPI_POSTS_URL, headers=headers, json={"username": username})
    if response.status_code != 200:
        print(f"❌ Failed to fetch posts for @{username}: {response.text}")
        return

    posts = response.json().get("data", {}).get("items", [])
    airtable_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{POSTS_TABLE}"

    for post in posts:
        image_url = post.get("image_versions2", {}).get("candidates", [{}])[0].get("url", "")
        caption = post.get("caption", {}).get("text", "")
        timestamp = post.get("taken_at")
        post_date = datetime.utcfromtimestamp(timestamp).isoformat() if timestamp else None
        post_link = f"https://www.instagram.com/p/{post.get('code', '')}"

        payload = {
            "fields": {
                "Username": [username],
                "Image": [{"url": image_url}],
                "Caption": caption,
                "Post Date": post_date,
                "Post Link": post_link
            }
        }

        try:
            requests.post(airtable_url, headers=AIRTABLE_HEADERS, json=payload)
        except Exception as e:
            print(f"❌ Error uploading post for @{username}: {e}")

    print(f"✅ Synced {len(posts)} posts for @{username}")

# --- MAIN ---
def main():
    try:
        records = get_all_accounts()
    except Exception as e:
        print(f"❌ Failed to fetch Airtable records: {e}")
        return

    print(f"🔢 Found {len(records)} accounts in Airtable.\n")

    for record in records:
        fields = record.get("fields", {})
        username = fields.get("Username")
        record_id = record.get("id")

        if not username:
            print("⚠️ Skipping: no Username provided.")
            continue

        try:
            follower_count = get_follower_count(username)
            update_airtable_account(record_id, follower_count)
            log_statistics_entry(username, follower_count)
            sync_instagram_posts(username)
            print(f"✅ Finished syncing for @{username}\n")

        except Exception as e:
            print(f"❌ Error processing @{username}: {e}\n")

    print("🏁 All accounts synced successfully.")

if __name__ == "__main__":
    main()

