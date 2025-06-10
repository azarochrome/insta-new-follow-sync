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
ROCKETAPI_MEDIA_URL = "https://v1.rocketapi.io/instagram/user/get_media_by_username"

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
    try:
        data = response.json()
        return data.get("response", {}).get("body", {}).get("data", {}).get("user", {}).get("edge_followed_by", {}).get("count", 0)
    except Exception as e:
        print(f"❌ Error parsing follower count for @{username}: {e}")
        return 0

def update_airtable_account(record_id, follower_count):
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ACCOUNTS_TABLE}/{record_id}"
    payload = {
        "fields": {
            "Latest Followers": follower_count,
            "Last Checked": datetime.utcnow().isoformat()
        }
    }
    response = requests.patch(url, headers=AIRTABLE_HEADERS, json=payload)
    print(f"✅ Airtable updated with {follower_count} followers")

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
    print(f"📝 Logged follower count for @{username}")

def airtable_record_exists(post_link):
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{POSTS_TABLE}"
    params = {
        "filterByFormula": f"{{Post Link}} = '{post_link}'"
    }
    response = requests.get(url, headers=AIRTABLE_HEADERS, params=params)
    return len(response.json().get("records", [])) > 0

def sync_instagram_posts(username):
    print(f"\n🖼 Fetching posts for @{username}...")

    headers = {
        "Authorization": f"Token {ROCKETAPI_TOKEN}",
        "Content-Type": "application/json"
    }

    end_cursor = None
    total_posts_synced = 0
    airtable_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{POSTS_TABLE}"

    while True:
        payload = {
            "username": username.strip(),
            "count": 12
        }
        if end_cursor:
            payload["max_id"] = end_cursor

        print("📤 Requesting media with payload:", json.dumps(payload))
        response = requests.post(ROCKETAPI_MEDIA_URL, headers=headers, json=payload)

        try:
            raw = response.json()
            body = raw.get("response", {}).get("body")
            if isinstance(body, str):
                body = json.loads(body)

            if not body or body.get("status") == "fail" or "items" not in body:
                print(f"🚫 Not authorized or no posts for @{username}: {body.get('message', 'No items')}")
                return

            posts = body["items"]

        except Exception as e:
            print(f"❌ Failed to parse RocketAPI response for @{username}: {e}")
            print(response.text)
            return

        if not posts:
            print(f"⚠️ No posts found for @{username} in this batch.")
            break

        for post in posts:
            shortcode = post.get("code") or post.get("shortcode")
            post_link = f"https://www.instagram.com/p/{shortcode}" if shortcode else None
            if not post_link:
                continue

            caption = post.get("caption", {}).get("text", "")
            timestamp = post.get("taken_at") or post.get("taken_at_timestamp")
            post_date = datetime.utcfromtimestamp(timestamp).isoformat() if timestamp else None
            image_url = post.get("image_versions2", {}).get("candidates", [{}])[0].get("url", "")
            like_count = post.get("like_count", 0)
            comment_count = post.get("comment_count", 0)

            # ✅ FIX: Better view count logic
            is_video = post.get("media_type") == 2
            view_count = post.get("view_count") if is_video else None

            # ✅ Check if post already exists
            existing_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{POSTS_TABLE}"
            params = {
                "filterByFormula": f"{{Post Link}} = '{post_link}'"
            }
            existing_response = requests.get(existing_url, headers=AIRTABLE_HEADERS, params=params)
            existing_records = existing_response.json().get("records", [])

            airtable_payload = {
                "fields": {
                    "Username": username,
                    "Image": [{"url": image_url}],
                    "Caption": caption,
                    "Post Date": post_date,
                    "Post Link": post_link,
                    "Likes": like_count,
                    "Comments": comment_count,
                    "Views": view_count,
                }
            }

            try:
                if existing_records:
                    # Update existing record
                    record_id = existing_records[0]["id"]
                    update_url = f"{existing_url}/{record_id}"
                    requests.patch(update_url, headers=AIRTABLE_HEADERS, json=airtable_payload)
                    print(f"🔁 Updated: {post_link}")
                else:
                    # Create new record
                    requests.post(existing_url, headers=AIRTABLE_HEADERS, json=airtable_payload)
                    print(f"✅ Synced: {post_link}")
                    total_posts_synced += 1
            except Exception as e:
                print(f"❌ Failed to sync Airtable post: {e}")

        end_cursor = body.get("next_max_id")
        if not end_cursor:
            break

    print(f"🎯 Total new posts synced for @{username}: {total_posts_synced}\n")

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
