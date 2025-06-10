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
ROCKETAPI_CLIPS_URL = "https://v1.rocketapi.io/instagram/user/get_clips"

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}

# --- FUNCTIONS ---
def get_all_accounts():
    print("\U0001F4E6 Fetching all Instagram accounts from Airtable...")
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ACCOUNTS_TABLE}"
    response = requests.get(url, headers=AIRTABLE_HEADERS)
    response.raise_for_status()
    return response.json().get("records", [])

def get_follower_count(username):
    print(f"\U0001F50D Fetching follower count for @{username}...")
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
    requests.patch(url, headers=AIRTABLE_HEADERS, json=payload)
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

def fetch_posts(endpoint_url, payload):
    headers = {
        "Authorization": f"Token {ROCKETAPI_TOKEN}",
        "Content-Type": "application/json"
    }
    return requests.post(endpoint_url, headers=headers, json=payload)

def sync_instagram_media(username, media_type="posts"):
    print(f"\n🖼 Fetching {media_type} for @{username}...")
    endpoint_url = ROCKETAPI_CLIPS_URL if media_type == "reels" else ROCKETAPI_MEDIA_URL
    payload = {"username": username.strip(), "count": 12} if media_type == "posts" else {}
    end_cursor = None

    total_synced = 0

    while True:
        if end_cursor:
            payload["max_id"] = end_cursor
        if media_type == "reels":
            user_id = get_user_id(username)
            if not user_id:
                print(f"❌ Could not find user ID for @{username}.")
                return
            payload["id"] = user_id

        print("📤 Requesting:", json.dumps(payload))
        response = fetch_posts(endpoint_url, payload)

        try:
            raw = response.json()
            body = raw.get("response", {}).get("body")
            if isinstance(body, str):
                body = json.loads(body)
            posts = body.get("items", [])
        except Exception as e:
            print(f"❌ Failed to parse {media_type} for @{username}: {e}")
            return

        if not posts:
            print(f"⚠️ No {media_type} found for @{username} in this batch.")
            break

        for post in posts:
            post = post.get("media", post)
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
            view_count = post.get("play_count") or post.get("view_count") if post.get("media_type") == 2 else None

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

            existing_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{POSTS_TABLE}"
            params = {"filterByFormula": f"{{Post Link}} = '{post_link}'"}
            existing_response = requests.get(existing_url, headers=AIRTABLE_HEADERS, params=params)
            existing_records = existing_response.json().get("records", [])

            try:
                if existing_records:
                    record_id = existing_records[0]["id"]
                    update_url = f"{existing_url}/{record_id}"
                    requests.patch(update_url, headers=AIRTABLE_HEADERS, json=airtable_payload)
                    print(f"🔁 Updated: {post_link}")
                else:
                    requests.post(existing_url, headers=AIRTABLE_HEADERS, json=airtable_payload)
                    print(f"✅ Synced: {post_link}")
                    total_synced += 1
            except Exception as e:
                print(f"❌ Airtable sync error: {e}")

        end_cursor = body.get("next_max_id")
        if not end_cursor:
            break

    print(f"🎯 Total new {media_type} synced for @{username}: {total_synced}\n")

def get_user_id(username):
    headers = {
        "Authorization": f"Token {ROCKETAPI_TOKEN}",
        "Content-Type": "application/json"
    }
    response = requests.post(ROCKETAPI_INFO_URL, headers=headers, json={"username": username})
    try:
        return response.json().get("response", {}).get("body", {}).get("user", {}).get("pk")
    except:
        return None

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
            sync_instagram_media(username, "posts")
            sync_instagram_media(username, "reels")
            print(f"✅ Finished syncing for @{username}\n")
        except Exception as e:
            print(f"❌ Error processing @{username}: {e}\n")

    print("🏁 All accounts synced successfully.")

if __name__ == "__main__":
    main()
