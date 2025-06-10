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
            "username": username,
            "count": 12
        }
        if end_cursor:
            payload["max_id"] = end_cursor

        print("📤 Requesting media with payload:", json.dumps(payload))
        response = requests.post(ROCKETAPI_MEDIA_URL, headers=headers, json=payload)

        try:
            res_data = response.json()
            print("📥 RocketAPI Response:")
            print(json.dumps(res_data, indent=2)[:1500])  # Truncated to avoid overload
        except Exception as e:
            print(f"❌ Failed to parse JSON for @{username}: {e}")
            print(response.text)
            return

        # Try multiple common schemas
        posts = []

        # RocketAPI V1 - Standard schema
        if "data" in res_data and "items" in res_data["data"]:
            posts = res_data["data"]["items"]

        # RocketAPI "response.body.data.user.edge_owner_to_timeline_media.edges" schema
        elif "response" in res_data:
            posts = (
                res_data.get("response", {})
                .get("body", {})
                .get("data", {})
                .get("user", {})
                .get("edge_owner_to_timeline_media", {})
                .get("edges", [])
            )
            posts = [edge.get("node", {}) for edge in posts]

        if not posts:
            print(f"⚠️ No posts found for @{username} in this batch.\n")
            break

        for post in posts:
            post_id = post.get("id")
            image_url = post.get("image_versions2", {}).get("candidates", [{}])[0].get("url", "")
            caption = post.get("caption", {}).get("text", "")
            timestamp = post.get("taken_at") or post.get("taken_at_timestamp")
            post_date = datetime.utcfromtimestamp(timestamp).isoformat() if timestamp else None
            shortcode = post.get("code") or post.get("shortcode")
            post_link = f"https://www.instagram.com/p/{shortcode}" if shortcode else None
            like_count = post.get("like_count") or post.get("edge_liked_by", {}).get("count", 0)
            comment_count = post.get("comment_count") or post.get("edge_media_to_comment", {}).get("count", 0)
            view_count = post.get("view_count", 0) if "video_versions" in post else None

            if not post_link:
                print(f"⚠️ Skipping post with no link or shortcode")
                continue

            if airtable_record_exists(post_link):
                print(f"🔁 Post already exists in Airtable: {post_link}")
                continue

            airtable_payload = {
                "fields": {
                    "Username": [username],
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
                requests.post(airtable_url, headers=AIRTABLE_HEADERS, json=airtable_payload)
                total_posts_synced += 1
                print(f"✅ Synced: {post_link}")
            except Exception as e:
                print(f"❌ Error uploading post to Airtable: {e}")

        # Check for next page
        end_cursor = res_data.get("data", {}).get("next_max_id")
        if not end_cursor:
            break

    print(f"🎯 Total posts synced for @{username}: {total_posts_synced}\n")
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
