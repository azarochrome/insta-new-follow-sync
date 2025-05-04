import os
import json
import requests
import urllib.parse
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
    print("📦 Fetching active Airtable records...")
    formula = "OR({Status} = '✅ OK', {Status} = 'Ad Issue?')"
    params = {
        "filterByFormula": formula
    }
    response = requests.get(AIRTABLE_URL, headers=headers, params=params)
    response.raise_for_status()
    return response.json().get("records", [])

def extract_sheet_id(sheet_url):
    try:
        return sheet_url.split("/d/")[1].split("/")[0]
    except (IndexError, AttributeError):
        return None
