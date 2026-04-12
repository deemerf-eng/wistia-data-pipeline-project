import os
import json
import logging
from datetime import datetime, timedelta, timezone

import boto3
import urllib.request
import urllib.parse
from botocore.exceptions import ClientError

# =========================
# Environment Variables
# =========================
WISTIA_SECRET_NAME = os.environ["WISTIA_SECRET_NAME"]
S3_BUCKET = os.environ["S3_BUCKET"]
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw").strip("/")
DDB_TABLE_NAME = os.environ["DDB_TABLE_NAME"]

PER_PAGE = int(os.environ.get("PER_PAGE", "100"))
DEFAULT_LOOKBACK_DAYS = int(os.environ.get("DEFAULT_LOOKBACK_DAYS", "1"))
WISTIA_API_VERSION = os.environ.get("WISTIA_API_VERSION", "2026-03")

# =========================
# AWS Clients
# =========================
s3 = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(DDB_TABLE_NAME)

# =========================
# Logging
# =========================
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# =========================
# Helpers
# =========================
def utc_now():
    return datetime.now(timezone.utc)

def date_str(dt):
    return dt.strftime("%Y-%m-%d")

def get_secret():
    response = secrets_client.get_secret_value(SecretId=WISTIA_SECRET_NAME)
    secret = response["SecretString"]

    try:
        secret_json = json.loads(secret)
        return secret_json.get("api_token") or secret_json.get("token") or secret
    except:
        return secret

def make_request(url, headers, params=None):
    if params:
        url = f"{url}?{urllib.parse.urlencode(params, doseq=True)}"

    req = urllib.request.Request(url, headers=headers)

    with urllib.request.urlopen(req) as response:
        data = response.read()
        return json.loads(data.decode("utf-8"))

def get_pipeline_state():
    try:
        resp = table.get_item(Key={"pipeline_name": "wistia_stats_pipeline"})
        return resp.get("Item", {})
    except ClientError:
        return {}

def update_pipeline_state(end_date):
    table.put_item(
        Item={
            "pipeline_name": "wistia_stats_pipeline",
            "last_successful_end_date": end_date,
            "last_run_ts": utc_now().isoformat()
        }
    )

def write_to_s3(data, key):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(data).encode("utf-8"),
        ContentType="application/json"
    )

def wistia_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "X-Wistia-API-Version": WISTIA_API_VERSION,
        "Accept": "application/json"
    }

# =========================
# Wistia API
# =========================
BASE_URL = "https://api.wistia.com/modern"

def get_all_media(token):
    url = f"{BASE_URL}/medias"
    headers = wistia_headers(token)

    params = {
        "per_page": PER_PAGE,
        "type": "Video",
        "archived": "false"
    }

    media = make_request(url, headers, params)

    # Defensive handling in case response shape differs
    if isinstance(media, list):
        return media
    elif isinstance(media, dict):
        return media.get("medias") or media.get("data") or media.get("results") or []
    else:
        return []

def get_media_stats_by_date(token, media_id, start_date, end_date):
    url = f"{BASE_URL}/stats/medias/{media_id}/by_date"
    headers = wistia_headers(token)

    params = {
        "start_date": start_date,
        "end_date": end_date
    }

    return make_request(url, headers, params)

def get_media_engagement(token, media_id):
    url = f"{BASE_URL}/stats/medias/{media_id}/engagement"
    headers = wistia_headers(token)

    return make_request(url, headers)

# =========================
# Lambda Handler
# =========================
def lambda_handler(event, context):
    logger.info("Starting Wistia ingestion...")

    token = get_secret()
    state = get_pipeline_state()

    if "last_successful_end_date" in state:
        # next day after last successful run
        start_dt = datetime.strptime(state["last_successful_end_date"], "%Y-%m-%d") + timedelta(days=1)
        start_date = start_dt.strftime("%Y-%m-%d")
    else:
        start_date = date_str(utc_now() - timedelta(days=DEFAULT_LOOKBACK_DAYS))

    end_date = date_str(utc_now())

    logger.info(f"Processing from {start_date} to {end_date}")

    media_list = get_all_media(token)
    logger.info(f"Found {len(media_list)} media")

    # Save media inventory too
    inventory_key = f"{RAW_PREFIX}/media_inventory/load_date={end_date}/media_inventory.json"
    write_to_s3(media_list, inventory_key)

    processed = 0
    failed = 0

    for media in media_list:
        media_id = media.get("hashed_id") or media.get("id")

        if not media_id:
            logger.warning(f"Skipping media with no id: {media}")
            continue

        try:
            stats = get_media_stats_by_date(token, media_id, start_date, end_date)
            stats_key = f"{RAW_PREFIX}/media_stats_by_date/load_date={end_date}/media_id={media_id}.json"
            write_to_s3(stats, stats_key)

            # optional engagement pull
            try:
                engagement = get_media_engagement(token, media_id)
                engagement_key = f"{RAW_PREFIX}/media_engagement/load_date={end_date}/media_id={media_id}.json"
                write_to_s3(engagement, engagement_key)
            except Exception as e:
                logger.warning(f"Engagement fetch failed for {media_id}: {str(e)}")

            processed += 1

        except Exception as e:
            failed += 1
            logger.error(f"Error processing {media_id}: {str(e)}")

    # only update watermark if no hard failures
    if failed == 0:
        update_pipeline_state(end_date)

    return {
        "statusCode": 200 if failed == 0 else 207,
        "body": json.dumps({
            "message": "Ingestion complete",
            "processed": processed,
            "failed": failed,
            "start_date": start_date,
            "end_date": end_date
        })
    }
