import random
import requests
import json
import os
import time
import string

from datetime import datetime, timedelta
from dateutil.parser import parse

from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account


load_dotenv()

# パラメータ設定
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
NOTION_DATABASE_INCIDENT = os.environ.get("NOTION_DATABASE_INCIDENT_ID")
NOTION_API_VERSION = os.environ.get("NOTION_API_VERSION")
endpoint_url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_INCIDENT}/query"

headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_API_VERSION
}

# BigQuery設定
credentials = service_account.Credentials.from_service_account_file(
    'secret/cw-spc-fourkeys.json')
PROJECT_ID = 'cw-spc-fourkeys'
DATASET_ID = 'four_keys'
TABLE_ID = 'revert'

# bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
# table_ref = bq_client.dataset(DATASET_ID).table(TABLE_ID)
# table = bq_client.get_table(table_ref)  # APIリクエスト

# 対象期間の設定
start_dt = parse('2023-04-01T00:00:00Z')  # 開始日
end_dt = parse('2023-07-10T00:00:00Z')


def import_revert_commit_history():

    # レスポンスの確認
    incidents = fetch_incidents()
    for incident in incidents:
        # ページのIDとタイトルを表示
        page_id = incident["id"]
        title = page["properties"]["Name"]["title"][0]["plain_text"] 

    cnt += len(bq_records)
    #   print("取込レコード数: ", len(bq_records))
    #   if len(bq_records) > 0:
    #     insert_bigquery(bq_records)
    for record in bq_records:
        print("リバートコミットのsha: ",record["id"])
revert_rate = cnt / total_commits if total_commits > 0 else 0
print("リバート率: ",revert_rate)


def fetch_incidents():
    response = requests.post(f"https://api.notion.com/v1/databases/{NOTION_DATABASE_INCIDENT}/query", headers=headers)
    notion_pages = response.json()

    notion_pages = notion_pages["results"]
    return pages


def create_bq_records(notion_pages):

    for result in notion_pages["results"]:
        properties = result["properties"]
        # 発覚日時が存在しない場合はスキップ
        if not properties["発覚日時"]["date"] or not properties["発覚日時"]["date"]["start"]:
            continue
        page_id = result["id"]
        # 
        start_date_str = properties["発覚日時"]["date"]["start"]
        # 日時（分まで）が記載されているかチェック
        if "T" in start_date_str:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%dT%H:%M:%S.%f%z')
        else:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        # 解消日時の取得
        if properties["解消日時"]["date"] and properties["解消日時"]["date"]["start"]:
            end_date_str = properties["解消日時"]["date"]["start"]
            # 日時（分まで）が記載されているかチェック
            if "T" in end_date_str:
                end_date = datetime.strptime(end_date_str, '%Y-%m-%dT%H:%M:%S.%f%z')
            else:
                end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        else:
            end_date = start_date + timedelta(hours=4)  # 発覚〜解消までの時間を4時間とする
        # 発生場所の取得
        incident_locations = properties["発生場所"].get("multi_select")
        # 場所がない、または"Enterprise"がある場合はスキップ
        if not incident_locations or any(item["name"] == "Enterprise" for item in incident_locations):
            continue
        # "Enterprise"以外の場所を取得
        incident_location_filtered = [item["name"] for item in incident_locations]

    return incident_location_filtered

def format_bq_columns(commit):

    formatted_commit = {
        "id": commit["sha"],
        "metadata": commit,
        "time_created": commit["commit"]["committer"]["date"],
        "signature": generate_signature_number(),
        "source": "github"
    }
    return formatted_commit


def generate_signature_number():
    prefix = "api="
    digits = string.digits

    # 45桁のランダムな数字列を生成
    random_number = ''.join(random.choice(digits) for _ in range(45 - len(prefix)))

    # 先頭にプレフィックスを追加
    random_number_with_prefix = prefix + random_number

    return random_number_with_prefix


def insert_bigquery(rows):

    # BigQueryにデータをアップロード
    errors = bq_client.insert_rows_json(table, rows)
    if errors:
        print("Encountered errors while inserting rows: {}".format(errors))
    else:
        print("Rows inserted successfully.")


if __name__ == "__main__":
    import_revert_commit_history()
