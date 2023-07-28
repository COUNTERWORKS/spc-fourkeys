import random
import requests
import json
import os
import string

from datetime import datetime

from dotenv import load_dotenv
from github import Github
from google.cloud import bigquery
from google.oauth2 import service_account


load_dotenv()


# TODO: 各スクリプトで共通なので定数化
# パラメータ設定
OWNER = "COUNTERWORKS"
REPOS = ["barbara", "corgi", "snuffy", "melanie", "shopcounter_rails"]
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")

g = Github(GITHUB_TOKEN)

# BigQuery設定
credentials = service_account.Credentials.from_service_account_file(
    'secret/cw-spc-fourkeys.json')
PROJECT_ID = 'cw-spc-fourkeys'
DATASET_ID = 'four_keys'
TABLE_ID = 'api_pull_request_commit'

bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
table_ref = bq_client.dataset(DATASET_ID).table(TABLE_ID)
table = bq_client.get_table(table_ref)  # APIリクエスト

# 対象期間の設定
start_dt = datetime(2023, 7, 16)
end_dt = datetime(2023, 7, 19)

def import_pull_request_history():

    for repo_name in REPOS:
      print("リポジトリ: ", repo_name)
      repo = g.get_repo(f"{OWNER}/{repo_name}")
      pull_request_records = fetch_pull_request(repo)
      bq_records = create_bq_records(pull_request_records, repo_name, start_dt, end_dt)
      record_cnt = len(bq_records)
      print("取込レコード数: ", record_cnt)
      if record_cnt > 0:
        insert_bigquery(bq_records)

def fetch_pull_request(repo):
    pull_requests = repo.get_pulls(state='all', sort='created', direction='desc')
    return pull_requests

def create_bq_records(pull_requests, repo_name, since, until):

    bq_records = []

    for pr in pull_requests:
        created_at = pr.created_at  # プルリクエストの作成日時
        if since <= created_at <= until:
            commits = pr.get_commits()
            for commit in commits:
                row = {
                    "id": commit.sha,
                    "pull_request_number": f"{repo_name}/{pr.number}",
                    "metadata": json.dumps(commit.raw_data),
                    "time_created": commit.commit.committer.date.isoformat(),
                    "signature": generate_signature_number(),
                    "source": "github",
                }
                bq_records.append(row)

    return bq_records


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
    import_pull_request_history()
