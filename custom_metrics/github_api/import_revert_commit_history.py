import random
import requests
import json
import os
import time
import string

from datetime import datetime
from dateutil.parser import parse

from dotenv import load_dotenv
from github import Github
from google.cloud import bigquery
from google.oauth2 import service_account


load_dotenv()

# パラメータ設定
OWNER = "COUNTERWORKS"
REPOS = ["barbara", "corgi", "snuffy", "melanie", "shopcounter_rails"]
# REPOS = ["barbara", "corgi", "snuffy", "melanie", "shopcounter_rails"]
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
HEADERS = {'Authorization': f'token {GITHUB_TOKEN}'}

g = Github(GITHUB_TOKEN)

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

    total_commits = 3065
    cnt = 0

    for repo_name in REPOS:
        print("リポジトリ: ", repo_name)
        commits_url = fetch_commits(repo_name)
        bq_records = create_bq_records(commits_url)
        cnt += len(bq_records)
    #   print("取込レコード数: ", len(bq_records))
    #   if len(bq_records) > 0:
    #     insert_bigquery(bq_records)
        for record in bq_records:
            print("リバートコミットのsha: ",record["id"])
    revert_rate = cnt / total_commits if total_commits > 0 else 0
    print("リバート率: ",revert_rate)


def fetch_commits(repo):
    commits_url = f"https://api.github.com/repos/{OWNER}/{repo}/commits"
    return commits_url


def create_bq_records(commits_url):
    params = {'sha': 'main'}  # mainブランチのコミットを取得

    page = 1

    is_before_start_dt = False
    bq_records = []

    while True:
        params['page'] = page
        response = requests.get(commits_url, headers=HEADERS, params=params)

        if response.status_code != 200:
            break

        commits = response.json()
        if not commits:
            break

        for commit in commits:
            commit_date = parse(commit['commit']['committer']['date'])
            if commit_date < start_dt:
                is_before_start_dt = True
                break
            if start_dt <= commit_date <= end_dt:
                if 'revert' in commit['commit']['message'].lower():
                    bq_record = format_bq_columns(commit)
                    bq_records.append(bq_record)

        if is_before_start_dt:
            break

        # GitHub API rate limit is 5000 requests per hour for authenticated requests
        # To avoid hitting the rate limit, we can add a delay
        time.sleep(0.5)  # Adjust this as needed
        page += 1

    return bq_records

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
