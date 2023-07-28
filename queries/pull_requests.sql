
"""
1.mainへのプルリクがmerge
 * 条件
   * event_type = pull_request
   * JSON_EXTRACT_SCALAR(e.metadata, '$.pull_request.base.ref') = main
   * merged_at is not null
  * 取得項目
    * number
    * pull_request.merge_commit_sha
    * pull_request.merged_at
    * pull_request.head.repo.name
2.mainへのプルリクに含まれているコミットのsha（developにマージされたコミット）を取得
* 条件
   * event_type = pull_request_commit
   * JSON_EXTRACT_SCALAR(e.metadata, '$.pull_request.base.ref') = main
   * merged_at is not null
  * 取得項目
    * number
    * pull_request.merge_commit_sha
    * pull_request.merged_at
    * pull_request.head.repo.name
3.2のコミットのshaからfeature → developのプルリクの番号を取得
4.3のプルリクに含まれているコミットのshaを取得
5.4から最も早いコミット時間が初回コミットとする
"""

-- 1.mainへマージされたプルリク
WITH main_merged_pull_request AS (
  SELECT
    id AS main_pull_request_id,
    JSON_EXTRACT_SCALAR(metadata, '$.merged_at') AS main_merged_time
  FROM `cw-spc-fourkeys.four_keys.api_pull_request` pull_requests
  WHERE
    JSON_EXTRACT_SCALAR(metadata, '$.base.ref') = "main"
    AND JSON_EXTRACT_SCALAR(metadata, '$.head.ref') = "develop"
    AND JSON_EXTRACT_SCALAR(metadata, '$.merged_at') IS NOT NULL
),
-- 2.mainへマージ済のdevelopのマージコミット
develop_merged_commits AS (
  SELECT
    commits.id AS develop_merge_commit_sha,
    commits.pull_request_id AS pull_request_id,
    main_merged_pull_request.main_merged_time,
    JSON_EXTRACT_SCALAR(commits.metadata, '$.commit.committer.date') AS develop_merged_time
  FROM main_merged_pull_request
  INNER JOIN four_keys.api_pull_request_commit commits
    ON main_merged_pull_request.main_pull_request_id = commits.pull_request_id
),
-- 3.2のプルリクNO
develop_merged_pull_request AS (
  SELECT
    pull_requests.id,
    develop_merged_commits.develop_merged_time,
    develop_merged_commits.main_merged_time
  FROM develop_merged_commits
  LEFT OUTER JOIN `cw-spc-fourkeys.four_keys.api_pull_request` pull_requests
    ON develop_merged_commits.develop_merge_commit_sha = JSON_EXTRACT_SCALAR(pull_requests.metadata, '$.merge_commit_sha')
  WHERE
    JSON_EXTRACT_SCALAR(pull_requests.metadata, '$.base.ref') = "develop"
    AND JSON_EXTRACT_SCALAR(pull_requests.metadata, '$.head.ref') LIKE "feature%"
    AND JSON_EXTRACT_SCALAR(pull_requests.metadata, '$.merged_at') IS NOT NULL
),
-- 4.3のコミット群
feature_commits AS (
  SELECT
    commits.id AS feature_commit_sha,
    commits.pull_request_id AS pull_request_id,
    JSON_EXTRACT_SCALAR(commits.metadata, '$.commit.committer.date') AS commit_time,
    develop_merged_pull_request.develop_merged_time,
    develop_merged_pull_request.main_merged_time,
  FROM develop_merged_pull_request
  LEFT OUTER JOIN cw-spc-fourkeys.four_keys.api_pull_request_commit commits
    ON develop_merged_pull_request.id = commits.pull_request_id
)

SELECT
pull_request_id,
MIN(commit_time) AS first_commit_time,
develop_merged_time,
main_merged_time
FROM feature_commits
WHERE pull_request_id like "shopcounter_rails%"
group by 1,3,4
LIMIT 100
