WITH src AS (
    SELECT
        *
    FROM
        {{ source(
            'klt',
            'kobo_asset'
        ) }}
)
SELECT
    "uid" AS asset_id,
    _dlt_id AS asset_load_id,
    TO_TIMESTAMP(
        _dlt_load_id :: DOUBLE PRECISION
    ) AT TIME ZONE 'UTC' AS loaded_at,
    date_created AT TIME ZONE 'UTC' AS created_at,
    date_modified AT TIME ZONE 'UTC' AS modified_at,
    date_deployed AT TIME ZONE 'UTC' AS deployed_at,
    deployment__last_submission_time AT TIME ZONE 'UTC' AS last_submission_at,
    "name" AS asset_name,
    deployment__active AS deployment_active,
    deployment__submission_count AS count_submissions
FROM
    src
