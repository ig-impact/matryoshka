WITH src AS (
    SELECT
        *
    FROM
        {{ source(
            'klt',
            'kobo_submission'
        ) }}
)
SELECT
    _id AS submission_id,
    _dlt_id AS submission_load_id,
  asset_uid as asset_id,
  TO_TIMESTAMP(
        _dlt_load_id :: DOUBLE PRECISION
    ) AT TIME ZONE 'UTC' AS loaded_at,
  _submission_time at time zone 'utc' as submitted_at
FROM
    src
