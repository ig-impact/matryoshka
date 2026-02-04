WITH computed_stats AS (
    SELECT
        submissions.asset_id,
        DATE_TRUNC('second', MAX(submissions.submitted_at))
        AS computed_last_submission_at
    FROM
        {{ ref('stg_klt__kobo_submission') }} AS submissions
    GROUP BY
        submissions.asset_id
),
observed_stats AS (
    SELECT
        stats.asset_id,
        DATE_TRUNC('second', stats.last_submission_at)
        AS observed_last_submission_at
    FROM
        {{ ref('stg_klt__kobo_asset_stats') }} AS stats
)
SELECT
    computed_stats.asset_id,
    computed_stats.computed_last_submission_at,
    observed_stats.observed_last_submission_at
FROM
    computed_stats
JOIN
    observed_stats
    ON computed_stats.asset_id = observed_stats.asset_id
WHERE
    computed_stats.computed_last_submission_at
    IS DISTINCT FROM observed_stats.observed_last_submission_at
