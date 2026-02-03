WITH computed_stats AS (
    SELECT
        submissions.asset_id,
        COUNT(submissions.submission_id) AS computed_count_submissions
    FROM
        {{ ref('stg_klt__kobo_submission') }} AS submissions
    GROUP BY
        submissions.asset_id
),
observed_stats AS (
    SELECT
        stats.asset_id,
        stats.count_submissions AS observed_count_submissions
    FROM
        {{ ref('stg_klt__kobo_asset_stats') }} AS stats
)
SELECT
    computed_stats.asset_id,
    computed_stats.computed_count_submissions,
    observed_stats.observed_count_submissions
FROM
    computed_stats
JOIN
    observed_stats
    ON computed_stats.asset_id = observed_stats.asset_id
WHERE
    computed_stats.computed_count_submissions IS NULL
    OR observed_stats.observed_count_submissions IS NULL
    OR computed_stats.computed_count_submissions
    < observed_stats.observed_count_submissions
