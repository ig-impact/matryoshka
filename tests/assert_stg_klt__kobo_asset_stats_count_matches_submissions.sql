WITH computed_stats AS (
    SELECT
        submissions.asset_id,
        COUNT(submissions.submission_id) AS computed_count_submissions,
        MIN(submissions.submitted_at) AS oldest_submission_at
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
),
exclusions AS (
    SELECT
        asset_id
    FROM
        {{ ref('submission_count_check_exclusions') }}
)
SELECT
    computed_stats.asset_id,
    computed_stats.computed_count_submissions,
    observed_stats.observed_count_submissions,
    computed_stats.oldest_submission_at
FROM
    computed_stats
LEFT JOIN
    observed_stats
    ON computed_stats.asset_id = observed_stats.asset_id
LEFT JOIN
    exclusions
    ON computed_stats.asset_id = exclusions.asset_id
WHERE
    exclusions.asset_id IS NULL
    AND (
        observed_stats.observed_count_submissions IS NULL
        OR (
            computed_stats.computed_count_submissions < observed_stats.observed_count_submissions
            AND computed_stats.oldest_submission_at >= CURRENT_DATE - INTERVAL '1 year'
        )
    )
