WITH kobo_asset AS (
    SELECT
        *
    FROM
        {{ ref('stg_klt__kobo_asset') }}
)
SELECT
    kobo_asset.created_at,
    kobo_asset.modified_at
FROM
    kobo_asset
WHERE
    kobo_asset.modified_at IS NULL
    OR kobo_asset.created_at IS NULL
    OR kobo_asset.created_at > kobo_asset.modified_at
