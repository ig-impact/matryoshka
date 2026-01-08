WITH kobo_asset_content AS (
    SELECT
        *
    FROM
        {{ source(
            'klt',
            'kobo_asset_content'
        ) }}
),
kobo_asset_content__survey as (
(
    SELECT
        *
    FROM
        {{ source(
            'klt',
            'kobo_asset_content__survey'
        ) }}
)

)
SELECT
    asset_uid AS asset_id,
    kac._dlt_id AS asset_load_id,
    TO_TIMESTAMP(
        kac._dlt_load_id :: DOUBLE PRECISION
    ) AT TIME ZONE 'UTC' AS loaded_at,
  kacs._xpath as xpath,
  kacs.name as question_name
FROM
    kobo_asset_content as kac
join
  kobo_asset_content__survey as kacs
on
kacs._dlt_parent_id = kac._dlt_id
