SELECT COUNT(DISTINCT user_id) AS unique_user_count
FROM {{ source("snowflake_raw_data", "IKAROS") }}
WHERE user_id IS NOT NULL