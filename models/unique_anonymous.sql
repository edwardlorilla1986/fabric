SELECT COUNT(DISTINCT anonymous_id) AS unique_anonymous_count
FROM {{ source("snowflake_raw_data", "IKAROS") }}
WHERE user_id IS NULL