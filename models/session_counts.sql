SELECT COUNT(DISTINCT anonymous_id) AS session_count
FROM {{ source("snowflake_raw_data", "IKAROS") }}