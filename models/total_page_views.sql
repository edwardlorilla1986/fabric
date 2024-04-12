SELECT COUNT(*) AS page_view_count
FROM {{ source("snowflake_raw_data", "IKAROS") }}