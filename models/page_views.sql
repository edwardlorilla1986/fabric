SELECT
    timestamp::TIMESTAMP AS event_timestamp,
    CASE
        WHEN user_id = '' OR user_id IS NULL THEN NULL
        ELSE CAST(user_id AS VARCHAR)
    END AS user_id,
    CAST(anonymous_id AS VARCHAR) AS anonymous_id
FROM {{ source("snowflake_raw_data", "IKAROS") }}