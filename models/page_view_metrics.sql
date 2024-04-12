with
    page_views as (
        select
            timestamp::timestamp as event_timestamp, 
            case
                when user_id = '' or user_id is null
                then null
                else cast(user_id as varchar)
            end as user_id,
            cast(anonymous_id as varchar) as anonymous_id
        from {{ source("snowflake_raw_data", "IKAROS") }}
    ),
    unique_users as (
        select count(distinct user_id) as unique_user_count
        from {{ source("snowflake_raw_data", "IKAROS") }}
        where user_id is not null
    ),
    unique_anonymous as (
        select count(distinct anonymous_id) as unique_anonymous_count
        from {{ source("snowflake_raw_data", "IKAROS") }}
        where user_id is null
    ),
    total_page_views as (
        select count(*) as page_view_count
        from {{ source("snowflake_raw_data", "IKAROS") }}
    ),
    session_counts as (
        select count(distinct anonymous_id) as session_count
        from {{ source("snowflake_raw_data", "IKAROS") }}
    ),
    averages as (
        select
            (select unique_user_count from unique_users) + (
                select unique_anonymous_count from unique_anonymous
            ) as total_unique_visitors,
            (select page_view_count from total_page_views) as total_page_views,
            (select session_count from session_counts) as total_sessions,
            (select page_view_count from total_page_views) / (
                select total_unique_visitors
                from
                    (
                        select
                            (select unique_user_count from unique_users) + (
                                select unique_anonymous_count from unique_anonymous
                            ) as total_unique_visitors
                    ) as subquery
            ) as average_pageviews_per_visitor,
            (select session_count from session_counts) / (
                select total_unique_visitors
                from
                    (
                        select
                            (select unique_user_count from unique_users) + (
                                select unique_anonymous_count from unique_anonymous
                            ) as total_unique_visitors
                    ) as subquery
            ) as average_sessions_per_visitor
    )

-- Final selection
select
    total_unique_visitors,
    total_page_views,
    total_sessions,
    average_pageviews_per_visitor,
    average_sessions_per_visitor
from averages
