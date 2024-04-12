with
    unique_users as (select * from {{ ref("unique_users") }}),
    unique_anonymous as (select * from {{ ref("unique_anonymous") }}),
    total_page_views as (select * from {{ ref("total_page_views") }}),
    session_counts as (select * from {{ ref("session_counts") }}),
    computed_metrics as (
        select
            (select unique_user_count from unique_users) + (
                select unique_anonymous_count from unique_anonymous
            ) as total_unique_visitors,
            (select page_view_count from total_page_views) as total_page_views,
            (select session_count from session_counts) as total_sessions,
            (select page_view_count from total_page_views) / (
                select total_unique_visitors from computed_metrics_subquery
            ) as average_pageviews_per_visitor,
            (select session_count from session_counts) / (
                select total_unique_visitors from computed_metrics_subquery
            ) as average_sessions_per_visitor
        from
            (
                select
                    (select unique_user_count from unique_users) + (
                        select unique_anonymous_count from unique_anonymous
                    ) as total_unique_visitors
                from dual
            ) as computed_metrics_subquery
    )
select
    total_unique_visitors,
    total_page_views,
    total_sessions,
    average_pageviews_per_visitor,
    average_sessions_per_visitor
from computed_metrics
