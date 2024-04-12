with
    computed_metrics as (
        select
            (uu.unique_user_count + ua.unique_anonymous_count) as total_unique_visitors,
            tpv.page_view_count as total_page_views,
            sc.session_count as total_sessions,
            tpv.page_view_count / nullif(
                (uu.unique_user_count + ua.unique_anonymous_count), 0
            ) as average_pageviews_per_visitor,
            sc.session_count / nullif(
                (uu.unique_user_count + ua.unique_anonymous_count), 0
            ) as average_sessions_per_visitor
        from {{ ref("unique_users") }} as uu
        join {{ ref("unique_anonymous") }} as ua on 1 = 1
        join {{ ref("total_page_views") }} as tpv on 1 = 1
        join {{ ref("session_counts") }} as sc on 1 = 1
    )
select
    total_unique_visitors,
    total_page_views,
    total_sessions,
    average_pageviews_per_visitor,
    average_sessions_per_visitor
from computed_metrics
