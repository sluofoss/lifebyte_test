WITH date_series AS (
    SELECT generate_series('2020-06-01'::date, '2020-09-30'::date, '1 day') AS date
)
, report_user_dates as (
    SELECT distinct u.login_hash, u.server_hash, u.currency, d.date as dt_report
    FROM users u
    CROSS JOIN date_series d
    WHERE u.enable = 1
)
, enabled_trade_daily_agg as (
    select 
        t.close_time::date as dt_report
        , t.login_hash
        , t.server_hash
        , case when t.symbol = 'USD,CHF' then 'USDCHF' else t.symbol end as symbol 
        , u.currency
        , sum(volume) as volume 
        --daily
        , count(distinct t.ticket_hash) as trade_count 
        -- is distinct necessary here?
    from trades t 
    inner join users u on 
        u.login_hash = t.login_hash
        and u.server_hash = t.server_hash
    where 
        u.enable = 1  
        and t.close_time >= '2020-06-01'
        and t.close_time  < '2020-10-01'
        -- this is questionable if the data goes back further (sum all req)
        -- need to filter out epoch time as well as those trade never settled
        -- also need to put additional filter condition here if any quality issue exist
    group by 
        dt_report
        , t.login_hash
        , t.server_hash
        , t.symbol
        , u.currency
)
, stg as ( 
    select 
        d.dt_report
        , d.login_hash
        , d.server_hash
        , t.symbol
        , d.currency
        , sum(coalesce(t.volume,0)) over (
            partition by 
            d.login_hash
            , d.server_hash
            , t.symbol
            --, t.currency
            order by d.dt_report asc
            rows between 6 PRECEDING and current row
        ) as sum_volume_prev_7d
        , sum(coalesce(t.volume,0)) over (
            partition by 
            d.login_hash
            , d.server_hash
            , t.symbol
            --, t.currency
            rows between unbounded PRECEDING and current row
        ) as sum_volume_prev_all
        ---------------------------------
        -- dependencies of final stage
        ---------------------------------
        , sum(coalesce(t.volume,0)) over (
            partition by 
            d.login_hash 
            , t.symbol
            rows between 6 preceding and current row
        ) as __login_symbol_volume_7d
        , sum(coalesce(t.trade_count,0)) over (
            partition by 
            d.login_hash
            rows between 6 preceding and current row
        ) as __login_count_7d
        -----------------------
        -- dependency end
        -----------------------
        , sum(
            case when 
                d.dt_report >= '2020-08-01' 
                and d.dt_report < '2020-09-01' 
            then coalesce(t.volume,0) else 0 end
        ) over (
            partition by
            d.login_hash
            , d.server_hash
            , t.symbol
            rows between unbounded preceding and current row
        ) as sum_volume_2020_08 
        , min(t.dt_report) over (
            partition by 
            d.login_hash 
            , d.server_hash
            , t.symbol
            --order by t.dt_report asc
        ) as date_first_trade
        , row_number() over (
            order by
            d.dt_report
            , d.login_hash
            , d.server_hash
            , t.symbol
        ) as "row_number"
    from enabled_trade_daily_agg t
    right join report_user_dates d on
        d.login_hash = t.login_hash
        and d.server_hash = t.server_hash
        and d.dt_report = t.dt_report
)
--, res as ( 
select 
    s.dt_report::date
    , s.login_hash::text
    , s.server_hash::text
    , s.symbol::text
    , s.currency::text
    , s.sum_volume_prev_7d::double precision
    , s.sum_volume_prev_all::double precision
    , dense_rank() over (
        partition by s.dt_report
        order by s.__login_symbol_volume_7d desc
    ) as rank_volume_symbol_prev_7d
    , rank() over (
        partition by s.dt_report
        order by s.__login_count_7d desc
    ) as rank_count_prev_7d
    , s.sum_volume_2020_08::double precision
    , s.date_first_trade::timestamp
    , s.row_number
from stg s
order by s.row_number
--)
/*
select * 
from users
where login_hash = '18D4C2E739573770F9DF198F0E51C1B9'
--order by dt_report
*/
/*
select 
    s.dt_report
    , s.login_hash
    , s.server_hash
    , s.symbol
    , s.currency
    , count(*) as cnt
from res s
group by
    s.dt_report
    , s.login_hash
    , s.server_hash
    , s.symbol
    , s.currency
order by cnt desc
*/

/*
select count(distinct dt_report)
from res
union all
select count(distinct date)
from date_series
*/

/*
select count(*), 'dates'
from date_series
union
select count(*), 'current res'
from res
union 
select count(*), 'distincts'
from (
    select distinct
    s.dt_report
    , s.login_hash
    , s.server_hash
    , s.symbol
    , s.currency
    from res s
) r
union
select count(*), 'uuid actual'
from (
    select 
    s.dt_report
    , s.login_hash
    , s.server_hash
    , s.symbol
    , s.currency
    from res s
) r
union
select count(*), 'enabled_trade_daily_agg'
from enabled_trade_daily_agg
union 
select count(*), 'report user dates'
from report_user_dates r
*/