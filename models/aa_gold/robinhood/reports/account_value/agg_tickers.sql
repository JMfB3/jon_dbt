with 
    latest_prices as (select ticker, coalesce(latest_price, 0) as latest_price from {{ ref('latest_prices') }}),
    dividends as (select * from {{ ref('dividends') }}),
    stock_orders as (select * from {{ ref('stock_orders') }}),
    
    orders as (
        select 
            robinhood_stock_order_id,
            ticker,
            average_price,
            status,
            side,
            case 
                when side = 'buy' then quantity 
                when side = 'sell' then (quantity * -1)
                else -999999999 end as quantity,
            created_at,
        from stock_orders
    ),
    
    base as (
        select 
            orders.*,
            iff(dividends.robinhood_dividend_id is null, (orders.average_price * orders.quantity), 0) as invested,
            iff(dividends.robinhood_dividend_id is not null, (orders.average_price * orders.quantity), 0) as dripped,
            (orders.quantity * latest_prices.latest_price) as current_value,
            dividends.robinhood_dividend_id,
            latest_prices.latest_price,
        from orders 
        left join dividends on orders.robinhood_stock_order_id = dividends.robinhood_drip_order_id
        left join latest_prices on orders.ticker = latest_prices.ticker 
        where 
            orders.ticker not in (
                'NVDA', -- stock split 
                'AMC', -- stock split
                'META', -- data before 2023 
                'IRBT', -- data before 2023?
                'GOOG', -- data before 2023
                'TPL', -- data before 2023
                'ROKU' -- data before 2023
            )
            -- orders.ticker = 'META'
            and 
            orders.created_at >= '2023-01-01'
    ), 

    aggs as (
        select 
            ticker,
            sum(invested) as total_invested,
            sum(dripped) as total_dripped,
            sum(iff(robinhood_dividend_id is null, current_value, 0)) as total_current_value_invest,
            sum(iff(robinhood_dividend_id is not null, current_value, 0)) as total_current_value_drip,
        from base 
        group by 1
    ),

    final as (
        select *, 
            ((total_current_value_invest + total_current_value_drip) - total_invested) as actual_net_gain,
        from aggs  
    )

select 
    final.ticker,
    to_char(final.actual_net_gain, '$999,999,999,990.00') as actual_net_gain,
    to_char(final.total_invested, '$999,999,999,990.00') as total_invested,
    to_char(final.total_dripped, '$999,999,999,990.00') as total_dripped,
    to_char(final.total_current_value_invest, '$999,999,999,990.00') as total_current_value_of_invested,
    to_char(final.total_current_value_drip, '$999,999,999,990.00') as total_current_value_of_dripped, 
    round((((total_invested + actual_net_gain) - total_invested) / total_invested), 2) * 100 as pct_gain, 

    final.actual_net_gain as actual_net_gain_raw ,
    final.total_invested as total_invested_raw ,
    final.total_dripped as total_dripped_raw ,
    final.total_current_value_invest as total_current_value_of_invested_raw ,
    final.total_current_value_drip as total_current_value_of_dripped_raw , 
from final 
order by pct_gain desc 
