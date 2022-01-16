with _latest_date as (
  select
    max(date) as date
  from
    {{ ref('clean_daily_year') }}
),
_data as (
  select
    date,
    close,
    symbol,
    avg(close) over (
      partition by symbol
      order by
        date asc rows 200 preceding
    ) as dma200,
    avg(close) over (
      partition by symbol
      order by
        date asc rows 50 preceding
    ) as dma50,
    avg(close) over (
      partition by symbol
      order by
        date asc rows 30 preceding
    ) as dma30
  from
    {{ ref('clean_daily_year') }}
)
select
  _data.date,
  symbol,
  close / dma30 as dma30_ratio,
  close / dma50 as dma50_ratio,
  close / dma200 as dma200_ratio,
  close,
  dma30,
  dma50,
  dma200
from
  _data
  inner join _latest_date
  on _latest_date.date = _data.date
