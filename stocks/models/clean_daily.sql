{{ config(
    materialized = 'table',
    partition_by ={ 'field': 'date',
    'data_type': 'date',
    'granularity': 'month' }
) }}

select
    distinct date,
    symbol,
    first_value(close) over (
        partition by date,
        symbol
        order by
            extracted desc
    ) as close,
    first_value(open) over (
        partition by date,
        symbol
        order by
            extracted desc
    ) as open,
    first_value(low) over (
        partition by date,
        symbol
        order by
            extracted desc
    ) as low,
    first_value(high) over (
        partition by date,
        symbol
        order by
            extracted desc
    ) as high,
    first_value(volume) over (
        partition by date,
        symbol
        order by
            extracted desc
    ) as volume
from
    {{ source(
        'stockbq',
        'raw_alpaca'
    ) }}
