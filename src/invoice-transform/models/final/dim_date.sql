{% set start_date = '2008-01-01' %}
{% set end_date = '2011-12-31' %}

with numbers as (
  select row_number() over (order by (select null)) - 1 as n
  from sys.all_objects
),

date_spine as (
  select
    dateadd(day, n, cast('{{ start_date }}' as date)) as date_day
  from numbers
  where dateadd(day, n, cast('{{ start_date }}' as date)) <= cast('{{ end_date }}' as date)
),

final as (
  select
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_key,
    date_day,
    year(date_day) as year,
    month(date_day) as month,
    day(date_day) as day,
    datepart(weekday, date_day) as day_of_week,
    datename(weekday, date_day) as day_name,
    left(datename(weekday, date_day), 3) as day_name_short,
    datename(month, date_day) as month_name,
    left(datename(month, date_day), 3) as month_name_short,
    iif(datepart(weekday, date_day) in (1, 7), 1, 0) as is_weekend,
    datepart(quarter, date_day) as quarter
  from date_spine
)

select * from final
