with invoices as (
  select 
    invoice,
    customer_id,
    country,
    invoice_date,
    quantity,
    price,
    stock_code
  from {{ ref('stg_raw__invoices') }}
),

generate_key as (
  select
    invoice,
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
    {{ dbt_utils.generate_surrogate_key(['country']) }} as country_key,
    {{ dbt_utils.generate_surrogate_key(['cast(invoice_date as date)']) }} as date_key,
    quantity,
    price,
    stock_code
  from invoices
),

aggregated as (
  select 
    invoice,
    customer_key,
    country_key,
    date_key,
    sum(quantity * price) as total_order_amount,
    sum(quantity) as total_number_of_items,
    count(distinct stock_code) as number_of_unique_products
  from generate_key
  group by invoice, customer_key, country_key, date_key
),

final as (
  select * from aggregated
)

select * from final
