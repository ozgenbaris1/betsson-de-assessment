with invoices as (
  select 
    invoice,
    stock_code,
    description,
    customer_id,
    country,
    invoice_date,
    quantity,
    price
  from {{ ref('stg_raw__invoices') }}
),

final as (
  select 
    invoice,
    {{ dbt_utils.generate_surrogate_key(['stock_code', 'description']) }} as product_key,
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
    {{ dbt_utils.generate_surrogate_key(['country']) }} as country_key,
    {{ dbt_utils.generate_surrogate_key(['cast(invoice_date as date)']) }} as date_key,
    quantity,
    price
  from invoices
)

select * from final
