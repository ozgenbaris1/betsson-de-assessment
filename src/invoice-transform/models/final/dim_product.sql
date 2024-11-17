with invoices as (
  select stock_code, description from {{ ref('stg_raw__invoices') }}
),

final as (
  select
    {{ dbt_utils.generate_surrogate_key(['stock_code', 'description']) }} as product_key,
    stock_code as product_id,
    description
  from invoices
)

select distinct * from final
