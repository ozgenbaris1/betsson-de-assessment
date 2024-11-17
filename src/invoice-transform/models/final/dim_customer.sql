with invoices as (
  select customer_id from {{ ref('stg_raw__invoices') }}
),

final as (
  select
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
    customer_id
  from invoices
)

select distinct * from final
