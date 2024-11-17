with invoices as (
  select country from {{ ref('stg_raw__invoices') }}
),

final as (
  select
    {{ dbt_utils.generate_surrogate_key(['country']) }} as country_key,
    country
  from invoices
)

select distinct * from final
