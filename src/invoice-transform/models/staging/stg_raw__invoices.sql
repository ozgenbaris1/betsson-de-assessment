with src as (
  select * from {{ source('raw', 'invoices') }}
),

cleansed as (
  select 
    invoice as invoice,
    stockcode as stock_code,
    isnull(trim(description), '#N/A') as description,
    quantity as quantity,
    invoicedate as invoice_date,
    isnull(price, 0) as price, -- NULL customer_ids will be filled with -1
    isnull(customerid, -1) as customer_id,
    /*
      if country = 'U.K.' then convert it to United Kingdom for consistency
      if country = 'Unspecified' convert it to NULL
    */
    nullif(iif(country = 'U.K.', 'United Kingdom', country), 'Unspecified') as country
  from src
),

final as (
  select 
    invoice,
    stock_code,
    description,
    quantity,
    invoice_date,
    price,
    customer_id,
    /*
      if country is NULL, replace it with the max country value for the same invoice. 
      This is because for some invoice values, there are some NULL and some valid countries.
      This way we can fill NULLs with an appropriate, non-NULL country. 
      
      If the country is still NULL (meaning the invoice has no valid country), replace it with '#N/A'.
    */
    isnull(isnull(country, max(country) over (partition by invoice)), '#N/A') as country
  from cleansed
)

select * from final
