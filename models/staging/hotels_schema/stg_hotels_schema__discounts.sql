with 

source as (

    select * from {{ source('hotels_schema', 'discounts') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['discount_name']) }} as discount_id,
	    discount_name::VARCHAR(256)                               as discount_name,
	    discount_rate::DECIMAL(10,2)                              as discount_percentage,
	    discount_desc::VARCHAR(256)                               as discount_desc,
	    discount_status::VARCHAR(256)                             as discount_status,
        CASE 
            WHEN lower(discount_status) = 'active' THEN TRUE
            ELSE FALSE
        END                                                       as DISCOUNT_STATUS_FLAG,
        _fivestran_synced::TIMESTAMP_TZ                           as datetimeload_utc
    from source
    
)

select * 
from renamed

UNION ALL --Union de un nuevo registro para poder enlazar los registros vacios en bookings/payments

{{ dbt_utils.generate_surrogate_key(['no_discount']) }}   as discount_id,
'no_discount'                                             as discount_name,
0                                                         as discount_percentage,
'No discount'                                             as discount_desc,
'Active'                                                  as discount_status,
FALSE                                                     as DISCOUNT_STATUS_FLAG,
'2024-11-28T08:58:17.303Z'                                as datetimeload_utc