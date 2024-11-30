
with 

discounts as (

    select * from {{ref('stg_hotels_schema__discounts') }}

),

renamed as (

    select
        discount_id,
	    discount_name,
	    discount_percentage,
	    discount_desc,
	    discount_status,
        discount_active_flag
    from discounts
    
)

select * 
from renamed

