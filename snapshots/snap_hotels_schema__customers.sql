{% snapshot snap_hotels_schema__customers %}

{{
  config(      
    target_schema='snapshots',      
    unique_key='customer_id',      
    strategy='timestamp',      
    updated_at= '_FIVETRAN_SYNCED'  
  )  
}} 

select * from {{ source('hotels_schema', 'customers') }}

{% endsnapshot %}