{% snapshot snap_hotels_schema__rooms %}

{{
    config(
      target_schema='snapshots',
      unique_key='room_id',
      strategy='check',
      check_cols=['price'],
        )
}}

select * from {{ source('hotels_schema', 'rooms') }}

{% endsnapshot %}