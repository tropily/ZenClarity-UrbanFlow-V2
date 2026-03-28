{% snapshot snap_vendor %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='vendor_id',
        strategy='check',
        check_cols=['vendor_name', 'status']
    )
}}

select
    vendor_id,
    vendor_name,
    status
from {{ ref('stg_vendor') }}

{% endsnapshot %}
