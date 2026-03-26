{% snapshot snap_interviews %}

{{
    config(
      target_schema='snapshots',
      unique_key='_OFFSET',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True,
    )
}}

  SELECT
    *,
    to_timestamp(_updated_micros::bigint / 1000000) AS updated_at
  FROM {{ source('raw', 'interviews') }}

{% endsnapshot %}
