version: 2

sources:
  - name: s3_external_raw
    description: databricks data lakehouse s3_external_raw schema.
    catalog: "{{ var('source_catalog') }}"
    tables:
      - name: branch_reports
        freshness:
          error_after:
            count: 3
            period: day
          filter: date_dim_key >= date_format(date_add(current_date(), -2), 'yyyyMMdd')
        loaded_at_field: to_timestamp(to_date(date_dim_key, 'yyyyMMdd'))
        description: This table uses date_dim_key as the partition key.
      - name: daily_credit_utilisation__snapshots
        freshness:
          error_after:
            count: 2
            period: day
          filter: snapshot_date_dim_key >= date_format(date_add(current_date(), -2), 'yyyyMMdd')
        loaded_at_field: to_timestamp(to_date(snapshot_date_dim_key, 'yyyyMMdd'))
        description: This table uses snapshot_date_dim_key as the partition key.
      - name: long_lake__dim_date
        description: dim_date table in hive_metastore.long_lake, it's static.
      - name: long_lake__dim_time
        description: dim_time table in hive_metastore.long_lake, it's static.
      - name: payment_payment_status_change_logs
      - name: payment_payments
      - name: payment_user_status_change_logs
      - name: payment_users
      - name: payment_webhook_events
      - name: raw_channel_notification_status_updates_event
        freshness:
          error_after:
            count: 2
            period: day
          filter: eventdate >= date_format(date_add(current_date(), -2), 'yyyy-MM-dd')
        loaded_at_field: to_timestamp(to_date(eventdate, 'yyyy-MM-dd'))
        description: This table uses eventdate as the partition key.
      - name: segment_logs_dev
      - name: segment_logs_prod
      - name: shredded_events
        freshness:
          error_after:
            count: 12
            period: hour
          filter: consolidated_date_dim_key >= date_format(date_add(current_date(), -2), 'yyyyMMdd')
        loaded_at_field: to_timestamp(collector_tstamp)
        description: This table uses consolidated_date_dim_key as the partition key.
      - name: voucher__campaign_created
      - name: voucher__voucher_code_blacklist
      - name: voucher__voucher_created
      - name: voucher__voucher_redeemed
      - name: voucher__voucher_refunded
      - name: braze_tradiecore_events_messages_contentcard_click
      - name: braze_tradiecore_events_messages_contentcard_impression
