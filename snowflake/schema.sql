create warehouse if not exists STREAMING_WH
    warehouse_size = 'XSMALL'
    auto_suspend = 60
    auto_resume = true;

create database if not exists STREAMING_ANALYTICS;
create schema if not exists STREAMING_ANALYTICS.RAW;
create schema if not exists STREAMING_ANALYTICS.CURATED;

create table if not exists STREAMING_ANALYTICS.RAW.TRANSACTIONS_JSON (
    raw_payload variant,
    kafka_topic string,
    kafka_partition number,
    kafka_offset number,
    loaded_at timestamp_ntz default current_timestamp()
);

create table if not exists STREAMING_ANALYTICS.CURATED.PROCESSED_TRANSACTIONS (
    event_id string,
    event_type string,
    event_time timestamp_ntz,
    account_id string,
    merchant_id string,
    amount number(18, 2),
    currency string,
    country string,
    channel string,
    status string,
    processed_at timestamp_ntz,
    primary key (event_id)
);

create table if not exists STREAMING_ANALYTICS.CURATED.TRANSACTION_WINDOWS (
    window_start timestamp_ntz,
    window_end timestamp_ntz,
    country string,
    status string,
    transaction_count number,
    total_amount number(18, 2),
    average_amount number(18, 2),
    max_event_time timestamp_ntz,
    processed_at timestamp_ntz
)
cluster by (window_start, country, status);

create stream if not exists STREAMING_ANALYTICS.RAW.TRANSACTIONS_STREAM
    on table STREAMING_ANALYTICS.RAW.TRANSACTIONS_JSON;

create task if not exists STREAMING_ANALYTICS.CURATED.LOAD_PROCESSED_TRANSACTIONS
    warehouse = STREAMING_WH
    schedule = '1 minute'
as
merge into STREAMING_ANALYTICS.CURATED.PROCESSED_TRANSACTIONS target
using (
    select
        raw_payload:event_id::string as event_id,
        raw_payload:event_type::string as event_type,
        raw_payload:event_time::timestamp_ntz as event_time,
        raw_payload:account_id::string as account_id,
        raw_payload:merchant_id::string as merchant_id,
        raw_payload:amount::number(18, 2) as amount,
        raw_payload:currency::string as currency,
        raw_payload:country::string as country,
        raw_payload:channel::string as channel,
        raw_payload:status::string as status,
        current_timestamp() as processed_at
    from STREAMING_ANALYTICS.RAW.TRANSACTIONS_STREAM
    where raw_payload:event_id is not null
) source
on target.event_id = source.event_id
when not matched then insert (
    event_id, event_type, event_time, account_id, merchant_id, amount,
    currency, country, channel, status, processed_at
) values (
    source.event_id, source.event_type, source.event_time, source.account_id,
    source.merchant_id, source.amount, source.currency, source.country,
    source.channel, source.status, source.processed_at
);
