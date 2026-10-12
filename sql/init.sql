create table if not exists processed_transactions (
    event_id text primary key,
    event_type text not null,
    event_time timestamptz not null,
    ingested_at timestamptz,
    account_id text not null,
    merchant_id text not null,
    amount numeric(18, 2) not null,
    currency text not null,
    country text not null,
    channel text not null,
    status text not null,
    kafka_topic text,
    kafka_partition integer,
    kafka_offset bigint,
    processed_at timestamptz not null default now()
);

create table if not exists transaction_windows (
    window_start timestamptz not null,
    window_end timestamptz not null,
    country text not null,
    status text not null,
    transaction_count bigint not null,
    total_amount numeric(18, 2) not null,
    average_amount numeric(18, 2) not null,
    max_event_time timestamptz not null,
    processed_at timestamptz not null default now(),
    primary key (window_start, window_end, country, status)
);

create table if not exists processing_errors (
    error_id bigserial primary key,
    raw_payload text,
    error_reason text not null,
    kafka_topic text,
    kafka_partition integer,
    kafka_offset bigint,
    created_at timestamptz not null default now()
);

create index if not exists idx_processed_transactions_event_time
    on processed_transactions (event_time desc);

create index if not exists idx_transaction_windows_freshness
    on transaction_windows (processed_at desc);
