create table orders (
    order_number BIGINT,
    price        DECIMAL(32,2),
    buyer_id     BIGINT,
    order_time   TIMESTAMP(3)
) with (
  'connector' = 'datagen',
  'rows-per-second' = '1',
  'fields.order_number.kind' = 'sequence',
  'fields.order_number.start' = '1',
  'fields.order_number.end' = '1000000',
  'fields.buyer_id.kind' = 'random',
  'fields.buyer_id.min' = '1',
  'fields.buyer_id.max' = '100',
  'fields.price.kind' = 'random',
  'fields.price.min' = '0.01',
  'fields.price.max' = '10000.0'
)
;

create table expensive_orders (
    order_number                BIGINT,
    price                       DECIMAL(32,2),
    buyer_id                    BIGINT,
    order_time                  TIMESTAMP(3),
    __create_timestamp          TIMESTAMP(3),
    __query_name                STRING,
    __query_description         STRING,
    __query_id                  STRING,
    __query_version             INT,
    __query_create_timestamp    TIMESTAMP(6)
) with (
  'connector' = 'print'
)
;

create table data_stream_example_output (
    id                          INT,
    name                        STRING,
    __create_timestamp          TIMESTAMP(3),
    __query_name                STRING,
    __query_description         STRING,
    __query_id                  STRING,
    __query_version             INT,
    __query_create_timestamp    TIMESTAMP(6)
) with (
  'connector' = 'print'
)
;