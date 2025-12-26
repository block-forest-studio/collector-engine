CREATE SCHEMA IF NOT EXISTS raw;

-- LOGS
CREATE TABLE IF NOT EXISTS raw.logs (
  chain_id          integer      NOT NULL,
  block_number      bigint       NOT NULL,
  block_hash        bytea        NOT NULL,
  transaction_hash  bytea        NOT NULL,
  log_index         integer      NOT NULL,
  address           bytea        NOT NULL,
  topic0            bytea        NULL,
  topic1            bytea        NULL,
  topic2            bytea        NULL,
  topic3            bytea        NULL,
  data              bytea        NOT NULL,
  removed           boolean      NOT NULL,

  CONSTRAINT logs_pk PRIMARY KEY (chain_id, transaction_hash, log_index)
);

CREATE INDEX IF NOT EXISTS logs_block_idx ON raw.logs (chain_id, block_number);

-- TRANSACTIONS
CREATE TABLE IF NOT EXISTS raw.transactions (
  chain_id                  integer     NOT NULL,
  hash                      bytea       NOT NULL,
  block_number              bigint      NULL,
  block_hash                bytea       NULL,
  transaction_index         integer     NULL,

  "from"                    bytea       NOT NULL,
  "to"                      bytea       NULL,

  gas                       bigint      NOT NULL,
  gas_price                 numeric     NULL,
  max_fee_per_gas           numeric     NULL,
  max_priority_fee_per_gas  numeric     NULL,

  input                     bytea       NOT NULL,
  nonce                     bigint      NOT NULL,
  value                     numeric     NOT NULL,
  type                      integer     NULL,

  v                         bigint      NOT NULL,
  r                         text        NOT NULL,
  s                         text        NOT NULL,
  y_parity                  integer     NULL,

  access_list               jsonb       NOT NULL DEFAULT '[]'::jsonb,

  CONSTRAINT txs_pk PRIMARY KEY (chain_id, hash)
);

CREATE INDEX IF NOT EXISTS txs_block_idx ON raw.transactions (chain_id, block_number);

-- RECEIPTS
CREATE TABLE IF NOT EXISTS raw.receipts (
  chain_id             integer   NOT NULL,
  transaction_hash     bytea      NOT NULL,

  block_number         bigint     NOT NULL,
  block_hash           bytea      NOT NULL,
  transaction_index    integer    NOT NULL,

  "from"               bytea      NOT NULL,
  "to"                 bytea      NULL,
  contract_address     bytea      NULL,

  status               integer    NULL,
  type                 integer    NULL,

  gas_used             bigint     NOT NULL,
  cumulative_gas_used  bigint     NOT NULL,
  effective_gas_price  numeric    NULL,

  logs_bloom           bytea      NULL,
  logs                 jsonb      NOT NULL DEFAULT '[]'::jsonb,

  CONSTRAINT receipts_pk PRIMARY KEY (chain_id, transaction_hash)
);

CREATE INDEX IF NOT EXISTS receipts_block_idx ON raw.receipts (chain_id, block_number);
