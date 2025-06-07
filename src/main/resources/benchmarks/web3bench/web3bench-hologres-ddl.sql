DROP TABLE IF EXISTS token_transfers CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS contracts CASCADE;
DROP TABLE IF EXISTS blocks CASCADE;

CREATE TABLE blocks (
  timestamp bigint,
  number bigint NOT NULL,
  hash varchar(66) PRIMARY KEY,
  parent_hash varchar(66) DEFAULT NULL,
  nonce varchar(42) DEFAULT NULL,
  sha3_uncles varchar(66) DEFAULT NULL,
  transactions_root varchar(66) DEFAULT NULL,
  state_root varchar(66) DEFAULT NULL,
  receipts_root varchar(66) DEFAULT NULL,
  miner varchar(42) DEFAULT NULL,
  difficulty decimal(38,0) DEFAULT NULL,
  total_difficulty decimal(38,0) DEFAULT NULL,
  size bigint DEFAULT NULL,
  extra_data text DEFAULT NULL,
  gas_limit bigint DEFAULT NULL,
  gas_used bigint DEFAULT NULL,
  transaction_count bigint DEFAULT NULL,
  base_fee_per_gas bigint DEFAULT NULL
);

CREATE TABLE transactions (
  hash varchar(66) PRIMARY KEY,
  nonce bigint,
  block_hash varchar(66),
  block_number bigint,
  transaction_index bigint,
  from_address varchar(42),
  to_address varchar(42),
  value decimal(38,0),
  gas bigint,
  gas_price bigint,
  input text,
  receipt_cumulative_gas_used bigint,
  receipt_gas_used bigint,
  receipt_contract_address varchar(42),
  receipt_root varchar(66),
  receipt_status bigint,
  block_timestamp bigint,
  max_fee_per_gas bigint,
  max_priority_fee_per_gas bigint,
  transaction_type bigint
);

CREATE TABLE contracts (
  address varchar(42) PRIMARY KEY,
  bytecode text,
  function_sighashes text,
  is_erc20 boolean,
  is_erc721 boolean,
  block_number bigint
);

CREATE TABLE token_transfers (
  token_address varchar(42),
  from_address varchar(42),
  to_address varchar(42),
  value decimal(38,0),
  transaction_hash varchar(66),
  block_number bigint,
  next_block_number bigint
);

-- Prepare for the workload RangeInsertTempTable
DROP TABLE IF EXISTS temp_table CASCADE;
CREATE TABLE temp_table (
  hash varchar(66) PRIMARY KEY,
  nonce bigint,
  block_hash varchar(66),
  block_number bigint,
  transaction_index bigint,
  from_address varchar(42),
  to_address varchar(42),
  value decimal(38,0),
  gas bigint,
  gas_price bigint,
  input text,
  receipt_cumulative_gas_used bigint,
  receipt_gas_used bigint,
  receipt_contract_address varchar(42),
  receipt_root varchar(66),
  receipt_status bigint,
  block_timestamp bigint,
  max_fee_per_gas bigint,
  max_priority_fee_per_gas bigint,
  transaction_type bigint
);