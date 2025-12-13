# 🧱 LEGO-style Architecture Overview

Think of the project as a LEGO set.
Each block has a precise shape, a clear responsibility, and can only connect in certain directions.

The most important rule when assembling:

Inner blocks do not know about outer blocks.

## The LEGO Blocks (Layers)

### 1. DOMAIN — “Data Shapes & Rules” block

This block defines what the data is and how it is transformed, without caring where it comes from or where it goes.

It must work the same with:
- Web3
- files
- databases
- mocks in unit tests

Contents:
- pure/
  - pure, deterministic functions:
    - block_ranges
    - log_to_row, transaction_to_row, receipt_to_row
    - bytes validation (b20_validate, b32_validate)
  - no I/O, no async, no web3, no pyarrow
- ports/
  - sockets (interfaces / Protocols) describing what the outside world must provide:
    - EvmReader
    - DatasetStore

Rules:
- Domain imports nothing from infrastructure
- Domain can be unit-tested without mocks

### 2. APPLICATION — “Instruction Manual” block

This block describes how the system works step by step.

It does not fetch data itself and does not store data itself.
Instead, it coordinates other blocks.

#### Examples:

- collect_logs
- collect_transactions
- collect_receipts
- shared orchestration logic like flush_buffer

#### Responsibilities:

- decides:
  - which blocks to process
  - batching
  - resume logic
  - ordering
- calls ports, not implementations
- uses pure domain functions

#### Rules:

- knows Domain
- does not know Web3, Parquet, RPC, file system details

### 3. INFRASTRUCTURE — “Real World Connectors” block

These are the blocks that actually touch the outside world.

They plug into domain ports and make them real.

#### Examples:

- adapters/evm
  - Web3EvmReader → plugs into EvmReader
- adapters/storage
  - ParquetDatasetStore → plugs into DatasetStore
- factories
  - evm_reader_factory
  - create_dataset_store
- registry
  - protocol configs
  - ABIs
  - chain-specific metadata
- config
  - env vars
  - paths
  - RPC URLs

#### Rules:
- infrastructure depends on domain
- infrastructure imports third-party libraries

### 4. INTERFACE — “Buttons & Controls” block

This is the block the user actually touches.

It assembles everything and starts the process.

#### Examples:

- CLI
- scheduled jobs
- future API / workers
- collector_tasks.py

#### Responsibilities:

- read config
- load protocol metadata
- create adapters via factories
- call application services

## How the LEGO Blocks Connect
```
[INTERFACE]
   (CLI / tasks / scheduler)
          |
          v
[APPLICATION]
   (collect_logs / collect_transactions / collect_receipts)
          |
          v
[DOMAIN]
   pure logic + ports
          ^
          |
[INFRASTRUCTURE]
   adapters + registry + config

```

Key idea:

- Application depends on Domain
- Infrastructure plugs into Domain
- Interface wires everything together

## Ports as LEGO Sockets

Ports are standard LEGO sockets.

#### Example sockets:

- EvmReader → “I can fetch data from the blockchain”
- DatasetStore → “I can persist datasets”

#### Adapters are LEGO blocks with matching studs:

- Web3EvmReader fits the EvmReader socket
- ParquetDatasetStore fits the DatasetStore socket

Because of this:

- you can swap Web3 → another RPC client
- you can swap Parquet → CSV → SQL
- application logic stays untouched

#### Import Rules (LEGO Compatibility)

Allowed connections:

- interface → infrastructure
- interface → application
- application → domain
- infrastructure → domain

Forbidden connections:

- domain → application / infrastructure / interface
- application → infrastructure (concrete adapters)

If you break these rules, LEGO blocks stop fitting.

#### Why This Works Well

- Easy unit testing (Domain + Application)
- Safe refactoring
- Clear ownership of logic
- No accidental coupling to Web3 / Parquet
- Natural extension to new protocols, chains, or storage backends
