# Isolated 15-Minute Demo Branch

This branch runs beside the production 5-minute strategy.

## Timeframes

- Pair state: 500 completed Bybit 1-hour candles.
- Cointegration regime: `ADF < -2.5`, `p_value < 0.10`, `beta > 0.10`.
- Signal: completed Bybit 15-minute candles.
- Signal rolling window: configured per pair as 100, 200, or 300 bars.
- Signal spread: `log(asset_1) - capped_beta * log(asset_2)`.
- Signal beta band: 0.7 to 1.3.
- Half-life unit: one hour. An HL value of 20 creates a 20-hour timeout.

At hourly boundaries, the state worker starts first and the signal worker uses
a longer delay so the latest hourly regime is available before signal update.

## Isolated Tables

- `signal_table_15min`
- `pair_state_15min`
- `trade_res_15min`
- `asset_locks_15min`

The scheduler, heartbeat, daily snapshot, and position-value tables are shared
and isolated by worker ID or bot ID.

## Installation

1. Apply `database/migrations/001_create_15min_branch.sql`.
2. Fill a pair CSV using
   `Working_layer/Signal_worker_15min/input_data/pairs_15min_template.csv`.
3. Validate the CSV:

   ```powershell
   python Working_layer\Signal_worker_15min\import_pairs_15min.py PAIRS.csv
   ```

4. Import after validation:

   ```powershell
   python Working_layer\Signal_worker_15min\import_pairs_15min.py PAIRS.csv --apply
   ```

5. Start the hourly state worker and allow one successful cycle.
6. Start the 15-minute signal worker and allow one successful cycle.
7. Confirm fresh rows and valid regime values in both new tables.
8. The two data workers are configured `RUNNING`. Keep `bot_15min_L1`
   on `SLEEP` until both tables contain fresh calculated values.
9. Change only `bot_15min_L1` defaults to `RUNNING`, then start
   `Execution_layer/Executors/bot_15min_L1/bot_15min_L1.py`.

## Market Data Storage

- `data/parquet_db/bybit_linear_15m`
- `data/parquet_db/bybit_linear_1h_15min_test`

These paths are separate from the production 5-minute cache.

## Safety

- Use `api_credentials_demo_15min.txt` for a separate Bybit demo account.
- Both market-data workers also use that credential profile, avoiding shared
  API rate-limit pressure with production.
- Keep all three scheduler entries on `SLEEP` during setup.
- The fixed-pair importer does not use or modify production selection tables.
- Trade-history levels and quarantine use only `trade_res_15min`.
