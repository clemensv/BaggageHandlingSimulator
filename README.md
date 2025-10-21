# Baggage Handling Simulator

A Python CLI that simulates airport baggage handling and publishes baggage and
passenger state changes as CloudEvents to an Azure Event Hubs or Microsoft
Fabric Eventstream endpoint simulating an airport baggage handling systems,
while persisting flight schedules to SQL Server, simulating an airline
operations system.

The purpose of the simulator is to provide a realistic event stream and
backing store for testing and demonstrating event-driven architectures,
serverless functions, stream processing, and analytics in Microsoft Fabric
and Microsoft Azure.

## Install

```pwsh
# from repo root
pip install -e .
```

## Configure

Set the following environment variables or pass flags:

- EVENTHUB_CONNECTION_STRING
- EVENTHUB_NAME
- SQLSERVER_CONNECTION_STRING
- SQLSERVER_FLIGHTS_TABLE (optional, default dbo.Flights)

Create the SQL table:

```sql
:r .\sql\create_flights.sql
```


## Run

```pwsh
# Dry-run: no Azure/SQL required. Per-event output only with --verbose.
bhsim --dry-run --clock-speed 120 --flight-interval-minutes 5 --duration-minutes 2

# Optional: show per-event/batch summaries in dry-run
bhsim --dry-run --verbose --clock-speed 120 --flight-interval-minutes 5 --duration-minutes 2

# Live: publish to Event Hubs and insert into SQL Server
bhsim --clock-speed 120 --flight-interval-minutes 5 --duration-minutes 2 \
  --eventhub-conn $env:EVENTHUB_CONNECTION_STRING --eventhub-name $env:EVENTHUB_NAME \
  --sql-conn $env:SQLSERVER_CONNECTION_STRING
```

## Notes

- Events use CloudEvents 1.0 JSON. Partition key is flightId for ordering per-flight.
- CloudEvents encoding can be set via `--ce-mode structured|binary` (default `structured`).
- Actual flight times: when a flight departs/arrives, the simulator updates `dbo.Flights` with `ActualDepartureUtc` and `ActualArrivalUtc`.
- Additional markers: when check-in closes the simulator updates `CheckinClosedUtc`; when all bags have been unloaded after arrival it updates `CompletedUtc`.
- Flight lifecycle events are emitted: Airport.Flight.Closed (end of check-in), Airport.Flight.Departed (at departure), Airport.Flight.Arrived (at arrival).
- Flight load is up to 95% of aircraft capacity. Bags tracked end-to-end.
- Loss, inspection, rejection, and not-collected rates are configurable.
- Use `--dry-run` to simulate without Azure/SQL dependencies (useful for demos and CI). Add `--verbose` to see per-event and per-batch summaries.
- All console output lines are prefixed with the current simulated timestamp for easier tracing.

## Command-line options

Arguments are also grouped in the CLI help (`bhsim -h`). Environment variables shown are optional and used as defaults.

- Output
  - `--dry-run`: Do not write to SQL or Event Hubs; print events to console only.
  - `--quiet`: Suppress rich console status output.
  - `--verbose`: Print per-event and per-batch send summaries (default: off).
  - `--ce-mode`: CloudEvents encoding mode `structured` (JSON envelope) or `binary` (data-only with CE attributes as properties). Default: `structured` (or `$env:SIM_CE_MODE`).

- Event Hubs
  - `--eventhub-conn`: Azure Event Hubs connection string. Default: `$env:EVENTHUB_CONNECTION_STRING`.
  - `--eventhub-name`: Event Hub name. Default: `$env:EVENTHUB_NAME`. If omitted, the simulator will try to extract it from `EntityPath=` in the connection string.

- SQL Server
  - `--sql-conn`: SQL Server ODBC connection string. Default: `$env:SQLSERVER_CONNECTION_STRING`.
  - `--sql-table`: SQL table for flights. Default: `dbo.Flights` (or `$env:SQLSERVER_FLIGHTS_TABLE`).

- Clock & cadence
  - `--seed`: Random seed for reproducibility. Default: none.
  - `--clock-offset-minutes`: Offset added to simulated now(). Default: 0 (or `$env:SIM_CLOCK_OFFSET_MINUTES`).
  - `--clock-speed`: Acceleration factor (e.g., 60.0 = 1 sim hour per real minute). Default: 60.0 (or `$env:SIM_CLOCK_SPEED`).
  - `--flight-interval-minutes`: Minutes between scheduled flights (sim time). Default: 5 (or `$env:SIM_FLIGHT_INTERVAL_MINUTES`).
  - `--max-active-flights`: Upper bound on concurrently active flights; `0` = unlimited. Default: 0 (or `$env:SIM_MAX_ACTIVE_FLIGHTS`).

- Behavior
  - `--loss-rate`: Fraction of checked bags lost (0.0–1.0). Default: 0.002 (or `$env:SIM_LOSS_RATE`).
  - `--inspect-rate`: Fraction of screened bags inspected. Default: 0.01 (or `$env:SIM_INSPECT_RATE`).
  - `--reject-rate`: Fraction of screened bags rejected. Default: 0.003 (or `$env:SIM_REJECT_RATE`).
  - `--not-collected-rate`: Fraction of delivered bags not collected. Default: 0.005 (or `$env:SIM_NOT_COLLECTED_RATE`).

- Airports & airlines
  - `--airports-file`: CSV of airport pairs (`IATA1,IATA2`) to override defaults.
  - `--airlines`: Comma-separated airline codes to override defaults (e.g., `FJ,AZ,SK`).

- Duration
  - `--duration-minutes`: Real minutes to run; `0` runs until Ctrl+C. Default: 0.

Run-to-completion mode:

- When `--duration-minutes` is not set (or `0`) and `--max-active-flights` is greater than `0`, the simulator will schedule up to the configured number of flights and then exit automatically once all flights have finished unloading and all events have been emitted.

Notes:

- In `--dry-run` mode, Event Hubs and SQL options are not required and are ignored.
