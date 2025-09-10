# Airport Baggage Handling Simulator Container

This container image runs the Airport Baggage Handling Simulator ("bhsim"). The simulator generates realistic flight, passenger, and baggage lifecycle events as [CloudEvents](https://cloudevents.io/) and publishes them to Azure Event Hubs. Optionally it records flight lifecycle markers in a SQL Server table for downstream analytics.

## Key Features

* Accelerated or real-time clock (configurable via `--clock-speed`)
* Flight scheduling with route-based block time estimation
* Passenger creation, check-in, baggage creation/screening/loading, flight departure/arrival, baggage delivery & exception events
* CloudEvents output (structured JSON or binary mode)
* Event Hubs batching window for efficient throughput
* Optional SQL Server persistence for flight state transitions

## Obtaining the Image

```shell
docker pull ghcr.io/clemensv/baggagehandlingsimulator:latest
```

## Running (Dry-Run: console only)

```shell
docker run --rm ghcr.io/clemensv/baggagehandlingsimulator:latest \
    --dry-run --duration-minutes 2 --clock-speed 120
```

## Running with Azure Event Hubs

```shell
docker run --rm \
    -e EVENTHUB_CONNECTION_STRING="Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...;EntityPath=hub" \
    -e EVENTHUB_NAME="hub" \
    ghcr.io/clemensv/baggagehandlingsimulator:latest \
    --duration-minutes 5 --clock-speed 120
```

If the Event Hub name (EntityPath) is embedded in the connection string, you can omit `-e EVENTHUB_NAME` and the simulator will attempt to extract it.

## Adding SQL Persistence

```shell
docker run --rm \
    -e EVENTHUB_CONNECTION_STRING="..." -e EVENTHUB_NAME="hub" \
    -e SQLSERVER_CONNECTION_STRING='Driver={ODBC Driver 18 for SQL Server};Server=tcp:<server>.database.windows.net,1433;Database=<db>;Encrypt=yes;TrustServerCertificate=no;Authentication=ActiveDirectoryInteractive' \
    ghcr.io/clemensv/baggagehandlingsimulator:latest \
    --duration-minutes 10 --clock-speed 60
```

When using Azure AD Interactive authentication inside a non-interactive container, prefer providing a SQL connection string using a service principal or managed identity (future enhancement) instead of `ActiveDirectoryInteractive`.

## Environment Variables (defaults shown)

| Variable | Purpose |
|----------|---------|
| EVENTHUB_CONNECTION_STRING | Event Hubs connection string |
| EVENTHUB_NAME | Event Hub name (optional if EntityPath present) |
| SQLSERVER_CONNECTION_STRING | SQL Server ODBC connection string |
| SIM_CLOCK_SPEED=60.0 | Sim hours per real hour (1=real time) |
| SIM_FLIGHT_INTERVAL_MINUTES=5 | Interval between scheduling flights (sim minutes) |
| SIM_MAX_ACTIVE_FLIGHTS=0 | Limit concurrent active flights (0=unlimited) |
| SIM_LOSS_RATE=0.002 | Fraction of bags lost |
| SIM_INSPECT_RATE=0.01 | Fraction of bags inspected |
| SIM_REJECT_RATE=0.003 | Fraction of screened bags rejected |
| SIM_NOT_COLLECTED_RATE=0.005 | Fraction of delivered bags not collected |
| SIM_CE_MODE=structured | CloudEvents mode (structured/binary) |
| SIM_DURATION_MINUTES=0 | Default run duration; 0 or 'forever' = until stopped |

CLI flags override environment variables.

## Real-Time Mode (clock-speed=1)

Setting `--clock-speed 1` changes behavior:

* Flights are scheduled to depart 1 hour from the current (real) time.
* Only short routes (≤ 1.5h block time) are scheduled.
* Check-in opens immediately and compressed ground operation windows are used.

## Minimal SQL Setup

Create table (example):

```sql
CREATE TABLE dbo.Flights (
    FlightId NVARCHAR(64) PRIMARY KEY,
    Airline NVARCHAR(8),
    FlightNumber NVARCHAR(16),
    Origin NVARCHAR(8),
    Destination NVARCHAR(8),
    DepartureUtc DATETIME2,
    ArrivalUtc DATETIME2,
    CheckinClosedUtc DATETIME2 NULL,
    CompletedUtc DATETIME2 NULL
);
```

## Deploy to Azure Container Instances

An ARM template (`azure-template.json`) can deploy the simulator. Before use, update it (or await template adaptation) to surface the Event Hub & SQL settings as parameters/environment variables. After publishing the image to your registry, deploy via portal or CLI.

## CloudEvent Example (truncated)

```json
{
    "specversion": "1.0",
    "type": "com.example.baggage.flight.checkin.started",
    "source": "bhsim://flight/FJ1234",
    "id": "...",
    "time": "2025-09-10T10:15:23.123456Z",
    "data": { "flightId": "...", "capacity": 320 }
}
```

## License

MIT

## Notes

* Use `--dry-run` for local experimentation without Event Hubs or SQL.
* Batch window (default 5s) groups same-flight events for efficiency.
* Set `--verbose` to print batch/send details.
* Ensure time sync (UTC) when correlating with downstream systems.
* Duration can be specified via `--duration-minutes` or `SIM_DURATION_MINUTES`; set to `forever` (or 0) for indefinite run.

