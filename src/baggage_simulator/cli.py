# SPDX-License-Identifier: MIT
from __future__ import annotations

import argparse
import asyncio
import os
from typing import Optional

from .simulator import SimulatorConfig, Simulator


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="bhsim",
        description="Airport baggage handling simulator publishing CloudEvents to Azure Event Hubs",
    )

    # Event Hubs
    eh = parser.add_argument_group("Event Hubs")
    eh.add_argument(
        "--eventhub-conn",
        default=os.getenv("EVENTHUB_CONNECTION_STRING"),
        help="Azure Event Hubs connection string (env: EVENTHUB_CONNECTION_STRING)",
    )
    eh.add_argument(
        "--eventhub-name",
        default=os.getenv("EVENTHUB_NAME"),
        help="Event Hub name (env: EVENTHUB_NAME)",
    )

    # SQL Server
    sql = parser.add_argument_group("SQL Server")
    sql.add_argument(
        "--sql-conn",
        default=os.getenv("SQLSERVER_CONNECTION_STRING"),
        help="SQL Server ODBC connection string (env: SQLSERVER_CONNECTION_STRING)",
    )
    sql.add_argument(
        "--sql-table",
        default=os.getenv("SQLSERVER_FLIGHTS_TABLE", "dbo.Flights"),
        help="SQL Server table for flights",
    )

    # Clock & cadence
    clk = parser.add_argument_group("Clock & cadence")
    clk.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    clk.add_argument(
        "--clock-offset-minutes",
        type=int,
        default=int(os.getenv("SIM_CLOCK_OFFSET_MINUTES", "0")),
        help="Real-time offset in minutes applied to simulated now()",
    )
    clk.add_argument(
        "--clock-speed",
        type=float,
        default=float(os.getenv("SIM_CLOCK_SPEED", "60.0")),
        help="Acceleration: 60.0 means 1 sim hour per real minute",
    )
    clk.add_argument(
        "--flight-interval-minutes",
        type=int,
        default=int(os.getenv("SIM_FLIGHT_INTERVAL_MINUTES", "5")),
        help="How often to schedule a new flight (sim minutes)",
    )
    clk.add_argument(
        "--max-active-flights",
        type=int,
        default=int(os.getenv("SIM_MAX_ACTIVE_FLIGHTS", "0")),
        help="Upper bound on concurrently active flights; 0 = unlimited",
    )

    # Behavior tuning
    beh = parser.add_argument_group("Behavior")
    beh.add_argument(
        "--loss-rate",
        type=float,
        default=float(os.getenv("SIM_LOSS_RATE", "0.002")),
        help="Fraction of checked bags lost (0.0-1.0)",
    )
    beh.add_argument(
        "--inspect-rate",
        type=float,
        default=float(os.getenv("SIM_INSPECT_RATE", "0.01")),
        help="Fraction of screened bags inspected",
    )
    beh.add_argument(
        "--reject-rate",
        type=float,
        default=float(os.getenv("SIM_REJECT_RATE", "0.003")),
        help="Fraction of screened bags rejected",
    )
    beh.add_argument(
        "--not-collected-rate",
        type=float,
        default=float(os.getenv("SIM_NOT_COLLECTED_RATE", "0.005")),
        help="Fraction of delivered bags not collected",
    )

    # Airports, airlines
    a = parser.add_argument_group("Airports & airlines")
    a.add_argument(
        "--airports-file",
        default=None,
        help="Optional CSV of airport pairs (IATA1,IATA2); default built-in",
    )
    a.add_argument(
        "--airlines",
        default=None,
        help="Comma-separated list of airline codes; default built-in",
    )

    def _parse_duration(value: str) -> int:
        v = value.strip().lower()
        if v in ("forever", "infinite", "inf", "unlimited"):
            return 0  # 0 already means run until Ctrl+C in simulator logic
        try:
            return int(v)
        except ValueError as e:
            raise argparse.ArgumentTypeError("duration-minutes must be an integer or 'forever'") from e

    parser.add_argument(
        "--duration-minutes",
        type=_parse_duration,
        default=_parse_duration(os.getenv("SIM_DURATION_MINUTES", "0")),
        help="Run time in real minutes; 0 or 'forever' = until Ctrl+C (env: SIM_DURATION_MINUTES)",
    )

    # Output
    out = parser.add_argument_group("Output")
    out.add_argument("--quiet", action="store_true", help="Suppress rich console status output")
    out.add_argument("--dry-run", action="store_true", help="Do not write to SQL or Event Hubs; print to console only")
    out.add_argument("--verbose", action="store_true", help="Print per-event send summaries (batches/events)")
    out.add_argument(
        "--ce-mode",
        choices=("structured", "binary"),
        default=os.getenv("SIM_CE_MODE", "structured"),
        help="CloudEvents encoding mode: 'structured' (JSON) or 'binary' (default structured)",
    )

    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    ns = parse_args(argv)

    # If user didn't supply an explicit Event Hub name, try to extract it from
    # the connection string segment `EntityPath=<hubname>` (common in Azure
    # Event Hubs connection strings). This lets users pass a full connection
    # string without separately specifying the hub name.
    eh_name = ns.eventhub_name
    eh_conn = ns.eventhub_conn
    if not eh_name and isinstance(eh_conn, str) and eh_conn:
        try:
            for part in eh_conn.split(";"):
                if part.strip().lower().startswith("entitypath="):
                    eh_name = part.split("=", 1)[1]
                    break
        except Exception:
            eh_name = ns.eventhub_name

    cfg = SimulatorConfig(
        eventhub_conn=ns.eventhub_conn,
        eventhub_name=eh_name,
        sql_conn=ns.sql_conn,
        sql_table=ns.sql_table,
        seed=ns.seed,
        clock_offset_minutes=ns.clock_offset_minutes,
        clock_speed=ns.clock_speed,
        flight_interval_minutes=ns.flight_interval_minutes,
        max_active_flights=ns.max_active_flights,
        loss_rate=ns.loss_rate,
        inspect_rate=ns.inspect_rate,
        reject_rate=ns.reject_rate,
        not_collected_rate=ns.not_collected_rate,
        airports_file=ns.airports_file,
        airlines=(ns.airlines.split(",") if ns.airlines else None),
        duration_minutes=ns.duration_minutes,
    ce_mode=ns.ce_mode,
    verbose=getattr(ns, "verbose", False),
        enable_console=(not ns.quiet),
        dry_run=ns.dry_run,
    )

    async def _run() -> None:
        sim = Simulator(cfg)
        try:
            await sim.run()
        finally:
            await sim.close()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
