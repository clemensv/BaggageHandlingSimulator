# SPDX-License-Identifier: MIT
from __future__ import annotations

import asyncio
import contextlib
import csv
import dataclasses
import json
from uuid import uuid4
import math
import os
import random
import string
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from cloudevents.http import CloudEvent
from cloudevents.conversion import to_json as cloudevent_to_json
from rich.console import Console
from rich.live import Live
from rich.text import Text
from pathlib import Path
import numbers

# CloudEvents helper using official SDK


def cloudevent(event_type: str, source: str, subject: str, data: Dict[str, Any], sim_time: Optional[datetime] = None) -> CloudEvent:
    attrs = {
        "id": _rand_id(),
        "type": event_type,
        "source": source,
        "subject": subject,
        "time": (sim_time or datetime.now(timezone.utc)).isoformat(),
        "datacontenttype": "application/json",
    }
    return CloudEvent(attrs, data)


ALLOWED_AIRCRAFT = {
    # type: (min, max) typical seats
    "B747": (366, 410),
    "A380": (500, 575),
    "B777": (235, 312),
    "A350": (300, 350),
    "B737": (130, 210),
    "A320": (140, 195),
    "CRJ700": (66, 78),
    "A220": (100, 135),
}

DEFAULT_AIRLINES = [
    "FJ",  # FictionJet
    "AZ",  # AeroZoom
    "SK",  # SkyNation
    "LT",  # LuftTram
]

DEFAULT_AIRPORT_PAIRS = [
    ("FRA", "LHR"),
    ("FRA", "JFK"),
    ("FRA", "CDG"),
    ("LHR", "JFK"),
    ("LHR", "SFO"),
    ("CDG", "NRT"),
    ("JFK", "LAX"),
    ("JFK", "MIA"),
    ("LAX", "SEA"),
    ("SEA", "SFO"),
    ("MUC", "FRA"),
    ("MUC", "LHR"),
]


@dataclass(slots=True)
class SimulatorConfig:
    eventhub_conn: str | None
    eventhub_name: str | None
    sql_conn: str | None
    sql_table: str = "dbo.Flights"
    # Real-time batching window for Event Hubs (seconds). Events for the same
    # flight/partition key are buffered and sent together within this window.
    eventhub_batch_window_seconds: float = 5.0
    # When true, print per-event/per-batch send summaries to the console.
    verbose: bool = False
    seed: Optional[int] = None
    clock_offset_minutes: int = 0
    clock_speed: float = 60.0
    flight_interval_minutes: int = 5
    max_active_flights: int = 0  # 0 = unlimited
    loss_rate: float = 0.002
    inspect_rate: float = 0.01
    reject_rate: float = 0.003
    not_collected_rate: float = 0.005
    airports_file: Optional[str] = None
    airlines: Optional[List[str]] = None
    duration_minutes: int = 0
    enable_console: bool = True
    dry_run: bool = False
    ce_mode: str = "structured"


@dataclass(slots=True)
class Flight:
    flight_id: str
    airline: str
    flight_number: str
    aircraft: str
    origin: str
    destination: str
    departure_utc: datetime
    arrival_utc: datetime
    capacity: int


@dataclass(slots=True)
class Bag:
    bag_id: str
    pax_id: str
    weight_kg: float
    status: str = "created"


@dataclass(slots=True)
class Passenger:
    pax_id: str
    name: str
    checked_in: bool = False
    bags: List[Bag] = field(default_factory=list)


class Simulator:
    # attribute declarations for static analysis
    _producer: Any | None
    _conn: Any | None
    _active_flights: Dict[str, Any]
    _console: Optional[Console]
    _live: Optional[Live]
    _pax_gender_toggle: bool
    _total_scheduled: int
    _airports: List[Tuple[str, str]]
    _airlines: List[str]
    _rng: random.Random
    _start_real: float
    _start_sim: datetime
    # Event Hubs batching buffers keyed by partition key (flightId).
    _eh_buffers: Dict[str, List[Any]]
    _eh_first_seen: Dict[str, float]
    # When all active flights are in-flight, mark the real-time moment to allow a jump.
    _inflight_only_since: Optional[float]
    # XReg schemas cache for lightweight validation
    _xreg_schemas: Dict[str, List[Tuple[str, Any]]]

    # The Simulator class encapsulates the entire simulation lifecycle:
    # - maintains an accelerated simulated clock (see now())
    # - lazily initialises external clients (Event Hubs, SQL) so dry-run works without deps
    # - schedules flights and advances per-flight timelines
    # - emits CloudEvents (structured or binary mode) and persists SQL markers
    # - provides a Rich non-scrolling Live header with pinned per-flight status lines

    def __init__(self, cfg: SimulatorConfig) -> None:
        self.cfg = cfg
        self._rng = random.Random(cfg.seed)
        self._start_real = time.monotonic()
        self._start_sim = datetime.now(timezone.utc) + timedelta(minutes=cfg.clock_offset_minutes)
        self._airports = self._load_airports(cfg.airports_file) or DEFAULT_AIRPORT_PAIRS
        self._airlines = cfg.airlines or DEFAULT_AIRLINES
        self._producer = None
        self._conn = None
        self._active_flights = {}
        self._console = Console(stderr=True) if cfg.enable_console else None
        self._live = None
        self._pax_gender_toggle = False
        self._total_scheduled = 0
        # load xreg schemas for lightweight validation (optional)
        self._xreg_schemas = self._load_xreg_schemas()
        # EH batching state
        self._eh_buffers = defaultdict(list)
        self._eh_first_seen = {}
        self._inflight_only_since = None
        # NOTE: _start_real/_start_sim form the baseline for the accelerated clock:
        # realtime elapsed = monotonic() - _start_real
        # simulated elapsed = realtime_elapsed * clock_speed
        # current simulated time = _start_sim + simulated_elapsed
        # This keeps the simulation monotonic and robust to system clock jumps.

    async def _ensure_clients(self) -> None:
        if self.cfg.dry_run:
            # In dry-run we do not require external services
            return
        # Lazy imports so dry-run can work without these dependencies installed
        from azure.eventhub.aio import EventHubProducerClient  # type: ignore
        import pyodbc  # type: ignore
        if not self._producer:
            if not (self.cfg.eventhub_conn and self.cfg.eventhub_name):
                raise RuntimeError("Event Hubs connection and name required")
            self._producer = EventHubProducerClient.from_connection_string(
                conn_str=self.cfg.eventhub_conn, eventhub_name=self.cfg.eventhub_name
            )
        if not self._conn:
            if not self.cfg.sql_conn:
                raise RuntimeError("SQL Server connection string required")
            
            # Special handling for Azure AD authentication
            if "Authentication=ActiveDirectoryInteractive" in self.cfg.sql_conn:
                try:
                    # Use Azure CLI authentication token
                    import subprocess
                    import struct
                    
                    # Get token from Azure CLI
                    result = subprocess.run(
                        ["az", "account", "get-access-token", "--resource", "https://database.windows.net/", "--query", "accessToken", "-o", "tsv"],
                        capture_output=True,
                        text=True,
                        check=True,
                        shell=True
                    )
                    token = result.stdout.strip()
                    
                    if token:
                        # Prepare token for SQL connection
                        SQL_COPT_SS_ACCESS_TOKEN = 1256
                        token_bytes = bytes(token, 'utf-8')
                        exptoken = b''
                        for i in token_bytes:
                            exptoken += bytes({i})
                            exptoken += bytes(1)
                        token_struct = struct.pack("=i", len(exptoken)) + exptoken
                        
                        # Remove Authentication parameter from connection string
                        conn_parts = [p.strip() for p in self.cfg.sql_conn.split(';') if p.strip() and not p.strip().startswith('Authentication=')]
                        clean_conn_str = ';'.join(conn_parts)
                        
                        # Connect using token
                        self._conn = pyodbc.connect(clean_conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct}, autocommit=True)
                        self._log("Connected to SQL Server using Azure CLI token", style="green")
                        return
                except subprocess.CalledProcessError as e: # pyright: ignore[reportPossiblyUnboundVariable]
                    self._log(f"Azure CLI not authenticated. Please run 'az login' first. Error: {e}", style="red")
                    raise RuntimeError("Azure CLI authentication required. Run 'az login' first.")
                except Exception as e:
                    self._log(f"Token auth failed: {e}", style="yellow")
                    # Fall through to try original connection string
            
            # Standard connection
            self._conn = pyodbc.connect(self.cfg.sql_conn, autocommit=True)

        # Clients are created lazily here. This method will raise if non-dry-run
        # mode is used but required connection info is missing. Lazy creation
        # allows tests and dry-run invocations to run without installing SDKs.

    def _load_xreg_schemas(self) -> Dict[str, List[Tuple[str, Any]]]:
        """Load message field definitions from message-definitions/airport-baggage.xreg.json.

        This is a best-effort, lightweight parser: it only extracts top-level field
        names and their declared Avro types for simple presence/type checks. It does
        NOT implement full Avro validation (unions, nested records, logical types,
        default values, enum resolution, etc.). The intent is to warn about obvious
        mismatches while keeping the simulator decoupled from a full schema-registry
        or Avro library dependency.
        """
        try:
            base = Path(__file__).resolve().parents[2] / "message-definitions" / "airport-baggage.xreg.json"
            if not base.exists():
                return {}
            data = json.loads(base.read_text(encoding="utf-8"))
            schemas: Dict[str, List[Tuple[str, Any]]] = {}
            # build a helper mapping for schemagroups
            schemagroups = data.get("schemagroups", {})
            # map schema id -> fields
            schema_fields: Dict[str, List[Tuple[str, Any]]] = {}
            for sg_name, sg in (schemagroups.items() if isinstance(schemagroups, dict) else []):
                sg_schemas = sg.get("schemas", {})
                for sname, sval in sg_schemas.items():
                    versions = sval.get("versions", {})
                    ver = versions.get(str(sval.get("defaultversionid") or "1")) or next(iter(versions.values()), None)
                    if not ver:
                        continue
                    sch = ver.get("schema") or {}
                    fields = [(f.get("name"), f.get("type")) for f in sch.get("fields", [])]
                    key = f"#/schemagroups/{sg_name}/schemas/{sname}"
                    schema_fields[key] = fields
            # now map messages to their schema fields
            for mg_name, mg in (data.get("messagegroups", {}) or {}).items():
                messages = mg.get("messages", {})
                for mtype, mdef in (messages.items() if isinstance(messages, dict) else []):
                    dsuri = mdef.get("dataschemauri")
                    if not dsuri:
                        continue
                    fields = schema_fields.get(dsuri)
                    if fields:
                        schemas[mtype] = fields
            return schemas
        except Exception:
            return {}

    def _validate_payload_against_xreg(self, event_type: str, payload: Dict[str, Any]) -> None:
        """Best-effort validation: check required fields exist and basic types match Avro declarations.

        Important notes:
        - This validation is intentionally permissive: it logs warnings but does not
        block event emission. The simulator's transport remains JSON by default.
        - For full Avro conformance, replace this with a proper Avro/Schema Registry
        integration and perform strict serialization/validation.
        """
        if not isinstance(payload, dict):
            return
        schema = self._xreg_schemas.get(event_type)
        if not schema:
            return
        for name, avro_type in schema:
            # avro_type may be a union or simple type
            expected_types = avro_type
            if isinstance(avro_type, list):
                expected_types = avro_type
            if expected_types is None:
                continue
            if name not in payload:
                self._log(f"⚠️ Schema mismatch: event {event_type} missing field '{name}'", style="yellow")
                continue
            val = payload[name]
            # basic checks
            ok = True
            if expected_types == "string" or expected_types == ["string"]:
                ok = isinstance(val, str)
            elif isinstance(expected_types, list) and any(t in {"float","double","int","long"} for t in expected_types):
                ok = isinstance(val, numbers.Number)
            elif expected_types in {"float","double"}:
                ok = isinstance(val, numbers.Number)
            # else: skip complex validation
            if not ok:
                self._log(f"⚠️ Schema mismatch: event {event_type} field '{name}' expected {expected_types}, got {type(val).__name__}", style="yellow")

    # clock
    def now(self) -> datetime:
        """Return current simulated time based on monotonic baseline and clock_speed.

        The simulation advances at `clock_speed` times real time. For example,
        clock_speed=60 makes 1 real second equal 1 simulated minute.
        """
        dt = time.monotonic() - self._start_real
        sim_elapsed = dt * self.cfg.clock_speed
        return self._start_sim + timedelta(seconds=sim_elapsed)

    def _load_airports(self, path: Optional[str]) -> Optional[List[Tuple[str, str]]]:
        if not path:
            return None
        pairs: List[Tuple[str, str]] = []
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.reader(f):
                if not row or row[0].startswith("#"):
                    continue
                if len(row) >= 2:
                    pairs.append((row[0].strip().upper(), row[1].strip().upper()))
        return pairs or None

    async def close(self) -> None:
        # stop live header first if active
        if self._live is not None:
            with contextlib.suppress(Exception):
                self._live.stop()
            self._live = None
        # Flush any outstanding EH buffers before closing
        with contextlib.suppress(Exception):
            if self._producer:
                await self._flush_due_eventhub_buffers(force=True)
        with contextlib.suppress(Exception):
            if self._producer:
                await self._producer.close()
        with contextlib.suppress(Exception):
            if self._conn:
                self._conn.close()

    async def run(self) -> None:
        """Main simulator loop.

        Responsibilities:
        - Ensure external clients are available (unless dry-run)
        - Start the Rich Live header for a compact non-scrolling UI
        - Schedule flights at configured intervals, respecting `max_active_flights`
        - Advance per-flight timelines by calling `_tick_flights`
        - Support run-to-completion mode when `duration_minutes <= 0` and
          a `max_active_flights` limit is set: schedule up to the limit and then
          exit once all flights have finished unloading and been retired.
        - Handle KeyboardInterrupt/CancelledError gracefully and always close
          external clients and the Live header in `finally`.
        """
        await self._ensure_clients()
        stop_at = None if self.cfg.duration_minutes <= 0 else time.monotonic() + self.cfg.duration_minutes * 60
        next_flight_at_sim = self.now()
        flight_interval = timedelta(minutes=self.cfg.flight_interval_minutes)
        # start non-scrolling time header
        if self._console and not self._live:
            try:
                from rich.live import Live as _Live
                from rich.text import Text as _Text
                self._live = _Live(_Text("", no_wrap=True), console=self._console, refresh_per_second=4)
                self._live.start()
            except Exception:
                self._live = None

        try:
            while True:
                now_sim = self.now()
                if self._live:
                    try:
                        from rich.text import Text as _Text
                        t = _Text(no_wrap=True)
                        # header
                        t.append(f"Sim: {now_sim.isoformat()}  |  speed x{self.cfg.clock_speed:.1f}  |  active flights {len(self._active_flights)}\n",
                                 style="bold white on blue")
                        # per-flight pinned lines
                        for st in self._active_flights.values():
                            try:
                                f = st.get("flight")
                                if not f:
                                    continue
                                tl = st.get("timeline", {})
                                # determine status
                                status = "Closed"
                                icon = "📦"
                                if now_sim < tl.get("checkin_start", now_sim):
                                    status = "Scheduled"
                                    icon = "🟦"
                                elif tl.get("checkin_start") <= now_sim <= tl.get("checkin_end"):
                                    status = "Check-in"
                                    icon = "🟢"
                                elif tl.get("load_start") <= now_sim < tl.get("depart"):
                                    status = "Loading"
                                    icon = "🟡"
                                elif "flight_departed" in st.get("emitted", set()) and "flight_arrived" not in st.get("emitted", set()):
                                    status = "In flight"
                                    icon = "✈️"
                                elif "flight_arrived" in st.get("emitted", set()):
                                    if not st.get("slot_released", False):
                                        status = "Arrived / Unloading"
                                        icon = "🟣"
                                    else:
                                        status = "Completed"
                                        icon = "✅"
                                # counts
                                # cumulative loaded count: any bag that has ever been loaded (includes later phases)
                                bags_dict: Dict[str, Bag] = st.get("bags", {})
                                loaded = sum(1 for b in bags_dict.values() if getattr(b, "status", "") in {"loaded", "unloaded", "cleared", "delivered", "withheld"})
                                delivered = sum(1 for b in bags_dict.values() if getattr(b, "status", "") == "delivered")
                                pax_list: List[Passenger] = st.get("passengers", [])
                                pax_checked_in = sum(1 for p in pax_list if getattr(p, "checked_in", False))
                                # Count bags actively in the flow (exclude exceptions) to line up with delivery pipeline
                                active_statuses = {"checked_in", "loaded", "unloaded", "cleared", "delivered"}
                                bags_checked_in = sum(1 for b in bags_dict.values() if getattr(b, "status", "") in active_statuses)
                                # Exceptions to explain gaps (not counted in bags:)
                                rej = sum(1 for b in bags_dict.values() if getattr(b, "status", "") == "rejected")
                                withh = sum(1 for b in bags_dict.values() if getattr(b, "status", "") == "withheld")
                                lost = sum(1 for b in bags_dict.values() if getattr(b, "status", "") == "lost")
                                t.append(
                                    f"{icon} {f.flight_number} {f.origin}->{f.destination} dep {f.departure_utc.strftime('%H:%M')} arr {f.arrival_utc.strftime('%H:%M')}  |  {status}  |  pax:{pax_checked_in}/{len(pax_list)} bags:{bags_checked_in}/{len(bags_dict)} (rej:{rej} withh:{withh} lost:{lost})  |  loaded:{loaded} delivered:{delivered}\n",
                                    style="cyan",
                                )
                            except Exception:
                                # don't let one flight line break the live header
                                continue
                        self._live.update(t)
                    except Exception:
                        pass
                # Only count flights that have not yet released their slot (slot released after last bag unloaded)
                active_slots_in_use = sum(1 for st in self._active_flights.values() if not st.get("slot_released", False))
                if self.cfg.duration_minutes <= 0 and self.cfg.max_active_flights > 0:
                    # One-shot scheduling up to max, then no more flights
                    can_schedule_more = (self._total_scheduled < self.cfg.max_active_flights and active_slots_in_use < self.cfg.max_active_flights)
                else:
                    can_schedule_more = (self.cfg.max_active_flights <= 0 or active_slots_in_use < self.cfg.max_active_flights)
                # If all active flights are currently in-flight (departed but not yet arrived),
                # start a 5s real-time timer and pause scheduling to allow a fast-forward jump.
                active_states = [st for st in self._active_flights.values() if not st.get("slot_released", False)]
                all_inflight = False
                if active_states:
                    try:
                        all_inflight = all(
                            ("flight_departed" in st.get("emitted", set())) and ("flight_arrived" not in st.get("emitted", set()))
                            for st in active_states
                        )
                    except Exception:
                        all_inflight = False
                if all_inflight:
                    if self._inflight_only_since is None:
                        self._inflight_only_since = time.monotonic()
                    # Pause scheduling while in this state
                    can_schedule_more = False
                else:
                    self._inflight_only_since = None

                if now_sim >= next_flight_at_sim and can_schedule_more:
                    flight = self._create_flight(now_sim)
                    self._insert_flight_sql(flight)
                    self._active_flights[flight.flight_id] = self._state_for_flight(flight, now_sim)
                    self._log(
                        f"✈️ Scheduled {flight.flight_number} {flight.origin}->{flight.destination} "
                        f"dep {flight.departure_utc.isoformat()}Z arr {flight.arrival_utc.isoformat()}Z "
                        f"aircraft {flight.aircraft} cap {flight.capacity}",
                        style="bold cyan",
                    )
                    self._total_scheduled += 1
                    next_flight_at_sim += flight_interval

                # progress flight timelines and emit events
                await self._tick_flights(now_sim)

                # Flush due Event Hubs buffers based on real-time age
                with contextlib.suppress(Exception):
                    await self._flush_due_eventhub_buffers()

                # If all active flights are in-flight and it has persisted for >= 5 real seconds,
                # jump the simulated clock to the earliest arrival.
                if self._inflight_only_since is not None and (time.monotonic() - self._inflight_only_since) >= 5.0:
                    # Re-evaluate active in-flight flights
                    inflight_states = [st for st in self._active_flights.values() if (not st.get("slot_released", False)) and ("flight_departed" in st.get("emitted", set())) and ("flight_arrived" not in st.get("emitted", set()))]
                    if inflight_states:
                        try:
                            target = min((st.get("timeline", {}).get("arrive") for st in inflight_states if isinstance(st.get("timeline", {}).get("arrive"), datetime)), default=None)
                        except Exception:
                            target = None
                        if isinstance(target, datetime):
                            now_current = self.now()
                            if target > now_current:
                                # Adjust baseline so that now() becomes 'target'
                                elapsed_real = time.monotonic() - self._start_real
                                self._start_sim = target - timedelta(seconds=elapsed_real * max(1.0, float(self.cfg.clock_speed)))
                                self._log(f"⏭️ Fast-forward to next arrival at {target.isoformat()} (all flights in flight)", style="yellow")
                    # Reset regardless to avoid repeated jumping without reassessment
                    self._inflight_only_since = None

                # check stop condition
                if stop_at and time.monotonic() >= stop_at:
                    break
                # If no duration specified and we scheduled up to the max, exit once all flights are retired
                if self.cfg.duration_minutes <= 0 and self.cfg.max_active_flights > 0:
                    if self._total_scheduled >= self.cfg.max_active_flights and len(self._active_flights) == 0:
                        break

                # Adaptive sleep: scale inversely with clock_speed so each loop advances a small,
                # consistent amount of simulated time. Target ~30s sim per tick; clamp to 10–200ms real time
                # to keep the UI responsive and CPU reasonable.
                sleep_s = max(0.01, min(0.2, 30.0 / max(1.0, self.cfg.clock_speed)))
                await asyncio.sleep(sleep_s)
        except (KeyboardInterrupt, asyncio.CancelledError):
            # Graceful shutdown on Ctrl+C or cancellation: log and fall through to cleanup
            try:
                self._log("Shutdown requested (Ctrl+C) — exiting gracefully.", style="yellow")
            except Exception:
                pass
        finally:
            # Ensure clients and live header are closed cleanly
            with contextlib.suppress(Exception):
                await self.close()

    def _estimate_duration_hours(self, origin: str, destination: str) -> float:
        """Return a plausible block time (gate-to-gate) in hours for a route.
        Uses a route table for known pairs and adds small variability; falls back to region heuristics.
        """
        # Explicit route table (hours)
        route_hours: Dict[Tuple[str, str], float] = {
            ("FRA", "LHR"): 1.5, ("LHR", "FRA"): 1.5,
            ("FRA", "JFK"): 8.5, ("JFK", "FRA"): 7.5,
            ("FRA", "CDG"): 1.25, ("CDG", "FRA"): 1.25,
            ("LHR", "JFK"): 8.0, ("JFK", "LHR"): 7.0,
            ("LHR", "SFO"): 11.0, ("SFO", "LHR"): 10.5,
            ("CDG", "NRT"): 12.0, ("NRT", "CDG"): 12.0,
            ("JFK", "LAX"): 6.0, ("LAX", "JFK"): 5.5,
            ("JFK", "MIA"): 3.0, ("MIA", "JFK"): 3.0,
            ("LAX", "SEA"): 2.5, ("SEA", "LAX"): 2.5,
            ("SEA", "SFO"): 2.0, ("SFO", "SEA"): 2.0,
            ("MUC", "FRA"): 1.0, ("FRA", "MUC"): 1.0,
            ("MUC", "LHR"): 2.0, ("LHR", "MUC"): 2.0,
        }
        key = (origin, destination)
        base = route_hours.get(key)
        if base is None:
            # Region fallback
            EU = {"FRA", "LHR", "CDG", "MUC"}
            US_EAST = {"JFK", "MIA"}
            US_WEST = {"LAX", "SEA", "SFO"}
            JP = {"NRT"}
            o, d = origin.upper(), destination.upper()
            if (o in EU and d in EU) or (o in US_WEST and d in US_WEST) or (o in US_EAST and d in US_EAST):
                base = 1.5 if o in EU else 2.2  # intra-region avg
            elif (o in US_EAST and d in US_WEST) or (o in US_WEST and d in US_EAST):
                base = 5.8  # transcon
            elif (o in EU and d in US_EAST) or (o in US_EAST and d in EU):
                base = 7.8 if o in US_EAST else 8.6  # eastbound a bit shorter
            elif (o in EU and d in US_WEST) or (o in US_WEST and d in EU):
                base = 10.5
            elif (o in JP and d in EU) or (o in EU and d in JP):
                base = 12.0
            elif (o in JP and (d in US_WEST or d in US_EAST)) or ((o in US_WEST or o in US_EAST) and d in JP):
                base = 10.8 if o in US_WEST or d in US_WEST else 12.5
            else:
                base = 6.0  # generic fallback
        # apply small variability and clamp to reasonable bounds
        factor = self._rng.uniform(0.9, 1.1)
        hours = base * factor
        return max(0.8 * base, min(1.2 * base, hours))

    def _create_flight(self, now_sim: datetime) -> Flight:
        """Create a plausible Flight instance.

        Scheduling rule used here ensures check-in will start within the next
        ~20 simulated minutes by setting departure to now + 3h..3h20m. Arrival
        is derived from a route-duration heuristic to produce realistic block times.
        
        Special case: When clock_speed is 1, schedule flights for 1 hour from current 
        time and begin check-in immediately.
        """
        airline = self._rng.choice(self._airlines)
        aircraft, (minc, maxc) = self._rng.choice(list(ALLOWED_AIRCRAFT.items()))
        capacity = self._rng.randint(minc, maxc)
        
        # Special handling for clock_speed = 1: only use short-duration routes (≤1.5 hours)
        if self.cfg.clock_speed == 1:
            # Real-time mode: restrict to routes with estimated duration <= 1.5h.
            # We predefine a small whitelist of known short routes (also verified in route table) and intersect
            # with configured airport pairs to avoid repeatedly sampling and rejecting.
            short_routes = {(
                "FRA", "LHR"), ("LHR", "FRA"), ("FRA", "CDG"), ("CDG", "FRA"), ("MUC", "FRA"), ("FRA", "MUC")
            }
            available_short_routes = [pair for pair in self._airports if pair in short_routes]
            if not available_short_routes:
                # Fallback: dynamically filter any route with duration <=1.5h from the configured list
                dyn_short = []
                for o, d in self._airports:
                    if self._estimate_duration_hours(o, d) <= 1.5:
                        dyn_short.append((o, d))
                available_short_routes = dyn_short or list(self._airports)
            origin, destination = self._rng.choice(available_short_routes)
            dep = now_sim + timedelta(hours=1)
        else:
            origin, destination = self._rng.choice(self._airports)
            # Normal scheduling: departure so that check-in starts within next 20 sim minutes
            # check-in start is departure - 3h, so set departure to now + 3h .. 3h20m
            dep_in = timedelta(hours=3, minutes=self._rng.uniform(0, 20))
            dep = now_sim + dep_in
            
        # plausible route-based duration
        dur_hours = self._estimate_duration_hours(origin, destination)
        arr = dep + timedelta(hours=dur_hours)
        flight_number = f"{airline}{self._rng.randint(10, 9999):04d}"
        flight_id = _rand_id()
        return Flight(
            flight_id=flight_id,
            airline=airline,
            flight_number=flight_number,
            aircraft=aircraft,
            origin=origin,
            destination=destination,
            departure_utc=dep,
            arrival_utc=arr,
            capacity=capacity,
        )

    def _insert_flight_sql(self, f: Flight) -> None:
        """Persist the scheduled flight to SQL (or log in dry-run).

        The SQL statement writes only the initial schedule row; actual departure
        and arrival times are updated later via `_update_flight_actual_time`.
        """
        # Dry-run: only log intended SQL action
        if self.cfg.dry_run or not self._conn:
            self._log(
                f"✈️ [dry-run] Insert flight {f.flight_number} {f.origin}->{f.destination} dep={f.departure_utc.isoformat()}Z arr={f.arrival_utc.isoformat()}Z",
                style="yellow",
            )
            return
        cur = self._conn.cursor()
        cur.execute(
            f"INSERT INTO {self.cfg.sql_table} (FlightId, Airline, FlightNumber, Aircraft, Origin, Destination, DepartureUtc, ArrivalUtc, Capacity) VALUES (?,?,?,?,?,?,?,?,?)",
            (
                f.flight_id,
                f.airline,
                f.flight_number,
                f.aircraft,
                f.origin,
                f.destination,
                f.departure_utc,
                f.arrival_utc,
                f.capacity,
            ),
        )

    def _state_for_flight(self, f: Flight, now_sim: datetime) -> Dict[str, Any]:
        """Create per-flight mutable state used by the simulator loop.

        This returns a dictionary containing passenger and bag lists, timeline
        checkpoints (checkin_start, checkin_end, load_start, depart, arrive),
        and tracking sets for emitted events and bag load/delivery progress.
        """
        # compute passenger count up to 95% load
        pax_count = math.floor(self._rng.uniform(0.7, 0.95) * f.capacity)
        pax_list = [Passenger(pax_id=_rand_pax_id(self._rng), name=_rand_name(self._rng)) for _ in range(pax_count)]
        # bags: each pax 0-2 bags, average about 1.2
        bags: Dict[str, Bag] = {}
        for p in pax_list:
            bag_count = max(0, min(2, int(self._rng.gauss(1.2, 0.6))))
            for _ in range(bag_count):
                b = Bag(bag_id=_rand_bag_id(self._rng), pax_id=p.pax_id, weight_kg=max(8.0, self._rng.gauss(18.0, 5.0)))
                p.bags.append(b)
                bags[b.bag_id] = b

        # Initial timeline with explicit offsets relative to departure:
        # - Check-in starts 3h before departure (or immediately if clock_speed=1)
        # - Check-in ends 60m before departure (i.e., 30m before loading starts)
        # - Loading starts 30m before departure
        if self.cfg.clock_speed == 1:
            # For real-time simulation: immediate check-in, shorter windows
            checkin_start = now_sim
            checkin_end = f.departure_utc - timedelta(minutes=10)  # Check-in closes 10min before departure
            load_start = f.departure_utc - timedelta(minutes=5)   # Loading starts 5min before departure
        else:
            # Normal scheduling: check-in starts 3h before departure
            checkin_start = f.departure_utc - timedelta(hours=3)
            checkin_end = f.departure_utc - timedelta(minutes=60)
            load_start = f.departure_utc - timedelta(minutes=30)
            
        timeline = {
            "depart": f.departure_utc,
            "arrive": f.arrival_utc,
            "checkin_start": checkin_start,
            "checkin_end": checkin_end,
            "load_start": load_start,
        }

        return {
            "flight": f,
            "passengers": pax_list,
            "bags": bags,
            "timeline": timeline,
            "emitted": set(),
            "loaded_bags": set(),
            "delivered_bags": set(),
            "slot_released": False,
        }

    async def _tick_flights(self, now_sim: datetime) -> None:
        """Advance each active flight and emit lifecycle events as their
        simulated timestamps are reached.

        High level phases handled:
        - Passenger check-in and baggage screening
        - Baggage loading prior to departure
        - Flight closed (check-in closed), departed, arrived
        - Unloading, customs screening/withholding, and belt delivery

        When all deliverable bags for a flight have been delivered or appropriately
        withheld/rejected, the flight is retired and its slot may be released.
        """
        done_ids: List[str] = []
        for fid, st in list(self._active_flights.items()):
            f: Flight = st["flight"]
            tl = st["timeline"]
            emitted: set[str] = st["emitted"]

            # 1) Check-in window
            if tl["checkin_start"] <= now_sim <= tl["checkin_end"]:
                await self._emit_checkin_phase(st, now_sim)

            # 2) Baggage loading — continue after the load window opens until the flight actually departs
            # This avoids stalls when the simulated clock passes the (sliding) depart time and we delay departure.
            if now_sim >= tl["load_start"] and "flight_departed" not in emitted:
                await self._emit_loading_phase(st, now_sim)

            # Flight closed (check-in ended) — only when all pax are checked in, otherwise extend window and downstream milestones
            if "flight_closed" not in emitted and now_sim >= tl["checkin_end"]:
                pax_total = len(st.get("passengers", []))
                pax_checked = sum(1 for p in st.get("passengers", []) if getattr(p, "checked_in", False))
                if pax_checked < pax_total:
                    # Extend timeline to allow remaining check-ins
                    delay = timedelta(minutes=10)
                    tl["checkin_end"] += delay
                    tl["load_start"] += delay
                    tl["depart"] += delay
                    tl["arrive"] += delay
                    self._log(f"⏰ Extended check-in for {f.flight_number}: {pax_checked}/{pax_total} checked in", style="yellow")
                else:
                    # Note check-in closed time in SQL
                    self._update_flight_markers(f.flight_id, closed_utc=now_sim)
                    await self._send(
                        f"{f.origin}",
                        cloudevent(
                            "Airport.Flight.Closed",
                            f"{f.origin}",
                            subject=f"flight/{f.flight_number}",
                            data={
                                "flightId": f.flight_id,
                                "flightNumber": f.flight_number,
                                "airline": f.airline,
                                "origin": f.origin,
                                "destination": f.destination,
                                "departureUtc": f.departure_utc.isoformat(),
                                "arrivalUtc": f.arrival_utc.isoformat(),
                            },
                            sim_time=now_sim,
                        ),
                    )
                    emitted.add("flight_closed")

            # Flight departed — only when all pax are checked in and all required bags are loaded; otherwise delay timeline
            if "flight_departed" not in emitted and now_sim >= tl["depart"]:
                pax_total = len(st.get("passengers", []))
                pax_checked = sum(1 for p in st.get("passengers", []) if getattr(p, "checked_in", False))
                bags: Dict[str, Bag] = st.get("bags", {})
                # Bags that must be loaded (checked-in and not rejected/lost)
                must_load = [b for b in bags.values() if b.status == "checked_in"]
                loaded_bags = st.get("loaded_bags", set())
                loaded_count = len([b for b in bags.values() if b.status == "loaded"])

                if pax_checked < pax_total:
                    # Still waiting for passengers — push depart ahead of current time to avoid stall
                    delay = timedelta(minutes=10)
                    old_depart = tl["depart"]
                    new_depart = max(old_depart + delay, now_sim + timedelta(minutes=5))
                    delta = new_depart - old_depart
                    tl["depart"] = new_depart
                    tl["arrive"] += delta
                    # Keep load_start/check-in offsets consistent with new depart (if not yet closed)
                    self._align_timeline_from_depart(st, now_sim)
                    self._log(
                        f"⏰ Delaying departure of {f.flight_number}: waiting for {pax_total - pax_checked} passengers",
                        style="yellow",
                    )
                elif len(must_load) > 0:
                    # Still have bags to load — push depart ahead so loading window resumes
                    delay = timedelta(minutes=5)
                    old_depart = tl["depart"]
                    new_depart = max(old_depart + delay, now_sim + timedelta(minutes=5))
                    delta = new_depart - old_depart
                    tl["depart"] = new_depart
                    tl["arrive"] += delta
                    self._align_timeline_from_depart(st, now_sim)
                    self._log(
                        f"⏰ Delaying departure of {f.flight_number}: {len(must_load)} bags still to load",
                        style="yellow",
                    )
                else:
                    # Update SQL with actual departure time
                    self._update_flight_actual_time(f.flight_id, actual_departure=now_sim)
                    await self._send(
                        f"{f.origin}",
                        cloudevent(
                            "Airport.Flight.Departed",
                            f"{f.origin}",
                            subject=f"flight/{f.flight_number}",
                            data={
                                "flightId": f.flight_id,
                                "flightNumber": f.flight_number,
                                "airline": f.airline,
                                "origin": f.origin,
                                "destination": f.destination,
                                "departureUtc": f.departure_utc.isoformat(),
                                "arrivalUtc": f.arrival_utc.isoformat(),
                            },
                            sim_time=now_sim,
                        ),
                    )
                    emitted.add("flight_departed")

            # 3) After arrival: unload, customs, belt delivery
            if now_sim >= tl["arrive"]:
                # Flight arrived
                if "flight_arrived" not in emitted:
                    # Update SQL with actual arrival time
                    self._update_flight_actual_time(f.flight_id, actual_arrival=now_sim)
                    await self._send(
                        f"{f.destination}",
                        cloudevent(
                            "Airport.Flight.Arrived",
                            f"{f.destination}",
                            subject=f"flight/{f.flight_number}",
                            data={
                                "flightId": f.flight_id,
                                "flightNumber": f.flight_number,
                                "airline": f.airline,
                                "origin": f.origin,
                                "destination": f.destination,
                                "departureUtc": f.departure_utc.isoformat(),
                                "arrivalUtc": f.arrival_utc.isoformat(),
                            },
                            sim_time=now_sim,
                        ),
                    )
                    emitted.add("flight_arrived")
                await self._emit_arrival_phase(st, now_sim)
                # If all bags delivered or withheld/not collected, we can retire the flight
                bags: Dict[str, Bag] = st["bags"]
                # Consider a bag lifecycle complete if it's delivered or withheld.
                total_non_excluded = sum(1 for b in bags.values() if b.status not in {"lost", "rejected"})
                processed_non_excluded = sum(1 for b in bags.values() if b.status in {"delivered", "withheld"})
                if processed_non_excluded >= total_non_excluded:
                    done_ids.append(fid)

        for fid in done_ids:
            self._active_flights.pop(fid, None)

    def _align_timeline_from_depart(self, st: Dict[str, Any], now_sim: Optional[datetime] = None) -> None:
        """Recompute timeline checkpoints from current departure time.

        Rules for normal operation:
        - load_start = max(now - 15m, depart - 30m) so the window always includes "now"
        - checkin_end = depart - 60m (only if flight not yet closed)
        - checkin_start = depart - 3h (only if flight not yet closed)
        
        Rules for clock-speed=1:
        - load_start = max(now - 15m, depart - 5m) 
        - checkin_end = depart - 10m (only if flight not yet closed)
        - checkin_start stays at original immediate time (only if flight not yet closed)
        
        Arrival stays as set by upstream logic.
        """
        tl = st.get("timeline", {})
        emitted: set[str] = st.get("emitted", set())
        depart: datetime = tl.get("depart")
        if not isinstance(depart, datetime):
            return
        # Always align load start relative to depart and ensure it includes current time window
        if now_sim is None:
            now_sim = self.now()
            
        if self.cfg.clock_speed == 1:
            # Shorter windows for real-time simulation
            tl["load_start"] = max(depart - timedelta(minutes=5), now_sim - timedelta(minutes=15))
            # Only shift check-in times if not yet closed
            if "flight_closed" not in emitted:
                tl["checkin_end"] = depart - timedelta(minutes=10)
                # Don't change checkin_start for clock_speed=1 - keep original immediate start
        else:
            # Normal operation
            tl["load_start"] = max(depart - timedelta(minutes=30), now_sim - timedelta(minutes=15))
            # Only shift check-in times if not yet closed
            if "flight_closed" not in emitted:
                tl["checkin_end"] = depart - timedelta(minutes=60)
                tl["checkin_start"] = depart - timedelta(hours=3)

    def _update_flight_actual_time(self, flight_id: str, actual_departure: Optional[datetime] = None, actual_arrival: Optional[datetime] = None) -> None:
        """Update the SQL row for the given flight with actual departure/arrival if provided."""
        if self.cfg.dry_run or not self._conn:
            parts = []
            if actual_departure:
                parts.append(f"ActualDepartureUtc={actual_departure.isoformat()}Z")
            if actual_arrival:
                parts.append(f"ActualArrivalUtc={actual_arrival.isoformat()}Z")
            if parts:
                self._log(f"✈️ [dry-run] Update flight {flight_id} set " + ", ".join(parts), style="yellow")
            return
        try:
            cur = self._conn.cursor()
            if actual_departure and actual_arrival:
                cur.execute(
                    f"UPDATE {self.cfg.sql_table} SET ActualDepartureUtc=?, ActualArrivalUtc=? WHERE FlightId=?",
                    (actual_departure, actual_arrival, flight_id),
                )
            elif actual_departure:
                cur.execute(
                    f"UPDATE {self.cfg.sql_table} SET ActualDepartureUtc=? WHERE FlightId=?",
                    (actual_departure, flight_id),
                )
            elif actual_arrival:
                cur.execute(
                    f"UPDATE {self.cfg.sql_table} SET ActualArrivalUtc=? WHERE FlightId=?",
                    (actual_arrival, flight_id),
                )
        except Exception as ex:
            # Log but don't crash the sim
            self._log(f"SQL update failed for flight {flight_id}: {ex}", style="red")

    def _update_flight_markers(self, flight_id: str, closed_utc: Optional[datetime] = None, completed_utc: Optional[datetime] = None) -> None:
        """Update CheckinClosedUtc and/or CompletedUtc for a flight."""
        if self.cfg.dry_run or not self._conn:
            parts: List[str] = []
            if closed_utc:
                parts.append(f"CheckinClosedUtc={closed_utc.isoformat()}Z")
            if completed_utc:
                parts.append(f"CompletedUtc={completed_utc.isoformat()}Z")
            if parts:
                self._log(f"✈️ [dry-run] Update flight {flight_id} set " + ", ".join(parts), style="yellow")
            return
        try:
            cur = self._conn.cursor()
            if closed_utc and completed_utc:
                cur.execute(
                    f"UPDATE {self.cfg.sql_table} SET CheckinClosedUtc=?, CompletedUtc=? WHERE FlightId=?",
                    (closed_utc, completed_utc, flight_id),
                )
            elif closed_utc:
                cur.execute(
                    f"UPDATE {self.cfg.sql_table} SET CheckinClosedUtc=? WHERE FlightId=?",
                    (closed_utc, flight_id),
                )
            elif completed_utc:
                cur.execute(
                    f"UPDATE {self.cfg.sql_table} SET CompletedUtc=? WHERE FlightId=?",
                    (completed_utc, flight_id),
                )
        except Exception as ex:
            self._log(f"SQL update failed for flight {flight_id}: {ex}", style="red")

    async def _emit_checkin_phase(self, st: Dict[str, Any], now_sim: datetime) -> None:
        f: Flight = st["flight"]
        pax: List[Passenger] = st["passengers"]
        bags: Dict[str, Bag] = st["bags"]
        emitted: set[str] = st["emitted"]

        # Capacity-aware, evenly distributed check-ins across the check-in window.
        start = st["timeline"]["checkin_start"]
        end = st["timeline"]["checkin_end"]
        total_window_secs = max(1.0, (end - start).total_seconds())
        elapsed_secs = max(0.0, (now_sim - start).total_seconds())
        progress = max(0.0, min(1.0, elapsed_secs / total_window_secs))

        # Use actual passenger count, not aircraft capacity
        target_population = len(pax)
        # Smooth target number checked-in by now (cumulative), round up to ensure completion
        ideal_checked_in_by_now = min(target_population, int(math.ceil(progress * target_population)))
        checked_in_count = sum(1 for p in pax if p.checked_in)
        deficit = max(0, ideal_checked_in_by_now - checked_in_count)

        if deficit <= 0:
            return

        # Perform check-ins to catch up to the ideal cumulative target this tick.
        remaining: List[Passenger] = [p for p in pax if not p.checked_in]
        # Increase batch size to ensure we can check in everyone in time
        # Use a more aggressive rate: at least 5% of remaining per tick
        batch = min(deficit, max(5, len(remaining) // 20, target_population // 36))  # 36 ticks over 3 hours at 0.3s/tick
        for p in self._rng.sample(remaining, k=min(batch, len(remaining))):
            p.checked_in = True
            await self._send(
                f"{f.origin}",
                cloudevent(
                    "Airport.Passenger.Checkin",
                    f"{f.origin}",
                    subject=f"flight/{f.flight_number}/pax/{p.pax_id}",
                    data={
                        "flightId": f.flight_id,
                        "flightNumber": f.flight_number,
                        "airline": f.airline,
                        "origin": f.origin,
                        "destination": f.destination,
                        "departureUtc": f.departure_utc.isoformat(),
                        "paxId": p.pax_id,
                        "name": p.name,
                    },
                    sim_time=now_sim,
                ),
            )
            for b in p.bags:
                # screening always after checkin
                await self._send(
                    f"{f.origin}",
                    cloudevent(
                        "Airport.Baggage.Checkin",
                        f"{f.origin}",
                        subject=f"flight/{f.flight_number}/bag/{b.bag_id}",
                        data={
                            "flightId": f.flight_id,
                            "flightNumber": f.flight_number,
                            "airline": f.airline,
                            "origin": f.origin,
                            "destination": f.destination,
                            "departureUtc": f.departure_utc.isoformat(),
                            "bagId": b.bag_id,
                            "paxId": p.pax_id,
                            "weightKg": round(b.weight_kg, 2),
                        },
                        sim_time=now_sim,
                    ),
                )
                b.status = "checked_in"

                # screening
                await self._send(
                    f"{f.origin}",
                    cloudevent(
                        "Airport.Baggage.Screened",
                        f"{f.origin}",
                        subject=f"flight/{f.flight_number}/bag/{b.bag_id}",
                        data={"flightId": f.flight_id, "bagId": b.bag_id},
                        sim_time=now_sim,
                    ),
                )
                if self._rng.random() < self.cfg.inspect_rate:
                    await self._send(
                        f"{f.origin}",
                        cloudevent(
                            "Airport.Baggage.Inspected",
                            f"{f.origin}",
                            subject=f"flight/{f.flight_number}/bag/{b.bag_id}",
                            data={"flightId": f.flight_id, "bagId": b.bag_id},
                            sim_time=now_sim,
                        ),
                    )
                if self._rng.random() < self.cfg.reject_rate:
                    b.status = "rejected"
                    await self._send(
                        f"{f.origin}",
                        cloudevent(
                            "Airport.Baggage.Rejected",
                            f"{f.origin}",
                            subject=f"flight/{f.flight_number}/bag/{b.bag_id}",
                            data={"flightId": f.flight_id, "bagId": b.bag_id},
                            sim_time=now_sim,
                        ),
                    )

    async def _emit_loading_phase(self, st: Dict[str, Any], now_sim: datetime) -> None:
        f: Flight = st["flight"]
        bags: Dict[str, Bag] = st["bags"]
        loaded: set[str] = st["loaded_bags"]
        # Dynamically load eligible bags over the load window; catch up if delayed.
        load_start = st["timeline"]["load_start"]
        # Continue loading as long as the load window has opened and the flight has not actually departed.
        # This avoids stalls when the accelerated clock skips past the (sliding) depart timestamp.
        if now_sim < load_start or ("flight_departed" in st.get("emitted", set())):
            return
        # Eligible backlog = checked-in (i.e., post-screening) bags not lost/rejected and not yet loaded
        candidates = [b for b in bags.values() if b.status == "checked_in" and b.bag_id not in loaded]
        backlog = len(candidates)
        if backlog <= 0:
            return
        # Compute a dynamic per-tick rate based on remaining time and backlog
        depart = st["timeline"]["depart"]
        minutes_left = max(0.1, (depart - now_sim).total_seconds() / 60.0)
        # Base evenly-distributed rate across remaining minutes (aim to finish well before depart)
        base_rate = max(1, math.ceil(backlog / max(10.0, minutes_left)))
        # Boost when within 10 minutes of departure
        if minutes_left <= 10.0:
            base_rate *= 3
        # Also ensure a reasonable floor to avoid tiny trickle rates
        base_rate = max(base_rate, max(5, backlog // 8))
        take = min(backlog, base_rate)
        for b in self._rng.sample(candidates, k=take):
            if self._rng.random() < self.cfg.loss_rate:
                b.status = "lost"
                await self._send(
                    f"{f.origin}",
                    cloudevent(
                        "Airport.Baggage.Lost",
                        f"{f.origin}",
                        subject=f"flight/{f.flight_number}/bag/{b.bag_id}",
                        data={"flightId": f.flight_id, "bagId": b.bag_id},
                        sim_time=now_sim,
                    ),
                )
                continue
            await self._send(
                f"{f.origin}",
                cloudevent(
                    "Airport.Baggage.Loaded",
                    f"{f.origin}",
                    subject=f"flight/{f.flight_number}/bag/{b.bag_id}",
                    data={"flightId": f.flight_id, "bagId": b.bag_id},
                    sim_time=now_sim,
                ),
            )
            loaded.add(b.bag_id)
            b.status = "loaded"

    async def _emit_arrival_phase(self, st: Dict[str, Any], now_sim: datetime) -> None:
        f: Flight = st["flight"]
        bags: Dict[str, Bag] = st["bags"]
        delivered: set[str] = st["delivered_bags"]
        loaded_set: set[str] = st["loaded_bags"]
        # unload loaded bags first
        to_unload = [b for b in bags.values() if b.status == "loaded"]
        if to_unload:
            # More aggressive unloading
            chunk = min(len(to_unload), max(10, len(to_unload)//3))
            for b in self._rng.sample(to_unload, k=min(chunk, len(to_unload))):
                await self._send(
                    f"{f.destination}",
                    cloudevent(
                        "Airport.Baggage.Unloaded",
                        f"{f.destination}",
                        subject=f"flight/{f.flight_number}/bag/{b.bag_id}",
                        data={"flightId": f.flight_id, "bagId": b.bag_id},
                        sim_time=now_sim,
                    ),
                )
                b.status = "unloaded"
        # if all previously loaded bags have been unloaded, release the slot for scheduling
        if not st.get("slot_released", False):
            all_unloaded = all(bags.get(bid, Bag("", "", 0.0, "")).status != "loaded" for bid in loaded_set) if loaded_set else True
            if all_unloaded and now_sim >= st["timeline"]["arrive"]:
                st["slot_released"] = True
                # Record completed (all bags unloaded)
                self._update_flight_markers(f.flight_id, completed_utc=now_sim)
                self._log(f"✈️ Slot released after unload for flight {f.flight_number}", style="dim")
        # customs
        customs_ready = [b for b in bags.values() if b.status == "unloaded"]
        if customs_ready:
            # More aggressive processing
            chunk = min(len(customs_ready), max(10, len(customs_ready)//3))
            for b in self._rng.sample(customs_ready, k=min(chunk, len(customs_ready))):
                if self._rng.random() < 0.03:
                    await self._send(
                        f"{f.destination}",
                        cloudevent(
                            "Airport.Baggage.CustomsWithheld",
                            f"{f.destination}",
                            subject=f"flight/{f.flight_number}/bag/{b.bag_id}",
                            data={"flightId": f.flight_id, "bagId": b.bag_id},
                            sim_time=now_sim,
                        ),
                    )
                    b.status = "withheld"
                else:
                    await self._send(
                        f"{f.destination}",
                        cloudevent(
                            "Airport.Baggage.Customs.Screened",
                            f"{f.destination}",
                            subject=f"flight/{f.flight_number}/bag/{b.bag_id}",
                            data={"flightId": f.flight_id, "bagId": b.bag_id},
                            sim_time=now_sim,
                        ),
                    )
                    b.status = "cleared"
        # belt delivery
        ready = [b for b in bags.values() if b.status == "cleared"]
        if ready:
            # More aggressive delivery
            chunk = min(len(ready), max(10, len(ready)//3))
            for b in self._rng.sample(ready, k=min(chunk, len(ready))):
                await self._send(
                    f"{f.destination}",
                    cloudevent(
                        "Airport.Baggage.DeliveredOnBelt",
                        f"{f.destination}",
                        subject=f"flight/{f.flight_number}/bag/{b.bag_id}",
                        data={"flightId": f.flight_id, "bagId": b.bag_id},
                        sim_time=now_sim,
                    ),
                )
                b.status = "delivered"
                delivered.add(b.bag_id)
                if self._rng.random() < self.cfg.not_collected_rate:
                    await self._send(
                        f"{f.destination}",
                        cloudevent(
                            "Airport.Baggage.NotCollected",
                            f"{f.destination}",
                            subject=f"flight/{f.flight_number}/bag/{b.bag_id}",
                            data={"flightId": f.flight_id, "bagId": b.bag_id},
                            sim_time=now_sim,
                        ),
                    )

    async def _send(self, source: str, evt: Any) -> None:
        """Send a CloudEvent to Event Hubs (or print in dry-run).

        Two modes are supported via `cfg.ce_mode`:
        - 'structured' (default): the entire CloudEvent JSON envelope (per the
          CloudEvents HTTP/JSON structured representation) is used as the Event
          Hub message body.
        - 'binary': the message body contains only the event `data` serialized
          as JSON, while the CloudEvent attributes are attached as EventData
          application properties prefixed with `cloudEvents:`. This follows the
          conceptual CloudEvents binary mapping when delivering events over a
          message broker that supports application properties.

        Note: This does not perform Avro binary serialization. If Avro binary
        transport is required, the payload must be serialized using an Avro
        encoder and the appropriate content-type set.
        """
        # Dry-run: optionally print event and skip sending
        if self.cfg.dry_run:
            event_type = ""
            subject: Optional[str] = None
            payload: Dict[str, Any] = {}
            if isinstance(evt, CloudEvent):
                payload = evt.data if isinstance(evt.data, dict) else {}
                try:
                    event_type = str(evt.get("type") or evt["type"])  # safe access
                except Exception:
                    event_type = ""
                try:
                    subject = evt.get("subject") if hasattr(evt, "get") else None
                except Exception:
                    subject = None
            elif isinstance(evt, dict):
                payload = (evt.get("data") if isinstance(evt, dict) else {}) or {}
                event_type = str(evt.get("type") or "")
                subject = evt.get("subject")
            if getattr(self.cfg, "verbose", False):
                emoji = self._emoji_for_event(event_type)
                mode = getattr(self.cfg, "ce_mode", "structured")
                self._log(
                    f"{emoji} [dry-run] mode={mode} {event_type} {subject or ''} flight={payload.get('flightId','')}",
                    style="yellow",
                )
            return

        # Ensure producer available
        assert self._producer
        mode = getattr(self.cfg, "ce_mode", "structured")

        # Normalize event type / subject / payload and collect CloudEvent attrs
        event_type = ""
        subject: Optional[str] = None
        payload: Dict[str, Any] = {}
        attrs: Dict[str, Any] = {}

        if isinstance(evt, CloudEvent):
            payload = evt.data if isinstance(evt.data, dict) else {}
            try:
                event_type = str(evt.get("type") or evt["type"])
            except Exception:
                event_type = ""
            try:
                subject = evt.get("subject") if hasattr(evt, "get") else None
            except Exception:
                subject = None
            # collect well-known CloudEvent attributes
            for k in ("id", "type", "source", "subject", "time", "datacontenttype"):
                try:
                    v = evt.get(k) if hasattr(evt, "get") else evt[k]
                except Exception:
                    v = None
                if v is not None:
                    attrs[k] = v
        elif isinstance(evt, dict):
            payload = (evt.get("data") if isinstance(evt, dict) else {}) or {}
            event_type = str(evt.get("type") or "")
            subject = evt.get("subject")
            for k in ("id", "type", "source", "subject", "time", "datacontenttype"):
                if k in evt:
                    attrs[k] = evt.get(k)
        else:
            # fallback: try to encode as-is
            try:
                payload = evt if isinstance(evt, dict) else {}
            except Exception:
                payload = {}

        # Validate payload shape if schema known
        try:
            if event_type:
                self._validate_payload_against_xreg(event_type, payload)
        except Exception:
            pass

        # Lazily import EventData
        from azure.eventhub import EventData  # type: ignore

        if mode == "structured":
            # structured mode: send full CloudEvent JSON envelope as body
            if isinstance(evt, CloudEvent):
                body = cloudevent_to_json(evt)
                if isinstance(body, str):
                    body = body.encode("utf-8")
            else:
                body = json.dumps(evt, ensure_ascii=False).encode("utf-8")
            data = EventData(body)
        else:
            # binary mode: body is the event data (JSON) and CloudEvent attributes are application properties prefixed with 'cloudEvents:'
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            data = EventData(body)
            props: Dict[str, Any] = {}
            for ak, av in attrs.items():
                props[f"cloudEvents:{ak}"] = str(av)
            if "type" not in attrs and event_type:
                props["cloudEvents:type"] = event_type
            if "subject" not in attrs and subject:
                props["cloudEvents:subject"] = subject
            # attach application properties to EventData (SDK exposes .properties)
            try:
                data.properties = props  # type: ignore[attr-defined]
            except Exception:
                try:
                    setattr(data, "properties", props)
                except Exception:
                    # give up silently; best-effort
                    pass

        # Buffer by partition key (flightId preferred) for real-time batching
        pk = str(payload.get("flightId") or subject or "")
        key = pk or "__default__"
        # enqueue
        self._eh_buffers[key].append((pk if pk else None, data, event_type, subject, payload))
        # record first-seen time
        self._eh_first_seen.setdefault(key, time.monotonic())

        # Opportunistically flush expired buffers here as well
        with contextlib.suppress(Exception):
            await self._flush_due_eventhub_buffers()

    def _emoji_for_event(self, event_type: str) -> str:
        t = (event_type or "").lower()
        if t.startswith("airport.passenger."):
            # alternate 👩 and 👨
            return self._person_emoji()
        if t.startswith("airport.flight."):
            return "✈️"
        # security/customs related baggage events -> police
        if "inspected" in t or "rejected" in t or "customs" in t or ("screened" in t and "passenger" not in t):
            return "👮"
        if "baggage" in t:
            return "🧳"
        return "📦"

    def _person_emoji(self) -> str:
        # Alternate male/female per passenger event
        self._pax_gender_toggle = not self._pax_gender_toggle
        return "👩" if self._pax_gender_toggle else "👨"

    def _log(self, message: str, style: Optional[str] = None) -> None:
        if self._console:
            ts = self.now().isoformat()
            msg = f"[{ts}] {message}"
            if style:
                self._console.print(msg, style=style)
            else:
                self._console.print(msg)

    async def _flush_due_eventhub_buffers(self, force: bool = False) -> None:
        """Flush Event Hubs buffers that have reached the batching window or all when force=True.

        Groups buffered EventData by partition key and sends them using size-aware batches.
        """
        if not self._producer:
            # Nothing to flush in dry-run or when producer not initialised
            self._eh_buffers.clear()
            self._eh_first_seen.clear()
            return
        now_rt = time.monotonic()
        window = max(0.1, float(getattr(self.cfg, "eventhub_batch_window_seconds", 5.0)))
        due_keys: List[str] = []
        for key, first in list(self._eh_first_seen.items()):
            if force or (now_rt - first >= window):
                due_keys.append(key)

        for key in due_keys:
            items = self._eh_buffers.get(key) or []
            if not items:
                # clean up markers
                self._eh_buffers.pop(key, None)
                self._eh_first_seen.pop(key, None)
                continue
            # Partition key (None allowed)
            pk = items[0][0]
            # Send in size-constrained batches
            i = 0
            sent_count = 0
            try:
                while i < len(items):
                    batch = await self._producer.create_batch(partition_key=pk)
                    # pack as many as fit
                    while i < len(items):
                        _, data, _, _, _ = items[i]
                        try:
                            batch.add(data)
                            i += 1
                            sent_count += 1
                        except ValueError:
                            break
                    if len(batch) > 0:  # type: ignore[arg-type]
                        await self._producer.send_batch(batch)
                # Log a single summary line for the batch flush
                # Try to report a representative event
                _, _, evt_type, subj, payload = items[-1]
                if getattr(self.cfg, "verbose", False):
                    emoji = self._emoji_for_event(str(evt_type or ""))
                    self._log(
                        f"{emoji} Sent batch x{sent_count} flight={payload.get('flightId','')} pk={pk or ''}",
                        style="green",
                    )
            except Exception as ex:
                self._log(f"Event Hubs send failed for pk={pk or ''}: {ex}", style="red")
            finally:
                # clear buffers for this key
                self._eh_buffers.pop(key, None)
                self._eh_first_seen.pop(key, None)


# helpers

def _rand_id() -> str:
    return os.urandom(12).hex()


def _rand_pax_id(rng: random.Random) -> str:
    return f"PAX-{rng.randint(100000, 999999)}"


def _rand_bag_id(rng: random.Random) -> str:
    return f"BG{rng.randint(10000000, 99999999)}"


_FIRST_NAMES = [
    "Alex",
    "Sam",
    "Jordan",
    "Taylor",
    "Chris",
    "Pat",
    "Jamie",
    "Robin",
]
_LAST_NAMES = [
    "Miller",
    "Schmidt",
    "Dubois",
    "Garcia",
    "Kowalski",
    "Murphy",
    "Novak",
    "Rossi",
]


def _rand_name(rng: random.Random) -> str:
    return f"{rng.choice(_FIRST_NAMES)} {rng.choice(_LAST_NAMES)}"
