"""Defines string names and create queries for tables and corresponding dataset file.
"""

AGENCY = "agency"
CALENDAR = "calendar"
CALENDAR_DATES = "calendar_dates"
FREQUENCIES = "frequencies"
ROUTES = "routes"
SERVICE_ALERTS = "service_alerts"
SHAPES = "shapes"
STOP_TIMES = "stop_times"
STOPS = "stops"
TRANSFERS = "transfers"
TRIPS = "trips"

CREATE_AGENCY = (
    f"CREATE table IF NOT EXISTS {AGENCY} ("
    "agency_id int primary key, "
    "agency_name text, "
    "agency_url text, "
    "agency_timezone text, "
    "agency_lang text, "
    "agency_phone text)"
)

CREATE_CALENDAR = (
    f"CREATE table IF NOT EXISTS {CALENDAR} ("
    "service_id int primary key, "
    "monday int, "
    "tuesday int, "
    "wednesday int, "
    "thursday int, "
    "friday int, "
    "saturday int, "
    "sunday int, "
    "start_date timestamp without time zone, "
    "end_date timestamp without time zone) "
)

CREATE_CALENDAR_DATES = (
    f"CREATE table IF NOT EXISTS {CALENDAR_DATES} ("
    "service_id int primary key, "
    "date timestamp without time zone, "
    "exception_type int)"
)

CREATE_FREQUENCIES = (
    f"CREATE table IF NOT EXISTS {FREQUENCIES} ("
    "trip_id int primary key, "
    "start_time timestamp without time zone, "
    "end_time timestamp without time zone, "
    "headway_secs int, "
    "exact_times array(timestamp without time zone))"
)

CREATE_ROUTES = (
    f"CREATE table IF NOT EXISTS {ROUTES} ("
    "route_id text primary key, "
    "agency_id int, "
    "route_short_name text, "
    "route_long_name text, "
    "route_type int, "
    "route_color text, "
    "route_text_color text, "
    "route_desc text)"
)

CREATE_SERVICE_ALERTS = (
    f"CREATE table IF NOT EXISTS {SERVICE_ALERTS} ("
    "gtfs_realtime_version text, "
    "incrementality text, "
    "timestamp int)"
)

CREATE_SHAPES = (
    f"CREATE table IF NOT EXISTS {SHAPES} ("
    "shape_id int, "
    "shape_pt_location geo_point, "
    "shape_pt_sequence int)"
)

# Using arrival and departure time as text as time strings seem to be malformed
# some e.g. 24:06:00, time greater than 24 hours? May be its an interval,
# I am not very sure so using text for now.
CREATE_STOP_TIMES = (
    f"CREATE table IF NOT EXISTS {STOP_TIMES} ("
    "trip_id int, "
    "arrival_time text, "
    "departure_time text, "
    "stop_id text, "
    "stop_sequence int, "
    "pickup_type int, "
    "drop_off_type int, "
    "stop_headsign text)"
)

CREATE_STOPS = (
    f"CREATE table IF NOT EXISTS {STOPS} ("
    "stop_id text primary key, "
    "stop_code text, "
    "stop_name text, "
    "stop_desc text, "
    "stop_location geo_point, "
    "location_type double precision, "
    "parent_station text, "
    "wheelchair_boarding text, "
    "platform_code text, "
    "zone_id text)"
)

CREATE_TRANSFERS = (
    f"CREATE table IF NOT EXISTS {TRANSFERS} ("
    "from_stop_id text, "
    "to_stop_id text, "
    "transfer_type int, "
    "min_transfer_time real, "
    "from_route_id text, "
    "to_route_id text, "
    "from_trip_id int, "
    "to_trip_id int)"
)

CREATE_TRIPS = (
    f"CREATE table IF NOT EXISTS {TRIPS} ("
    "route_id text, "
    "service_id int, "
    "trip_id int, "
    "trip_headsign text, "
    "trip_short_name text, "
    "direction_id int, "
    "block_id int, "
    "shape_id int, "
    "wheelchair_accessible boolean, "
    "bikes_allowed boolean)"
)

TABLES = [
    AGENCY,
    CALENDAR,
    CALENDAR_DATES,
    FREQUENCIES,
    ROUTES,
    SERVICE_ALERTS,
    SHAPES,
    STOP_TIMES,
    STOPS,
    TRANSFERS,
    TRIPS,
]

CREATE_TABLE_QUERIES = {
    AGENCY: CREATE_AGENCY,
    CALENDAR: CREATE_CALENDAR,
    CALENDAR_DATES: CREATE_CALENDAR_DATES,
    FREQUENCIES: CREATE_FREQUENCIES,
    ROUTES: CREATE_ROUTES,
    SERVICE_ALERTS: CREATE_SERVICE_ALERTS,
    SHAPES: CREATE_SHAPES,
    STOP_TIMES: CREATE_STOP_TIMES,
    STOPS: CREATE_STOPS,
    TRANSFERS: CREATE_TRANSFERS,
    TRIPS: CREATE_TRIPS,
}
