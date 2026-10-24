-- Tables of the order management system (database oms), a source system owned by the
-- Operations department. The generator applies this file before loading; it is safe to re-run.
-- Every timestamp is timestamptz and is written in UTC. updated_ts is what DP1's incremental
-- JDBC pull reads; courier_changes is append-only and is read by changed_ts.

CREATE TABLE IF NOT EXISTS hubs (
    hub_id        smallint PRIMARY KEY,
    name          text NOT NULL UNIQUE,
    city          text NOT NULL,
    area_type     text NOT NULL,
    lat           double precision NOT NULL,
    lon           double precision NOT NULL,
    capacity_tier text NOT NULL,
    created_ts    timestamptz NOT NULL,
    updated_ts    timestamptz NOT NULL
);

-- One courier, current state. Each change overwrites the row and is also appended to courier_changes.
CREATE TABLE IF NOT EXISTS couriers (
    courier_id      integer PRIMARY KEY,
    home_hub_id     smallint NOT NULL REFERENCES hubs,
    vehicle_type    text NOT NULL,
    rating          numeric(2, 1) NOT NULL,
    employment_type text NOT NULL,
    age             smallint NOT NULL,
    hired_ts        timestamptz NOT NULL,
    updated_ts      timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS couriers_updated_ts ON couriers (updated_ts);

-- Audit table: one row per change, holding every tracked attribute after the change. The first
-- row of each courier is its hiring. A rating review that leaves the rating unchanged still
-- writes a row, so the SCD2 dimension must compare attributes, not count rows.
CREATE TABLE IF NOT EXISTS courier_changes (
    change_id       bigint PRIMARY KEY,
    courier_id      integer NOT NULL REFERENCES couriers,
    changed_ts      timestamptz NOT NULL,
    change_reason   text NOT NULL,
    home_hub_id     smallint NOT NULL REFERENCES hubs,
    vehicle_type    text NOT NULL,
    rating          numeric(2, 1) NOT NULL,
    employment_type text NOT NULL
);
CREATE INDEX IF NOT EXISTS courier_changes_changed_ts ON courier_changes (changed_ts);

-- One customer. The home point is where their parcels are dropped.
CREATE TABLE IF NOT EXISTS customers (
    customer_id    bigint PRIMARY KEY,
    service_hub_id smallint NOT NULL REFERENCES hubs,
    home_lat       double precision NOT NULL,
    home_lon       double precision NOT NULL,
    segment        text NOT NULL,
    signup_ts      timestamptz NOT NULL,
    updated_ts     timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS customers_updated_ts ON customers (updated_ts);

-- One parcel order. It copies the seed's legacy layout: the order and pickup times are local LA
-- time typed as text next to a local order_date, and text fields keep the seed's spellings
-- (trailing spaces, "Metropolitian", "NaN" literals). promised_ts and delivered_ts come from the
-- delivery system and are clean. updated_ts is the last lifecycle change (the delivery).
CREATE TABLE IF NOT EXISTS orders (
    order_id     bigint PRIMARY KEY,
    customer_id  bigint NOT NULL REFERENCES customers,
    hub_id       smallint NOT NULL REFERENCES hubs,
    courier_id   integer NOT NULL REFERENCES couriers,
    category     text NOT NULL,
    order_date   date NOT NULL,
    order_time   text NOT NULL,
    pickup_time  text NOT NULL,
    promised_ts  timestamptz NOT NULL,
    delivered_ts timestamptz NOT NULL,
    status       text NOT NULL,
    weather      text NOT NULL,
    traffic      text NOT NULL,
    vehicle      text NOT NULL,
    area         text NOT NULL,
    store_lat    double precision NOT NULL,
    store_lon    double precision NOT NULL,
    drop_lat     double precision NOT NULL,
    drop_lon     double precision NOT NULL,
    updated_ts   timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS orders_updated_ts ON orders (updated_ts);
