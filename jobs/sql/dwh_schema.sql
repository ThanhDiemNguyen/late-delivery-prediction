-- The warehouse (database dwh): gold's star schema for BI tools, with declared primary and
-- foreign keys, plus one flat table. Applied by jobs/dp2_warehouse.py; safe to re-run.
--   dimensions     every row of gold (dim_courier keeps all SCD2 versions)
--   fact_delivery  the last 90 days of the period
--   obt_delivery   the last 30 days, one wide row per order with its dimensions' attributes

CREATE TABLE IF NOT EXISTS dim_date (
    date_key     integer PRIMARY KEY,
    date         date NOT NULL UNIQUE,
    day_of_week  smallint NOT NULL,
    is_weekend   boolean NOT NULL,
    iso_week     smallint NOT NULL,
    month        smallint NOT NULL,
    is_peak_week boolean NOT NULL,
    peak_name    text,
    is_holiday   boolean NOT NULL,
    holiday_name text
);

CREATE TABLE IF NOT EXISTS dim_hub (
    hub_id        smallint PRIMARY KEY,
    name          text NOT NULL,
    city          text NOT NULL,
    area_type     text NOT NULL,
    lat           double precision NOT NULL,
    lon           double precision NOT NULL,
    capacity_tier text NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id bigint PRIMARY KEY,
    hub_id      smallint NOT NULL REFERENCES dim_hub,
    home_lat    double precision NOT NULL,
    home_lon    double precision NOT NULL,
    segment     text NOT NULL,
    signup_ts   timestamptz NOT NULL
);

-- H3 drop zones (resolution 7), from Apache Sedona.
CREATE TABLE IF NOT EXISTS dim_zone (
    zone_id      bigint PRIMARY KEY,
    h3_index     text NOT NULL UNIQUE,
    resolution   smallint NOT NULL,
    center_lat   double precision NOT NULL,
    center_lon   double precision NOT NULL,
    area_km2     double precision NOT NULL,
    boundary_wkt text NOT NULL,
    main_hub_id  smallint NOT NULL REFERENCES dim_hub,
    customers    integer NOT NULL
);

-- SCD type 2: one row per version; valid_to_ts is exclusive and 9999-12-31 while open.
CREATE TABLE IF NOT EXISTS dim_courier (
    courier_sk      bigint PRIMARY KEY,
    courier_id      integer NOT NULL,
    hub_id          smallint NOT NULL REFERENCES dim_hub,
    vehicle_type    text NOT NULL,
    rating          numeric(2, 1) NOT NULL,
    employment_type text NOT NULL,
    valid_from_ts   timestamptz NOT NULL,
    valid_to_ts     timestamptz NOT NULL,
    is_current      boolean NOT NULL,
    CHECK (valid_from_ts < valid_to_ts)
);

CREATE TABLE IF NOT EXISTS fact_delivery (
    order_id     bigint PRIMARY KEY,
    courier_sk   bigint NOT NULL REFERENCES dim_courier,
    hub_id       smallint NOT NULL REFERENCES dim_hub,
    customer_id  bigint NOT NULL REFERENCES dim_customer,
    date_key     integer NOT NULL REFERENCES dim_date,
    order_ts     timestamptz NOT NULL,
    pickup_ts    timestamptz NOT NULL,
    delivered_ts timestamptz NOT NULL,
    promised_ts  timestamptz NOT NULL,
    distance_km  double precision NOT NULL,
    delivery_min double precision NOT NULL,
    promised_min double precision NOT NULL,
    is_late      boolean NOT NULL,
    traffic      text,
    weather      text,
    vehicle      text NOT NULL,
    area         text NOT NULL,
    category     text NOT NULL,
    order_hour   smallint NOT NULL
);

-- One big table: no foreign keys by design; every attribute a report needs is on the row.
CREATE TABLE IF NOT EXISTS obt_delivery (
    order_id             bigint PRIMARY KEY,
    order_date           date NOT NULL,
    order_hour           smallint NOT NULL,
    day_of_week          smallint NOT NULL,
    is_weekend           boolean NOT NULL,
    is_peak_week         boolean NOT NULL,
    is_holiday           boolean NOT NULL,
    hub_id               smallint NOT NULL,
    hub_name             text NOT NULL,
    hub_area_type        text NOT NULL,
    hub_capacity_tier    text NOT NULL,
    courier_id           integer NOT NULL,
    courier_vehicle_type text NOT NULL,
    courier_rating       numeric(2, 1) NOT NULL,
    courier_employment   text NOT NULL,
    customer_id          bigint NOT NULL,
    customer_segment     text NOT NULL,
    traffic              text,
    weather              text,
    category             text NOT NULL,
    distance_km          double precision NOT NULL,
    delivery_min         double precision NOT NULL,
    promised_min         double precision NOT NULL,
    is_late              boolean NOT NULL,
    order_ts             timestamptz NOT NULL,
    pickup_ts            timestamptz NOT NULL,
    delivered_ts         timestamptz NOT NULL,
    promised_ts          timestamptz NOT NULL
);

-- Added with the H3 zones: the drop zone of each delivery. Idempotent, for warehouses created before.
ALTER TABLE fact_delivery ADD COLUMN IF NOT EXISTS drop_zone_id bigint;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fact_delivery_drop_zone_id_fkey') THEN
        ALTER TABLE fact_delivery ADD CONSTRAINT fact_delivery_drop_zone_id_fkey FOREIGN KEY (drop_zone_id) REFERENCES dim_zone;
    END IF;
END $$;

-- Indexes for the analysts' queries on obt_delivery; each is measured with EXPLAIN (ANALYZE, BUFFERS)
-- in docs/storage_optimization.md. The load fills obt_delivery in order_ts order, so the BRIN index
-- can skip every block range outside a time window.
CREATE INDEX IF NOT EXISTS obt_delivery_hub_promised ON obt_delivery (hub_id, promised_ts);
CREATE INDEX IF NOT EXISTS obt_delivery_late_hub_delivered ON obt_delivery (hub_id, delivered_ts) WHERE is_late;
CREATE INDEX IF NOT EXISTS obt_delivery_order_ts_brin ON obt_delivery USING brin (order_ts);
