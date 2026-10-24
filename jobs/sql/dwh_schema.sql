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
