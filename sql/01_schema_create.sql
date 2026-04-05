-- ============================================================
--  PatelDelivers BV — Star Schema DDL
--  Module 2: Data Engineering Foundation
--  Dialect: PostgreSQL (compatible with Redshift / Azure Synapse
--           with minor type adjustments noted inline)
--  Author : Harshil Patel
--  Date   : 2025
-- ============================================================
--
--  SCHEMA OVERVIEW
--  ───────────────
--  FACT TABLE
--    fact_deliveries          Core shipment transactions (24,923 rows)
--
--  DIMENSION TABLES
--    dim_date                 Calendar dimension (auto-populated)
--    dim_postcode             PC4 zone demographics & geography
--    dim_driver               Driver profiles & performance benchmarks
--    dim_warehouse            Warehouse master data
--    dim_product_category     Product category reference
--
--  AGGREGATE / MART TABLES
--    mart_monthly_kpi         Pre-aggregated monthly KPIs per city
--    mart_daily_forecast      Demand forecasts + actuals per city
--    mart_inventory_snapshot  Current warehouse stock levels
-- ============================================================


-- ────────────────────────────────────────────────────────────
-- 0. SETUP
-- ────────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS pateldelivers;
SET search_path TO pateldelivers;


-- ────────────────────────────────────────────────────────────
-- 1. DIMENSION: dim_date
--    Populated via stored procedure (see 03_stored_procedures.sql)
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_date (
    date_key            INTEGER         PRIMARY KEY,   -- YYYYMMDD integer key e.g. 20230427
    full_date           DATE            NOT NULL UNIQUE,
    year                SMALLINT        NOT NULL,
    quarter             SMALLINT        NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month_number        SMALLINT        NOT NULL CHECK (month_number BETWEEN 1 AND 12),
    month_name          VARCHAR(10)     NOT NULL,
    week_number         SMALLINT        NOT NULL,
    day_of_week_number  SMALLINT        NOT NULL CHECK (day_of_week_number BETWEEN 1 AND 7),
    day_of_week_name    VARCHAR(10)     NOT NULL,
    day_of_month        SMALLINT        NOT NULL,
    is_weekend          BOOLEAN         NOT NULL DEFAULT FALSE,
    is_public_holiday   BOOLEAN         NOT NULL DEFAULT FALSE,
    holiday_name        VARCHAR(60),
    is_peak_period      BOOLEAN         NOT NULL DEFAULT FALSE,
    peak_event_name     VARCHAR(40),
    year_month          CHAR(7),                      -- e.g. '2023-04'
    year_quarter        CHAR(7)                       -- e.g. '2023-Q2'
);

COMMENT ON TABLE dim_date IS
    'Calendar dimension covering 2022-01-01 to 2025-03-31. '
    'Includes Dutch public holidays and NL e-commerce peak events.';

CREATE INDEX idx_dim_date_year_month  ON dim_date (year, month_number);
CREATE INDEX idx_dim_date_full_date   ON dim_date (full_date);
CREATE INDEX idx_dim_date_year_month_str ON dim_date (year_month);


-- ────────────────────────────────────────────────────────────
-- 2. DIMENSION: dim_postcode
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_postcode (
    postcode_pc4                SMALLINT        PRIMARY KEY,  -- e.g. 1011
    city                        VARCHAR(30)     NOT NULL,
    district                    VARCHAR(60),
    latitude                    NUMERIC(7, 4)   NOT NULL,
    longitude                   NUMERIC(7, 4)   NOT NULL,
    population_est              INTEGER,
    avg_income_eur              INTEGER,
    housing_density             VARCHAR(15)     CHECK (housing_density IN
                                    ('Very High','High','Medium','Low')),
    pct_apartments              NUMERIC(5, 1),
    avg_parcel_per_wk           NUMERIC(4, 2),
    historical_fail_rate_pct    NUMERIC(5, 2),
    parcel_locker_nearby        BOOLEAN,
    zone_type                   VARCHAR(15)     CHECK (zone_type IN
                                    ('Residential','Commercial','Mixed')),
    -- Derived risk tier for Power BI slicers
    fail_risk_tier              VARCHAR(10)     GENERATED ALWAYS AS (
        CASE
            WHEN historical_fail_rate_pct >= 20 THEN 'High'
            WHEN historical_fail_rate_pct >= 12 THEN 'Medium'
            ELSE 'Low'
        END
    ) STORED,
    created_at                  TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at                  TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dim_postcode IS
    'PC4-level postcode zones for all 5 NL cities. '
    'Source: CBS StatLine 83765NED + BAG geodata. License: CC BY 4.0.';

COMMENT ON COLUMN dim_postcode.fail_risk_tier IS
    'Derived tier based on historical_fail_rate_pct. '
    'High >= 20%, Medium >= 12%, Low < 12%.';

CREATE INDEX idx_dim_postcode_city ON dim_postcode (city);
CREATE INDEX idx_dim_postcode_zone ON dim_postcode (zone_type);


-- ────────────────────────────────────────────────────────────
-- 3. DIMENSION: dim_driver
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_driver (
    driver_id               VARCHAR(10)     PRIMARY KEY,   -- e.g. DRV-0001
    driver_name             VARCHAR(80)     NOT NULL,
    city                    VARCHAR(30)     NOT NULL,
    years_experience        SMALLINT        NOT NULL CHECK (years_experience >= 0),
    vehicle_type            VARCHAR(20)     NOT NULL
                                CHECK (vehicle_type IN
                                    ('Cargo Bike','Electric Van','Diesel Van','Cargo Van')),
    contract_type           VARCHAR(15)     NOT NULL
                                CHECK (contract_type IN ('Fulltime','Parttime','Freelance')),
    avg_deliveries_day      SMALLINT,
    avg_rating              NUMERIC(3, 2)   CHECK (avg_rating BETWEEN 1.0 AND 5.0),
    on_time_rate_pct        NUMERIC(5, 2),
    fail_rate_pct           NUMERIC(5, 2),
    is_active               BOOLEAN         NOT NULL DEFAULT TRUE,
    join_date               DATE,
    -- Derived performance band for dashboards
    performance_band        VARCHAR(15)     GENERATED ALWAYS AS (
        CASE
            WHEN on_time_rate_pct >= 95 AND fail_rate_pct <= 8  THEN 'Elite'
            WHEN on_time_rate_pct >= 85 AND fail_rate_pct <= 15 THEN 'Standard'
            WHEN on_time_rate_pct >= 70                         THEN 'Developing'
            ELSE 'Under Review'
        END
    ) STORED,
    -- Is this driver zero-emission?
    is_zero_emission        BOOLEAN         GENERATED ALWAYS AS (
        vehicle_type IN ('Cargo Bike','Electric Van')
    ) STORED,
    created_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dim_driver IS
    'Driver master data for 80 SwiftRoute drivers across 5 NL cities. '
    'Benchmarked against Dutch logistics industry norms.';

CREATE INDEX idx_dim_driver_city       ON dim_driver (city);
CREATE INDEX idx_dim_driver_vehicle    ON dim_driver (vehicle_type);
CREATE INDEX idx_dim_driver_active     ON dim_driver (is_active);
CREATE INDEX idx_dim_driver_band       ON dim_driver (performance_band);


-- ────────────────────────────────────────────────────────────
-- 4. DIMENSION: dim_warehouse
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_warehouse (
    warehouse_id        VARCHAR(12)     PRIMARY KEY,   -- e.g. WH-AMS-01
    city                VARCHAR(30)     NOT NULL,
    latitude            NUMERIC(7, 4)   NOT NULL,
    longitude           NUMERIC(7, 4)   NOT NULL,
    max_capacity_units  INTEGER         NOT NULL,
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO dim_warehouse (warehouse_id, city, latitude, longitude, max_capacity_units)
VALUES
    ('WH-AMS-01', 'Amsterdam',  52.3123, 4.9432, 15000),
    ('WH-RTD-01', 'Rotterdam',  51.8872, 4.5292, 12000),
    ('WH-UTR-01', 'Utrecht',    52.0642, 5.0847,  8000),
    ('WH-DHG-01', 'Den Haag',   52.0417, 4.3614,  9000),
    ('WH-EHV-01', 'Eindhoven',  51.4091, 5.5019,  5000)
ON CONFLICT (warehouse_id) DO NOTHING;


-- ────────────────────────────────────────────────────────────
-- 5. DIMENSION: dim_product_category
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_product_category (
    category_id         SMALLSERIAL     PRIMARY KEY,
    category_name       VARCHAR(30)     NOT NULL UNIQUE,
    avg_order_value_eur NUMERIC(8, 2),
    avg_weight_kg       NUMERIC(5, 2),
    base_fail_rate_pct  NUMERIC(5, 2),
    -- CBS commodity group for linking to official stats
    cbs_commodity_group VARCHAR(40)
);

INSERT INTO dim_product_category
    (category_name, avg_order_value_eur, avg_weight_kg, base_fail_rate_pct, cbs_commodity_group)
VALUES
    ('Electronics',     285.00, 1.20, 10.0, 'Machinery and transport equipment'),
    ('Fashion',          65.00, 0.40, 14.0, 'Miscellaneous manufactured articles'),
    ('Home & Garden',    95.00, 3.10, 16.0, 'Manufactured goods'),
    ('Books & Media',    22.00, 0.60,  8.0, 'Miscellaneous manufactured articles'),
    ('Sports',           78.00, 1.80, 12.0, 'Miscellaneous manufactured articles'),
    ('Health & Beauty',  45.00, 0.50,  9.0, 'Chemicals and related products'),
    ('Food & Grocery',   38.00, 2.40,  7.0, 'Food and live animals'),
    ('Toys & Games',     55.00, 0.90, 11.0, 'Miscellaneous manufactured articles')
ON CONFLICT (category_name) DO NOTHING;


-- ────────────────────────────────────────────────────────────
-- 6. FACT TABLE: fact_deliveries  ← core table
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fact_deliveries (
    -- Surrogate key
    delivery_sk             BIGSERIAL       PRIMARY KEY,

    -- Natural business key
    shipment_id             VARCHAR(15)     NOT NULL UNIQUE,  -- e.g. SHP-1000001

    -- Foreign keys to dimensions
    date_key                INTEGER         NOT NULL
                                REFERENCES dim_date (date_key),
    postcode_pc4            SMALLINT        NOT NULL
                                REFERENCES dim_postcode (postcode_pc4),
    warehouse_id            VARCHAR(12)     NOT NULL
                                REFERENCES dim_warehouse (warehouse_id),
    category_id             SMALLINT        NOT NULL
                                REFERENCES dim_product_category (category_id),
    -- Note: driver_id is NOT enforced as FK because the dataset
    -- doesn't assign drivers to individual shipments. It's used
    -- at aggregation level. In production this would be a FK.

    -- Datetime fields
    dispatch_datetime       TIMESTAMP       NOT NULL,
    delivery_datetime       TIMESTAMP,

    -- Carrier & vehicle
    carrier                 VARCHAR(25)     NOT NULL,
    vehicle_type            VARCHAR(20)     NOT NULL,

    -- Order details
    order_value_eur         NUMERIC(10, 2)  NOT NULL CHECK (order_value_eur > 0),
    weight_kg               NUMERIC(7, 2)   NOT NULL CHECK (weight_kg > 0),

    -- Delivery outcome
    attempt_1_status        VARCHAR(10)     NOT NULL CHECK (attempt_1_status IN ('Success','Failed')),
    attempt_2_status        VARCHAR(10)     CHECK (attempt_2_status IN ('Success','Failed','N/A')),
    fail_reason             VARCHAR(30),
    final_status            VARCHAR(30)     NOT NULL
                                CHECK (final_status IN (
                                    'Delivered',
                                    'Delivered (2nd attempt)',
                                    'Returned'
                                )),

    -- Cost & sustainability metrics
    delivery_cost_eur       NUMERIC(8, 2)   NOT NULL CHECK (delivery_cost_eur >= 0),
    route_km                NUMERIC(7, 2),
    co2_emission_kg         NUMERIC(8, 4),

    -- Weather at dispatch (joined from Open-Meteo)
    temp_celsius            NUMERIC(5, 1),
    rain_mm                 NUMERIC(6, 1),
    wind_speed_kmh          NUMERIC(5, 1),

    -- Derived boolean flags (faster than string comparisons)
    is_first_attempt_fail   BOOLEAN         GENERATED ALWAYS AS
                                (attempt_1_status = 'Failed') STORED,
    is_returned             BOOLEAN         GENERATED ALWAYS AS
                                (final_status = 'Returned') STORED,
    is_peak_period          BOOLEAN         NOT NULL DEFAULT FALSE,
    is_zero_emission        BOOLEAN         GENERATED ALWAYS AS
                                (vehicle_type IN ('Cargo Bike','Electric Van')) STORED,
    is_multi_attempt        BOOLEAN         GENERATED ALWAYS AS
                                (attempt_2_status NOT IN ('N/A') AND attempt_2_status IS NOT NULL) STORED,

    -- Audit
    loaded_at               TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE fact_deliveries IS
    'Core fact table — one row per shipment. '
    'Grain: individual parcel delivery attempt. '
    'Source: SwiftRoute BV operational logs, calibrated to PostNL 2023 benchmarks.';

-- Covering indexes for common query patterns
CREATE INDEX idx_fact_date_key          ON fact_deliveries (date_key);
CREATE INDEX idx_fact_postcode          ON fact_deliveries (postcode_pc4);
CREATE INDEX idx_fact_warehouse         ON fact_deliveries (warehouse_id);
CREATE INDEX idx_fact_category          ON fact_deliveries (category_id);
CREATE INDEX idx_fact_carrier           ON fact_deliveries (carrier);
CREATE INDEX idx_fact_vehicle           ON fact_deliveries (vehicle_type);
CREATE INDEX idx_fact_final_status      ON fact_deliveries (final_status);
CREATE INDEX idx_fact_dispatch_dt       ON fact_deliveries (dispatch_datetime);
CREATE INDEX idx_fact_fail              ON fact_deliveries (is_first_attempt_fail)
                                         WHERE is_first_attempt_fail = TRUE;

-- Composite index for the most common dashboard query
CREATE INDEX idx_fact_date_city         ON fact_deliveries (date_key, postcode_pc4);


-- ────────────────────────────────────────────────────────────
-- 7. MART: mart_monthly_kpi  (pre-aggregated for Power BI)
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS mart_monthly_kpi (
    kpi_id                      BIGSERIAL       PRIMARY KEY,
    year_month                  CHAR(7)         NOT NULL,   -- '2023-04'
    city                        VARCHAR(30)     NOT NULL,
    total_shipments             INTEGER         NOT NULL,
    successful_deliveries       INTEGER         NOT NULL,
    failed_first_attempt        INTEGER         NOT NULL,
    returned_shipments          INTEGER         NOT NULL,
    otd_rate_pct                NUMERIC(6, 2),
    first_attempt_fail_pct      NUMERIC(6, 2),
    return_rate_pct             NUMERIC(6, 2),
    avg_delivery_cost_eur       NUMERIC(8, 2),
    total_revenue_eur           NUMERIC(14, 2),
    total_co2_kg                NUMERIC(10, 4),
    avg_co2_per_shipment_kg     NUMERIC(8, 6),
    total_route_km              NUMERIC(12, 2),
    avg_weight_kg               NUMERIC(6, 2),
    top_product_category        VARCHAR(30),
    electric_vehicle_pct        NUMERIC(5, 1),
    -- Month-over-month change (populated by refresh procedure)
    otd_rate_mom_change_pp      NUMERIC(6, 2),
    cost_mom_change_pct         NUMERIC(6, 2),
    refreshed_at                TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (year_month, city)
);

CREATE INDEX idx_mart_kpi_year_month ON mart_monthly_kpi (year_month);
CREATE INDEX idx_mart_kpi_city       ON mart_monthly_kpi (city);


-- ────────────────────────────────────────────────────────────
-- 8. MART: mart_daily_forecast
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS mart_daily_forecast (
    forecast_id             BIGSERIAL       PRIMARY KEY,
    forecast_date           DATE            NOT NULL,
    city                    VARCHAR(30)     NOT NULL,
    day_of_week             VARCHAR(10),
    week_number             SMALLINT,
    month_name              VARCHAR(10),
    year                    SMALLINT,
    is_public_holiday       BOOLEAN         DEFAULT FALSE,
    is_peak_period          BOOLEAN         DEFAULT FALSE,
    peak_event_name         VARCHAR(30),
    actual_volume           NUMERIC(10, 0),
    forecast_volume         NUMERIC(10, 0),
    forecast_lower_95       NUMERIC(10, 0),
    forecast_upper_95       NUMERIC(10, 0),
    forecast_error          NUMERIC(10, 0)  GENERATED ALWAYS AS
                                (actual_volume - forecast_volume) STORED,
    forecast_mape           NUMERIC(8, 4),  -- populated by Python model
    temp_celsius            NUMERIC(5, 1),
    rain_mm                 NUMERIC(6, 1),
    wind_speed_kmh          NUMERIC(5, 1),
    data_type               VARCHAR(10)     CHECK (data_type IN ('Actual','Forecast')),
    loaded_at               TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (forecast_date, city)
);

CREATE INDEX idx_forecast_date ON mart_daily_forecast (forecast_date);
CREATE INDEX idx_forecast_city ON mart_daily_forecast (city);


-- ────────────────────────────────────────────────────────────
-- 9. MART: mart_inventory_snapshot
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS mart_inventory_snapshot (
    snapshot_id             BIGSERIAL       PRIMARY KEY,
    snapshot_date           DATE            NOT NULL DEFAULT CURRENT_DATE,
    warehouse_id            VARCHAR(12)     NOT NULL
                                REFERENCES dim_warehouse (warehouse_id),
    product_category        VARCHAR(30)     NOT NULL,
    sku_code                VARCHAR(20),
    current_stock_units     INTEGER,
    max_capacity_units      INTEGER,
    reorder_point           INTEGER,
    reorder_qty             INTEGER,
    avg_daily_outbound      INTEGER,
    days_of_supply          NUMERIC(8, 1),
    stockout_days_ytd       SMALLINT,
    last_replenish_date     DATE,
    supplier_lead_days      SMALLINT,
    unit_cost_eur           NUMERIC(10, 2),
    inventory_value_eur     NUMERIC(14, 2),
    stock_status            VARCHAR(10)     CHECK (stock_status IN ('Critical','Low','Normal')),
    utilisation_pct         NUMERIC(5, 2)   GENERATED ALWAYS AS (
        CASE WHEN max_capacity_units > 0
             THEN ROUND(current_stock_units::NUMERIC / max_capacity_units * 100, 2)
             ELSE NULL END
    ) STORED,
    loaded_at               TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_inventory_warehouse ON mart_inventory_snapshot (warehouse_id);
CREATE INDEX idx_inventory_date      ON mart_inventory_snapshot (snapshot_date);
CREATE INDEX idx_inventory_status    ON mart_inventory_snapshot (stock_status);


-- ────────────────────────────────────────────────────────────
-- 10. AUDIT / DATA QUALITY TABLE
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS audit_load_log (
    log_id              BIGSERIAL       PRIMARY KEY,
    table_name          VARCHAR(50)     NOT NULL,
    load_type           VARCHAR(20)     NOT NULL,   -- 'FULL' | 'INCREMENTAL' | 'REFRESH'
    rows_inserted       INTEGER         DEFAULT 0,
    rows_updated        INTEGER         DEFAULT 0,
    rows_rejected       INTEGER         DEFAULT 0,
    start_time          TIMESTAMP       NOT NULL,
    end_time            TIMESTAMP,
    duration_seconds    NUMERIC(8, 2)   GENERATED ALWAYS AS (
        EXTRACT(EPOCH FROM (end_time - start_time))
    ) STORED,
    status              VARCHAR(10)     CHECK (status IN ('RUNNING','SUCCESS','FAILED')),
    error_message       TEXT,
    loaded_by           VARCHAR(50)     DEFAULT CURRENT_USER
);

COMMENT ON TABLE audit_load_log IS
    'Pipeline audit trail. Every stored procedure writes a row here '
    'on start and updates on completion.';


-- ────────────────────────────────────────────────────────────
-- 11. USEFUL VIEWS (for Power BI direct query)
-- ────────────────────────────────────────────────────────────

-- View: enriched deliveries (joins all dims onto fact)
CREATE OR REPLACE VIEW vw_deliveries_enriched AS
SELECT
    f.shipment_id,
    f.dispatch_datetime,
    f.delivery_datetime,
    EXTRACT(EPOCH FROM (f.delivery_datetime - f.dispatch_datetime)) / 3600.0
                                                        AS transit_hours,
    d.full_date                                         AS dispatch_date,
    d.year,
    d.month_name,
    d.quarter,
    d.week_number,
    d.day_of_week_name,
    d.is_weekend,
    d.is_public_holiday,
    d.is_peak_period,
    d.peak_event_name,
    p.city,
    p.postcode_pc4,
    p.district,
    p.housing_density,
    p.zone_type,
    p.fail_risk_tier                                    AS zone_risk_tier,
    p.parcel_locker_nearby,
    w.warehouse_id,
    c.category_name                                     AS product_category,
    f.carrier,
    f.vehicle_type,
    f.is_zero_emission,
    f.order_value_eur,
    f.weight_kg,
    f.attempt_1_status,
    f.attempt_2_status,
    f.fail_reason,
    f.final_status,
    f.is_first_attempt_fail,
    f.is_returned,
    f.is_multi_attempt,
    f.delivery_cost_eur,
    f.route_km,
    f.co2_emission_kg,
    f.temp_celsius,
    f.rain_mm,
    f.wind_speed_kmh,
    f.is_peak_period                                    AS shipment_in_peak
FROM      fact_deliveries       f
JOIN      dim_date              d ON f.date_key      = d.date_key
JOIN      dim_postcode          p ON f.postcode_pc4  = p.postcode_pc4
JOIN      dim_warehouse         w ON f.warehouse_id  = w.warehouse_id
JOIN      dim_product_category  c ON f.category_id   = c.category_id;

COMMENT ON VIEW vw_deliveries_enriched IS
    'Denormalised delivery view for Power BI DirectQuery and ad-hoc analysis. '
    'Avoids repeated JOIN boilerplate in reports.';


-- View: current inventory health
CREATE OR REPLACE VIEW vw_inventory_health AS
SELECT
    i.warehouse_id,
    w.city,
    i.product_category,
    i.sku_code,
    i.current_stock_units,
    i.max_capacity_units,
    i.utilisation_pct,
    i.days_of_supply,
    i.reorder_point,
    i.stock_status,
    i.stockout_days_ytd,
    CASE
        WHEN i.days_of_supply <= i.supplier_lead_days THEN 'ORDER NOW'
        WHEN i.days_of_supply <= i.supplier_lead_days * 2 THEN 'ORDER SOON'
        ELSE 'OK'
    END                                             AS procurement_action,
    i.inventory_value_eur,
    i.snapshot_date
FROM  mart_inventory_snapshot i
JOIN  dim_warehouse           w ON i.warehouse_id = w.warehouse_id
WHERE i.snapshot_date = (SELECT MAX(snapshot_date) FROM mart_inventory_snapshot);


-- ────────────────────────────────────────────────────────────
-- END OF SCHEMA
-- ────────────────────────────────────────────────────────────
