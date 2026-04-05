CREATE TABLE dim_postcodes (
    postcode_pc4             SMALLINT        PRIMARY KEY,
    city                     VARCHAR(30)     NOT NULL,
    district                 VARCHAR(60),
    latitude                 DECIMAL(7,4)    NOT NULL,
    longitude                DECIMAL(7,4)    NOT NULL,
    population_est           INT,
    avg_income_eur           INT,
    housing_density          VARCHAR(15),
    pct_apartments           DECIMAL(5,1),
    avg_parcel_per_wk        DECIMAL(4,2),
    historical_fail_rate_pct DECIMAL(5,2),
    parcel_locker_nearby     VARCHAR(3),
    zone_type                VARCHAR(15)
);

CREATE TABLE dim_drivers (
    driver_id               VARCHAR(10)     NOT NULL PRIMARY KEY,
    driver_name             VARCHAR(80)     NOT NULL,
    city                    VARCHAR(30)     NOT NULL,
    years_experience        INT             NOT NULL,
    vehicle_type            VARCHAR(20)     NOT NULL,
    contract_type           VARCHAR(15)     NOT NULL,
    avg_deliveries_day      INT,
    avg_rating              DECIMAL(3,2),
    on_time_rate_pct        DECIMAL(5,2),
    fail_rate_pct           DECIMAL(5,2),
    is_active               VARCHAR(3),
    join_date               DATE
);

CREATE TABLE warehouse_inventory (
    warehouse_id            VARCHAR(12)     NOT NULL,
    city                    VARCHAR(30)     NOT NULL,
    product_category        VARCHAR(30)     NOT NULL,
    sku_code                VARCHAR(25),
    current_stock_units     INT,
    max_capacity_units      INT,
    reorder_point           INT,
    reorder_qty             INT,
    avg_daily_outbound      INT,
    days_of_supply          DECIMAL(8,1),
    stockout_days_ytd       INT,
    last_replenish_date     VARCHAR(15),
    supplier_lead_days      INT,
    unit_cost_eur           DECIMAL(10,2),
    inventory_value_eur     DECIMAL(14,2),
    stock_status            VARCHAR(10),
    PRIMARY KEY (warehouse_id, product_category)
);

CREATE TABLE daily_volume_forecast (
    date                    VARCHAR(20)     NOT NULL,
    city                    VARCHAR(30)     NOT NULL,
    day_of_week             VARCHAR(15),
    week_number             INT,
    month_name              VARCHAR(15),
    year_num                INT,
    is_public_holiday       VARCHAR(3),
    is_peak_period          VARCHAR(3),
    peak_event              VARCHAR(40),
    actual_volume           DECIMAL(10,1),
    forecast_volume         INT,
    forecast_lower_95       INT,
    forecast_upper_95       INT,
    temp_celsius            DECIMAL(5,1),
    rain_mm                 DECIMAL(6,1),
    wind_speed_kmh          DECIMAL(5,1),
    data_type               VARCHAR(10),
    PRIMARY KEY (forecast_date, city)
);

