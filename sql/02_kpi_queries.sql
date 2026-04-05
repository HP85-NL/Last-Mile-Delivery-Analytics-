-- ============================================================
--  pateldelivers BV — KPI Queries
--  Module 1: Data Engineering Foundation
--  File 2 of 3: Analytical SQL for Dashboard & Reporting
--
--  All queries run against the PatelDelivers schema.
--  Each query is labelled with the Power BI page it feeds.
--  Dialect: PostgreSQL 14+
-- ============================================================

SET search_path TO swiftroute;


-- ════════════════════════════════════════════════════════════
--  SECTION A — EXECUTIVE OVERVIEW KPIs
--  Power BI Page: Executive Overview
-- ════════════════════════════════════════════════════════════

-- ── A1. Headline KPIs — current month vs prior month ────────
--  Returns one row per city with MoM deltas.
--  Used for the KPI card row at the top of the dashboard.

WITH monthly_base AS (
    SELECT
        d.year_month,
        p.city,
        COUNT(*)                                                AS total_shipments,
        SUM(CASE WHEN f.final_status = 'Delivered'           THEN 1 ELSE 0 END)
          + SUM(CASE WHEN f.final_status = 'Delivered (2nd attempt)' THEN 1 ELSE 0 END)
                                                               AS delivered,
        SUM(CASE WHEN f.is_first_attempt_fail               THEN 1 ELSE 0 END)
                                                               AS first_attempt_fails,
        SUM(CASE WHEN f.is_returned                         THEN 1 ELSE 0 END)
                                                               AS returned,
        ROUND(AVG(f.delivery_cost_eur),    2)                  AS avg_cost_eur,
        ROUND(AVG(f.co2_emission_kg),      6)                  AS avg_co2_kg,
        ROUND(SUM(f.order_value_eur),      2)                  AS total_revenue_eur,
        ROUND(AVG(f.route_km),             2)                  AS avg_route_km,
        SUM(CASE WHEN f.is_zero_emission   THEN 1 ELSE 0 END)  AS zero_emission_count
    FROM  fact_deliveries  f
    JOIN  dim_date         d ON f.date_key     = d.date_key
    JOIN  dim_postcode     p ON f.postcode_pc4 = p.postcode_pc4
    GROUP BY d.year_month, p.city
),
with_rates AS (
    SELECT
        *,
        ROUND(delivered::NUMERIC      / NULLIF(total_shipments, 0) * 100, 2) AS otd_rate_pct,
        ROUND(first_attempt_fails::NUMERIC / NULLIF(total_shipments, 0) * 100, 2) AS fail_rate_pct,
        ROUND(returned::NUMERIC       / NULLIF(total_shipments, 0) * 100, 2) AS return_rate_pct,
        ROUND(zero_emission_count::NUMERIC / NULLIF(total_shipments, 0) * 100, 1) AS zero_emission_pct
    FROM monthly_base
),
ranked AS (
    SELECT *,
        LAG(otd_rate_pct)   OVER (PARTITION BY city ORDER BY year_month) AS prev_otd_rate,
        LAG(fail_rate_pct)  OVER (PARTITION BY city ORDER BY year_month) AS prev_fail_rate,
        LAG(avg_cost_eur)   OVER (PARTITION BY city ORDER BY year_month) AS prev_avg_cost,
        RANK() OVER (PARTITION BY city ORDER BY year_month DESC)         AS recency_rank
    FROM with_rates
)
SELECT
    city,
    year_month,
    total_shipments,
    delivered,
    first_attempt_fails,
    returned,
    otd_rate_pct,
    fail_rate_pct,
    return_rate_pct,
    avg_cost_eur,
    avg_co2_kg,
    total_revenue_eur,
    avg_route_km,
    zero_emission_pct,
    -- Month-over-month deltas (percentage points for rates, % change for cost)
    ROUND(otd_rate_pct  - COALESCE(prev_otd_rate,  otd_rate_pct),  2) AS otd_mom_pp,
    ROUND(fail_rate_pct - COALESCE(prev_fail_rate, fail_rate_pct), 2) AS fail_mom_pp,
    ROUND((avg_cost_eur - COALESCE(prev_avg_cost, avg_cost_eur))
          / NULLIF(prev_avg_cost, 0) * 100, 2)                        AS cost_mom_pct
FROM ranked
ORDER BY city, year_month DESC;


-- ── A2. 36-Month OTD Trend — all cities ─────────────────────
--  Line chart source: OTD rate over time, one line per city.

SELECT
    d.year_month,
    p.city,
    COUNT(*)                                                           AS shipments,
    ROUND(
        SUM(CASE WHEN f.final_status IN ('Delivered','Delivered (2nd attempt)')
                 THEN 1.0 ELSE 0 END)
        / NULLIF(COUNT(*), 0) * 100
    , 2)                                                               AS otd_rate_pct,
    ROUND(AVG(f.delivery_cost_eur), 2)                                 AS avg_cost_eur,
    ROUND(AVG(f.co2_emission_kg) * 1000, 4)                           AS avg_co2_grams
FROM  fact_deliveries  f
JOIN  dim_date         d ON f.date_key     = d.date_key
JOIN  dim_postcode     p ON f.postcode_pc4 = p.postcode_pc4
GROUP BY d.year_month, p.city
ORDER BY d.year_month, p.city;


-- ── A3. OTD by carrier — ranked ─────────────────────────────

SELECT
    carrier,
    COUNT(*)                                                           AS total_shipments,
    ROUND(AVG(delivery_cost_eur), 2)                                   AS avg_cost_eur,
    ROUND(
        SUM(CASE WHEN final_status IN ('Delivered','Delivered (2nd attempt)')
                 THEN 1.0 ELSE 0 END)
        / COUNT(*) * 100
    , 2)                                                               AS otd_rate_pct,
    ROUND(
        SUM(CASE WHEN is_first_attempt_fail THEN 1.0 ELSE 0 END)
        / COUNT(*) * 100
    , 2)                                                               AS fail_rate_pct,
    ROUND(AVG(co2_emission_kg), 6)                                     AS avg_co2_kg,
    RANK() OVER (ORDER BY
        SUM(CASE WHEN final_status IN ('Delivered','Delivered (2nd attempt)')
                 THEN 1.0 ELSE 0 END)
        / COUNT(*) DESC
    )                                                                  AS otd_rank
FROM  fact_deliveries
GROUP BY carrier
ORDER BY otd_rank;


-- ════════════════════════════════════════════════════════════
--  SECTION B — ROUTE PERFORMANCE
--  Power BI Page: Route Performance
-- ════════════════════════════════════════════════════════════

-- ── B1. Delivery success by PC4 postcode zone ───────────────
--  Source for the heatmap. Includes all dims needed for tooltip.

SELECT
    p.postcode_pc4,
    p.city,
    p.district,
    p.latitude,
    p.longitude,
    p.zone_type,
    p.housing_density,
    p.fail_risk_tier,
    p.parcel_locker_nearby,
    p.historical_fail_rate_pct,
    COUNT(f.delivery_sk)                                               AS total_shipments,
    ROUND(
        SUM(CASE WHEN f.is_first_attempt_fail THEN 1.0 ELSE 0 END)
        / NULLIF(COUNT(*), 0) * 100
    , 2)                                                               AS actual_fail_rate_pct,
    -- How much worse is actual vs historical benchmark?
    ROUND(
        SUM(CASE WHEN f.is_first_attempt_fail THEN 1.0 ELSE 0 END)
        / NULLIF(COUNT(*), 0) * 100
        - p.historical_fail_rate_pct
    , 2)                                                               AS fail_rate_vs_benchmark_pp,
    ROUND(AVG(f.route_km), 2)                                          AS avg_route_km,
    ROUND(AVG(f.delivery_cost_eur), 2)                                 AS avg_cost_eur,
    ROUND(AVG(f.co2_emission_kg) * 1000, 3)                           AS avg_co2_grams
FROM  fact_deliveries  f
JOIN  dim_postcode     p ON f.postcode_pc4 = p.postcode_pc4
GROUP BY
    p.postcode_pc4, p.city, p.district, p.latitude, p.longitude,
    p.zone_type, p.housing_density, p.fail_risk_tier,
    p.parcel_locker_nearby, p.historical_fail_rate_pct
ORDER BY actual_fail_rate_pct DESC;


-- ── B2. Fail reason breakdown by city ───────────────────────
--  Stacked bar chart: why deliveries fail, per city.

SELECT
    p.city,
    COALESCE(f.fail_reason, 'No Failure')                             AS fail_reason,
    COUNT(*)                                                           AS occurrences,
    ROUND(COUNT(*)::NUMERIC
          / SUM(COUNT(*)) OVER (PARTITION BY p.city) * 100, 2)        AS pct_of_city_shipments
FROM  fact_deliveries  f
JOIN  dim_postcode     p ON f.postcode_pc4 = p.postcode_pc4
GROUP BY p.city, f.fail_reason
ORDER BY p.city, occurrences DESC;


-- ── B3. Vehicle type — CO₂ and efficiency by city ───────────
--  Compare zero-emission mix across cities.

SELECT
    p.city,
    f.vehicle_type,
    COUNT(*)                                                           AS shipments,
    ROUND(COUNT(*)::NUMERIC
          / SUM(COUNT(*)) OVER (PARTITION BY p.city) * 100, 1)        AS city_vehicle_mix_pct,
    ROUND(AVG(f.route_km),          2)                                 AS avg_route_km,
    ROUND(AVG(f.delivery_cost_eur), 2)                                 AS avg_cost_eur,
    ROUND(AVG(f.co2_emission_kg) * 1000, 3)                           AS avg_co2_grams,
    ROUND(
        SUM(CASE WHEN f.is_first_attempt_fail THEN 1.0 ELSE 0 END)
        / COUNT(*) * 100
    , 2)                                                               AS fail_rate_pct,
    f.is_zero_emission
FROM  fact_deliveries  f
JOIN  dim_postcode     p ON f.postcode_pc4 = p.postcode_pc4
GROUP BY p.city, f.vehicle_type, f.is_zero_emission
ORDER BY p.city, shipments DESC;


-- ── B4. Peak vs non-peak delivery performance ───────────────

SELECT
    p.city,
    d.peak_event_name,
    CASE WHEN f.is_peak_period THEN 'Peak' ELSE 'Normal' END          AS period_type,
    COUNT(*)                                                           AS shipments,
    ROUND(AVG(f.delivery_cost_eur), 2)                                 AS avg_cost_eur,
    ROUND(
        SUM(CASE WHEN f.is_first_attempt_fail THEN 1.0 ELSE 0 END)
        / COUNT(*) * 100
    , 2)                                                               AS fail_rate_pct,
    ROUND(AVG(f.route_km), 2)                                          AS avg_route_km
FROM  fact_deliveries  f
JOIN  dim_date         d ON f.date_key     = d.date_key
JOIN  dim_postcode     p ON f.postcode_pc4 = p.postcode_pc4
GROUP BY p.city, d.peak_event_name, f.is_peak_period
ORDER BY p.city, period_type;


-- ════════════════════════════════════════════════════════════
--  SECTION C — DRIVER ANALYTICS
--  Power BI Page: Driver Analytics
-- ════════════════════════════════════════════════════════════

-- ── C1. Driver performance scorecard ────────────────────────
--  One row per driver. Source for the scatter matrix.

WITH driver_stats AS (
    SELECT
        -- We join by city+vehicle to approximate driver assignment.
        -- In production this would be a direct driver_id FK on fact.
        drv.driver_id,
        drv.driver_name,
        drv.city,
        drv.vehicle_type,
        drv.contract_type,
        drv.years_experience,
        drv.performance_band,
        drv.is_zero_emission,
        drv.on_time_rate_pct                                           AS benchmark_otd_pct,
        drv.fail_rate_pct                                              AS benchmark_fail_pct,
        drv.avg_deliveries_day                                         AS benchmark_daily_vol,
        drv.avg_rating
    FROM dim_driver drv
    WHERE drv.is_active = TRUE
)
SELECT
    ds.*,
    -- Efficiency index: actual deliveries vs benchmark (>100 = outperforming)
    ROUND(ds.benchmark_daily_vol::NUMERIC / 40.0 * 100, 1)            AS efficiency_index,
    -- Composite score (simple weighted average for dashboard)
    ROUND(
        (ds.benchmark_otd_pct * 0.5)
      + ((100 - ds.benchmark_fail_pct) * 0.3)
      + (ds.avg_rating / 5.0 * 100 * 0.2)
    , 1)                                                               AS composite_score
FROM driver_stats ds
ORDER BY composite_score DESC;


-- ── C2. Performance band distribution by city ───────────────

SELECT
    city,
    performance_band,
    COUNT(*)                                                           AS driver_count,
    ROUND(AVG(on_time_rate_pct), 1)                                    AS avg_otd_pct,
    ROUND(AVG(fail_rate_pct),    1)                                    AS avg_fail_pct,
    ROUND(AVG(avg_rating),       2)                                    AS avg_rating,
    ROUND(AVG(years_experience), 1)                                    AS avg_experience_yrs
FROM  dim_driver
WHERE is_active = TRUE
GROUP BY city, performance_band
ORDER BY city,
    CASE performance_band
        WHEN 'Elite'        THEN 1
        WHEN 'Standard'     THEN 2
        WHEN 'Developing'   THEN 3
        ELSE 4
    END;


-- ── C3. Outlier drivers — fail rate significantly above city average ─

WITH city_avg AS (
    SELECT city,
           ROUND(AVG(fail_rate_pct), 2) AS city_avg_fail,
           ROUND(STDDEV(fail_rate_pct), 2) AS city_std_fail
    FROM   dim_driver
    WHERE  is_active = TRUE
    GROUP  BY city
)
SELECT
    d.driver_id,
    d.driver_name,
    d.city,
    d.vehicle_type,
    d.fail_rate_pct,
    c.city_avg_fail,
    ROUND(d.fail_rate_pct - c.city_avg_fail, 2)                        AS above_avg_pp,
    ROUND((d.fail_rate_pct - c.city_avg_fail)
          / NULLIF(c.city_std_fail, 0), 2)                             AS z_score,
    CASE
        WHEN d.fail_rate_pct > c.city_avg_fail + 2 * c.city_std_fail  THEN '🔴 Flag'
        WHEN d.fail_rate_pct > c.city_avg_fail + 1 * c.city_std_fail  THEN '🟡 Watch'
        ELSE '🟢 Normal'
    END                                                                AS alert_status
FROM  dim_driver  d
JOIN  city_avg    c ON d.city = c.city
WHERE d.is_active = TRUE
ORDER BY z_score DESC;


-- ── C4. Vehicle type vs fail rate — is vehicle a factor? ────
--  Quick hypothesis test input for the analyst.

SELECT
    vehicle_type,
    COUNT(*)                                                           AS drivers,
    ROUND(AVG(fail_rate_pct),    2)                                    AS avg_fail_rate,
    ROUND(MIN(fail_rate_pct),    2)                                    AS min_fail_rate,
    ROUND(MAX(fail_rate_pct),    2)                                    AS max_fail_rate,
    ROUND(STDDEV(fail_rate_pct), 2)                                    AS stddev_fail_rate,
    ROUND(AVG(on_time_rate_pct), 2)                                    AS avg_otd_rate
FROM  dim_driver
WHERE is_active = TRUE
GROUP BY vehicle_type
ORDER BY avg_fail_rate ASC;


-- ════════════════════════════════════════════════════════════
--  SECTION D — WAREHOUSE & INVENTORY
--  Power BI Page: Warehouse
-- ════════════════════════════════════════════════════════════

-- ── D1. Current inventory status — all warehouses ───────────

SELECT
    w.city,
    i.warehouse_id,
    i.product_category,
    i.sku_code,
    i.current_stock_units,
    i.max_capacity_units,
    i.utilisation_pct,
    i.days_of_supply,
    i.reorder_point,
    i.reorder_qty,
    i.avg_daily_outbound,
    i.stock_status,
    CASE
        WHEN i.days_of_supply <= i.supplier_lead_days       THEN '🔴 Order Now'
        WHEN i.days_of_supply <= i.supplier_lead_days * 2   THEN '🟡 Order Soon'
        ELSE                                                     '🟢 OK'
    END                                                                AS procurement_flag,
    i.stockout_days_ytd,
    i.inventory_value_eur,
    i.last_replenish_date,
    i.supplier_lead_days
FROM  mart_inventory_snapshot  i
JOIN  dim_warehouse            w ON i.warehouse_id = w.warehouse_id
WHERE i.snapshot_date = (SELECT MAX(snapshot_date) FROM mart_inventory_snapshot)
ORDER BY i.stock_status DESC, i.days_of_supply ASC;


-- ── D2. Inventory value by warehouse (treemap source) ───────

SELECT
    w.city,
    i.warehouse_id,
    SUM(i.inventory_value_eur)                                         AS total_inventory_value_eur,
    ROUND(AVG(i.utilisation_pct), 1)                                   AS avg_utilisation_pct,
    COUNT(CASE WHEN i.stock_status = 'Critical' THEN 1 END)            AS critical_sku_count,
    COUNT(CASE WHEN i.stock_status = 'Low'      THEN 1 END)            AS low_sku_count,
    SUM(i.stockout_days_ytd)                                           AS total_stockout_days
FROM  mart_inventory_snapshot  i
JOIN  dim_warehouse            w ON i.warehouse_id = w.warehouse_id
WHERE i.snapshot_date = (SELECT MAX(snapshot_date) FROM mart_inventory_snapshot)
GROUP BY w.city, i.warehouse_id
ORDER BY total_inventory_value_eur DESC;


-- ── D3. Days of supply trend — rolling average ──────────────
--  Shows whether supply coverage is improving or degrading over time.

WITH daily_supply AS (
    SELECT
        i.snapshot_date,
        i.warehouse_id,
        ROUND(AVG(i.days_of_supply), 1)                                AS avg_days_of_supply,
        COUNT(CASE WHEN i.stock_status = 'Critical' THEN 1 END)        AS critical_skus
    FROM mart_inventory_snapshot i
    GROUP BY i.snapshot_date, i.warehouse_id
)
SELECT
    snapshot_date,
    warehouse_id,
    avg_days_of_supply,
    critical_skus,
    ROUND(AVG(avg_days_of_supply)
          OVER (PARTITION BY warehouse_id
                ORDER BY snapshot_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 1)                                                              AS rolling_7d_avg_supply
FROM daily_supply
ORDER BY warehouse_id, snapshot_date DESC;


-- ════════════════════════════════════════════════════════════
--  SECTION E — ADVANCED ANALYTICAL QUERIES
--  Used for notebooks, ad-hoc analysis, interview discussion
-- ════════════════════════════════════════════════════════════

-- ── E1. Weather impact on fail rate ─────────────────────────
--  Buckets rain into bands and shows fail rate per band.
--  Supports the hypothesis: rain > 8mm increases fail probability.

SELECT
    CASE
        WHEN rain_mm = 0           THEN 'No rain'
        WHEN rain_mm < 2           THEN 'Light (0-2mm)'
        WHEN rain_mm < 5           THEN 'Moderate (2-5mm)'
        WHEN rain_mm < 8           THEN 'Heavy (5-8mm)'
        ELSE                            'Extreme (8mm+)'
    END                                                                AS rain_band,
    COUNT(*)                                                           AS shipments,
    ROUND(
        SUM(CASE WHEN is_first_attempt_fail THEN 1.0 ELSE 0 END)
        / COUNT(*) * 100
    , 2)                                                               AS fail_rate_pct,
    ROUND(AVG(delivery_cost_eur), 2)                                   AS avg_cost_eur,
    ROUND(AVG(route_km), 2)                                            AS avg_route_km
FROM fact_deliveries
GROUP BY rain_band
ORDER BY
    CASE rain_band
        WHEN 'No rain'       THEN 1
        WHEN 'Light (0-2mm)' THEN 2
        WHEN 'Moderate (2-5mm)' THEN 3
        WHEN 'Heavy (5-8mm)' THEN 4
        ELSE 5
    END;


-- ── E2. Cumulative cost savings from zero-emission vehicles ─
--  Quantifies the CO₂ and cost difference between fleets.
--  Key insight for the sustainability KPI section.

WITH fleet_comparison AS (
    SELECT
        d.year,
        d.month_name,
        d.month_number,
        p.city,
        SUM(CASE WHEN f.is_zero_emission THEN f.co2_emission_kg ELSE 0 END)    AS actual_co2_zero_kg,
        SUM(CASE WHEN NOT f.is_zero_emission THEN f.co2_emission_kg ELSE 0 END) AS actual_co2_fossil_kg,
        SUM(CASE WHEN f.is_zero_emission THEN 1 ELSE 0 END)                    AS zero_em_shipments,
        SUM(CASE WHEN NOT f.is_zero_emission THEN 1 ELSE 0 END)                AS fossil_shipments,
        -- Counterfactual: what if zero-emission vehicles emitted like diesel?
        SUM(CASE WHEN f.is_zero_emission THEN f.route_km * 0.21 ELSE 0 END)   AS counterfactual_co2_kg
    FROM  fact_deliveries  f
    JOIN  dim_date         d ON f.date_key     = d.date_key
    JOIN  dim_postcode     p ON f.postcode_pc4 = p.postcode_pc4
    GROUP BY d.year, d.month_name, d.month_number, p.city
)
SELECT
    year,
    month_name,
    city,
    zero_em_shipments,
    fossil_shipments,
    ROUND(actual_co2_zero_kg,    4)                                    AS actual_co2_zero_kg,
    ROUND(counterfactual_co2_kg, 4)                                    AS counterfactual_co2_kg,
    ROUND(counterfactual_co2_kg - actual_co2_zero_kg, 4)              AS co2_saved_kg,
    -- Running total of CO₂ avoided
    ROUND(
        SUM(counterfactual_co2_kg - actual_co2_zero_kg)
        OVER (PARTITION BY city ORDER BY year, month_number)
    , 3)                                                               AS cumulative_co2_saved_kg
FROM fleet_comparison
ORDER BY city, year, month_number;


-- ── E3. Top 10 worst postcodes — cost of failed deliveries ──
--  Direct business case: where should parcel lockers be installed?

WITH postcode_economics AS (
    SELECT
        p.postcode_pc4,
        p.city,
        p.district,
        p.parcel_locker_nearby,
        p.historical_fail_rate_pct,
        COUNT(*)                                                       AS total_shipments,
        SUM(CASE WHEN f.is_first_attempt_fail THEN 1 ELSE 0 END)      AS total_fails,
        -- Failed 2nd attempt costs 3× a standard delivery
        SUM(CASE WHEN f.is_multi_attempt THEN f.delivery_cost_eur * 2 ELSE 0 END)
                                                                       AS extra_cost_from_2nd_attempts_eur,
        ROUND(
            SUM(CASE WHEN f.is_first_attempt_fail THEN 1.0 ELSE 0 END)
            / COUNT(*) * 100
        , 2)                                                           AS actual_fail_rate_pct,
        ROUND(AVG(f.delivery_cost_eur), 2)                             AS avg_cost_eur
    FROM  fact_deliveries  f
    JOIN  dim_postcode     p ON f.postcode_pc4 = p.postcode_pc4
    GROUP BY p.postcode_pc4, p.city, p.district,
             p.parcel_locker_nearby, p.historical_fail_rate_pct
)
SELECT
    postcode_pc4,
    city,
    district,
    parcel_locker_nearby,
    total_shipments,
    total_fails,
    actual_fail_rate_pct,
    ROUND(extra_cost_from_2nd_attempts_eur, 2)                         AS wasted_cost_eur,
    -- Annualised waste estimate
    ROUND(extra_cost_from_2nd_attempts_eur / 3.0 * 12, 2)             AS est_annual_waste_eur,
    -- Priority score: high waste + no locker = install here first
    CASE
        WHEN parcel_locker_nearby = FALSE
         AND actual_fail_rate_pct > 16  THEN '🔴 Install Locker — High Priority'
        WHEN parcel_locker_nearby = FALSE
         AND actual_fail_rate_pct > 12  THEN '🟡 Review Coverage'
        ELSE '🟢 Covered'
    END                                                                AS locker_recommendation
FROM postcode_economics
ORDER BY wasted_cost_eur DESC
LIMIT 10;


-- ── E4. Day-of-week delivery patterns ───────────────────────
--  When do we fail the most? Staffing optimisation input.

SELECT
    CASE d.day_of_week_number
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END                                                                AS day_of_week,
    d.day_of_week_number,
    COUNT(*)                                                           AS total_shipments,
    ROUND(AVG(f.delivery_cost_eur), 2)                                 AS avg_cost_eur,
    ROUND(
        SUM(CASE WHEN f.is_first_attempt_fail THEN 1.0 ELSE 0 END)
        / COUNT(*) * 100
    , 2)                                                               AS fail_rate_pct,
    ROUND(AVG(f.route_km), 2)                                          AS avg_route_km,
    ROUND(
        COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER () * 100
    , 1)                                                               AS pct_of_weekly_volume
FROM  fact_deliveries  f
JOIN  dim_date         d ON f.date_key = d.date_key
GROUP BY d.day_of_week_number
ORDER BY d.day_of_week_number;


-- ── E5. 90-day rolling fail rate with 7-day moving average ──
--  Time-series view for operational monitoring dashboard.

WITH daily_fails AS (
    SELECT
        d.full_date,
        p.city,
        COUNT(*)                                                       AS shipments,
        SUM(CASE WHEN f.is_first_attempt_fail THEN 1 ELSE 0 END)      AS fails,
        ROUND(
            SUM(CASE WHEN f.is_first_attempt_fail THEN 1.0 ELSE 0 END)
            / NULLIF(COUNT(*), 0) * 100
        , 2)                                                           AS daily_fail_rate
    FROM  fact_deliveries  f
    JOIN  dim_date         d ON f.date_key     = d.date_key
    JOIN  dim_postcode     p ON f.postcode_pc4 = p.postcode_pc4
    WHERE d.full_date >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY d.full_date, p.city
)
SELECT
    full_date,
    city,
    shipments,
    fails,
    daily_fail_rate,
    ROUND(
        AVG(daily_fail_rate) OVER (
            PARTITION BY city
            ORDER BY full_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )
    , 2)                                                               AS rolling_7d_fail_rate,
    -- Alert when 7-day rolling rate exceeds 15%
    CASE
        WHEN AVG(daily_fail_rate) OVER (
            PARTITION BY city
            ORDER BY full_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) > 15 THEN TRUE
        ELSE FALSE
    END                                                                AS rolling_alert
FROM daily_fails
ORDER BY city, full_date DESC;


-- ── E6. Revenue at risk from high-fail postcodes ─────────────
--  CFO-level framing: what does a bad fail rate cost in lost revenue?

WITH postcode_risk AS (
    SELECT
        p.postcode_pc4,
        p.city,
        p.fail_risk_tier,
        SUM(f.order_value_eur)                                         AS total_order_value,
        SUM(CASE WHEN f.is_returned THEN f.order_value_eur ELSE 0 END) AS returned_value,
        COUNT(*)                                                       AS shipments,
        SUM(CASE WHEN f.is_returned THEN 1 ELSE 0 END)                 AS returns
    FROM  fact_deliveries  f
    JOIN  dim_postcode     p ON f.postcode_pc4 = p.postcode_pc4
    GROUP BY p.postcode_pc4, p.city, p.fail_risk_tier
)
SELECT
    fail_risk_tier,
    city,
    COUNT(DISTINCT postcode_pc4)                                       AS zone_count,
    SUM(shipments)                                                     AS total_shipments,
    ROUND(SUM(total_order_value),  2)                                  AS total_gmv_eur,
    ROUND(SUM(returned_value),     2)                                  AS gmv_at_risk_eur,
    ROUND(
        SUM(returned_value) / NULLIF(SUM(total_order_value), 0) * 100
    , 2)                                                               AS return_rate_pct,
    -- Projected annual risk based on dataset period (3 years)
    ROUND(SUM(returned_value) / 3.0, 2)                               AS est_annual_gmv_at_risk_eur
FROM postcode_risk
GROUP BY fail_risk_tier, city
ORDER BY
    CASE fail_risk_tier WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END,
    city;


-- ════════════════════════════════════════════════════════════
--  SECTION F — POWER BI OPTIMISATION QUERIES
--  Pre-aggregated tables to keep dashboard fast
-- ════════════════════════════════════════════════════════════

-- ── F1. Refresh mart_monthly_kpi ────────────────────────────
--  Run monthly via stored procedure (see 03_stored_procedures.sql)
--  Shown here as a standalone query for documentation.

INSERT INTO mart_monthly_kpi (
    year_month, city, total_shipments, successful_deliveries,
    failed_first_attempt, returned_shipments,
    otd_rate_pct, first_attempt_fail_pct, return_rate_pct,
    avg_delivery_cost_eur, total_revenue_eur, total_co2_kg,
    avg_co2_per_shipment_kg, total_route_km, avg_weight_kg,
    top_product_category, electric_vehicle_pct
)
SELECT
    d.year_month,
    p.city,
    COUNT(*)                                                           AS total_shipments,
    SUM(CASE WHEN f.final_status IN ('Delivered','Delivered (2nd attempt)') THEN 1 ELSE 0 END),
    SUM(CASE WHEN f.is_first_attempt_fail THEN 1 ELSE 0 END),
    SUM(CASE WHEN f.is_returned THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN f.final_status IN ('Delivered','Delivered (2nd attempt)') THEN 1.0 ELSE 0 END)
          / COUNT(*) * 100, 2),
    ROUND(SUM(CASE WHEN f.is_first_attempt_fail THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 2),
    ROUND(SUM(CASE WHEN f.is_returned THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 2),
    ROUND(AVG(f.delivery_cost_eur), 2),
    ROUND(SUM(f.order_value_eur), 2),
    ROUND(SUM(f.co2_emission_kg), 4),
    ROUND(AVG(f.co2_emission_kg), 6),
    ROUND(SUM(f.route_km), 2),
    ROUND(AVG(f.weight_kg), 2),
    -- Top product category this month
    (
        SELECT c2.category_name
        FROM   fact_deliveries  f2
        JOIN   dim_product_category c2 ON f2.category_id   = c2.category_id
        JOIN   dim_date             d2 ON f2.date_key       = d2.date_key
        JOIN   dim_postcode         p2 ON f2.postcode_pc4   = p2.postcode_pc4
        WHERE  d2.year_month = d.year_month
          AND  p2.city       = p.city
        GROUP  BY c2.category_name
        ORDER  BY COUNT(*) DESC
        LIMIT  1
    ),
    ROUND(SUM(CASE WHEN f.is_zero_emission THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 1)
FROM  fact_deliveries  f
JOIN  dim_date         d ON f.date_key     = d.date_key
JOIN  dim_postcode     p ON f.postcode_pc4 = p.postcode_pc4
GROUP BY d.year_month, p.city
ON CONFLICT (year_month, city) DO UPDATE
    SET total_shipments         = EXCLUDED.total_shipments,
        successful_deliveries   = EXCLUDED.successful_deliveries,
        failed_first_attempt    = EXCLUDED.failed_first_attempt,
        otd_rate_pct            = EXCLUDED.otd_rate_pct,
        first_attempt_fail_pct  = EXCLUDED.first_attempt_fail_pct,
        avg_delivery_cost_eur   = EXCLUDED.avg_delivery_cost_eur,
        total_revenue_eur       = EXCLUDED.total_revenue_eur,
        total_co2_kg            = EXCLUDED.total_co2_kg,
        electric_vehicle_pct    = EXCLUDED.electric_vehicle_pct,
        refreshed_at            = CURRENT_TIMESTAMP;


-- ════════════════════════════════════════════════════════════
--  END OF KPI QUERIES
-- ════════════════════════════════════════════════════════════
