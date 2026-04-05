-- Quantify Recommendation 1: Parcel lockers in Rotterdam

SELECT
    f.postcode_pc4,
    f.city,
    p.district,
    COUNT(*)                                        AS total_shipments,
    SUM(CASE WHEN f.attempt_1_status = 'Failed'
             THEN 1 ELSE 0 END)                    AS total_fails,
    ROUND(
        SUM(CASE WHEN f.attempt_1_status = 'Failed'
                 THEN 1.0 ELSE 0 END)
        / COUNT(*) * 100
    , 1)                                            AS fail_rate_pct,
    ROUND(
        SUM(CASE WHEN f.attempt_1_status = 'Failed'
                 THEN 1.0 ELSE 0 END)
        * AVG(f.delivery_cost_eur)
    , 0)                                            AS wasted_cost_3yr_eur,
    ROUND(
        SUM(CASE WHEN f.attempt_1_status = 'Failed'
                 THEN 1.0 ELSE 0 END)
        * AVG(f.delivery_cost_eur)
        / 3
    , 0)                                            AS wasted_cost_annual_eur,
    ROUND(
        SUM(CASE WHEN f.attempt_1_status = 'Failed'
                 THEN co2_emission_kg ELSE 0 END)
        * 1000 / 3
    , 1)                                            AS wasted_co2_annual_grams,
    -- If locker reduces not-home failures by 60%
    ROUND(
        SUM(CASE WHEN f.attempt_1_status = 'Failed'
                 AND f.fail_reason = 'Not home'
                 THEN 1.0 ELSE 0 END)
        * AVG(f.delivery_cost_eur)
        * 0.60 / 3
    , 0)                                            AS annual_saving_eur
FROM fact_deliveries f
JOIN dim_postcodes p
    ON f.postcode_pc4 = p.postcode_pc4
WHERE f.postcode_pc4 IN (3025, 3012, 3021)
GROUP BY f.postcode_pc4, f.city, p.district
ORDER BY wasted_cost_3yr_eur DESC;

-- Quantify Recommendation 2: Predictive dispatch 

WITH not_home_stats AS (
    SELECT
        city,
        COUNT(*)                                    AS total_shipments,
        SUM(CASE WHEN attempt_1_status = 'Failed'
                 AND fail_reason = 'Not home'
                 THEN 1 ELSE 0 END)                AS not_home_fails,
        ROUND(
            SUM(CASE WHEN attempt_1_status = 'Failed'
                     AND fail_reason = 'Not home'
                     THEN 1.0 ELSE 0 END)
            / COUNT(*) * 100
        , 2)                                        AS not_home_rate_pct,
        ROUND(
            SUM(CASE WHEN attempt_1_status = 'Failed'
                     AND fail_reason = 'Not home'
                     THEN delivery_cost_eur ELSE 0 END)
        , 0)                                        AS not_home_wasted_cost,
        ROUND(
            SUM(CASE WHEN attempt_1_status = 'Failed'
                     AND fail_reason = 'Not home'
                     THEN co2_emission_kg ELSE 0 END) * 1000
        , 1)                                        AS not_home_wasted_co2_g
    FROM fact_deliveries
    GROUP BY city
)

SELECT
    city,
    total_shipments,
    not_home_fails,
    not_home_rate_pct,
    not_home_wasted_cost                            AS wasted_cost_3yr_eur,
    ROUND(not_home_wasted_cost / 3, 0)              AS wasted_cost_annual_eur,
    not_home_wasted_co2_g                           AS wasted_co2_3yr_grams,
    -- Predictive model catches 70% of not-home cases before dispatch
    ROUND(not_home_wasted_cost * 0.70 / 3, 0)      AS annual_saving_eur,
    ROUND(not_home_wasted_co2_g * 0.70 / 3, 1)     AS annual_co2_saving_grams
FROM not_home_stats
ORDER BY annual_saving_eur DESC;

-- combine impact of all 3 recommendations
WITH recommendation_summary AS (

    -- Rec 1: Parcel lockers in 3 Rotterdam postcodes
    SELECT
        'Parcel lockers — Rotterdam 3025, 3012, 3021'   AS recommendation,
        SUM(CASE WHEN attempt_1_status = 'Failed'
                 AND fail_reason = 'Not home'
                 THEN 1 ELSE 0 END)                    AS impacted_deliveries,
        ROUND(
            SUM(CASE WHEN attempt_1_status = 'Failed'
                     AND fail_reason = 'Not home'
                     THEN delivery_cost_eur ELSE 0 END)
            * 0.60 / 3
        , 0)                                            AS annual_cost_saving_eur,
        ROUND(
            SUM(CASE WHEN attempt_1_status = 'Failed'
                     AND fail_reason = 'Not home'
                     THEN co2_emission_kg ELSE 0 END)
            * 1000 * 0.60 / 3
        , 1)                                            AS annual_co2_saving_grams
    FROM fact_deliveries
    WHERE postcode_pc4 IN (3025, 3012, 3021)

    UNION ALL

    -- Rec 2: Predictive dispatch — target not home failures
    SELECT
        'Predictive dispatch — all cities',
        SUM(CASE WHEN attempt_1_status = 'Failed'
                 AND fail_reason = 'Not home'
                 THEN 1 ELSE 0 END),
        ROUND(
            SUM(CASE WHEN attempt_1_status = 'Failed'
                     AND fail_reason = 'Not home'
                     THEN delivery_cost_eur ELSE 0 END)
            * 0.70 / 3
        , 0),
        ROUND(
            SUM(CASE WHEN attempt_1_status = 'Failed'
                     AND fail_reason = 'Not home'
                     THEN co2_emission_kg ELSE 0 END)
            * 1000 * 0.70 / 3
        , 1)
    FROM fact_deliveries

    UNION ALL

    -- Rec 3: Fleet electrification from 50% to 80%
    SELECT
        'Fleet electrification — 50% to 80%',
        SUM(CASE WHEN vehicle_type IN ('Cargo Van','Diesel Van')
                 THEN 1 ELSE 0 END),
        ROUND(
            SUM(CASE WHEN vehicle_type IN ('Cargo Van','Diesel Van')
                     THEN delivery_cost_eur ELSE 0 END)
            * 0.02 / 3
        , 0),
        ROUND(
            SUM(CASE WHEN vehicle_type IN ('Cargo Van','Diesel Van')
                     THEN co2_emission_kg ELSE 0 END)
            * 1000 * 0.30 / 3
        , 1)
    FROM fact_deliveries
)

SELECT
    recommendation,
    impacted_deliveries,
    annual_cost_saving_eur,
    annual_co2_saving_grams,
    -- Scale to PostNL size (45,000x multiplier)
    ROUND(annual_cost_saving_eur * 45000, 0)        AS postnl_scale_saving_eur
FROM recommendation_summary;
