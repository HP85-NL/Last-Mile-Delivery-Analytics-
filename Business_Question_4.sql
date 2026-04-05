SELECT
    f.postcode_pc4,
    f.city,
    COUNT(*)                                        AS total_shipments,
    SUM(CASE WHEN f.attempt_1_status = 'Failed'
             THEN 1 ELSE 0 END)                    AS total_fails,
    ROUND(
        SUM(CASE WHEN f.attempt_1_status = 'Failed'
                 THEN 1.0 ELSE 0 END)
        / COUNT(*) * 100
    , 1)                                            AS fail_rate_pct
FROM fact_deliveries f
GROUP BY f.postcode_pc4, f.city
ORDER BY fail_rate_pct DESC
LIMIT 10;

-- Join Fact_deliveries with dim_postcodes
SELECT
    f.postcode_pc4,
    f.city,
    p.district,
    p.zone_type,
    p.parcel_locker_nearby,
    p.historical_fail_rate_pct,
    COUNT(*)                                        AS total_shipments,
    SUM(CASE WHEN f.attempt_1_status = 'Failed'
             THEN 1 ELSE 0 END)                    AS total_fails,
    ROUND(
        SUM(CASE WHEN f.attempt_1_status = 'Failed'
                 THEN 1.0 ELSE 0 END)
        / COUNT(*) * 100
    , 1)                                            AS actual_fail_rate_pct,
    ROUND(
        SUM(CASE WHEN f.attempt_1_status = 'Failed'
                 THEN 1.0 ELSE 0 END)
        / COUNT(*) * 100
        - p.historical_fail_rate_pct
    , 1)                                            AS vs_historical_pp,
    ROUND(
        SUM(CASE WHEN f.attempt_1_status = 'Failed'
                 THEN 1.0 ELSE 0 END)
        * AVG(f.delivery_cost_eur)
    , 0)                                            AS wasted_cost_eur
FROM fact_deliveries f
JOIN dim_postcodes p 
    ON f.postcode_pc4 = p.postcode_pc4
GROUP BY 
    f.postcode_pc4, f.city, p.district, 
    p.zone_type, p.parcel_locker_nearby,
    p.historical_fail_rate_pct
ORDER BY actual_fail_rate_pct DESC
LIMIT 10;

-- Parcel locker ROI 
SELECT
    f.postcode_pc4,
    f.city,
    p.district,
    p.parcel_locker_nearby,
    p.zone_type,
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
    , 0)                                            AS wasted_cost_eur,
    ROUND(
        SUM(CASE WHEN f.attempt_1_status = 'Failed'
                 THEN 1.0 ELSE 0 END)
        * AVG(f.delivery_cost_eur) * 4
    , 0)                                            AS est_annual_waste_eur,
    CASE
        WHEN p.parcel_locker_nearby = 'No'
         AND ROUND(
                SUM(CASE WHEN f.attempt_1_status = 'Failed'
                         THEN 1.0 ELSE 0 END)
                / COUNT(*) * 100
             , 1) > 11
        THEN 'Install locker - High Priority'
        WHEN p.parcel_locker_nearby = 'No'
        THEN 'Review coverage'
        ELSE 'Monitor'
    END                                             AS recommendation
FROM fact_deliveries f
JOIN dim_postcodes p
    ON f.postcode_pc4 = p.postcode_pc4
GROUP BY
    f.postcode_pc4, f.city, p.district,
    p.parcel_locker_nearby, p.zone_type
ORDER BY wasted_cost_eur DESC
LIMIT 15;

-- 
-- CASE
--         WHEN p.parcel_locker_nearby = 'No'
--          AND ROUND(
--                 SUM(CASE WHEN f.attempt_1_status = 'Failed'
--                          THEN 1.0 ELSE 0 END)
--                 / COUNT(*) * 100
--              , 1) > 13    (we have changed to 11, for more priority and get better result)
--         THEN 'Install locker - High Priority'
--         WHEN p.parcel_locker_nearby = 'No'
--         THEN 'Review coverage'
--         ELSE 'Monitor'
--     END                                             AS recommendation