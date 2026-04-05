-- total fails
SELECT
    fail_reason,
    COUNT(*) AS total_fails
FROM fact_deliveries
WHERE attempt_1_status = 'Failed'
GROUP BY fail_reason
ORDER BY total_fails DESC;

-- add CO2 and cost for each failure 
SELECT
    fail_reason,
    COUNT(*)                                        AS total_fails,
    ROUND(
        COUNT(*) * 100.0 
        / SUM(COUNT(*)) OVER ()
    , 1)                                            AS pct_of_all_fails,
    ROUND(
        SUM(delivery_cost_eur)
    , 0)                                            AS wasted_cost_eur,
    ROUND(
        SUM(co2_emission_kg) * 1000
    , 1)                                            AS wasted_co2_grams
FROM fact_deliveries
WHERE attempt_1_status = 'Failed'
GROUP BY fail_reason
ORDER BY total_fails DESC;

-- add city breakdown to find where "Not home" is worst
SELECT
    city,
    fail_reason,
    COUNT(*)                                        AS total_fails,
    ROUND(
        COUNT(*) * 100.0
        / SUM(COUNT(*)) OVER (PARTITION BY city)
    , 1)                                            AS pct_within_city
FROM fact_deliveries
WHERE attempt_1_status = 'Failed'
GROUP BY city, fail_reason
ORDER BY city, total_fails DESC;

