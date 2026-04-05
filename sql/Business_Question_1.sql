select * from fact_deliveries;
-- 1
SELECT 
    city,
    COUNT(*) AS total_shipments
FROM fact_deliveries
GROUP BY city
ORDER BY total_shipments DESC;

-- 2 
SELECT 
    city,
    COUNT(*)                                    AS total_shipments,
    SUM(CASE WHEN attempt_1_status = 'Failed' 
             THEN 1 ELSE 0 END)                AS total_fails
FROM fact_deliveries
GROUP BY city
ORDER BY total_shipments DESC;

-- 3
SELECT 
    city,
    COUNT(*)                                        AS total_shipments,
    SUM(CASE WHEN attempt_1_status = 'Failed' 
             THEN 1 ELSE 0 END)                    AS total_fails,
    ROUND(
        SUM(CASE WHEN attempt_1_status = 'Failed' 
                 THEN 1.0 ELSE 0 END)
        / COUNT(*) * 100
    , 1)                                            AS fail_rate_pct
FROM fact_deliveries
GROUP BY city
ORDER BY fail_rate_pct DESC;

-- 4
SELECT 
    city,
    COUNT(*)                                        AS total_shipments,
    SUM(CASE WHEN attempt_1_status = 'Failed' 
             THEN 1 ELSE 0 END)                    AS total_fails,
    ROUND(
        SUM(CASE WHEN attempt_1_status = 'Failed' 
                 THEN 1.0 ELSE 0 END)
        / COUNT(*) * 100
    , 1)                                            AS fail_rate_pct,
    ROUND(AVG(delivery_cost_eur), 2)                AS avg_cost_eur,
    ROUND(
        SUM(CASE WHEN attempt_1_status = 'Failed' 
                 THEN 1.0 ELSE 0 END)
        * AVG(delivery_cost_eur)
    , 0)                                            AS wasted_cost_eur
FROM fact_deliveries
GROUP BY city
ORDER BY fail_rate_pct DESC;

-- 5
SELECT 
    city,
    COUNT(*)                                            AS total_shipments,
    SUM(CASE WHEN attempt_1_status = 'Failed' 
             THEN 1 ELSE 0 END)                        AS total_fails,
    ROUND(
        SUM(CASE WHEN attempt_1_status = 'Failed' 
                 THEN 1.0 ELSE 0 END)
        / COUNT(*) * 100
    , 1)                                                AS fail_rate_pct,
    ROUND(
        SUM(CASE WHEN attempt_1_status = 'Failed' 
                 THEN 1.0 ELSE 0 END)
        * AVG(delivery_cost_eur)
    , 0)                                                AS wasted_cost_eur,
    ROUND(
        SUM(CASE WHEN attempt_1_status = 'Failed' 
                 THEN co2_emission_kg ELSE 0 END) * 1000
    , 1)                                                AS wasted_co2_grams,
    ROUND(
        SUM(CASE WHEN vehicle_type IN ('Cargo Bike','Electric Van')
                 THEN 1.0 ELSE 0 END)
        / COUNT(*) * 100
    , 1)                                                AS zero_emission_pct
FROM fact_deliveries
GROUP BY city
ORDER BY wasted_cost_eur DESC;
