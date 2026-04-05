-- Total CO2 picture 
SELECT
    ROUND(SUM(co2_emission_kg), 2)                  AS total_co2_kg,
    ROUND(AVG(co2_emission_kg) * 1000, 2)           AS avg_co2_grams_per_delivery,
    ROUND(
        SUM(CASE WHEN attempt_1_status = 'Failed'
                 THEN co2_emission_kg ELSE 0 END)
    , 2)                                            AS co2_from_failed_kg,
    ROUND(
        SUM(CASE WHEN attempt_1_status = 'Failed'
                 THEN co2_emission_kg ELSE 0 END)
        / SUM(co2_emission_kg) * 100
    , 1)                                            AS wasted_co2_pct
FROM fact_deliveries;

-- vahicle type breakdown 
SELECT
    vehicle_type,
    COUNT(*)                                        AS total_shipments,
    ROUND(
        COUNT(*) * 100.0
        / SUM(COUNT(*)) OVER ()
    , 1)                                            AS fleet_share_pct,
    ROUND(SUM(co2_emission_kg), 3)                  AS total_co2_kg,
    ROUND(AVG(co2_emission_kg) * 1000, 2)           AS avg_co2_grams,
    ROUND(
        SUM(CASE WHEN attempt_1_status = 'Failed'
                 THEN 1.0 ELSE 0 END)
        / COUNT(*) * 100
    , 1)                                            AS fail_rate_pct,
    ROUND(
        SUM(co2_emission_kg)
        / SUM(COUNT(*)) OVER () * 100
    , 1)                                            AS share_of_total_co2_pct
FROM fact_deliveries
GROUP BY vehicle_type
ORDER BY total_co2_kg DESC;

-- Quantify the carbon saving opportunity 
SELECT
    vehicle_type,
    COUNT(*)                                        AS shipments,
    ROUND(SUM(co2_emission_kg), 3)                  AS actual_co2_kg,
    ROUND(AVG(co2_emission_kg) * 1000, 2)           AS avg_co2_grams,
    ROUND(
        SUM(CASE WHEN vehicle_type IN ('Cargo Bike','Electric Van')
                 THEN 1.0 ELSE 0 END)
        / COUNT(*) * 100
    , 1)                                            AS zero_em_pct,
    -- What if ALL shipments used electric van emissions?
    ROUND(
        COUNT(*) * 0.0006
    , 3)                                            AS co2_if_all_electric_kg,
    -- Carbon saved vs current
    ROUND(
        SUM(co2_emission_kg) - (COUNT(*) * 0.0006)
    , 3)                                            AS co2_saving_potential_kg
FROM fact_deliveries
GROUP BY vehicle_type
ORDER BY actual_co2_kg DESC;
