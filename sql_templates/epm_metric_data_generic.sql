SELECT 
--CAST(week_stop_date AS DATE) AS week_,
--date,
expert_id,
-- year_month,
metric,
icp_client,
--tenure_group,
site,
SUM(numerator) AS num,
SUM(denominator) AS den,
ROUND(
COALESCE(
CAST(SUM(numerator) AS DOUBLE) /
NULLIF(CAST(SUM(denominator) AS DOUBLE), 0.0),
0.000
),
3
) AS calc
FROM 
hive.care.expert_performance_metrics a
LEFT OUTER JOIN 
hive.care.l4_asurion_umt_ppx_pay_calendar d ON a."date" = CAST(d.event_date AS DATE)
WHERE 
LOWER(metric) = :metric
AND icp_client = :icp_client
AND a."date" between DATE :start_date and DATE :end_date 
 --and expert_id in ()
GROUP BY 1, 2, 3, 4

