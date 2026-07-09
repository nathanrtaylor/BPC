WITH DateSelector AS (
    SELECT 
        DATE(:start_date) AS StartDate,
        DATE(:end_date) AS EndDate
),

base AS (
    SELECT
        COALESCE(a.client, 'Client Unknown') AS client,
        COALESCE(a.business_unit, 'Business Unit Unknown') AS business_unit,
        COALESCE(a.expert_id, m.expert_id) AS expert_id,
        b.interaction_id,
        MAX(b.override) AS override,
        MAX(b.fmip_flag) AS fmip_flag
    FROM hive.care.l3_verizon_mtl_fmip_interactions b
    CROSS JOIN DateSelector d
    LEFT JOIN hive.care.call_fmip_staging a
        ON a.interaction_line_id = b.interaction_line_id
    LEFT JOIN hive.care.l3_asurion_whole_home_expert_master m
        ON lower(trim(m.win_auth)) = lower(trim(b.created_by))
       AND COALESCE(
            a.segdate,
            CAST(date_add('hour', -5, CAST(b.created_date AS timestamp)) AS date)
       ) BETWEEN m.hire_dt AND m.term_dt
    WHERE trim(b.created_by) <> 'Web'
      AND CAST(date_add('hour', -5, CAST(b.created_date AS timestamp)) AS date)
            BETWEEN d.StartDate AND d.EndDate
      AND COALESCE(a.client, 'Client Unknown') = :client
      AND COALESCE(a.business_unit, 'Business Unit Unknown') = :business_unit
    GROUP BY
        COALESCE(a.client, 'Client Unknown'),
        COALESCE(a.business_unit, 'Business Unit Unknown'),
        COALESCE(a.expert_id, m.expert_id),
        b.interaction_id
)

SELECT
    client,
    business_unit,
    expert_id,
    SUM(override) AS num,
    SUM(fmip_flag) AS den,
    ROUND(
        COALESCE(
            CAST(SUM(override) AS DOUBLE) / NULLIF(CAST(SUM(fmip_flag) AS DOUBLE), 0.0),
            0.000
        ),
        3
    ) AS calc
FROM base
GROUP BY
    client,
    business_unit,
    expert_id;