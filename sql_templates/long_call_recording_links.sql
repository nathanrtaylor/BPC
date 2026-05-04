WITH base AS (
  SELECT
    b.expert_id,
    b.recording_link,
    a.event_date_cst,
    a.segstart,
    a.segstop,
    date_diff('second', a.segstart, a.segstop) AS talk_time_seconds,
    a.interaction_id
  FROM hive.care.l3_asurion_twilio_interaction_detail a
  INNER JOIN hive.care.l3_asurion_interaction_detail_tags b
    ON a.reservation_id = b.reservation_id
  WHERE b.client = :client
    AND b.business_unit = :business_unit
    AND a.event_date_cst BETWEEN DATE :start_date AND DATE :end_date
    AND b.recording_link IS NOT NULL
    AND date_diff('second', a.segstart, a.segstop) >= :min_outdial_seconds
    AND b.expert_id IN :expert_ids
),
ranked AS (
  SELECT
    expert_id,
    recording_link,
    row_number() OVER (
      PARTITION BY expert_id
      ORDER BY 
        talk_time_seconds DESC,     -- longest call first
        event_date_cst DESC,               -- most recent date
        segstart DESC              -- most recent timestamp
    ) AS rn
  FROM base
)
SELECT
  expert_id,
  recording_link
  FROM ranked
WHERE rn = 1;