WITH cohort AS (
  SELECT stay_id, ARDS_DIAGNOSIS
  FROM `mimic-big-query.ards_dataset.new_ards_cohort`
),
invasive_vent AS (
  SELECT
    v.stay_id,  -- Directly use stay_id from the ventilation table
    -- Calculate duration in minutes (change to HOUR, DAY, etc. if needed)
    DATETIME_DIFF(v.endtime, v.starttime, MINUTE) AS vent_minutes
  FROM `physionet-data.mimiciv_derived.ventilation` v
  WHERE v.ventilation_status = 'InvasiveVent'
),
vent_agg AS (
  SELECT
    stay_id,  -- Use stay_id here as well
    SUM(vent_minutes) AS mech_vent_duration_minutes
  FROM invasive_vent
  GROUP BY stay_id
)
SELECT
  c.stay_id,  -- Change to stay_id
  c.ARDS_DIAGNOSIS,
  IFNULL(v.mech_vent_duration_minutes, 0) AS mech_vent_duration_minutes
FROM cohort c
LEFT JOIN vent_agg v
  ON c.stay_id = v.stay_id;  -- Join on stay_id
