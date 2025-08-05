WITH cohort AS (
  SELECT
    stay_id  -- Change to stay_id instead of hadm_id
  FROM `mimic-big-query.ards_dataset.new_ards_cohort`
),
norepi_agg AS (
  SELECT
    v.stay_id,  -- Change to stay_id instead of hadm_id
    -- Sum of (dose × interval_minutes) / total_minutes
    SAFE_DIVIDE(
      SUM(v.norepinephrine_equivalent_dose * TIMESTAMP_DIFF(v.endtime, v.starttime, MINUTE)),
      SUM(TIMESTAMP_DIFF(v.endtime, v.starttime, MINUTE))
    ) AS time_weighted_avg_norepi,
    MAX(v.norepinephrine_equivalent_dose) AS max_norepi
  FROM `physionet-data.mimiciv_derived.norepinephrine_equivalent_dose` v
  WHERE v.stay_id IN (SELECT stay_id FROM cohort)  -- Filter by stay_id from cohort
  GROUP BY v.stay_id  -- Group by stay_id instead of hadm_id
)
SELECT
  c.stay_id,  -- Change to stay_id instead of hadm_id
  IFNULL(n.time_weighted_avg_norepi, 0) AS avg_norepinephrine_equiv,
  IFNULL(n.max_norepi, 0) AS max_norepinephrine_equiv
FROM cohort c
LEFT JOIN norepi_agg n
  ON c.stay_id = n.stay_id;  -- Join on stay_id
