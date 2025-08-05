WITH ards_cohort AS (
  SELECT stay_id  -- Change to stay_id instead of hadm_id
  FROM `mimic-big-query.ards_dataset.new_ards_cohort`
),
sofa_cohort AS (
  -- Retrieve SOFA data without joining with icustay_detail, using stay_id directly
  SELECT s.*
  FROM `physionet-data.mimiciv_derived.sofa` s
  WHERE s.stay_id IN (SELECT stay_id FROM ards_cohort)  -- Filter by stay_id from ards_cohort
),
cns_rank AS (
  -- For CNS: find the worst (max) CNS score and return its corresponding GCS value
  SELECT
    stay_id,  -- Change to stay_id instead of hadm_id
    cns AS c_sofa_max_cns,
    gcs_min,
    ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY cns DESC) AS rn  -- Change to stay_id
  FROM sofa_cohort
),
renal_rank AS (
  -- For Renal: find the worst (max) Renal score and return its corresponding creatinine and urine output values
  SELECT
    stay_id,  -- Change to stay_id instead of hadm_id
    renal AS c_sofa_max_renal,
    creatinine_max,
    uo_24hr,
    ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY renal DESC) AS rn  -- Change to stay_id
  FROM sofa_cohort
),
resp_rank AS (
  -- For Respiration: find the worst (max) Respiration score and return its corresponding PaO₂/FiO₂ ratio (using vented value if available)
  SELECT
    stay_id,  -- Change to stay_id instead of hadm_id
    respiration AS c_sofa_max_respiration,
    pao2fio2ratio_vent,
    pao2fio2ratio_novent,
    ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY respiration DESC) AS rn  -- Change to stay_id
  FROM sofa_cohort
),
cv_rank AS (
  -- For Cardiovascular: find the worst (max) Cardiovascular score and return its corresponding mean BP and norepinephrine rate
  SELECT
    stay_id,  -- Change to stay_id instead of hadm_id
    cardiovascular AS c_sofa_max_cardiovascular,
    meanbp_min,
    rate_norepinephrine,
    ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY cardiovascular DESC) AS rn  -- Change to stay_id
  FROM sofa_cohort
),
first24hr_cns AS (
  -- For the first 24 hours: get the row with the highest aggregated CNS score and its corresponding GCS value
  SELECT 
    stay_id,  -- Change to stay_id instead of hadm_id
    cns_24hours AS c_first24hr_sofa_max_cns, 
    gcs_min AS first24hr_cns_gcs
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY cns_24hours DESC) AS rn  -- Change to stay_id
    FROM sofa_cohort
    WHERE hr < 24
  )
  WHERE rn = 1
),
first24hr_renal AS (
  -- For the first 24 hours: get the row with the highest aggregated Renal score and its corresponding creatinine and urine output values
  SELECT 
    stay_id,  -- Change to stay_id instead of hadm_id
    renal_24hours AS c_first24hr_sofa_max_renal, 
    creatinine_max AS first24hr_renal_creatinine, 
    uo_24hr AS first24hr_renal_urineoutput
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY renal_24hours DESC) AS rn  -- Change to stay_id
    FROM sofa_cohort
    WHERE hr < 24
  )
  WHERE rn = 1
),
first24hr_resp AS (
  -- For the first 24 hours: get the row with the highest aggregated Respiration score and its corresponding PaO₂/FiO₂ ratio
  SELECT 
    stay_id,  -- Change to stay_id instead of hadm_id
    respiration_24hours AS c_first24hr_sofa_max_respiration, 
    COALESCE(pao2fio2ratio_vent, pao2fio2ratio_novent) AS first24hr_respiration_pao2fio2ratio
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY respiration_24hours DESC) AS rn  -- Change to stay_id
    FROM sofa_cohort
    WHERE hr < 24
  )
  WHERE rn = 1
),
first24hr_cv AS (
  -- For the first 24 hours: get the row with the highest aggregated Cardiovascular score and its corresponding mean BP and norepinephrine rate
  SELECT 
    stay_id,  -- Change to stay_id instead of hadm_id
    cardiovascular_24hours AS c_first24hr_sofa_max_cardiovascular, 
    meanbp_min AS first24hr_cardiovascular_meanbp,
    rate_norepinephrine AS first24hr_cardiovascular_rate_norepinephrine
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY cardiovascular_24hours DESC) AS rn  -- Change to stay_id
    FROM sofa_cohort
    WHERE hr < 24
  )
  WHERE rn = 1
),
avg_full AS (
  -- Calculate average SOFA component scores and corresponding measurements across the full ICU stay
  SELECT
    stay_id,  -- Change to stay_id instead of hadm_id
    AVG(cns) AS c_sofa_avg_cns,
    AVG(gcs_min) AS sofa_cns_avg_gcs,
    
    AVG(renal) AS c_sofa_avg_renal,
    AVG(creatinine_max) AS sofa_renal_avg_creatinine,
    AVG(uo_24hr) AS sofa_renal_avg_urineoutput,
    
    AVG(respiration) AS c_sofa_avg_respiration,
    AVG(COALESCE(pao2fio2ratio_vent, pao2fio2ratio_novent)) AS sofa_respiration_avg_pao2fio2ratio,
    
    AVG(cardiovascular) AS c_sofa_avg_cardiovascular,
    AVG(meanbp_min) AS sofa_cardiovascular_avg_meanbp,
    AVG(rate_norepinephrine) AS sofa_cardiovascular_avg_rate_norepinephrine
  FROM sofa_cohort
  GROUP BY stay_id  -- Group by stay_id instead of hadm_id
)
SELECT
  sc.stay_id,  -- Change to stay_id instead of hadm_id
  
  -- Full ICU Worst Values:
  -- Worst CNS component and its underlying GCS value
  cn.gcs_min               AS sofa_cns_worst_gcs,
  cn.c_sofa_max_cns,
  
  -- Worst Renal component and its underlying measurements
  re.creatinine_max        AS sofa_renal_worst_creatinine,
  re.uo_24hr               AS sofa_renal_worst_urineoutput,
  re.c_sofa_max_renal,
  
  -- Worst Respiration component and its corresponding PaO₂/FiO₂ ratio (using vented value if available)
  COALESCE(rp.pao2fio2ratio_vent, rp.pao2fio2ratio_novent)
                           AS sofa_respiration_worst_pao2fio2ratio,
  rp.c_sofa_max_respiration,
  
  -- Worst Cardiovascular component and its corresponding measurements including vasopressor rates
  cv.rate_norepinephrine   AS sofa_cardiovascular_worst_rate_norepinephrine,
  cv.meanbp_min            AS sofa_cardiovascular_worst_meanbp,
  cv.c_sofa_max_cardiovascular,
  
  -- First 24‑Hour Aggregated Values and Corresponding Measurements:
  f24c.first24hr_cns_gcs,
  f24c.c_first24hr_sofa_max_cns,
  
  f24r.first24hr_renal_creatinine,
  f24r.first24hr_renal_urineoutput,
  f24r.c_first24hr_sofa_max_renal,
  
  f24resp.first24hr_respiration_pao2fio2ratio,
  f24resp.c_first24hr_sofa_max_respiration,
  
  f24cv.first24hr_cardiovascular_meanbp,
  f24cv.first24hr_cardiovascular_rate_norepinephrine,
  f24cv.c_first24hr_sofa_max_cardiovascular,
  
  -- Full ICU Average Values:
  sofa_cns_avg_gcs,
  c_sofa_avg_cns,
  
  sofa_renal_avg_creatinine,
  sofa_renal_avg_urineoutput,
  c_sofa_avg_renal,
  
  sofa_respiration_avg_pao2fio2ratio,
  c_sofa_avg_respiration,
  
  sofa_cardiovascular_avg_rate_norepinephrine,
  sofa_cardiovascular_avg_meanbp,
  c_sofa_avg_cardiovascular
FROM (SELECT DISTINCT stay_id FROM sofa_cohort) sc
LEFT JOIN cns_rank cn  ON sc.stay_id = cn.stay_id  AND cn.rn = 1  -- Change to stay_id
LEFT JOIN renal_rank re ON sc.stay_id = re.stay_id AND re.rn = 1  -- Change to stay_id
LEFT JOIN resp_rank rp ON sc.stay_id = rp.stay_id AND rp.rn = 1  -- Change to stay_id
LEFT JOIN cv_rank cv  ON sc.stay_id = cv.stay_id  AND cv.rn = 1  -- Change to stay_id
LEFT JOIN first24hr_cns f24c ON sc.stay_id = f24c.stay_id
LEFT JOIN first24hr_renal f24r ON sc.stay_id = f24r.stay_id
LEFT JOIN first24hr_resp f24resp ON sc.stay_id = f24resp.stay_id
LEFT JOIN first24hr_cv f24cv ON sc.stay_id = f24cv.stay_id
LEFT JOIN avg_full af ON sc.stay_id = af.stay_id;
