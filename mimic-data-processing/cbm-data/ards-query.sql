--------------------------------------------------------------------------------
-- Combined Query for ARDS Cohort: Respiratory Illnesses, Ventilation, SOFA, 
-- and Norepinephrine Dosing Metrics.
--------------------------------------------------------------------------------

WITH 
-- 1. ARDS Cohort: Start with admissions flagged as ARDS.
cohort AS (
  SELECT 
    hadm_id,
    stay_id,
    subject_id,
    ARDS_DIAGNOSIS
  FROM `mimic-big-query.ards_dataset.new_ards_cohort`
),

--------------------------------------------------------------------------------
-- 2. Respiratory Illnesses Extraction and Classification.
--------------------------------------------------------------------------------
-- Step 2a: Extract Diagnoses from MIMIC-IV (ICD-9 and ICD-10 codes).
diag AS (
  SELECT
    hadm_id,
    CASE WHEN icd_version = 9 THEN icd_code ELSE NULL END AS icd9_code,
    CASE WHEN icd_version = 10 THEN icd_code ELSE NULL END AS icd10_code
  FROM `physionet-data.mimiciv_hosp.diagnoses_icd`
),

-- Step 2b: Classify each admission’s diagnoses into six respiratory illness groups.
respiratory_illnesses AS (
  SELECT
    hadm_id,
    -- Upper Respiratory Infections
    MAX(CASE 
      WHEN (SUBSTR(icd9_code, 1, 3) BETWEEN '460' AND '466') OR 
           (SUBSTR(icd10_code, 1, 3) BETWEEN 'J00' AND 'J06')
      THEN 1 ELSE 0 END) AS upper_respiratory_infections,
      
    -- Influenza and Pneumonia
    MAX(CASE 
      WHEN (SUBSTR(icd9_code, 1, 3) IN ('480','481','482','483','484','485','486','487')) OR 
           (SUBSTR(icd10_code, 1, 3) BETWEEN 'J09' AND 'J18')
      THEN 1 ELSE 0 END) AS influenza_pneumonia,
      
    -- Other Acute Lower Respiratory Infections
    MAX(CASE 
      WHEN (SUBSTR(icd9_code, 1, 3) BETWEEN '466' AND '469') OR 
           (SUBSTR(icd10_code, 1, 3) BETWEEN 'J20' AND 'J22')
      THEN 1 ELSE 0 END) AS acute_lower_respiratory_infections,
      
    -- Chronic Lower Respiratory Diseases
    MAX(CASE 
      WHEN (SUBSTR(icd9_code, 1, 3) BETWEEN '490' AND '496') OR 
           (SUBSTR(icd10_code, 1, 3) BETWEEN 'J40' AND 'J47')
      THEN 1 ELSE 0 END) AS chronic_lower_respiratory_diseases,
      
    -- Lung Diseases Due to External Agents
    MAX(CASE 
      WHEN (SUBSTR(icd9_code, 1, 3) BETWEEN '500' AND '508') OR 
           (SUBSTR(icd10_code, 1, 3) BETWEEN 'J60' AND 'J70')
      THEN 1 ELSE 0 END) AS lung_diseases_due_to_external_agents,
      
    -- Other Respiratory Diseases
    MAX(CASE 
      WHEN (SUBSTR(icd9_code, 1, 3) BETWEEN '510' AND '519') OR 
           (SUBSTR(icd10_code, 1, 3) BETWEEN 'J80' AND 'J99')
      THEN 1 ELSE 0 END) AS other_respiratory_diseases
  FROM diag
  GROUP BY hadm_id
),

-- Step 2c: Merge respiratory illness classifications with the ARDS cohort 
-- and add comorbidity severity indicators.
resp_final AS (
  SELECT
    c.hadm_id,
    c.ARDS_DIAGNOSIS,
    r.upper_respiratory_infections,
    r.influenza_pneumonia,
    r.acute_lower_respiratory_infections,
    r.chronic_lower_respiratory_diseases,
    r.lung_diseases_due_to_external_agents,
    r.other_respiratory_diseases,
    -- Moderate respiratory comorbidity indicator: exactly 1 or 2 conditions.
    CASE
      WHEN (r.upper_respiratory_infections + r.influenza_pneumonia + 
            r.acute_lower_respiratory_infections + r.chronic_lower_respiratory_diseases +
            r.lung_diseases_due_to_external_agents + r.other_respiratory_diseases)
           IN (1, 2)
      THEN 1 ELSE 0
    END AS c_mod_resp_comorbidity,
    -- Severe respiratory comorbidity indicator: 3 or more conditions.
    CASE
      WHEN (r.upper_respiratory_infections + r.influenza_pneumonia + 
            r.acute_lower_respiratory_infections + r.chronic_lower_respiratory_diseases +
            r.lung_diseases_due_to_external_agents + r.other_respiratory_diseases)
           >= 3
      THEN 1 ELSE 0
    END AS c_svr_resp_comorbidity
  FROM cohort c
  LEFT JOIN respiratory_illnesses r
    ON c.hadm_id = r.hadm_id
),

--------------------------------------------------------------------------------
-- 3. Mechanical Ventilation Duration.
--------------------------------------------------------------------------------
-- Calculate the total duration (in minutes) of invasive ventilation per admission.
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
),

--------------------------------------------------------------------------------
-- 4. SOFA Score Metrics.
--------------------------------------------------------------------------------
sofa_cohort AS (
  -- Retrieve SOFA data without joining with icustay_detail, using stay_id directly
  SELECT s.*
  FROM `physionet-data.mimiciv_derived.sofa` s
  WHERE s.stay_id IN (SELECT stay_id FROM cohort)  -- Filter by stay_id from ards_cohort
),
cns_rank AS (
  -- For CNS: find the worst (max) CNS score and return its corresponding GCS value
  SELECT
    stay_id,  
    cns AS c_sofa_max_cns,
    gcs_min,
    ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY cns DESC) AS rn  
  FROM sofa_cohort
),
renal_rank AS (
  -- For Renal: find the worst (max) Renal score and return its corresponding creatinine and urine output values
  SELECT
    stay_id,  
    renal AS c_sofa_max_renal,
    creatinine_max,
    uo_24hr,
    ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY renal DESC) AS rn  
  FROM sofa_cohort
),
resp_rank AS (
  -- For Respiration: find the worst (max) Respiration score and return its corresponding PaO₂/FiO₂ ratio (using vented value if available)
  SELECT
    stay_id,  
    respiration AS c_sofa_max_respiration,
    pao2fio2ratio_vent,
    pao2fio2ratio_novent,
    ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY respiration DESC) AS rn  
  FROM sofa_cohort
),
cv_rank AS (
  -- For Cardiovascular: find the worst (max) Cardiovascular score and return its corresponding mean BP and norepinephrine rate
  SELECT
    stay_id,  
    cardiovascular AS c_sofa_max_cardiovascular,
    meanbp_min,
    rate_norepinephrine,
    ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY cardiovascular DESC) AS rn  
  FROM sofa_cohort
),
first24hr_cns AS (
  -- For the first 24 hours: get the row with the highest aggregated CNS score and its corresponding GCS value
  SELECT 
    stay_id, 
    cns_24hours AS c_first24hr_sofa_max_cns, 
    gcs_min AS first24hr_cns_gcs
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY cns_24hours DESC) AS rn  
    FROM sofa_cohort
    WHERE hr < 24
  )
  WHERE rn = 1
),
first24hr_renal AS (
  -- For the first 24 hours: get the row with the highest aggregated Renal score and its corresponding creatinine and urine output values
  SELECT 
    stay_id, 
    renal_24hours AS c_first24hr_sofa_max_renal, 
    creatinine_max AS first24hr_renal_creatinine, 
    uo_24hr AS first24hr_renal_urineoutput
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY renal_24hours DESC) AS rn  
    FROM sofa_cohort
    WHERE hr < 24
  )
  WHERE rn = 1
),
first24hr_resp AS (
  -- For the first 24 hours: get the row with the highest aggregated Respiration score and its corresponding PaO₂/FiO₂ ratio
  SELECT 
    stay_id,  
    respiration_24hours AS c_first24hr_sofa_max_respiration, 
    COALESCE(pao2fio2ratio_vent, pao2fio2ratio_novent) AS first24hr_respiration_pao2fio2ratio
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY respiration_24hours DESC) AS rn 
    FROM sofa_cohort
    WHERE hr < 24
  )
  WHERE rn = 1
),
first24hr_cv AS (
  -- For the first 24 hours: get the row with the highest aggregated Cardiovascular score and its corresponding mean BP and norepinephrine rate
  SELECT 
    stay_id,  
    cardiovascular_24hours AS c_first24hr_sofa_max_cardiovascular, 
    meanbp_min AS first24hr_cardiovascular_meanbp,
    rate_norepinephrine AS first24hr_cardiovascular_rate_norepinephrine
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY cardiovascular_24hours DESC) AS rn
    FROM sofa_cohort
    WHERE hr < 24
  )
  WHERE rn = 1
),
avg_full AS (
  -- Calculate average SOFA component scores and corresponding measurements across the full ICU stay
  SELECT
    stay_id,  
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
  GROUP BY stay_id  
),
final_sofa AS (
  -- Combine selected worst, first 24-hour, and average SOFA metrics into one row per admission.
  SELECT
  sc.stay_id,
  cn.gcs_min               AS sofa_cns_worst_gcs,
  cn.c_sofa_max_cns,
  re.creatinine_max        AS sofa_renal_worst_creatinine,
  re.uo_24hr               AS sofa_renal_worst_urineoutput,
  re.c_sofa_max_renal,
  COALESCE(rp.pao2fio2ratio_vent, rp.pao2fio2ratio_novent)
                           AS sofa_respiration_worst_pao2fio2ratio,
  rp.c_sofa_max_respiration,
  cv.rate_norepinephrine   AS sofa_cardiovascular_worst_rate_norepinephrine,
  cv.meanbp_min            AS sofa_cardiovascular_worst_meanbp,
  cv.c_sofa_max_cardiovascular,
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
LEFT JOIN cns_rank cn  ON sc.stay_id = cn.stay_id  AND cn.rn = 1  
LEFT JOIN renal_rank re ON sc.stay_id = re.stay_id AND re.rn = 1  
LEFT JOIN resp_rank rp ON sc.stay_id = rp.stay_id AND rp.rn = 1  
LEFT JOIN cv_rank cv  ON sc.stay_id = cv.stay_id  AND cv.rn = 1  
LEFT JOIN first24hr_cns f24c ON sc.stay_id = f24c.stay_id
LEFT JOIN first24hr_renal f24r ON sc.stay_id = f24r.stay_id
LEFT JOIN first24hr_resp f24resp ON sc.stay_id = f24resp.stay_id
LEFT JOIN first24hr_cv f24cv ON sc.stay_id = f24cv.stay_id
LEFT JOIN avg_full af ON sc.stay_id = af.stay_id
),

--------------------------------------------------------------------------------
-- 5. Norepinephrine Equivalent Dosing Metrics.
--------------------------------------------------------------------------------
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

--------------------------------------------------------------------------------
-- Final SELECT: Join all components on hadm_id and stay_id to produce one row per admission.
--------------------------------------------------------------------------------
SELECT
  c.hadm_id,
  c.stay_id,
  c.subject_id,
  c.ARDS_DIAGNOSIS,
  -- Respiratory Illnesses & Comorbidity Indicators
  r.upper_respiratory_infections,
  r.influenza_pneumonia,
  r.acute_lower_respiratory_infections,
  r.chronic_lower_respiratory_diseases,
  r.lung_diseases_due_to_external_agents,
  r.other_respiratory_diseases,
  r.c_mod_resp_comorbidity,
  r.c_svr_resp_comorbidity,
  -- Mechanical Ventilation Duration (in minutes)
  IFNULL(v.mech_vent_duration_minutes, 0) AS mech_vent_duration_minutes,
  -- SOFA Score Metrics (Full ICU, First 24hr, and Average values)
  s.sofa_cns_worst_gcs,
  s.c_sofa_max_cns,
  s.sofa_renal_worst_creatinine,
  s.sofa_renal_worst_urineoutput,
  s.c_sofa_max_renal,
  s.sofa_respiration_worst_pao2fio2ratio,
  s.c_sofa_max_respiration,
  s.sofa_cardiovascular_worst_rate_norepinephrine,
  s.sofa_cardiovascular_worst_meanbp,
  s.c_sofa_max_cardiovascular,
  s.first24hr_cns_gcs,
  s.c_first24hr_sofa_max_cns,
  s.first24hr_renal_creatinine,
  s.first24hr_renal_urineoutput,
  s.c_first24hr_sofa_max_renal,
  s.first24hr_respiration_pao2fio2ratio,
  s.c_first24hr_sofa_max_respiration,
  s.first24hr_cardiovascular_meanbp,
  s.first24hr_cardiovascular_rate_norepinephrine,
  s.c_first24hr_sofa_max_cardiovascular,
  s.sofa_cns_avg_gcs,
  s.c_sofa_avg_cns,
  s.sofa_renal_avg_creatinine,
  s.sofa_renal_avg_urineoutput,
  s.c_sofa_avg_renal,
  s.sofa_respiration_avg_pao2fio2ratio,
  s.c_sofa_avg_respiration,
  s.sofa_cardiovascular_avg_rate_norepinephrine,
  s.sofa_cardiovascular_avg_meanbp,
  s.c_sofa_avg_cardiovascular,
  -- Norepinephrine Dosing Metrics
  IFNULL(n.time_weighted_avg_norepi, 0) AS avg_norepinephrine_equiv,
  IFNULL(n.max_norepi, 0) AS max_norepinephrine_equiv
FROM cohort c
  -- Join respiratory illness classifications.
  LEFT JOIN resp_final r
    ON c.hadm_id = r.hadm_id
  -- Join ventilation duration.
  LEFT JOIN vent_agg v
    ON c.stay_id = v.stay_id
  -- Join SOFA score metrics.
  LEFT JOIN final_sofa s
    ON c.stay_id = s.stay_id
  -- Join norepinephrine dosing metrics.
  LEFT JOIN norepi_agg n
    ON c.stay_id = n.stay_id
ORDER BY c.hadm_id;
