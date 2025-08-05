-- This query extracts and classifies respiratory illnesses from MIMIC-IV hospital diagnoses.
-- 
-- Steps:
-- 1. Extracts ICD-9 and ICD-10 codes for hospital admissions.
-- 2. Categorizes diagnoses into six respiratory illness groups.
-- 3. Filters for patients in a pre-defined ARDS cohort.
-- 4. Merges illness classifications with the ARDS cohort.
-- 5. Adds indicators for moderate (1-2 conditions) and severe (≥3 conditions) respiratory comorbidities.
-- 6. Outputs a final table with admission IDs, ARDS status, respiratory illness categories, and comorbidity severity indicators.


-- Step 1: Extract Diagnoses for Respiratory Illnesses
WITH diag AS (
    SELECT
        hadm_id,
        CASE WHEN icd_version = 9 THEN icd_code ELSE NULL END AS icd9_code,
        CASE WHEN icd_version = 10 THEN icd_code ELSE NULL END AS icd10_code
    FROM `physionet-data.mimiciv_hosp.diagnoses_icd`
),

-- Step 2: Classify Respiratory Illnesses
respiratory_illnesses AS (
    SELECT
        hadm_id,
        -- Upper Respiratory Infections
        MAX(CASE WHEN 
            SUBSTR(icd9_code, 1, 3) BETWEEN '460' AND '466' OR
            SUBSTR(icd10_code, 1, 3) BETWEEN 'J00' AND 'J06'
            THEN 1 ELSE 0 END) AS upper_respiratory_infections,

        -- Influenza and Pneumonia
        MAX(CASE WHEN 
            SUBSTR(icd9_code, 1, 3) IN ('480', '481', '482', '483', '484', '485', '486', '487') OR
            SUBSTR(icd10_code, 1, 3) BETWEEN 'J09' AND 'J18'
            THEN 1 ELSE 0 END) AS influenza_pneumonia,

        -- Other Acute Lower Respiratory Infections
        MAX(CASE WHEN 
            SUBSTR(icd9_code, 1, 3) BETWEEN '466' AND '469' OR
            SUBSTR(icd10_code, 1, 3) BETWEEN 'J20' AND 'J22'
            THEN 1 ELSE 0 END) AS acute_lower_respiratory_infections,

        -- Chronic Lower Respiratory Diseases
        MAX(CASE WHEN 
            SUBSTR(icd9_code, 1, 3) BETWEEN '490' AND '496' OR
            SUBSTR(icd10_code, 1, 3) BETWEEN 'J40' AND 'J47'
            THEN 1 ELSE 0 END) AS chronic_lower_respiratory_diseases,

        -- Lung Diseases Due to External Agents
        MAX(CASE WHEN 
            SUBSTR(icd9_code, 1, 3) BETWEEN '500' AND '508' OR
            SUBSTR(icd10_code, 1, 3) BETWEEN 'J60' AND 'J70'
            THEN 1 ELSE 0 END) AS lung_diseases_due_to_external_agents,

        -- Other Respiratory Diseases
        MAX(CASE WHEN 
            SUBSTR(icd9_code, 1, 3) BETWEEN '510' AND '519' OR
            SUBSTR(icd10_code, 1, 3) BETWEEN 'J80' AND 'J99'
            THEN 1 ELSE 0 END) AS other_respiratory_diseases
    FROM diag
    GROUP BY hadm_id
),

-- Step 3: Filter for Cohort-Specific Patients
cohort AS (
    SELECT hadm_id, ARDS_DIAGNOSIS
    FROM `mimic-big-query.ards_dataset.new_ards_cohort`
),

-- Step 4: Generate Final Output with Columns for Each Respiratory Illness Category
final_output AS (
    SELECT
        c.hadm_id,
        c.ARDS_DIAGNOSIS,
        r.upper_respiratory_infections,
        r.influenza_pneumonia,
        r.acute_lower_respiratory_infections,
        r.chronic_lower_respiratory_diseases,
        r.lung_diseases_due_to_external_agents,
        r.other_respiratory_diseases,
        -- Moderate pre-existing respiratory comorbidity (1 or 2 conditions)
        CASE
            WHEN (r.upper_respiratory_infections + r.influenza_pneumonia + r.acute_lower_respiratory_infections +
                  r.chronic_lower_respiratory_diseases + r.lung_diseases_due_to_external_agents + r.other_respiratory_diseases) IN (1, 2)
            THEN 1 ELSE 0
        END AS c_mod_resp_comorbidity,
        -- Severe pre-existing respiratory comorbidity (3 or more conditions)
        CASE
            WHEN (r.upper_respiratory_infections + r.influenza_pneumonia + r.acute_lower_respiratory_infections +
                  r.chronic_lower_respiratory_diseases + r.lung_diseases_due_to_external_agents + r.other_respiratory_diseases) >= 3
            THEN 1 ELSE 0
        END AS c_svr_resp_comorbidity
    FROM cohort c
    LEFT JOIN respiratory_illnesses r ON c.hadm_id = r.hadm_id
)

-- Step 5: Select Final Output
SELECT * FROM final_output;

