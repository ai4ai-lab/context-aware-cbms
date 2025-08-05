WITH ards_subjects AS (
    SELECT
        hadm_id,
        subject_id
    FROM
        `mimic-big-query.ards_dataset.ards_cohort` 
),
aggregated_reports AS (
    SELECT
        ards_subjects.hadm_id,
        machine.study_id,
        CONCAT(
            IFNULL(machine.report_0, ''), ' ',
            IFNULL(machine.report_1, ''), ' ',
            IFNULL(machine.report_2, ''), ' ',
            IFNULL(machine.report_3, ''), ' ',
            IFNULL(machine.report_4, ''), ' ',
            IFNULL(machine.report_5, ''), ' ',
            IFNULL(machine.report_6, ''), ' ',
            IFNULL(machine.report_7, ''), ' ',
            IFNULL(machine.report_8, ''), ' ',
            IFNULL(machine.report_9, ''), ' ',
            IFNULL(machine.report_10, ''), ' ',
            IFNULL(machine.report_11, ''), ' ',
            IFNULL(machine.report_12, ''), ' ',
            IFNULL(machine.report_13, ''), ' ',
            IFNULL(machine.report_14, ''), ' ',
            IFNULL(machine.report_15, ''), ' ',
            IFNULL(machine.report_16, ''), ' ',
            IFNULL(machine.report_17, '')
        ) AS report_text
    FROM
        ards_subjects
    JOIN
        `physionet-data.mimiciv_ecg.machine_measurements` machine
    ON
        ards_subjects.subject_id = machine.subject_id
),
discharge_notes AS (
    SELECT
        ards.hadm_id,
        discharge.note_id AS discharge_note_id,
        discharge.text AS discharge_text
    FROM 
        `mimic-big-query.ards_dataset.ards_cohort` ards
    JOIN 
        `physionet-data.mimiciv_note.discharge` discharge
    ON 
        ards.hadm_id = discharge.hadm_id
),
radiology_notes AS (
    SELECT
        ards.hadm_id,
        STRING_AGG(CAST(radiology.note_id AS STRING), ', ') AS radiology_note_ids,
        STRING_AGG(radiology.text, ' ||| ') AS radiology_texts
    FROM 
        `mimic-big-query.ards_dataset.ards_cohort` ards
    JOIN 
        `physionet-data.mimiciv_note.radiology` radiology
    ON 
        ards.hadm_id = radiology.hadm_id
    GROUP BY 
        ards.hadm_id
)
SELECT
    a.hadm_id,
    d.discharge_note_id,
    d.discharge_text,
    r.radiology_note_ids,
    r.radiology_texts,
    STRING_AGG(DISTINCT CAST(ar.study_id AS STRING), ', ') AS ecd_study_ids,
    STRING_AGG(ar.report_text, ' ||| ') AS ecd_combined_reports
FROM
    `mimic-big-query.ards_dataset.ards_cohort` a
LEFT JOIN
    discharge_notes d ON a.hadm_id = d.hadm_id
LEFT JOIN
    radiology_notes r ON a.hadm_id = r.hadm_id
LEFT JOIN
    aggregated_reports ar ON a.hadm_id = ar.hadm_id
GROUP BY
    a.hadm_id, d.discharge_note_id, d.discharge_text, r.radiology_note_ids, r.radiology_texts
ORDER BY
    a.hadm_id;
